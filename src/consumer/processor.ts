// Per-message processing for the consumer: map, write, then commit. No Kafka client or
// database connection here; index.ts passes them in as functions, so the ordering rules
// below are unit-tested with fakes (processor.test.ts).
//
// The rules:
// - The offset is committed only after the write succeeded (at-least-once delivery).
// - A stale write (0 rows: the LSN guard refused it) and a tombstone are still committed.
// - eachMessage never throws (see the comment on it). A failure calls onFailure once, and
//   from then on no message is written or committed, so the next run resumes from the last
//   committed offset.
import type { TraceWriter } from '../shared/trace.ts';
import { mapEvent, type Statement } from './mapping.ts';

/** The fields of a Kafka message this module reads (offset and timestamp are strings). */
export type Message = { offset: string; timestamp: string; value: Buffer | null };

export type TopicPartitionOffset = { topic: string; partition: number; offset: string };

export interface ProcessorDeps {
  topic: string;
  /** Runs one statement on dest-db. rowCount is 1 if applied, 0 if the LSN guard refused it. */
  query: (statement: Statement) => Promise<{ rowCount: number | null }>;
  commitOffsets: (offsets: TopicPartitionOffset[]) => Promise<void>;
  trace: TraceWriter;
  log: (line: string) => void;
  /**
   * Called once, for the first message that fails; the caller stops the process. It runs
   * inside eachMessage and must not wait for the consumer to disconnect: disconnect()
   * waits for eachMessage to return, which would deadlock.
   */
  onFailure: (offset: string, err: unknown) => void;
  /** Called after a write and before its commit. The crash test exits the process here. */
  afterWrite?: (outcome: string) => void;
}

export interface Processor {
  eachMessage: (payload: { partition: number; message: Message }) => Promise<void>;
  /** Stops taking new messages and waits until the one in progress is committed or failed. */
  stop: () => Promise<void>;
  readonly counts: { applied: number; stale: number; tombstones: number };
}

/** Debezium's two timestamps, for tracing only (mapEvent validates what the write needs). */
function debeziumTimes(value: unknown): { sourceTsMs?: number; debeziumTsMs?: number } {
  const payload = (value as { payload?: { ts_ms?: number; source?: { ts_ms?: number } } } | null)?.payload;
  return {
    sourceTsMs: payload?.source?.ts_ms, // commit time in the source database (from the WAL)
    debeziumTsMs: payload?.ts_ms,       // when Debezium processed the change
  };
}

export function createProcessor(deps: ProcessorDeps): Processor {
  const { topic, query, commitOffsets, trace, log, onFailure, afterWrite } = deps;
  const counts = { applied: 0, stale: 0, tombstones: 0 };
  let stopping = false;
  let inFlight: Promise<void> | null = null;

  async function handle(partition: number, message: Message): Promise<void> {
    const value: unknown = message.value === null ? null : JSON.parse(message.value.toString('utf8'));
    const mapped = mapEvent(value);
    const where = { partition, offset: message.offset };

    // Fields that link this change's entries to the generator's source.commit entry.
    const ids = mapped.kind === 'apply'
      ? { table: 'orders', op: mapped.op, key: { id: String(mapped.id) }, txId: String(mapped.txId), lsn: String(mapped.lsn) }
      : {};
    trace({
      hop: 'consumer.received',
      ...ids,
      detail: {
        ...where,
        tombstone: mapped.kind === 'skip',
        kafkaTimestampMs: Number(message.timestamp), // record timestamp (CreateTime, set by the producer)
        ...debeziumTimes(value),
      },
    });

    let outcome: string;
    if (mapped.kind === 'skip') {
      counts.tombstones++;
      outcome = 'tombstone, nothing to write';
    } else {
      const { rowCount } = await query(mapped.statement);
      // 1 row: the change was newer than the row's source_lsn. 0 rows: the guard's WHERE was
      // false, so this change (or a newer one) had already been applied.
      const applied = rowCount === 1;
      counts[applied ? 'applied' : 'stale']++;
      trace({ hop: 'dest.applied', ...ids, detail: { ...where, result: applied ? 'applied' : 'stale' } });
      outcome = `${mapped.op} id=${mapped.id} lsn=${mapped.lsn} tx=${mapped.txId} → ` +
        (applied ? 'applied' : 'stale, skipped by LSN guard');
      afterWrite?.(`p${partition} offset ${message.offset}: ${outcome}`);
    }

    // The committed offset is the next one to read, so offset + 1. BigInt because Kafka
    // offsets are int64 and arrive as strings.
    const next = (BigInt(message.offset) + 1n).toString();
    await commitOffsets([{ topic, partition, offset: next }]);
    trace({ hop: 'consumer.committed', ...ids, detail: { ...where, committed: next } });
    log(`p${partition} offset ${message.offset}: ${outcome}; committed ${next}`);
  }

  return {
    counts,

    // Never throws. A throw makes the client queue a seek back to this message, and that
    // seek can run after disconnect() has started and crash the client ("Assignment can
    // only be called while connected"), as the dest-db-down test showed. We don't need the
    // seek: once stopping, nothing more is committed, so the next run resumes from the last
    // committed offset. (autoCommit is off, so the client never commits on its own.)
    async eachMessage({ partition, message }) {
      if (stopping) return; // not processed and not committed: redelivered next run
      inFlight = handle(partition, message);
      try {
        await inFlight;
      } catch (err) {
        stopping = true;
        onFailure(message.offset, err);
      } finally {
        inFlight = null;
      }
    },

    async stop() {
      stopping = true;
      await inFlight?.catch(() => {}); // its failure, if any, was already reported
    },
  };
}
