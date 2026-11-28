// Builds per-change timelines from the trace file's entries. Pure (no file access), so it
// is unit-tested with hand-written entries (timeline.test.ts), and step 8's latency
// numbers can reuse it.
//
// Why this isn't just "group by txId":
// - Transaction ids, LSNs and Kafka offsets all start over after `docker compose down -v`,
//   while logs/trace.jsonl survives. A change is identified by txId + LSN + the source
//   commit time from the WAL (sourceTsMs), which together don't repeat.
// - One change can be processed more than once (a crash before the commit, a replay, a
//   resend by Debezium). Each consumer.received starts an attempt; the dest.applied and
//   consumer.committed entries that follow it for the same partition and offset belong to it.
// - The generator's source.commit has no LSN. It is matched by txId, table, op and key,
//   taking the entry closest in time to the WAL commit time.
import type { Op, TraceEvent } from '../shared/trace.ts';

/** How far apart the generator's source.commit time and the WAL commit time may be. */
export const MATCH_TOLERANCE_MS = 60_000;

/** One delivery of a change to the consumer. Times are epoch ms. */
export interface Attempt {
  partition: number;
  offset: string;
  /** Kafka record timestamp (CreateTime, set by the producer in Kafka Connect). */
  kafkaTimestampMs?: number;
  /** When Debezium processed the change (payload.ts_ms, Kafka Connect's clock). */
  debeziumTsMs?: number;
  /** When the consumer received the record (host clock). */
  receivedAt: number;
  result?: 'applied' | 'stale';
  appliedAt?: number;
  committedAt?: number;
  /** The committed offset (the next one to read). */
  committed?: string;
}

export interface Change {
  table?: string;
  op?: Op;
  key?: Record<string, string>;
  txId?: string;
  lsn?: string;
  /** Commit time in the source database, from the WAL (source-db's clock). */
  sourceTsMs?: number;
  /** When the generator logged the commit (host clock). Absent for snapshot reads. */
  generatorCommitAt?: number;
  /** Empty if the consumer hasn't received the change (yet). */
  attempts: Attempt[];
}

export type Status = 'committed' | 'not committed' | 'not received';

export function status(change: Change): Status {
  if (change.attempts.length === 0) return 'not received';
  return change.attempts.some((a) => a.committedAt !== undefined) ? 'committed' : 'not committed';
}

/** The time a change is sorted and displayed by: its WAL commit time if known. */
export function changeTime(change: Change): number {
  return change.sourceTsMs ?? change.generatorCommitAt ?? change.attempts[0]?.receivedAt ?? 0;
}

const num = (v: unknown): number | undefined => (typeof v === 'number' ? v : undefined);
const sameKey = (a?: Record<string, string>, b?: Record<string, string>) =>
  JSON.stringify(a ?? {}) === JSON.stringify(b ?? {});

/** @param events Trace entries in file order (the order they were written). */
export function buildTimelines(events: TraceEvent[]): Change[] {
  const changes = new Map<string, Change>();
  // The latest attempt per consumer process and partition/offset, for attaching the
  // applied and committed entries that follow its consumer.received.
  const openAttempts = new Map<string, Attempt>();
  const commits: TraceEvent[] = [];

  for (const e of events) {
    const d = e.detail ?? {};
    const where = `${e.emitter}|${String(d.partition)}|${String(d.offset)}`;

    if (e.hop === 'source.commit') {
      commits.push(e);
    } else if (e.hop === 'consumer.received') {
      const attempt: Attempt = {
        partition: Number(d.partition),
        offset: String(d.offset),
        kafkaTimestampMs: num(d.kafkaTimestampMs),
        debeziumTsMs: num(d.debeziumTsMs),
        receivedAt: Date.parse(e.ts),
      };
      openAttempts.set(where, attempt);
      if (d.tombstone === true) continue; // not a change; its commit still attaches above

      const sourceTsMs = num(d.sourceTsMs);
      const id = `${e.txId}|${e.lsn}|${sourceTsMs}`;
      let change = changes.get(id);
      if (!change) {
        change = { table: e.table, op: e.op, key: e.key, txId: e.txId, lsn: e.lsn, sourceTsMs, attempts: [] };
        changes.set(id, change);
      }
      change.attempts.push(attempt);
    } else if (e.hop === 'dest.applied' || e.hop === 'consumer.committed') {
      const attempt = openAttempts.get(where);
      if (!attempt) continue; // its consumer.received isn't in the file (e.g. truncated)
      if (e.hop === 'dest.applied') {
        attempt.result = d.result === 'stale' ? 'stale' : 'applied';
        attempt.appliedAt = Date.parse(e.ts);
      } else {
        attempt.committedAt = Date.parse(e.ts);
        attempt.committed = String(d.committed);
      }
    }
  }

  // Match each consumed change to the generator commit closest to its WAL commit time.
  const used = new Set<TraceEvent>();
  for (const change of changes.values()) {
    if (change.sourceTsMs === undefined) continue;
    let best: TraceEvent | undefined;
    let bestGap = Infinity;
    for (const c of commits) {
      if (used.has(c) || c.txId !== change.txId || c.table !== change.table || c.op !== change.op) continue;
      if (!sameKey(c.key, change.key)) continue;
      const gap = Math.abs(Date.parse(c.ts) - change.sourceTsMs);
      if (gap <= MATCH_TOLERANCE_MS && gap < bestGap) [best, bestGap] = [c, gap];
    }
    if (best) {
      used.add(best);
      change.generatorCommitAt = Date.parse(best.ts);
    }
  }

  // Generator commits the consumer never received.
  const result = [...changes.values()];
  for (const c of commits) {
    if (used.has(c)) continue;
    result.push({ table: c.table, op: c.op, key: c.key, txId: c.txId, generatorCommitAt: Date.parse(c.ts), attempts: [] });
  }
  return result.sort((a, b) => changeTime(a) - changeTime(b));
}
