// CDC consumer: reads Debezium change events from Kafka and applies them to dest-db.
//
// Delivery is at-least-once: each message is written first and its offset committed
// after, so a crash between the two redelivers the message. That is harmless because
// every statement is guarded by the row's source LSN (see mapping.ts and db/dest/init).
//
// Any failure stops the process without committing (see `fail` below); restarting it
// resumes from the last committed offset.
//
// Flags: --crash-after-write  fault injection for the crash test: exits right after the
//                             first write of this run, before committing its offset.
import './check-node.ts'; // first: must run before the Kafka client loads its native binary
import kafka from '@confluentinc/kafka-javascript';
import pg from 'pg';
import { createTraceWriter } from '../shared/trace.ts';
import { mapEvent } from './mapping.ts';

const { Kafka, logLevel } = kafka.KafkaJS;

// Fixed by the connector config (connect/source-orders.json): topic.prefix "shop" plus
// schema and table. It only changes if that config does.
const TOPIC = 'shop.public.orders';

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing environment variable ${name} (see .env.example)`);
  return value;
}

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');
// Committed offsets are stored per group id. A new group id has none, so it starts from
// the beginning of the topic (fromBeginning below): a full replay.
const GROUP_ID = requireEnv('CONSUMER_GROUP_ID');
const CRASH_AFTER_WRITE = process.argv.includes('--crash-after-write');

const trace = createTraceWriter('consumer');

// ---------- Destination database ----------

// A pool of one connection rather than a single Client: if dest-db restarts, the pool
// discards the broken connection and opens a new one on the next query. One connection
// is enough because messages are applied one at a time.
const pool = new pg.Pool({
  host: process.env.DEST_POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.DEST_POSTGRES_PORT ?? 5434),
  user: requireEnv('CONSUMER_POSTGRES_USER'),
  password: requireEnv('CONSUMER_POSTGRES_PASSWORD'),
  database: requireEnv('DEST_POSTGRES_DB'),
  max: 1,
  connectionTimeoutMillis: 5000,
  query_timeout: 5000,
});
// Errors on an idle connection would otherwise crash the process; the next query reconnects.
pool.on('error', (err) => console.error(`consumer: idle connection error: ${err.message}`));

// ---------- Kafka ----------

const consumer = new Kafka({ kafkaJS: { brokers: BROKERS, logLevel: logLevel.WARN } }).consumer({
  kafkaJS: {
    groupId: GROUP_ID,
    // Only used when the group has no committed offset yet.
    fromBeginning: true,
    // We commit ourselves, after each write.
    autoCommit: false,
  },
  // KIP-848: the broker assigns partitions and tracks liveness. Session timeout and
  // heartbeat interval are broker settings under this protocol, so they're not set here.
  'group.protocol': 'consumer',
});

// ---------- Processing ----------

const counts = { applied: 0, stale: 0, tombstones: 0 };
let stopping = false;
let inFlight: Promise<void> | null = null;

type Message = { offset: string; timestamp: string; value: Buffer | null };

/** Debezium's two timestamps, for tracing only (mapEvent validates what the write needs). */
function debeziumTimes(value: unknown): { sourceTsMs?: number; debeziumTsMs?: number } {
  const payload = (value as { payload?: { ts_ms?: number; source?: { ts_ms?: number } } } | null)?.payload;
  return {
    sourceTsMs: payload?.source?.ts_ms, // commit time in the source database (from the WAL)
    debeziumTsMs: payload?.ts_ms,       // when Debezium processed the change
  };
}

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
    const { rowCount } = await pool.query(mapped.statement);
    // 1 row: the change was newer than the row's source_lsn. 0 rows: the guard's WHERE was
    // false, so this change (or a newer one) had already been applied.
    const applied = rowCount === 1;
    counts[applied ? 'applied' : 'stale']++;
    trace({ hop: 'dest.applied', ...ids, detail: { ...where, result: applied ? 'applied' : 'stale' } });
    outcome = `${mapped.op} id=${mapped.id} lsn=${mapped.lsn} tx=${mapped.txId} → ` +
      (applied ? 'applied' : 'stale, skipped by LSN guard');

    if (CRASH_AFTER_WRITE) {
      // Simulated crash in the at-least-once window: written but not committed. A plain
      // exit skips disconnect(), so the broker also sees a dead member, as in a real crash.
      console.error(`p${partition} offset ${message.offset}: ${outcome}; --crash-after-write: exiting before commit`);
      process.exit(1);
    }
  }

  // The committed offset is the next one to read, so offset + 1. BigInt because Kafka
  // offsets are int64 and arrive as strings.
  const next = (BigInt(message.offset) + 1n).toString();
  await consumer.commitOffsets([{ topic: TOPIC, partition, offset: next }]);
  trace({ hop: 'consumer.committed', ...ids, detail: { ...where, committed: next } });
  console.log(`p${partition} offset ${message.offset}: ${outcome}; committed ${next}`);
}

// ---------- Shutdown ----------

async function shutdown(reason: string, exitCode: number): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.log(`consumer: stopping (${reason})`);
  // Watchdog: if a clean close hangs, exit anyway. Correctness doesn't depend on it (the
  // committed offset is already right); the cost is that the broker only drops this member
  // after its session timeout, so the next start waits for the partition.
  setTimeout(() => {
    console.error('consumer: shutdown took over 10 s, exiting without a clean disconnect');
    process.exit(exitCode || 1);
  }, 10_000).unref();
  // Let the message being processed finish its write and commit (or fail).
  await inFlight?.catch(() => {});
  try {
    await consumer.disconnect(); // leaves the group, so the partition is reassigned at once
  } finally {
    await pool.end();
  }
  console.log(
    `consumer: stopped. This run: ${counts.applied} applied, ${counts.stale} stale, ` +
      `${counts.tombstones} tombstones`,
  );
  process.exitCode = exitCode;
}

/**
 * Stops the process after a failed message: the message is not committed, so the next run
 * retries it. Called from inside eachMessage, so it must not await the shutdown:
 * disconnect() waits for eachMessage to return, which would deadlock.
 */
function fail(offset: string, err: unknown): void {
  console.error(`consumer: failed at offset ${offset}, not committed: ${(err as Error).stack ?? err}`);
  void shutdown('error', 1);
}

let signals = 0;
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    if (++signals > 1) {
      console.error('consumer: second signal, exiting without waiting');
      process.exit(130);
    }
    void shutdown(signal, 0);
  });
}

// ---------- Main ----------

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });
console.log(`consumer: group "${GROUP_ID}" reading ${TOPIC} from ${BROKERS.join(',')} (Ctrl+C to stop)`);

await consumer.run({
  // Never throws. A throw makes the client queue a seek back to this message, and that
  // seek can run after disconnect() has started and crash the client ("Assignment can
  // only be called while connected"), as the dest-db-down test showed. We don't need the
  // seek: once stopping, nothing more is committed, so the next run resumes from the last
  // committed offset. (autoCommit is off, so the client never commits on its own.)
  eachMessage: async ({ partition, message }) => {
    if (stopping) return; // not processed and not committed: redelivered next run
    inFlight = handle(partition, message);
    try {
      await inFlight;
    } catch (err) {
      fail(message.offset, err);
    } finally {
      inFlight = null;
    }
  },
});
