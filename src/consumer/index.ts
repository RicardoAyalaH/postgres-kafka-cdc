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
import { createProcessor } from './processor.ts';

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

// Map, write, commit: see processor.ts for the ordering rules and processor.test.ts for
// their tests.
const processor = createProcessor({
  topic: TOPIC,
  query: (statement) => pool.query(statement),
  commitOffsets: (offsets) => consumer.commitOffsets(offsets),
  trace,
  log: (line) => console.log(line),
  onFailure: (offset, err) => fail(offset, err),
  afterWrite: CRASH_AFTER_WRITE
    ? (outcome) => {
        // Simulated crash in the at-least-once window: written but not committed. A plain
        // exit skips disconnect(), so the broker also sees a dead member, as in a real crash.
        console.error(`${outcome}; --crash-after-write: exiting before commit`);
        process.exit(1);
      }
    : undefined,
});

// ---------- Shutdown ----------

let stopping = false;

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
  // Take no new messages; let the one being processed finish its write and commit (or fail).
  await processor.stop();
  try {
    await consumer.disconnect(); // leaves the group, so the partition is reassigned at once
  } finally {
    await pool.end();
  }
  console.log(
    `consumer: stopped. This run: ${processor.counts.applied} applied, ` +
      `${processor.counts.stale} stale, ${processor.counts.tombstones} tombstones`,
  );
  process.exitCode = exitCode;
}

/**
 * Stops the process after a failed message: the message is not committed, so the next run
 * retries it. Called from inside eachMessage (via onFailure), so it must not await the
 * shutdown: disconnect() waits for eachMessage to return, which would deadlock.
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

// eachMessage never throws; see processor.ts.
await consumer.run({ eachMessage: processor.eachMessage });
