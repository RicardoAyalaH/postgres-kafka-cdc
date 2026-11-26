// CDC consumer: reads Debezium change events from Kafka and applies them to dest-db.
//
// Delivery is at-least-once: each message is written first and its offset committed
// after, so a crash between the two redelivers the message. That is harmless because
// every statement is guarded by the row's source LSN (see mapping.ts and db/dest/init).
//
// Any failure stops the process without committing (see `fail` below); restarting it
// resumes from the last committed offset.
import kafka from '@confluentinc/kafka-javascript';
import pg from 'pg';
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

class StoppingError extends Error {
  override name = 'StoppingError';
}

type Message = { offset: string; value: Buffer | null };

async function handle(partition: number, message: Message): Promise<void> {
  const value: unknown = message.value === null ? null : JSON.parse(message.value.toString('utf8'));
  const mapped = mapEvent(value);

  let outcome: string;
  if (mapped.kind === 'skip') {
    counts.tombstones++;
    outcome = 'tombstone, nothing to write';
  } else {
    const { rowCount } = await pool.query(mapped.statement);
    const applied = rowCount === 1;
    counts[applied ? 'applied' : 'stale']++;
    outcome = `${mapped.op} id=${mapped.id} lsn=${mapped.lsn} tx=${mapped.txId} → ` +
      (applied ? 'applied' : 'stale, skipped by LSN guard');
  }

  // The committed offset is the next one to read, so offset + 1. BigInt because Kafka
  // offsets are int64 and arrive as strings.
  const next = (BigInt(message.offset) + 1n).toString();
  await consumer.commitOffsets([{ topic: TOPIC, partition, offset: next }]);
  console.log(`p${partition} offset ${message.offset}: ${outcome}; committed ${next}`);
}

// ---------- Shutdown ----------

async function shutdown(reason: string, exitCode: number): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.log(`consumer: stopping (${reason})`);
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
 * Stops the process after a failed message. Called from inside eachMessage, so it must
 * not await the shutdown: disconnect() waits for eachMessage to return, which would deadlock.
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
  eachMessage: async ({ partition, message }) => {
    // Throwing makes the client seek back to this message instead of moving on, and
    // nothing is committed, so the next run starts here.
    if (stopping) throw new StoppingError('consumer is stopping');
    inFlight = handle(partition, message);
    try {
      await inFlight;
    } catch (err) {
      fail(message.offset, err);
      throw err;
    } finally {
      inFlight = null;
    }
  },
});
