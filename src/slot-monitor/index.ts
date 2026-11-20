// Slot monitor: polls the source database's replication slot and records its state
// in the trace as hop "slot.status". Runs as its own process on purpose: it must keep
// reporting while Kafka Connect or the consumer is down, which is exactly when the
// slot falls behind and Postgres starts retaining WAL.
//
// Usage: npm run slot-monitor -- --interval-ms 2000 --slot debezium_orders
// Stop with Ctrl+C.

import { parseArgs } from 'node:util';
import { setTimeout as sleep } from 'node:timers/promises';
import pg from 'pg';
import { createTraceWriter } from '../shared/trace.ts';

// ---------- CLI arguments ----------

const { values: args } = parseArgs({
  options: {
    'interval-ms': { type: 'string', default: '2000' },
    slot: { type: 'string', default: 'debezium_orders' },
  },
});

const INTERVAL_MS = Number(args['interval-ms']);
if (!Number.isInteger(INTERVAL_MS) || INTERVAL_MS < 100) {
  throw new Error(`--interval-ms must be an integer >= 100, got "${args['interval-ms']}"`);
}
const SLOT = args.slot;

// ---------- Database ----------

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing environment variable ${name} (see .env.example)`);
  return value;
}

// A pool of one connection rather than a single Client: if the database restarts, the
// pool discards the broken connection and opens a new one on the next query.
const pool = new pg.Pool({
  host: process.env.SOURCE_POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.SOURCE_POSTGRES_PORT ?? 5433),
  user: requireEnv('MONITOR_POSTGRES_USER'),
  password: requireEnv('MONITOR_POSTGRES_PASSWORD'),
  database: requireEnv('SOURCE_POSTGRES_DB'),
  max: 1,
  connectionTimeoutMillis: 5000,
  query_timeout: 5000,
});
// Errors on an idle connection (e.g. the server shut down) are emitted here; without a
// listener they would crash the process. The next query simply reconnects.
pool.on('error', (err) => console.error(`slot-monitor: idle connection error: ${err.message}`));

// pg_wal_lsn_diff returns numeric, which pg delivers as a string. Byte counts stay far
// below 2^53, so converting them to JS numbers is safe.
const SLOT_QUERY = `
  SELECT active,
         wal_status,
         restart_lsn::text         AS restart_lsn,
         confirmed_flush_lsn::text AS confirmed_flush_lsn,
         pg_current_wal_lsn()::text AS current_lsn,
         pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS confirmed_lag_bytes,
         pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)        AS retained_wal_bytes
  FROM pg_replication_slots
  WHERE slot_name = $1`;

interface SlotRow {
  active: boolean;
  wal_status: string | null;
  restart_lsn: string | null;
  confirmed_flush_lsn: string | null;
  current_lsn: string;
  confirmed_lag_bytes: string | null;
  retained_wal_bytes: string | null;
}

const toBytes = (v: string | null): number | null => (v === null ? null : Number(v));

// ---------- Main loop ----------

const trace = createTraceWriter('slot-monitor');

async function tick(): Promise<void> {
  const { rows } = await pool.query<SlotRow>(SLOT_QUERY, [SLOT]);
  const row = rows[0];

  if (!row) {
    // A missing slot is a real state worth recording (e.g. after a source volume reset).
    trace({ hop: 'slot.status', detail: { slot: SLOT, exists: false } });
    console.log(`${new Date().toISOString()}  ${SLOT}: SLOT DOES NOT EXIST`);
    return;
  }

  const detail = {
    slot: SLOT,
    exists: true,
    active: row.active,
    walStatus: row.wal_status,
    confirmedFlushLsn: row.confirmed_flush_lsn,
    restartLsn: row.restart_lsn,
    currentLsn: row.current_lsn,
    confirmedLagBytes: toBytes(row.confirmed_lag_bytes),
    retainedWalBytes: toBytes(row.retained_wal_bytes),
  };
  trace({ hop: 'slot.status', lsn: row.confirmed_flush_lsn ?? undefined, detail });
  console.log(
    `${new Date().toISOString()}  ${SLOT}: ${row.active ? 'active  ' : 'INACTIVE'}` +
      `  lag ${detail.confirmedLagBytes} B  retained ${detail.retainedWalBytes} B` +
      `  wal_status ${row.wal_status}`,
  );
}

const stop = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => stop.abort());
}

console.log(`slot-monitor: polling slot "${SLOT}" every ${INTERVAL_MS} ms (Ctrl+C to stop)\n`);

// Sequential loop instead of setInterval: a slow query can never overlap the next tick.
while (!stop.signal.aborted) {
  try {
    await tick();
  } catch (err) {
    // Database down or unreachable: report and try again next tick.
    console.error(`${new Date().toISOString()}  slot-monitor: query failed: ${(err as Error).message}`);
  }
  try {
    await sleep(INTERVAL_MS, undefined, { signal: stop.signal });
  } catch {
    break; // aborted by Ctrl+C / SIGTERM
  }
}

await pool.end();
console.log('slot-monitor: stopped');
