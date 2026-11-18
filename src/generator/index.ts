// Change generator: performs a seeded, repeatable mix of inserts, updates and deletes
// on the source `orders` table. Each change runs in its own transaction and is
// recorded in the trace as hop 1 ("source.commit").
//
// Usage: npm run generate -- --count 20 --seed 1 --delay-ms 250
//
// Runs are repeatable when started from the same table contents (e.g. an empty table):
// the same seed produces the same sequence of operations.

import { parseArgs } from 'node:util';
import { setTimeout as sleep } from 'node:timers/promises';
import pg from 'pg';
import { createTraceWriter, type Op } from '../shared/trace.ts';

// ---------- CLI arguments ----------

const { values: args } = parseArgs({
  options: {
    count: { type: 'string', default: '20' },
    seed: { type: 'string', default: '1' },
    'delay-ms': { type: 'string', default: '250' },
  },
});

function toInt(value: string, name: string, min: number): number {
  const n = Number(value);
  if (!Number.isInteger(n) || n < min) {
    throw new Error(`${name} must be an integer >= ${min}, got "${value}"`);
  }
  return n;
}

const COUNT = toInt(args.count, '--count', 1);
const SEED = toInt(args.seed, '--seed', 0);
const DELAY_MS = toInt(args['delay-ms'], '--delay-ms', 0);

// ---------- Seeded randomness ----------

/** mulberry32: tiny, fast PRNG. Same seed -> same sequence. Not for cryptography. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const random = mulberry32(SEED);

function pick<T>(items: readonly T[]): T {
  const item = items[Math.floor(random() * items.length)];
  if (item === undefined) throw new Error('pick() called with an empty list');
  return item;
}

/** Random amount between 5.00 and 500.00, as a string to keep numeric precision. */
function randomAmount(): string {
  const cents = 500 + Math.floor(random() * 49_501);
  return (cents / 100).toFixed(2);
}

// ---------- Domain ----------

type Action = 'insert' | 'advance' | 'adjust' | 'cancel' | 'delete';

/** Relative likelihood of each action. Falls back to insert when no row qualifies. */
const ACTION_WEIGHTS: ReadonlyArray<readonly [Action, number]> = [
  ['insert', 40],
  ['advance', 30], // move status one step along the lifecycle
  ['adjust', 10],  // change the amount of a pending order
  ['cancel', 10],
  ['delete', 10],  // remove a finished (delivered or cancelled) order
];

const NEXT_STATUS: Readonly<Record<string, string>> = {
  pending: 'paid',
  paid: 'shipped',
  shipped: 'delivered',
};

const CUSTOMERS = [
  'Alice Moreno', 'Bilal Khan', 'Chen Wei', 'Dana Okafor', 'Elena Petrova',
  'Felipe Souza', 'Grace Kim', 'Hiro Tanaka', 'Ines Duarte', 'Jonas Berg',
] as const;

function pickAction(): Action {
  const total = ACTION_WEIGHTS.reduce((sum, [, w]) => sum + w, 0);
  let roll = random() * total;
  for (const [action, weight] of ACTION_WEIGHTS) {
    roll -= weight;
    if (roll < 0) return action;
  }
  return 'insert';
}

interface OrderRow {
  id: string;            // bigint arrives from pg as a string
  customer_name: string;
  status: string;
  amount: string;        // numeric arrives from pg as a string
}

interface ChangeResult {
  op: Op;
  id: string;
  /** Changed columns. Updates show [before, after]; inserts show the new values. */
  changes: Record<string, unknown>;
}

// ---------- Database ----------

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing environment variable ${name} (see .env.example)`);
  return value;
}

const client = new pg.Client({
  host: process.env.SOURCE_POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.SOURCE_POSTGRES_PORT ?? 5433),
  user: requireEnv('SOURCE_POSTGRES_USER'),
  password: requireEnv('SOURCE_POSTGRES_PASSWORD'),
  database: requireEnv('SOURCE_POSTGRES_DB'),
});

/** Rows eligible for an action, ordered by id so the seeded pick is deterministic. */
async function candidates(statuses: readonly string[]): Promise<OrderRow[]> {
  const { rows } = await client.query<OrderRow>(
    'SELECT id, customer_name, status, amount FROM orders WHERE status = ANY($1) ORDER BY id',
    [statuses],
  );
  return rows;
}

/** Performs one action inside the current transaction; null if no row qualifies. */
async function perform(action: Action): Promise<ChangeResult | null> {
  switch (action) {
    case 'insert': {
      const customer = pick(CUSTOMERS);
      const amount = randomAmount();
      const { rows } = await client.query<OrderRow>(
        'INSERT INTO orders (customer_name, amount) VALUES ($1, $2) RETURNING id, customer_name, status, amount',
        [customer, amount],
      );
      const row = rows[0]!;
      return {
        op: 'c',
        id: row.id,
        changes: { customer_name: row.customer_name, status: row.status, amount: row.amount },
      };
    }
    case 'advance': {
      const rows = await candidates(Object.keys(NEXT_STATUS));
      if (rows.length === 0) return null;
      const row = pick(rows);
      const next = NEXT_STATUS[row.status]!;
      await client.query('UPDATE orders SET status = $2 WHERE id = $1', [row.id, next]);
      return { op: 'u', id: row.id, changes: { status: [row.status, next] } };
    }
    case 'adjust': {
      const rows = await candidates(['pending']);
      if (rows.length === 0) return null;
      const row = pick(rows);
      const amount = randomAmount();
      await client.query('UPDATE orders SET amount = $2 WHERE id = $1', [row.id, amount]);
      return { op: 'u', id: row.id, changes: { amount: [row.amount, amount] } };
    }
    case 'cancel': {
      const rows = await candidates(['pending', 'paid']);
      if (rows.length === 0) return null;
      const row = pick(rows);
      await client.query("UPDATE orders SET status = 'cancelled' WHERE id = $1", [row.id]);
      return { op: 'u', id: row.id, changes: { status: [row.status, 'cancelled'] } };
    }
    case 'delete': {
      const rows = await candidates(['delivered', 'cancelled']);
      if (rows.length === 0) return null;
      const row = pick(rows);
      await client.query('DELETE FROM orders WHERE id = $1', [row.id]);
      return { op: 'd', id: row.id, changes: { deleted: row } };
    }
  }
}

/**
 * The 32-bit transaction id of the current transaction. pg_current_xact_id() returns
 * a 64-bit value (epoch << 32 | xid); the low 32 bits are the classic xid, which is
 * what we expect to match Debezium's source.txId (to be confirmed in step 4).
 */
async function currentTxId(): Promise<string> {
  const { rows } = await client.query<{ tx: string }>(
    'SELECT (pg_current_xact_id()::text::bigint % 4294967296)::text AS tx',
  );
  return rows[0]!.tx;
}

// ---------- Main loop ----------

const OP_LABEL: Record<Op, string> = { c: 'INSERT', u: 'UPDATE', d: 'DELETE', r: 'READ' };

function describe(changes: Record<string, unknown>): string {
  return Object.entries(changes)
    .map(([col, v]) =>
      Array.isArray(v) ? `${col}: ${v[0]} → ${v[1]}`
      : col === 'deleted' ? `(was ${(v as OrderRow).status}, ${(v as OrderRow).amount})`
      : `${col}=${v}`)
    .join('  ');
}

async function main(): Promise<void> {
  const trace = createTraceWriter('generator');
  await client.connect();
  console.log(`generator: ${COUNT} changes, seed ${SEED}, ${DELAY_MS} ms apart\n`);

  const width = String(COUNT).length;
  for (let i = 1; i <= COUNT; i++) {
    const chosen = pickAction();
    let action = chosen;

    await client.query('BEGIN');
    let txId: string;
    let result: ChangeResult;
    try {
      txId = await currentTxId();
      let performed = await perform(action);
      if (!performed) {
        action = 'insert'; // nothing qualified (e.g. no finished orders to delete yet)
        performed = (await perform(action))!;
      }
      result = performed;
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    }

    trace({
      hop: 'source.commit',
      table: 'orders',
      op: result.op,
      key: { id: result.id },
      txId,
      detail: {
        action,
        ...(action !== chosen && { fallbackFrom: chosen }),
        changes: result.changes,
      },
    });

    console.log(
      `[${String(i).padStart(width)}/${COUNT}] tx ${txId}  ${OP_LABEL[result.op]} orders id=${result.id}  ${describe(result.changes)}`,
    );

    if (DELAY_MS > 0 && i < COUNT) await sleep(DELAY_MS);
  }
}

try {
  await main();
} catch (err) {
  console.error('generator failed:', err);
  process.exitCode = 1;
} finally {
  await client.end().catch(() => {});
}
