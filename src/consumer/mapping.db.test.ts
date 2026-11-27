// Runs the statements from mapEvent against Postgres, to check that it accepts them and
// that the LSN guard behaves as designed. Uses the test database on dest-db
// (DEST_TEST_POSTGRES_DB, created by db/dest/init/02-test-db.sh): same schema and grants
// as the replica, always empty. Needs dest-db running (docker compose up -d dest-db); run
// with `npm run test:db`.
//
// Connects as the consumer's role, so every privilege check is the consumer's own. Each
// test runs in a transaction that is rolled back, so the database stays empty.
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { mapEvent } from './mapping.ts';

type Payload = { op: string; after: Record<string, unknown> | null; source: Record<string, unknown> };

const value = (name: string): { payload: Payload } | null =>
  JSON.parse(readFileSync(new URL(`../../fixtures/debezium/${name}`, import.meta.url), 'utf8')).value;

const variant = (name: string, edit: (p: Payload) => void) => {
  const v = structuredClone(value(name))!;
  edit(v.payload);
  return v;
};

process.loadEnvFile(new URL('../../.env', import.meta.url));
const testDb = process.env.DEST_TEST_POSTGRES_DB;
// Never the replica: these tests assume an empty table they own.
if (!testDb || testDb === process.env.DEST_POSTGRES_DB) {
  throw new Error('DEST_TEST_POSTGRES_DB must name the test database, not the replica (see .env.example)');
}
const client = new pg.Client({
  host: process.env.DEST_POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.DEST_POSTGRES_PORT ?? 5434),
  user: process.env.CONSUMER_POSTGRES_USER,
  password: process.env.CONSUMER_POSTGRES_PASSWORD,
  database: testDb,
});

/** Maps and applies one event; returns the number of rows the statement changed. */
async function apply(event: unknown): Promise<number> {
  const mapped = mapEvent(event);
  if (mapped.kind === 'skip') return 0;
  return (await client.query(mapped.statement)).rowCount ?? 0;
}

async function row(id: number) {
  const { rows } = await client.query(
    `SELECT id::int, status, amount::text, source_lsn::bigint::text AS lsn, deleted
     FROM orders WHERE id = $1`,
    [id],
  );
  return rows[0];
}

beforeAll(async () => {
  await client.connect();
  // Every test rolls back, so rows here mean something committed: a test that forgot its
  // transaction, or a database that isn't the test database.
  const { rows } = await client.query('SELECT count(*)::int AS n FROM orders');
  if (rows[0].n !== 0) throw new Error(`expected an empty orders table in ${testDb}, found ${rows[0].n} rows`);
});
afterAll(() => client.end());
beforeEach(() => client.query('BEGIN'));
afterEach(() => client.query('ROLLBACK'));

describe('mapped statements against dest-db', () => {
  it('applies every fixture and stores exact values', async () => {
    expect(await apply(value('snapshot-read.json'))).toBe(1);
    expect(await apply(value('update-amount.json'))).toBe(1);
    expect(await apply(value('create.json'))).toBe(1);

    expect(await row(3)).toEqual({ id: 3, status: 'pending', amount: '146.22', lsn: '26819584', deleted: false });

    // Microseconds survive: the stored value equals the full-precision source string.
    const { rows } = await client.query(
      `SELECT created_at = '2026-11-23T06:25:40.766470Z'::timestamptz AS exact FROM orders WHERE id = 3`,
    );
    expect(rows[0].exact).toBe(true);
  });

  it('applying the same event twice changes nothing the second time', async () => {
    expect(await apply(value('create.json'))).toBe(1);
    const first = await row(7);
    expect(await apply(value('create.json'))).toBe(0);
    expect(await row(7)).toEqual(first);
  });

  it('an older event arriving after a newer one is skipped', async () => {
    const newer = variant('update-amount.json', (p) => { p.after!.status = 'paid'; });
    const older = variant('update-amount.json', (p) => {
      p.after!.status = 'pending';
      p.source.lsn = (p.source.lsn as number) - 100;
    });
    expect(await apply(newer)).toBe(1);
    expect(await apply(older)).toBe(0);
    expect((await row(3)).status).toBe('paid');
  });

  it('a delete is soft, and a replayed older event cannot bring the row back', async () => {
    // Order 5's real history: snapshot read (lsn 26819432), then the delete (lsn 26821432).
    const snapshotOf5 = variant('snapshot-read.json', (p) => {
      p.after!.id = 5;
      p.source.lsn = 26819432;
    });
    expect(await apply(snapshotOf5)).toBe(1);
    expect(await apply(value('delete.json'))).toBe(1);
    expect(await apply(value('tombstone.json'))).toBe(0);

    expect(await apply(snapshotOf5)).toBe(0); // replay
    expect(await row(5)).toMatchObject({ deleted: true, lsn: '26821432' });
    const { rows } = await client.query('SELECT count(*)::int AS n FROM orders_live WHERE id = 5');
    expect(rows[0].n).toBe(0);
  });

  it('a delete for a row that was never applied changes nothing', async () => {
    expect(await apply(value('delete.json'))).toBe(0);
    expect(await row(5)).toBeUndefined();
  });

  it('the consumer role cannot hard-delete, so deletes must stay soft', async () => {
    // 42501 = insufficient_privilege. The error aborts the transaction; afterEach rolls it back.
    await expect(client.query('DELETE FROM orders WHERE id = 5')).rejects.toMatchObject({ code: '42501' });
  });
});
