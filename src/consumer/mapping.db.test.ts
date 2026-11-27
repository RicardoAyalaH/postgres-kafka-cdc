// Runs the statements from mapEvent against the real destination database, to check that
// Postgres accepts them and that the LSN guard behaves as designed. Needs dest-db running
// (docker compose up -d dest-db); run with `npm run test:db`. Each test runs in a
// transaction that starts from an empty table and is rolled back, so the replica's real
// rows are never changed.
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
// Connects as the owner only to empty the table inside each test's transaction (dest-db
// usually holds the consumer's replica, and the tests need a known starting state). The
// statements under test then run as the consumer's role: see beforeEach.
const client = new pg.Client({
  host: process.env.DEST_POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.DEST_POSTGRES_PORT ?? 5434),
  user: process.env.DEST_POSTGRES_USER,
  password: process.env.DEST_POSTGRES_PASSWORD,
  database: process.env.DEST_POSTGRES_DB,
});
const consumerRole = process.env.CONSUMER_POSTGRES_USER!;

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

beforeAll(() => client.connect());
afterAll(() => client.end());
beforeEach(async () => {
  await client.query('BEGIN');
  // Rolled back after each test, so the real rows come back untouched. While a test runs,
  // this uncommitted delete row-locks the table, so a running consumer waits briefly.
  await client.query('DELETE FROM orders');
  // From here on, privilege checks are the consumer's own: the tests prove its grants
  // are enough (and that DELETE is refused). SET LOCAL ends with the transaction.
  await client.query(`SET LOCAL ROLE ${client.escapeIdentifier(consumerRole)}`);
});
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
