import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { mapEvent, UnmappableEventError } from './mapping.ts';

type Payload = {
  op: string;
  before: Record<string, unknown> | null;
  after: Record<string, unknown> | null;
  source: Record<string, unknown>;
};

/** The parsed Kafka message value of a fixture (what the consumer passes to mapEvent). */
const value = (name: string): { schema: unknown; payload: Payload } | null =>
  JSON.parse(readFileSync(new URL(`../../fixtures/debezium/${name}`, import.meta.url), 'utf8')).value;

/** A deep copy of a fixture's value, for building variants the topic doesn't contain. */
const variant = (name: string, edit: (p: Payload) => void) => {
  const v = structuredClone(value(name))!;
  edit(v.payload);
  return v;
};

describe('mapEvent', () => {
  it.each(['snapshot-read.json', 'create.json', 'update-amount.json'])(
    '%s becomes an LSN-guarded upsert of the after image',
    (file) => {
      const { payload } = value(file)!;
      const after = payload.after!;
      const mapped = mapEvent(value(file));

      expect(mapped).toMatchObject({
        kind: 'apply', op: payload.op, id: after.id, txId: payload.source.txId, lsn: payload.source.lsn,
      });
      if (mapped.kind !== 'apply') return;
      expect(mapped.statement.text).toMatch(/^INSERT INTO orders/);
      expect(mapped.statement.text).toContain('WHERE orders.source_lsn < EXCLUDED.source_lsn');
      expect(mapped.statement.values).toEqual([
        after.id, after.customer_name, after.status, after.amount,
        after.created_at, after.updated_at, payload.source.lsn,
      ]);
    },
  );

  it('passes amount and timestamps through as exact strings', () => {
    const mapped = mapEvent(value('update-amount.json'));
    if (mapped.kind !== 'apply') throw new Error('expected apply');
    const [, , , amount, createdAt] = mapped.statement.values;
    expect(amount).toBe('146.22');
    expect(createdAt).toBe('2026-11-23T06:25:40.766470Z'); // microseconds kept
  });

  it('delete becomes an LSN-guarded soft delete by id', () => {
    const { payload } = value('delete.json')!;
    const mapped = mapEvent(value('delete.json'));

    expect(mapped).toMatchObject({ kind: 'apply', op: 'd', id: 5, lsn: payload.source.lsn });
    if (mapped.kind !== 'apply') return;
    expect(mapped.statement.text).toMatch(/^UPDATE orders SET deleted = true/);
    expect(mapped.statement.text).toContain('source_lsn < $2');
    expect(mapped.statement.values).toEqual([5, payload.source.lsn]);
  });

  it('delete needs only the primary key in before (default replica identity)', () => {
    const keyOnly = variant('delete.json', (p) => {
      p.before = { id: 5, customer_name: null, status: null, amount: null, created_at: null, updated_at: null };
    });
    expect(mapEvent(keyOnly)).toEqual(mapEvent(value('delete.json')));
  });

  it('skips tombstones', () => {
    expect(mapEvent(value('tombstone.json'))).toEqual({ kind: 'skip', reason: 'tombstone' });
  });

  it('throws on an operation it does not handle, such as truncate', () => {
    const truncate = variant('create.json', (p) => { p.op = 't'; p.after = null; });
    expect(() => mapEvent(truncate)).toThrow(UnmappableEventError);
    expect(() => mapEvent(truncate)).toThrow('unsupported op "t"');
  });

  it('throws when a required field is missing or has the wrong type', () => {
    const noAmount = variant('create.json', (p) => { delete p.after!.amount; });
    expect(() => mapEvent(noAmount)).toThrow('after.amount: expected string, got undefined');

    const numericAmount = variant('create.json', (p) => { p.after!.amount = 1234; });
    expect(() => mapEvent(numericAmount)).toThrow('after.amount: expected string, got 1234');

    const lsnAsString = variant('create.json', (p) => { p.source.lsn = '26819584'; });
    expect(() => mapEvent(lsnAsString)).toThrow('source.lsn: expected a safe integer');
  });

  it('throws instead of rounding an id beyond 2^53', () => {
    const hugeId = variant('create.json', (p) => { p.after!.id = 2 ** 53 + 2; });
    expect(() => mapEvent(hugeId)).toThrow('after.id: expected a safe integer');
  });

  it('throws on something that is not a Debezium envelope', () => {
    expect(() => mapEvent({ hello: 'world' })).toThrow('message value.payload: expected object');
  });
});
