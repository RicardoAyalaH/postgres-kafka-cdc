// Checks the assumptions the consumer makes about Debezium's message format, using the
// real captured messages in fixtures/debezium/. If the fixtures are recaptured with
// different connector settings, these fail first, with a clear message.
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

type Fixture = {
  topic: string;
  partition: number;
  offset: number;
  key: { payload: { id: number } };
  value: {
    payload: {
      op: string;
      before: Record<string, unknown> | null;
      after: Record<string, unknown> | null;
      source: { lsn: unknown; txId: unknown; snapshot: string };
    };
  } | null;
};

const load = (name: string): Fixture =>
  JSON.parse(readFileSync(new URL(`../../fixtures/debezium/${name}`, import.meta.url), 'utf8'));

const changes = [
  { file: 'snapshot-read.json', op: 'r', hasBefore: false, hasAfter: true },
  { file: 'create.json', op: 'c', hasBefore: false, hasAfter: true },
  { file: 'update-amount.json', op: 'u', hasBefore: true, hasAfter: true },
  { file: 'delete.json', op: 'd', hasBefore: true, hasAfter: false },
] as const;

describe('Debezium fixtures', () => {
  describe.each(changes)('$file', ({ file, op, hasBefore, hasAfter }) => {
    const { key, value } = load(file);

    it(`is a "${op}" event with the expected row images`, () => {
      expect(value?.payload.op).toBe(op);
      expect(value?.payload.before !== null).toBe(hasBefore);
      expect(value?.payload.after !== null).toBe(hasAfter);
    });

    it('carries the source LSN as an integer (the idempotency guard compares it)', () => {
      expect(Number.isSafeInteger(value?.payload.source.lsn)).toBe(true);
    });

    it('has amount as a decimal string (decimal.handling.mode=string)', () => {
      for (const row of [value?.payload.before, value?.payload.after]) {
        if (row) expect(row.amount).toMatch(/^\d+\.\d{2}$/);
      }
    });

    it('has the row id in the message key', () => {
      const row = value?.payload.after ?? value?.payload.before;
      expect(key.payload.id).toBe(row?.id);
    });
  });

  it('tombstone directly follows the delete, with the same key and a null value', () => {
    const del = load('delete.json');
    const tombstone = load('tombstone.json');
    expect(tombstone.value).toBeNull();
    expect(tombstone.key.payload.id).toBe(del.key.payload.id);
    expect(tombstone.offset).toBe(del.offset + 1);
  });
});
