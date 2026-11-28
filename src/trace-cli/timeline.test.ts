import { describe, expect, it } from 'vitest';
import type { Op, TraceEvent } from '../shared/trace.ts';
import { buildTimelines, MATCH_TOLERANCE_MS, status } from './timeline.ts';

const T0 = Date.parse('2026-11-27T00:50:23.049Z'); // a WAL commit time
const iso = (ms: number) => new Date(ms).toISOString();

type Ids = { txId: string; op: Op; id: string; lsn?: string };

const commit = (at: number, { txId, op, id }: Ids): TraceEvent => ({
  ts: iso(at), emitter: 'generator', hop: 'source.commit', table: 'orders', op, key: { id }, txId,
});

/** The three consumer entries of one attempt, or a prefix of them (a crash). */
function attempt(
  at: number, offset: string, ids: Ids & { lsn: string }, sourceTsMs: number,
  { result = 'applied', commit = true }: { result?: 'applied' | 'stale'; commit?: boolean } = {},
): TraceEvent[] {
  const common = { emitter: 'consumer', table: 'orders', op: ids.op, key: { id: ids.id }, txId: ids.txId, lsn: ids.lsn };
  const where = { partition: 0, offset };
  return [
    { ...common, ts: iso(at), hop: 'consumer.received',
      detail: { ...where, tombstone: false, sourceTsMs, debeziumTsMs: sourceTsMs + 150, kafkaTimestampMs: sourceTsMs + 250 } },
    { ...common, ts: iso(at + 2), hop: 'dest.applied', detail: { ...where, result } },
    ...(commit
      ? [{ ...common, ts: iso(at + 3), hop: 'consumer.committed' as const, detail: { ...where, committed: String(BigInt(offset) + 1n) } }]
      : []),
  ];
}

const insert23 = { txId: '813', op: 'c', id: '23', lsn: '26856456' } as const;

describe('buildTimelines', () => {
  it('joins the generator commit and the consumer hops of one change', () => {
    const [change, ...rest] = buildTimelines([commit(T0 + 3, insert23), ...attempt(T0 + 600, '47', insert23, T0)]);

    expect(rest).toEqual([]);
    expect(change).toMatchObject({ txId: '813', lsn: '26856456', sourceTsMs: T0, generatorCommitAt: T0 + 3 });
    expect(change!.attempts).toEqual([{
      partition: 0, offset: '47', debeziumTsMs: T0 + 150, kafkaTimestampMs: T0 + 250,
      receivedAt: T0 + 600, result: 'applied', appliedAt: T0 + 602, committedAt: T0 + 603, committed: '48',
    }]);
    expect(status(change!)).toBe('committed');
  });

  it('shows a crash before the commit and the redelivery as two attempts of one change', () => {
    const [change, ...rest] = buildTimelines([
      commit(T0 + 3, insert23),
      ...attempt(T0 + 600, '47', insert23, T0, { commit: false }), // --crash-after-write
      ...attempt(T0 + 60_000, '47', insert23, T0, { result: 'stale' }),
    ]);

    expect(rest).toEqual([]);
    expect(change!.attempts.map((a) => [a.result, a.committed])).toEqual([['applied', undefined], ['stale', '48']]);
    expect(status(change!)).toBe('committed');
  });

  it('keeps changes apart when transaction ids repeat after a reset, matching each to its own commit', () => {
    const update18 = { txId: '813', op: 'u', id: '18', lsn: '26800000' } as const;
    const weekLater = T0 + 7 * 24 * 3600_000;
    // Same txId, op and key a week apart (Postgres restarted its counter); even the LSN repeats.
    const changes = buildTimelines([
      commit(T0 + 3, update18),
      ...attempt(T0 + 500, '10', update18, T0),
      commit(weekLater + 4, update18),
      ...attempt(weekLater + 500, '10', update18, weekLater),
    ]);

    expect(changes.map((c) => [c.sourceTsMs, c.generatorCommitAt, c.attempts.length])).toEqual([
      [T0, T0 + 3, 1],
      [weekLater, weekLater + 4, 1],
    ]);
  });

  it('lists a generator commit the consumer has not received', () => {
    const [change] = buildTimelines([commit(T0, insert23)]);

    expect(change).toMatchObject({ txId: '813', generatorCommitAt: T0, attempts: [] });
    expect(status(change!)).toBe('not received');
  });

  it('does not match a commit further away than the tolerance', () => {
    const changes = buildTimelines([
      commit(T0 - MATCH_TOLERANCE_MS - 1, insert23),
      ...attempt(T0 + 500, '47', insert23, T0),
    ]);

    expect(changes.map((c) => [c.generatorCommitAt !== undefined, c.attempts.length])).toEqual([
      [true, 0], // the old commit, on its own
      [false, 1], // the consumed change, unmatched
    ]);
  });

  it('includes snapshot reads, which have no generator commit', () => {
    const read5 = { txId: '780', op: 'r', id: '5', lsn: '26819432' } as const;
    const [change] = buildTimelines(attempt(T0 + 500, '4', read5, T0));

    expect(change).toMatchObject({ op: 'r' });
    expect(change!.generatorCommitAt).toBeUndefined();
    expect(status(change!)).toBe('committed');
  });

  it('skips tombstones (they are not changes)', () => {
    const where = { partition: 0, offset: '9' };
    const changes = buildTimelines([
      { ts: iso(T0), emitter: 'consumer', hop: 'consumer.received', detail: { ...where, tombstone: true } },
      { ts: iso(T0 + 1), emitter: 'consumer', hop: 'consumer.committed', detail: { ...where, committed: '10' } },
    ]);

    expect(changes).toEqual([]);
  });

  it('sorts changes by commit time and ignores slot snapshots', () => {
    const later = { txId: '900', op: 'c', id: '30', lsn: '27000000' } as const;
    const changes = buildTimelines([
      ...attempt(T0 + 5000, '50', later, T0 + 4000),
      { ts: iso(T0), emitter: 'slot-monitor', hop: 'slot.status', detail: { slot: 'debezium_orders' } },
      commit(T0, insert23),
    ]);

    expect(changes.map((c) => c.txId)).toEqual(['813', '900']);
  });
});
