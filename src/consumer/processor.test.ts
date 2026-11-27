// The consumer's ordering rules (processor.ts), tested with fakes for the database and the
// Kafka commit. Each fake records into one shared `calls` list, so tests can assert the
// order of writes and commits, not only that both happened.
import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { UnmappableEventError, type Statement } from './mapping.ts';
import { createProcessor, type Message, type ProcessorDeps, type TopicPartitionOffset } from './processor.ts';

const TOPIC = 'shop.public.orders';

/** A Kafka message carrying a fixture's value, as the client delivers it (bytes, string offset). */
function message(fixture: string, offset: string): Message {
  const { value } = JSON.parse(readFileSync(new URL(`../../fixtures/debezium/${fixture}`, import.meta.url), 'utf8'));
  return { offset, timestamp: '1764000000000', value: value === null ? null : Buffer.from(JSON.stringify(value)) };
}

type Call = { query: Statement } | { commit: TopicPartitionOffset[] };

/** A processor with recording fakes. `rowCount` and the overrides decide how the fakes behave. */
function setup(overrides: Partial<ProcessorDeps> & { rowCount?: number } = {}) {
  const calls: Call[] = [];
  const deps: ProcessorDeps = {
    topic: TOPIC,
    query: vi.fn(async (statement: Statement) => {
      calls.push({ query: statement });
      return { rowCount: overrides.rowCount ?? 1 };
    }),
    commitOffsets: vi.fn(async (offsets: TopicPartitionOffset[]) => {
      calls.push({ commit: offsets });
    }),
    trace: vi.fn(),
    log: () => {},
    onFailure: vi.fn(),
    ...overrides,
  };
  return { processor: createProcessor(deps), deps, calls };
}

const commits = (calls: Call[]) => calls.filter((c) => 'commit' in c);

describe('processor', () => {
  it('writes, then commits offset + 1', async () => {
    const { processor, calls, deps } = setup();

    await processor.eachMessage({ partition: 0, message: message('create.json', '7') });

    expect(calls.map((c) => Object.keys(c)[0])).toEqual(['query', 'commit']);
    expect(calls[1]).toEqual({ commit: [{ topic: TOPIC, partition: 0, offset: '8' }] });
    expect(processor.counts).toEqual({ applied: 1, stale: 0, tombstones: 0 });
    expect(deps.onFailure).not.toHaveBeenCalled();
  });

  it('commits a stale write (0 rows), so a redelivered message is not retried forever', async () => {
    const { processor, calls } = setup({ rowCount: 0 });

    await processor.eachMessage({ partition: 0, message: message('update-amount.json', '7') });

    expect(commits(calls)).toEqual([{ commit: [{ topic: TOPIC, partition: 0, offset: '8' }] }]);
    expect(processor.counts).toEqual({ applied: 0, stale: 1, tombstones: 0 });
  });

  it('commits a tombstone without writing', async () => {
    const { processor, calls, deps } = setup();

    await processor.eachMessage({ partition: 0, message: message('tombstone.json', '7') });

    expect(deps.query).not.toHaveBeenCalled();
    expect(commits(calls)).toHaveLength(1);
    expect(processor.counts).toEqual({ applied: 0, stale: 0, tombstones: 1 });
  });

  it('computes the next offset exactly beyond 2^53 (int64 offsets)', async () => {
    const { processor, calls } = setup();

    // 2^53 + 1: as a JS number this would round to 2^53, and + 1 would give the same value.
    await processor.eachMessage({ partition: 0, message: message('create.json', '9007199254740993') });

    expect(calls[1]).toEqual({ commit: [{ topic: TOPIC, partition: 0, offset: '9007199254740994' }] });
  });

  it('does not commit when the write fails, reports it once, and does not throw', async () => {
    const error = new Error('connection refused');
    const { processor, deps } = setup({ query: vi.fn().mockRejectedValue(error) });

    // Resolves rather than rejects: a throw would make the client seek back (see processor.ts).
    await expect(processor.eachMessage({ partition: 0, message: message('create.json', '7') })).resolves.toBeUndefined();

    expect(deps.commitOffsets).not.toHaveBeenCalled();
    expect(deps.onFailure).toHaveBeenCalledExactlyOnceWith('7', error);
  });

  it('after a failure, neither writes nor commits later messages', async () => {
    const query = vi.fn().mockRejectedValueOnce(new Error('connection refused')).mockResolvedValue({ rowCount: 1 });
    const { processor, deps } = setup({ query });

    await processor.eachMessage({ partition: 0, message: message('create.json', '7') });
    await processor.eachMessage({ partition: 0, message: message('update-amount.json', '8') });

    expect(query).toHaveBeenCalledTimes(1); // only the failed attempt
    expect(deps.commitOffsets).not.toHaveBeenCalled();
    expect(deps.onFailure).toHaveBeenCalledTimes(1);
  });

  it('stops on an event it cannot map, without writing or committing', async () => {
    const { processor, deps } = setup();
    const truncate = message('create.json', '7');
    const value = JSON.parse(truncate.value!.toString());
    value.payload.op = 't';
    truncate.value = Buffer.from(JSON.stringify(value));

    await processor.eachMessage({ partition: 0, message: truncate });

    expect(deps.query).not.toHaveBeenCalled();
    expect(deps.commitOffsets).not.toHaveBeenCalled();
    expect(deps.onFailure).toHaveBeenCalledExactlyOnceWith('7', expect.any(UnmappableEventError));
  });

  it('reports a failed commit like a failed write (the next run redelivers; the guard makes it stale)', async () => {
    const { processor, deps } = setup({ commitOffsets: vi.fn().mockRejectedValue(new Error('coordinator not available')) });

    await expect(processor.eachMessage({ partition: 0, message: message('create.json', '7') })).resolves.toBeUndefined();

    expect(deps.query).toHaveBeenCalledTimes(1);
    expect(deps.onFailure).toHaveBeenCalledTimes(1);
  });

  it('calls afterWrite between the write and the commit (the crash test exits there)', async () => {
    const order: string[] = [];
    const { processor } = setup({
      query: async () => (order.push('query'), { rowCount: 1 }),
      commitOffsets: async () => void order.push('commit'),
      afterWrite: () => void order.push('afterWrite'),
    });

    await processor.eachMessage({ partition: 0, message: message('create.json', '7') });

    expect(order).toEqual(['query', 'afterWrite', 'commit']);
  });

  it('after stop(), neither writes nor commits new messages', async () => {
    const { processor, deps } = setup();

    await processor.stop();
    await processor.eachMessage({ partition: 0, message: message('create.json', '7') });

    expect(deps.query).not.toHaveBeenCalled();
    expect(deps.commitOffsets).not.toHaveBeenCalled();
  });

  it('stop() waits for the message in progress to be written and committed', async () => {
    // A write that only finishes when the test says so: the state when Ctrl+C arrives mid-write.
    let finishWrite!: () => void;
    const { processor, deps } = setup({
      query: () => new Promise((resolve) => (finishWrite = () => resolve({ rowCount: 1 }))),
    });

    const processing = processor.eachMessage({ partition: 0, message: message('create.json', '7') });
    let stopped = false;
    const stopping = processor.stop().then(() => (stopped = true));

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(stopped).toBe(false); // still waiting for the write
    expect(deps.commitOffsets).not.toHaveBeenCalled();

    finishWrite();
    await stopping;
    await processing;
    expect(deps.commitOffsets).toHaveBeenCalledExactlyOnceWith([{ topic: TOPIC, partition: 0, offset: '8' }]);
  });

  it('traces received, applied and committed with the ids that link them to the source change', async () => {
    const { processor, deps } = setup();

    await processor.eachMessage({ partition: 0, message: message('create.json', '7') });

    const hops = vi.mocked(deps.trace).mock.calls.map(([event]) => event);
    expect(hops.map((e) => e.hop)).toEqual(['consumer.received', 'dest.applied', 'consumer.committed']);
    const [first] = hops;
    for (const e of hops) {
      expect(e).toMatchObject({ table: 'orders', txId: first!.txId, lsn: first!.lsn });
    }
  });
});
