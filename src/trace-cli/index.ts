// Prints per-change timelines from the trace file (logs/trace.jsonl): when each change
// was committed in the source, processed by Debezium, written to Kafka, and received,
// applied and committed by the consumer, including redeliveries.
//
// Usage: npm run trace -- [--tx 813] [--last 10] [--file logs/trace.jsonl]
//
// Times come from three clocks (source-db container, Kafka Connect container, host), so a
// difference between hops on different clocks includes any offset between those clocks.
// These are single timelines for inspection, not latency statistics.
import { readFileSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { DEFAULT_TRACE_FILE, type TraceEvent } from '../shared/trace.ts';
import { buildTimelines, changeTime, status, type Change } from './timeline.ts';

const { values: args } = parseArgs({
  options: {
    tx: { type: 'string' },
    last: { type: 'string', default: '10' },
    file: { type: 'string', default: process.env.TRACE_FILE ?? DEFAULT_TRACE_FILE },
  },
});
const last = Number(args.last);
if (!Number.isInteger(last) || last < 1) throw new Error(`--last must be an integer >= 1, got "${args.last}"`);

// ---------- Read ----------

const events: TraceEvent[] = [];
let unreadable = 0;
for (const line of readFileSync(args.file, 'utf8').split('\n')) {
  if (line.trim() === '') continue;
  try {
    events.push(JSON.parse(line));
  } catch {
    unreadable++; // e.g. a line cut off by a crash mid-write
  }
}

const all = buildTimelines(events);
const selected = (args.tx === undefined ? all : all.filter((c) => c.txId === args.tx)).slice(-last);

// ---------- Format ----------

const OP = { r: 'snapshot read', c: 'insert', u: 'update', d: 'delete' } as const;

const time = (ms: number) => new Date(ms).toISOString().replace('T', ' ').replace('Z', '');

function delta(ms: number): string {
  const sign = ms < 0 ? '-' : '+';
  const abs = Math.abs(ms);
  if (abs < 1000) return `${sign}${abs} ms`;
  if (abs < 60_000) return `${sign}${(abs / 1000).toFixed(1)} s`;
  if (abs < 3_600_000) return `${sign}${(abs / 60_000).toFixed(1)} min`;
  return `${sign}${(abs / 3_600_000).toFixed(1)} h`;
}

type Row = { at: number; what: string; clock: string };

function rows(change: Change): Row[] {
  const out: Row[] = [];
  if (change.sourceTsMs !== undefined) out.push({ at: change.sourceTsMs, what: 'committed in source (WAL)', clock: 'source-db' });
  if (change.generatorCommitAt !== undefined) out.push({ at: change.generatorCommitAt, what: 'generator logged commit', clock: 'host' });

  let seen = { debezium: undefined as number | undefined, kafka: undefined as number | undefined };
  change.attempts.forEach((a, i) => {
    // Shown again only if they differ, i.e. Debezium sent the change again as a new record.
    if (a.debeziumTsMs !== undefined && a.debeziumTsMs !== seen.debezium) {
      out.push({ at: a.debeziumTsMs, what: 'Debezium processed', clock: 'Connect' });
    }
    if (a.kafkaTimestampMs !== undefined && a.kafkaTimestampMs !== seen.kafka) {
      out.push({ at: a.kafkaTimestampMs, what: `Kafka record, offset ${a.offset}`, clock: 'Connect' });
    }
    seen = { debezium: a.debeziumTsMs, kafka: a.kafkaTimestampMs };

    const n = change.attempts.length > 1 ? ` (attempt ${i + 1})` : '';
    out.push({ at: a.receivedAt, what: `consumer received${n}`, clock: 'host' });
    if (a.appliedAt !== undefined) out.push({ at: a.appliedAt, what: a.result === 'stale' ? 'stale, skipped by LSN guard' : 'applied to dest-db', clock: 'host' });
    if (a.committedAt !== undefined) out.push({ at: a.committedAt, what: `offset committed (${a.committed})`, clock: 'host' });
    else if (i < change.attempts.length - 1) out.push({ at: a.appliedAt ?? a.receivedAt, what: 'not committed (redelivered)', clock: 'host' });
  });
  return out;
}

function print(change: Change): void {
  const key = Object.entries(change.key ?? {}).map(([k, v]) => `${k}=${v}`).join(' ');
  const lsn = change.lsn === undefined ? '' : `  lsn ${change.lsn}`;
  console.log(`tx ${change.txId ?? '?'}  ${change.op ? OP[change.op] : '?'} ${change.table ?? ''} ${key}${lsn}  [${status(change)}]`);
  const t0 = changeTime(change);
  for (const r of rows(change)) {
    console.log(`  ${time(r.at)}  ${r.what.padEnd(32)} ${r.clock.padEnd(10)} ${delta(r.at - t0).padStart(10)}`);
  }
  console.log();
}

// ---------- Output ----------

const counts = { committed: 0, 'not committed': 0, 'not received': 0 };
for (const c of all) counts[status(c)]++;
console.log(
  `${args.file}: ${all.length} changes (${counts.committed} committed, ${counts['not committed']} not committed, ` +
    `${counts['not received']} not received)` + (unreadable ? `; ${unreadable} unreadable lines skipped` : ''),
);
console.log(
  `Showing ${selected.length}${args.tx === undefined ? '' : ` with tx ${args.tx}`}, oldest first. ` +
    'Times are UTC; differences are from the source commit and include offsets between clocks.\n',
);
for (const change of selected) print(change);
