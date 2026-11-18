// Shared trace format. Every component of the pipeline appends one JSON object per
// line to the same file, so a single change can be followed hop by hop.

import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

/** The hops a change passes through, in pipeline order. */
export type Hop =
  | 'source.commit'      // generator: change committed in the source database
  | 'debezium.captured'  // Debezium read it from the WAL (taken from the event's payload.ts_ms)
  | 'kafka.produced'     // record appended to a Kafka partition (record timestamp)
  | 'consumer.received'  // consumer pulled the record
  | 'dest.applied'       // change written to the destination database
  | 'consumer.committed' // consumer committed its Kafka offset
  | 'slot.status';       // periodic replication-slot snapshot, not tied to one change

/** Debezium's operation codes: c = create, u = update, d = delete, r = read (snapshot). */
export type Op = 'c' | 'u' | 'd' | 'r';

export interface TraceEvent {
  /** Wall-clock time the entry was recorded (ISO 8601, UTC). */
  ts: string;
  hop: Hop;
  /** Which process wrote the entry: "generator", "consumer", ... */
  emitter: string;
  table?: string;
  op?: Op;
  /**
   * Primary key of the changed row. Values are always strings: Postgres bigint can
   * exceed JavaScript's safe integer range, so `pg` returns it as a string anyway.
   */
  key?: Record<string, string>;
  /** Postgres transaction id. Shared by all hops of the same change. */
  txId?: string;
  /** Position of the change in the WAL. Known from the Debezium event onward. */
  lsn?: string;
  detail?: Record<string, unknown>;
}

export type TraceWriter = (event: Omit<TraceEvent, 'ts' | 'emitter'>) => void;

export const DEFAULT_TRACE_FILE = resolve('logs', 'trace.jsonl');

/**
 * Returns a function that appends trace events to the trace file.
 * Writes are synchronous so an entry is on disk before the caller moves on;
 * at the event rates of this project that cost is negligible.
 */
export function createTraceWriter(
  emitter: string,
  file: string = process.env.TRACE_FILE ?? DEFAULT_TRACE_FILE,
): TraceWriter {
  mkdirSync(dirname(file), { recursive: true });
  return (event) => {
    const entry: TraceEvent = { ts: new Date().toISOString(), emitter, ...event };
    appendFileSync(file, JSON.stringify(entry) + '\n');
  };
}
