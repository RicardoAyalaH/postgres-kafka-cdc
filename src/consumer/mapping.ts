// Maps one Debezium change event (the parsed Kafka message value) to one SQL statement
// for the destination database. Pure: no Kafka, no database, so it is unit-tested
// against the captured fixtures in fixtures/debezium/.
//
// Idempotency: every statement is guarded by the row's source_lsn, so applying the same
// event twice, or an older event after a newer one, changes nothing (see db/dest/init).

/** A parameterized statement in the shape `pg` accepts: `client.query({ text, values })`. */
export type Statement = { text: string; values: unknown[] };

export type MappedEvent =
  | { kind: 'skip'; reason: 'tombstone' }
  | {
      kind: 'apply';
      op: 'r' | 'c' | 'u' | 'd';
      id: number;
      /** Source transaction id, used to correlate trace entries across hops. */
      txId: number;
      /** Source WAL position of this change; the idempotency guard compares it. */
      lsn: number;
      statement: Statement;
    };

export class UnmappableEventError extends Error {
  override name = 'UnmappableEventError';
}

// r/c/u: insert or overwrite the row from the `after` image, but only if this change is
// newer than the one already applied. When the WHERE is false, Postgres leaves the row
// as it is and reports 0 rows affected.
const UPSERT = `
INSERT INTO orders (id, customer_name, status, amount, created_at, updated_at, source_lsn, deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, false)
ON CONFLICT (id) DO UPDATE SET
  customer_name = EXCLUDED.customer_name,
  status        = EXCLUDED.status,
  amount        = EXCLUDED.amount,
  created_at    = EXCLUDED.created_at,
  updated_at    = EXCLUDED.updated_at,
  source_lsn    = EXCLUDED.source_lsn,
  deleted       = false
WHERE orders.source_lsn < EXCLUDED.source_lsn`.trim();

// d: soft delete using only the key and the LSN, not the `before` image (which holds only
// the primary key unless the source table uses REPLICA IDENTITY FULL). If the row is
// missing, nothing happens: with in-order consumption that means its earlier events are
// no longer in the topic, so they can't be replayed to resurrect it.
const SOFT_DELETE = `
UPDATE orders SET deleted = true, source_lsn = $2
WHERE id = $1 AND source_lsn < $2`.trim();

/**
 * @param value The Kafka message value parsed as JSON (`{ schema, payload }` from the JSON
 *   converter with embedded schemas), or `null` for a tombstone.
 * @throws UnmappableEventError if the event isn't one this consumer understands. Callers
 *   should stop rather than skip, so the replica never silently diverges.
 */
export function mapEvent(value: unknown): MappedEvent {
  if (value === null) return { kind: 'skip', reason: 'tombstone' };

  const payload = field(value, 'payload', 'object', 'message value');
  const op = field(payload, 'op', 'string', 'payload');
  const source = field(payload, 'source', 'object', 'payload');
  const txId = safeInteger(source, 'txId', 'source');
  const lsn = safeInteger(source, 'lsn', 'source');

  switch (op) {
    case 'r':
    case 'c':
    case 'u': {
      const after = field(payload, 'after', 'object', 'payload');
      const id = safeInteger(after, 'id', 'after');
      return {
        kind: 'apply', op, id, txId, lsn,
        statement: {
          text: UPSERT,
          values: [
            id,
            field(after, 'customer_name', 'string', 'after'),
            field(after, 'status', 'string', 'after'),
            // Decimal string (decimal.handling.mode=string): passed as text so it stays exact.
            field(after, 'amount', 'string', 'after'),
            // ISO-8601 with microseconds: passed as text, not through a JS Date (milliseconds).
            field(after, 'created_at', 'string', 'after'),
            field(after, 'updated_at', 'string', 'after'),
            lsn,
          ],
        },
      };
    }
    case 'd': {
      // The primary key is always in `before`, whatever the table's replica identity.
      const before = field(payload, 'before', 'object', 'payload');
      const id = safeInteger(before, 'id', 'before');
      return { kind: 'apply', op, id, txId, lsn, statement: { text: SOFT_DELETE, values: [id, lsn] } };
    }
    default:
      throw new UnmappableEventError(`unsupported op ${JSON.stringify(op)}`);
  }
}

type Kinds = { object: Record<string, unknown>; string: string };

function field<K extends keyof Kinds>(obj: unknown, name: string, kind: K, where: string): Kinds[K] {
  const v = (obj as Record<string, unknown>)[name];
  const ok = kind === 'object' ? typeof v === 'object' && v !== null : typeof v === kind;
  if (!ok) throw new UnmappableEventError(`${where}.${name}: expected ${kind}, got ${JSON.stringify(v)}`);
  return v as Kinds[K];
}

// JSON numbers are doubles; ids and LSNs are int64. Refuse values beyond 2^53 rather than
// silently rounding them (an LSN that large would mean 9 PB of WAL, so this won't happen).
function safeInteger(obj: Record<string, unknown>, name: string, where: string): number {
  const v = obj[name];
  if (!Number.isSafeInteger(v)) {
    throw new UnmappableEventError(`${where}.${name}: expected a safe integer, got ${JSON.stringify(v)}`);
  }
  return v as number;
}
