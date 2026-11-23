-- Destination schema. Runs once, when the dest-db data volume is first initialized.
--
-- A replica of source orders, written only by the CDC consumer. It deliberately does not
-- copy the source's CHECK constraints, defaults or identity: the source already enforced
-- them, and a replica that rejects a row would stall the pipeline.

CREATE TABLE orders (
    id            bigint        PRIMARY KEY,  -- plain bigint: ids come from the source
    customer_name text          NOT NULL,
    status        text          NOT NULL,
    amount        numeric(10,2) NOT NULL,
    created_at    timestamptz   NOT NULL,
    updated_at    timestamptz   NOT NULL,

    -- Idempotency guard. source_lsn is the source WAL position (Debezium's source.lsn) of
    -- the last change applied to this row. The consumer upserts with
    --   ON CONFLICT (id) DO UPDATE ... WHERE orders.source_lsn < EXCLUDED.source_lsn
    -- so a redelivered or replayed event (same or older LSN) changes nothing.
    source_lsn    bigint        NOT NULL,

    -- Soft delete. A deleted row stays as a marker that keeps its source_lsn, so a replayed
    -- older insert or update can't bring it back. Markers are never purged (a limitation).
    deleted       boolean       NOT NULL DEFAULT false
);

-- What readers should query: current orders, without the delete markers.
CREATE VIEW orders_live AS
    SELECT id, customer_name, status, amount, created_at, updated_at
    FROM orders
    WHERE NOT deleted;
