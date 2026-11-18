-- Source schema. Runs once, when the source-db data volume is first initialized
-- (files in /docker-entrypoint-initdb.d are skipped if data already exists).

CREATE TABLE orders (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name text          NOT NULL,
    status        text          NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    -- numeric on purpose: Debezium's default encoding of numeric is a known gotcha (see step 4).
    amount        numeric(10,2) NOT NULL CHECK (amount >= 0),
    created_at    timestamptz   NOT NULL DEFAULT now(),
    updated_at    timestamptz   NOT NULL DEFAULT now()
);

-- Keep updated_at current on every UPDATE.
CREATE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- REPLICA IDENTITY FULL: the WAL records the entire old row for UPDATE and DELETE,
-- so Debezium events carry a complete "before" image (not just the primary key).
-- Costs extra WAL volume; chosen here for visibility while learning.
ALTER TABLE orders REPLICA IDENTITY FULL;
