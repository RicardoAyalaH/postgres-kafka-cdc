# Debezium event fixtures

Real messages read from the `shop.public.orders` topic, not hand-written.
Used as inputs for unit tests of the consumer's event → SQL mapping.

Captured with Debezium 3.7.1.Final, the `pgoutput` plugin, the JSON converter
with embedded schemas, `REPLICA IDENTITY FULL`, and
`decimal.handling.mode=string`. That means `amount` (`numeric(10,2)`) arrives
as a decimal string with its scale kept, e.g. `"146.50"`, which Postgres
accepts directly for a `numeric` column. (With Debezium's default, `precise`,
it was a base64-encoded unscaled integer, e.g. `"MIw="` for `124.28`.)

The source table had 6 rows before the connector was registered, so the topic
starts with 6 snapshot reads (offsets 0–5), followed by live changes.

| File | `op` | What it shows |
|---|---|---|
| `snapshot-read.json` | `r` | Initial snapshot row: `before` is null, `source.snapshot` is `"true"`. `source.txId` is the snapshot's own transaction, not the one that wrote the row. |
| `create.json` | `c` | Live insert: `before` is null. |
| `update-amount.json` | `u` | Live update of `amount`: full `before` and `after`. |
| `delete.json` | `d` | Live delete: `after` is null, `before` is the full old row. |
| `tombstone.json` | — | Follows every delete: same key, `value` is null. Used by log compaction. |

If a connector setting that changes the event format is altered (for example
`decimal.handling.mode`), recapture these instead of editing them by hand.
