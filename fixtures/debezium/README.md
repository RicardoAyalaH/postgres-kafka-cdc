# Debezium event fixtures

Real messages read from the `shop.public.orders` topic, not hand-written.
Used as inputs for unit tests of the consumer's event → SQL mapping.

Captured with Debezium 3.7.1.Final, the `pgoutput` plugin, the JSON converter
with embedded schemas, `REPLICA IDENTITY FULL`, and the default
`decimal.handling.mode=precise`. That means `amount` is a base64-encoded,
big-endian two's-complement unscaled integer with scale 2. Example:
`"MIw="` → `0x308C` → 12428 → `124.28`.

| File | `op` | What it shows |
|---|---|---|
| `snapshot-read.json` | `r` | Initial snapshot row: `before` is null, `source.snapshot` is `"true"`. `source.txId` is the snapshot's own transaction, not the one that wrote the row. |
| `create.json` | `c` | Live insert: `before` is null. |
| `update-amount.json` | `u` | Live update of `amount`: full `before` and `after`. |
| `delete.json` | `d` | Live delete: `after` is null, `before` is the full old row. |
| `tombstone.json` | — | Follows every delete: same key, `value` is null. Used by log compaction. |

If a connector setting that changes the event format is altered (for example
`decimal.handling.mode`), recapture these instead of editing them by hand.
