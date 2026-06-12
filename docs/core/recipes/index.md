# Recipes

End-to-end, copy-pasteable plans that build a complete dataset and
show the strategies working together. Each recipe is a single
`generate(...)` / `generate_table(...)` call you can run as-is, then
adapt.

If you're picking a strategy in isolation, start from
[../column-strategies/index.md](../column-strategies/index.md); if you
want the conceptual tour first, see [../README.md](../README.md).

## Foundational

The building blocks every other recipe composes from.

- [multi-table.md](multi-table.md) — the canonical `customers` /
  `orders` dataset: two tables joined by a foreign key, generated in one
  call and verified for referential integrity. Showcases `PrimaryKey`,
  `ForeignKeyRef`, and multi-table `generate`.
- [sequential-timestamps.md](sequential-timestamps.md) — evenly-spaced
  (fixed-interval) timestamps, one row per second/hour/day. Showcases
  `SequenceColumn` + `ExpressionColumn` composition and the ANSI /
  session-time-zone caveats.

## Industry datasets

Larger, domain-shaped plans — each rebuilds a classic v0 dataset on the
core engine.

- [iot-telematics.md](iot-telematics.md) — a fleet of GPS devices
  emitting `lat` / `lon` / `heading` time-series with a WKT `POINT(...)`
  string, modeled as a `devices` dimension plus a `telemetry` fact.
  Showcases FK fan-out, `RangeColumn`, and derived expressions.
- [stock-ticker.md](stock-ticker.md) — a daily OHLCV (open / high / low
  / close / volume) series, one row per `(symbol, trading day)`, with
  internally-consistent prices in `decimal(11,2)`. Showcases a
  dimension + fact split, `SequenceColumn`, and derived OHLC
  expressions.
- [process-historian.md](process-historian.md) — a manufacturing
  process-historian feed, one row per tag reading, where each device
  reports consistently from the same plant and tag across thousands of
  rows. Showcases `seed_from` per-key correlation.
- [realistic-customers.md](realistic-customers.md) — a CRM-style
  `customers` table with real-shaped names, emails, companies, phones,
  and addresses. Showcases `FakerColumn` for PII and `PatternColumn` for
  the customer ID.
- [retail-star-schema.md](retail-star-schema.md) — a retail star schema
  with two dimensions (`products`, `customers`) and two facts (`orders`,
  `order_items`), where `order_items` carries **two foreign keys at
  once** (to `orders` and `products`) and computes a
  `quantity × unit_price` line total. Showcases multi-FK facts and
  derived measures.

## See also

- [../README.md](../README.md) — the core documentation map
- [../column-strategies/index.md](../column-strategies/index.md) — pick
  a single strategy
- [../relationships/foreign-keys.md](../relationships/foreign-keys.md) —
  the FK rules behind the multi-table recipes
