# Column strategies

Every column carries a *strategy* in its `gen` field — the recipe for
producing that column's values. There are twelve strategies, one for
each shape of synthetic data the engine knows how to make. This page is
the map; each strategy has its own page with both authoring styles, the
full field table, and its validation rules.

Strategies are the discriminated union `ColumnStrategy` in
`dbldatagen.core.spec.schema`; the discriminator is the `strategy`
literal (`"range"`, `"values"`, `"faker"`, …). When a plan is loaded
from YAML/JSON, that literal is what selects the model.

## The twelve strategies

| Strategy | What it produces | DSL helper | Doc page |
| --- | --- | --- | --- |
| `RangeColumn` | Numbers from a `[min, max]` range, optionally lattice-snapped to a `step` | `integer`, `double`, `decimal` | [numeric-ranges.md](numeric-ranges.md) |
| `ValuesColumn` | A pick from an explicit list of allowed values, optionally weighted | `text` | [categorical-values.md](categorical-values.md) |
| `FakerColumn` | Realistic values from a Faker provider (`first_name`, `company`, `ipv4`, …) | `faker` | [text-and-patterns.md](text-and-patterns.md) |
| `PatternColumn` | Strings from a template with `{seq}`, `{uuid}`, `{digit:N}`, `{hex:N}`, `{alpha:N}` placeholders | `pattern`, `pk_pattern` | [text-and-patterns.md](text-and-patterns.md) |
| `SequenceColumn` | A monotonic integer sequence (`start + i * step`) | `pk_auto` | [sequences-and-ids.md](sequences-and-ids.md) |
| `UUIDColumn` | Deterministic UUIDs (v5 from seed + row index) | `pk_uuid` | [sequences-and-ids.md](sequences-and-ids.md) |
| `ExpressionColumn` | A column computed by Spark SQL over earlier columns | `expression` | [expressions.md](expressions.md) |
| `TimestampColumn` | Random timestamps within an ISO date/time range | `timestamp` | [timestamps.md](timestamps.md) |
| `ConstantColumn` | The same literal value on every row | `constant` | [constants.md](constants.md) |
| `ForeignKeyColumn` | Values resolved from another table's primary key | `fk` | [../relationships/foreign-keys.md](../relationships/foreign-keys.md) |
| `StructColumn` | A nested Spark struct grouping child columns | `struct` | [nested.md](nested.md) |
| `ArrayColumn` | A variable-length array of generated elements | `array` | [nested.md](nested.md) |

## How to choose

Start from the shape of the value, not the Spark type. For a numeric
quantity drawn from a range — ages, prices, scores — reach for
`RangeColumn`, skewing it with a `distribution` when real data wouldn't
be uniform. When the column can only hold a handful of known labels —
status, country, plan tier — use `ValuesColumn`, and weight it when the
labels aren't equally likely. `FakerColumn` covers everything that
looks like real-world text (names, addresses, emails) and
`PatternColumn` covers structured identifiers you can describe as a
template (`"ORD-{digit:4}-{alpha:3}"`).

For keys, `SequenceColumn` gives clean auto-incrementing integers,
`UUIDColumn` gives reproducible UUIDs, and `pk_pattern` gives
human-readable string keys. `ForeignKeyColumn` is the only strategy
that pulls its values from *another* table — use it to wire a child
table to its parent's PK. `ConstantColumn` stamps a fixed value (an
environment marker, a batch id). `ExpressionColumn` is the escape hatch:
when the value is a function of columns you've already generated, write
the Spark SQL directly. `StructColumn` and `ArrayColumn` add nesting
once the row stops being flat.

Every strategy that samples from a range or a list accepts a
`distribution`, so the same `RangeColumn` can be uniform, Gaussian,
log-normal, Zipfian, or exponential — see
[distributions.md](../distributions.md).

## See also

- [../concepts/authoring-styles.md](../concepts/authoring-styles.md) —
  schema form vs DSL form, and the full helper → strategy map
- [../concepts/plans-and-tables.md](../concepts/plans-and-tables.md) —
  how columns compose into tables and plans
- [distributions.md](../distributions.md) — the shared `distribution`
  field and every distribution it accepts
