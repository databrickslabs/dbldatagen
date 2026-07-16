# Sequences and IDs

Two strategies produce key-shaped columns: `SequenceColumn` gives a
monotonic integer sequence (`1, 2, 3, …`), and `UUIDColumn` gives
deterministic UUID strings. Both are common choices for primary keys,
but neither marks the column as a key on its own — you still declare a
`PrimaryKey` on the `TableSpec`.

## When to use

- **`SequenceColumn`** — clean auto-incrementing integer keys, or any
  evenly-spaced integer series (`start + i * step`). It's the natural
  PK for `customers.customer_id` / `orders.order_id` in the running
  example.
- **`UUIDColumn`** — when you need globally-unique string identifiers
  that don't reveal a row count, but still want reproducible output:
  the same `(table seed, row index)` always yields the same UUID.

For human-readable string keys like `"CUST-000042"`, reach for
`pk_pattern` / `PatternColumn` instead — see
[text-and-patterns.md](text-and-patterns.md).

## SequenceColumn

The value at row `i` is `start + i * step`. With the defaults
(`start=1`, `step=1`) that's `1, 2, 3, …`.

### Schema form

```python
from dbldatagen.core import DataGenPlan, TableSpec, ColumnSpec, PrimaryKey
from dbldatagen.core.spec.schema import SequenceColumn

TableSpec(
    name="customers",
    rows=200,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
        # ... other columns ...
    ],
)
```

Note the two halves: `gen=SequenceColumn(...)` builds the *column*, and
`primary_key=PrimaryKey(columns=["customer_id"])` on the `TableSpec`
marks it as the key. The sequence column does **not** declare itself as
a primary key.

### DSL form

```python
from dbldatagen.core import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg

TableSpec(
    name="customers",
    rows=200,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        datagendg.pk_auto("customer_id"),
        # ... other columns ...
    ],
)
```

`pk_auto(name)` builds a `ColumnSpec` with `DataType.LONG` and a default
`SequenceColumn()` (so `start=1, step=1`). Like the schema form, it only
builds the column — you still add `PrimaryKey(columns=[name])` to the
`TableSpec`. To override `start`/`step`, use the schema form or pass
them through: `pk_auto` itself takes only the name.

### Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["sequence"]` | `"sequence"` | Discriminator; set automatically. |
| `start` | `int` | `1` | Value at row `0`. |
| `step` | `int` | `1` | Increment per row. May be negative (descending sequence); must not be `0`. |

### Validation & gotchas

- **`step` must not be `0`.** A zero step would make every row the same
  value — use `ConstantColumn` for that. The model raises
  `ValueError("step must not be 0")`.
- **`step` may be negative**, giving a descending sequence
  (`start, start-step, …`).
- **int64 overflow is checked at plan time.** The owning `TableSpec`
  validates that the last emitted value `start + (rows - 1) * step` fits
  in signed int64 (`[-2**63, 2**63-1]`). An adversarial
  `(start, step, rows)` combination raises a `ValueError` naming the
  table, column, and overflowing magnitude — rather than surfacing as an
  opaque `ARITHMETIC_OVERFLOW` deep in the Spark job. At realistic
  scales (hundreds of millions to a few billion rows with a unit step)
  there is no overflow path.
- `pk_auto` produces a `LONG` column. If you want a narrower integer
  type, set `dtype=` yourself via the schema form.

## UUIDColumn

Deterministic UUIDs, generated as version-5 UUIDs from the table seed
plus the row index. The strategy has no parameters beyond its
discriminator.

### Schema form

```python
from dbldatagen.core import DataGenPlan, TableSpec, ColumnSpec, PrimaryKey
from dbldatagen.core.spec.schema import UUIDColumn, DataType

TableSpec(
    name="events",
    rows=1000,
    primary_key=PrimaryKey(columns=["event_id"]),
    columns=[
        ColumnSpec(name="event_id", dtype=DataType.STRING, gen=UUIDColumn()),
        # ... other columns ...
    ],
)
```

### DSL form

```python
from dbldatagen.core import TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg

TableSpec(
    name="events",
    rows=1000,
    primary_key=PrimaryKey(columns=["event_id"]),
    columns=[
        datagendg.pk_uuid("event_id"),
        # ... other columns ...
    ],
)
```

`pk_uuid(name)` builds a `ColumnSpec` with `DataType.STRING` and a
`UUIDColumn()` generator. As with `pk_auto`, it does not mark the column
as the key — declare `PrimaryKey(columns=[name])` on the `TableSpec`.

### Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["uuid"]` | `"uuid"` | Discriminator; set automatically. No other fields. |

### Validation & gotchas

- **Deterministic, not random.** Output is reproducible: the same
  `(table seed, row index)` always yields the same UUID string. Change
  the plan seed (or row position) to get a different value.
- The column's natural type is `STRING`; `pk_uuid` sets that for you.

## Marking the primary key

Both strategies pair with `PrimaryKey` on the `TableSpec`:

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `columns` | `list[str]` | — | Names of the columns forming the key, in declaration order. Non-empty, no duplicates; each name must match a `ColumnSpec.name` on the table. |

Composite PKs (more than one column) are accepted, but a
`ForeignKeyRef` cannot point into a composite-PK table — FK refs are
single-column. Duplicate column names in a `PrimaryKey` are rejected at
plan time.

A `PrimaryKey` is **required** only when another table foreign-keys into
this one; for standalone tables it's optional.

## See also

- [text-and-patterns.md](text-and-patterns.md) — `PatternColumn` /
  `pk_pattern` for human-readable string keys
- [../relationships/foreign-keys.md](../relationships/foreign-keys.md) —
  referencing a primary key from a child table
- [../concepts/plans-and-tables.md](../concepts/plans-and-tables.md) —
  how `PrimaryKey` fits on a `TableSpec`
- [../concepts/determinism-and-seeds.md](../concepts/determinism-and-seeds.md)
  — what makes `UUIDColumn` reproducible
- [index.md](index.md) — all strategies at a glance
