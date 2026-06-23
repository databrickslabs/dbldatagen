# Correlated columns: `seed_from`

By default every column draws its randomness from its own name and the
row index, so two columns vary independently. Sometimes you want them to
**agree**: every row with the same `device_id` should get the same
`country`; every order in the same `region` should land in the same
time window. `seed_from` ties one column's per-cell seed to the *value*
of another column instead of to the row index.

This is the core engine's equivalent of v0's `baseColumn`.

## How it works

Set `seed_from="<other_column>"` on the dependent column. The engine
then derives that column's per-cell seed from the value of the named
column rather than from the row id. The consequence:

> **Two rows with the same value in the `seed_from` column produce the
> same generated value in the dependent column.**

The dependent column still varies *across* different source values —
it's a deterministic function of the source value, not a constant.
Because the per-column seed also folds in the column name, two columns
that both `seed_from` the same source (e.g. `country` and
`manufacturer` both keyed off `device_id`) stay independent of each
other while each being internally consistent.

## Schema form

```python
from dbldatagen.core import DataGenPlan, TableSpec, ColumnSpec, PrimaryKey
from dbldatagen.core.spec.schema import SequenceColumn, RangeColumn, ValuesColumn, DataType

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="events",
            rows=200,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                ColumnSpec(name="id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="device_id",
                    dtype=DataType.INT,
                    gen=RangeColumn(min=1, max=50),
                ),
                ColumnSpec(
                    name="country",
                    dtype=DataType.STRING,
                    gen=ValuesColumn(values=["US", "DE", "JP", "BR"]),
                    seed_from="device_id",     # same device_id -> same country
                ),
            ],
        ),
    ],
)
```

## DSL form

Every DSL column helper that takes `seed_from` accepts it as a keyword
argument — `integer`, `double`, `decimal`, `text`, `faker`,
`timestamp`, and `pattern`:

```python
from dbldatagen.core import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="devices",
            rows=50,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[datagendg.pk_auto("id")],
        ),
        TableSpec(
            name="events",
            rows=200,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                datagendg.pk_auto("id"),
                datagendg.fk("device_id", ref="devices.id"),
                # Same device_id always maps to the same country …
                datagendg.text("country", values=["US", "DE", "JP", "BR"],
                               seed_from="device_id"),
                # … and the same manufacturer (independent of country).
                datagendg.text("manufacturer", values=["Apple", "Samsung", "Google"],
                               seed_from="device_id"),
                # No seed_from: varies per row.
                datagendg.integer("value", min=1, max=1000),
            ],
        ),
    ],
)
```

This is the exact shape exercised in
`tests/core/engine/test_seed_from.py`: grouping `events` by `device_id`
yields exactly one distinct `country` per device, while `value` (no
`seed_from`) varies row by row.

## YAML form

```yaml
seed: 42

tables:
  - name: events
    rows: 200
    primary_key:
      columns: [id]
    columns:
      - name: id
        gen:
          strategy: sequence
          start: 1
          step: 1
      - name: device_id
        dtype: int
        gen:
          strategy: range
          min: 1
          max: 50
      - name: country
        dtype: string
        gen:
          strategy: values
          values: [US, DE, JP, BR]
        seed_from: device_id
```

## Rules and limits

- **The referenced column must exist.** A `seed_from` pointing at a
  non-existent column raises a `ValueError` at plan resolution time
  (before Spark runs), naming the bad reference. **Declaration order
  does not matter** — `seed_from` columns are resolved by name in a
  later generation phase, after the source column has materialized, so
  the source may appear before *or* after the dependent column in the
  `columns` list.
- **No chains.** A column's `seed_from` may not point at another column
  that *itself* has a `seed_from` (e.g. `a → b → c`). Chains are
  rejected at resolution with a clear error — the `seed_from` phase
  applies its columns in declaration order rather than topological
  order, so a chain would break at Spark plan time. Point every
  correlated column directly at the same independent source.
- **Works with any value strategy.** `ValuesColumn`, `RangeColumn`,
  `TimestampColumn`, and the rest all honor `seed_from`.
- **Null masking is correlated too.** When the dependent column has a
  `null_fraction`, rows sharing a source value share the same
  null/non-null outcome — a device that resolves to NULL does so
  consistently across all its rows.

## Correlated vs. foreign keys

These solve different problems:

- A **foreign key** ([foreign-keys.md](foreign-keys.md)) makes a child
  column reference a *parent table's* primary key, across tables.
- **`seed_from`** correlates two columns *within the same table* so
  equal source values yield equal dependent values.

They compose: in the DSL example above, `device_id` is a foreign key
into `devices`, and `country` / `manufacturer` are correlated to that
FK within `events`.

## See also

- [concepts/determinism-and-seeds.md](../concepts/determinism-and-seeds.md)
  — the seed model `seed_from` builds on
- [relationships/foreign-keys.md](foreign-keys.md) — cross-table
  references
- [concepts/plans-and-tables.md](../concepts/plans-and-tables.md) —
  the `DataGenPlan` / `TableSpec` / `ColumnSpec` building blocks
