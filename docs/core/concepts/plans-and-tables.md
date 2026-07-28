# Plans, tables, and columns

The core modules use nested building blocks. From the outside in:

```
DataGenPlan          # the whole dataset
└── TableSpec        # one table
    └── ColumnSpec   # one column
        └── gen      # the generation strategy (SequenceColumn, RangeColumn, …)
```

## DataGenPlan

The top-level object. Holds the list of tables and the global seed.

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `tables` | `list[TableSpec]` | — | At least one table required. |
| `seed` | `int` | `42` | Global seed; propagated to tables. Omitting it emits a `UserWarning` — see [determinism-and-seeds.md](determinism-and-seeds.md). |
| `default_locale` | `str` | `"en_US"` | Default Faker locale for `FakerColumn`s that don't set their own. |

The plan validates relationships eagerly. Duplicate table names are
rejected, and foreign-key references are resolved (with cycle detection) 
when the plan is resolved for generation.

```python
plan = DataGenPlan(seed=42, tables=[customers, orders])
```

## TableSpec

Defines a single table to generate. One or more `TableSpec` instances
can be added to a single `DataGenPlan`.

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | `str` | — | Valid identifier (`[A-Za-z_][A-Za-z0-9_]*`). Used as the dict key in `generate()` output and as the parent name in FK refs. |
| `columns` | `list[ColumnSpec]` | — | At least one; names unique within the table. Order matters for `ExpressionColumn` and `seed_from`. |
| `rows` | `int \| str` | — | Row count. Accepts a readable string: `"10K"`, `"5M"`, `"1B"`. Must be positive. |
| `primary_key` | `PrimaryKey \| None` | `None` | Required when another table FKs into this one; optional otherwise. |
| `seed` | `int \| None` | `None` | Usually left unset and propagated from the plan. Set explicitly only when generating a lone table via `generate_table`. |

```python
TableSpec(
    name="orders",
    rows="1M",                                # readable string form
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[...],
)
```

## ColumnSpec

Defines a single column to generate. The `gen` field specifies the 
strategy that determines how values are generated. Other fields
capture metadata.

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | `str` | — | Column name. |
| `gen` | one of the column strategies | — | `SequenceColumn`, `RangeColumn`, `ValuesColumn`, `TimestampColumn`, `FakerColumn`, `PatternColumn`, `ExpressionColumn`, `ConstantColumn`, `UUIDColumn`, `ForeignKeyColumn`, `StructColumn`, `ArrayColumn`. See [column-strategies/index.md](../column-strategies/index.md). |
| `dtype` | `DataType \| None` | `None` | Optional output type. When `None`, the engine picks the natural type for the strategy. |
| `null_fraction` | `float` | `0.0` | Probability `[0.0, 1.0]` that a row emits `NULL` for this column. Must be `0.0` or `>= 0.0001`. See [nullable-and-dirty-data.md](../nullable-and-dirty-data.md). |
| `nullable` | `bool` | `False` | Whether the column is declared nullable in the Spark schema. Orthogonal to `null_fraction` (schema metadata only). |
| `seed_from` | `str \| None` | `None` | Derive this column's per-cell seed from another column — see [correlated-columns.md](../relationships/correlated-columns.md). |
| `foreign_key` | `ForeignKeyRef \| None` | `None` | Set together with `gen=ForeignKeyColumn()` — see [foreign-keys.md](../relationships/foreign-keys.md). |
| `precision`, `scale` | `int \| None` | `None` | For `DataType.DECIMAL` columns. |

```python
ColumnSpec(name="amount", dtype=DataType.DOUBLE, gen=RangeColumn(min=5.0, max=500.0))
```

### `DataType`

The output types, mapped to Spark SQL types:

`INT` (alias `INTEGER`), `LONG`, `FLOAT`, `DOUBLE`, `STRING`,
`BOOLEAN`, `DATE`, `TIMESTAMP`, `DECIMAL`.

When loading from JSON/YAML, the alternate spellings `"integer"`,
`"bool"`, and `"str"` are also accepted and normalized.

## How the pieces connect

- `PrimaryKey(columns=[...])` on a `TableSpec` marks the table's key.
- `ForeignKeyRef(ref="customers.customer_id")` on a child column points
  at `"<table>.<column>"` of a parent's primary key.
- The plan is resolved into generation order (parents before children)
  before any data is produced.

See [getting-started.md](../getting-started.md) for these all working
together, and [authoring-styles.md](authoring-styles.md) for the
compact DSL form of the same models.
