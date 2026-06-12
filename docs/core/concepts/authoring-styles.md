# Authoring styles: schema vs DSL

A plan can be written two equivalent ways. Both produce the same
`DataGenPlan` and the same data — choose whichever your team finds more
readable.

## Schema form (Pydantic models)

Explicit construction of `ColumnSpec` and the strategy models. Verbose
but self-documenting, and it's exactly what a JSON/YAML plan
deserializes into (so the field names match the file format).

```python
from dbldatagen.core import DataGenPlan, TableSpec, ColumnSpec, PrimaryKey
from dbldatagen.core.spec.schema import SequenceColumn, RangeColumn, DataType

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="orders",
            rows=1000,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="amount", dtype=DataType.DOUBLE,
                           gen=RangeColumn(min=5.0, max=500.0)),
            ],
        ),
    ],
)
```

## DSL form (`datagendg` helpers)

Lowercase factory helpers that each return a `ColumnSpec`. Compact, and
deliberately mirrors the PySpark convention of
`import pyspark.sql.functions as F`.

```python
from dbldatagen.core import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="orders",
            rows=1000,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                datagendg.pk_auto("order_id"),
                datagendg.double("amount", 5.0, 500.0),
            ],
        ),
    ],
)
```

## Why the `datagendg.` prefix

The helpers are intentionally **not** flat-importable from
`dbldatagen.core`. Several of the lowercase names (`decimal`, `array`,
`struct`) would shadow stdlib modules, and `faker` would shadow the
PyPI package. The `datagendg.` prefix keeps every call unambiguous
without renaming the helpers. Direct imports
(`from dbldatagen.core.spec.dsl import integer`) still work if you
prefer them.

## Helper → strategy map

Each DSL helper is a thin wrapper that builds a `ColumnSpec` with a
particular strategy and dtype:

| DSL helper | Builds | dtype |
| --- | --- | --- |
| `pk_auto(name)` | `SequenceColumn` | LONG |
| `pk_uuid(name)` | `UUIDColumn` | STRING |
| `pk_pattern(name, template)` | `PatternColumn` | STRING |
| `fk(name, ref, ...)` | `ForeignKeyColumn` + `ForeignKeyRef` | inferred from parent PK |
| `integer(name, min, max)` | `RangeColumn` | INT |
| `double(name, min, max)` | `RangeColumn` | DOUBLE |
| `decimal(name, min, max, precision, scale)` | `RangeColumn` | DECIMAL |
| `text(name, values)` | `ValuesColumn` | STRING |
| `faker(name, provider, ...)` | `FakerColumn` | STRING (override via `dtype=`) |
| `timestamp(name, start, end)` | `TimestampColumn` | TIMESTAMP |
| `pattern(name, template)` | `PatternColumn` | STRING |
| `expression(name, expr)` | `ExpressionColumn` | inferred from the SQL |
| `constant(name, value, dtype=None)` | `ConstantColumn` | inferred (or set) |
| `struct(name, fields)` | `StructColumn` | derived from fields |
| `array(name, ...)` | `ArrayColumn` | array of element type |

> **Note:** the `pk_*` helpers only build the *column*; they do not
> mark it as the primary key. You still declare
> `PrimaryKey(columns=[name])` on the `TableSpec` (as in the examples
> above). `pk_auto` produces a `LONG` sequence.

Most helpers accept `seed_from=` and forward extra keyword args (e.g.
`distribution=`, `step=`) straight to the underlying strategy. See each
[column strategy page](../column-strategies/index.md) for both forms
side by side.
