# Authoring styles: schema vs DSL

A plan can be written two equivalent ways. Both produce the same
`DataGenPlan` and the same data. Choose the authoring style your 
team finds more readable.

## Schema form (Pydantic models)

You can construct `ColumnSpec` objects directly and pass a strategy 
models for each column. This form is equivalent to what is
deserialized from a spec loaded from a JSON or YAML file.

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

You can also use factory helpers that return a `ColumnSpec`. This
simplifies authoring. Import the `dsl` module to access the helper
methods for defining column specs.

The helpers are intentionally **not** flat-importable from
`dbldatagen.core`. Several of the lowercase names (`decimal`, `array`,
`struct`) would shadow stdlib modules, and `faker` would shadow the
PyPI package. The `datagendg.` prefix keeps every call unambiguous
without renaming the helpers. Direct imports
(`from dbldatagen.core.spec.dsl import integer`) still work if you
prefer them.

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

## Helper → strategy map

Each DSL helper is a thin wrapper that builds a `ColumnSpec` with a
particular strategy and data type:

| DSL Helper                                  | Strategy                             | Data Type                      |
|---------------------------------------------|--------------------------------------|--------------------------------|
| `pk_auto(name)`                             | `SequenceColumn`                     | LONG                           |
| `pk_uuid(name)`                             | `UUIDColumn`                         | STRING                         |
| `pk_pattern(name, template)`                | `PatternColumn`                      | STRING                         |
| `fk(name, ref, ...)`                        | `ForeignKeyColumn` + `ForeignKeyRef` | inferred from parent PK        |
| `integer(name, min, max)`                   | `RangeColumn`                        | INT                            |
| `double(name, min, max)`                    | `RangeColumn`                        | DOUBLE                         |
| `decimal(name, min, max, precision, scale)` | `RangeColumn`                        | DECIMAL                        |
| `text(name, values)`                        | `ValuesColumn`                       | STRING                         |
| `faker(name, provider, ...)`                | `FakerColumn`                        | STRING (override via `dtype=`) |
| `timestamp(name, start, end)`               | `TimestampColumn`                    | TIMESTAMP                      |
| `pattern(name, template)`                   | `PatternColumn`                      | STRING                         |
| `expression(name, expr)`                    | `ExpressionColumn`                   | inferred from the SQL          |
| `constant(name, value, dtype=None)`         | `ConstantColumn`                     | inferred (or set)              |
| `struct(name, fields)`                      | `StructColumn`                       | derived from fields            |
| `array(name, ...)`                          | `ArrayColumn`                        | array of element type          |

> **Note:** `pk_*` helpers only build the *column*. They do not
> mark the column as the primary key. You must still declare
> `PrimaryKey(columns=[name])` on the `TableSpec` (as in the examples
> above).

Most helpers accept `seed_from=` and extra keyword args (e.g.`distribution=`, 
`step=`) that are passed to the underlying strategy. See each[column strategy page](../column-strategies/index.md) 
to compare both forms side-by-side.
