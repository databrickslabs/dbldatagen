# Constants

`ConstantColumn` puts the same literal value on every row — environment
markers, version stamps, batch ids, or a fixed null literal.

## When to use

Use a constant when the column has no per-row variation. If you find
yourself reaching for `PatternColumn` with a template that has no
placeholders, that's a constant — the engine will redirect you. If every
row should hold the *same* value but you want a real type other than the
one Spark infers from the Python literal, set `dtype`.

## Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import ConstantColumn, DataType

ColumnSpec(name="environment", gen=ConstantColumn(value="prod"))

# With an explicit Spark type
ColumnSpec(name="schema_version", dtype=DataType.INT,
           gen=ConstantColumn(value=3))
```

## DSL form

```python
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import DataType

datagendg.constant("environment", "prod")
datagendg.constant("schema_version", 3, dtype=DataType.INT)
```

`constant(name, value, dtype=None)` builds a `ColumnSpec` with a
`ConstantColumn` generator. When `dtype` is `None` (the default), Spark
infers the column type from the Python type of `value`.

## Fields

`ConstantColumn` itself carries only the value; the optional `dtype`
lives on the `ColumnSpec`.

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["constant"]` | `"constant"` | Discriminator; set automatically. |
| `value` | `Any` | *(required)* | The literal emitted on every row. Any JSON-serialisable type; preserved literally through plan dump/reload. |
| `dtype` *(on `ColumnSpec`)* | `DataType \| None` | `None` | Optional Spark type override. When `None`, Spark infers it from `value`. |

## Validation & gotchas

`ConstantColumn` has no validator of its own — `value` is accepted as-is.
The relevant checks come from the owning `ColumnSpec`:

- **DECIMAL fit.** When the column is `dtype=DataType.DECIMAL` with an
  explicit `precision`/`scale`, a numeric constant whose magnitude
  doesn't fit `decimal(precision, scale)` is rejected at plan time, and
  a non-finite (NaN/Inf) numeric constant is rejected outright (it would
  otherwise ship to the engine as `F.lit(nan)`).
- **No NaN/Inf via DECIMAL.** As above, the finiteness check only runs
  on the DECIMAL path; a non-finite constant on a non-decimal column is
  not screened by the spec.

## See also

- [text-and-patterns.md](text-and-patterns.md) — `PatternColumn`, which
  rejects placeholder-free templates and points you here
- [expressions.md](expressions.md) — for a value computed from other
  columns rather than a fixed literal
- [index.md](index.md) — all twelve strategies
