# Expressions

`ExpressionColumn` computes a column with a Spark SQL expression over
other columns in the same table — `quantity * unit_price`,
`concat(first_name, ' ', last_name)`, a `CASE WHEN`, anything Spark SQL
can express. It's the escape hatch for values that are a function of
columns you've already generated.

## When to use

Use an expression when a column is *derived* from other columns rather
than sampled independently — totals, labels, concatenations, bucketing.
For a fixed value on every row use [`ConstantColumn`](constants.md); for
an independently sampled value use one of the sampling strategies.

## Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import RangeColumn, ExpressionColumn, DataType

ColumnSpec(name="quantity", dtype=DataType.INT, gen=RangeColumn(min=1, max=20)),
ColumnSpec(name="unit_price", dtype=DataType.DOUBLE, gen=RangeColumn(min=1.0, max=50.0)),
# Referenced columns are declared *before* this one:
ColumnSpec(name="total", gen=ExpressionColumn(expr="quantity * unit_price")),
```

A label derived with `CASE WHEN`, mined from `tests/core/engine/test_engine.py`:

```python
ColumnSpec(name="a", gen=RangeColumn(min=0, max=100)),
ColumnSpec(name="bucket",
           gen=ExpressionColumn(expr="case when a < 50 then 'low' else 'high' end")),
```

## DSL form

```python
from dbldatagen.core.spec import dsl as datagendg

datagendg.expression("total", "quantity * unit_price")
datagendg.expression("customer_label", "concat('cid-', customer_id)")
# Control the type by casting inside the SQL — there is no dtype param:
datagendg.expression("total_dec", "cast(quantity * unit_price as decimal(11, 2))")
```

`expression(name, expr)` builds a `ColumnSpec` with an
`ExpressionColumn` generator. There is **no `dtype` parameter** — an
expression's output type is always inferred by Spark from the SQL. To
pin a type, cast inside the expression (e.g. `cast(... as date)`).

## Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["expression"]` | `"expression"` | Discriminator; set automatically. |
| `expr` | `str` | *(required)* | A Spark SQL expression string, min length 1. Referenced column names must exist on the same table, must not be FK / Faker / `seed_from` columns, and should be declared earlier (see below). |

> The output type is **inferred from the SQL** and is not declarable:
> setting `dtype`, `precision`, or `scale` on a `ColumnSpec` whose `gen`
> is an `ExpressionColumn` is rejected at plan time (see below). Cast
> inside the expression to control the type.

## Declaration order matters

Declare every column an expression references **before** the expression
itself. Expressions evaluate in the engine's first projection pass (a
single flat `select`), where Spark resolves a sibling reference only
against columns that appear earlier in the list — a forward reference to
a not-yet-projected column fails at Spark evaluation time with
`UNRESOLVED_COLUMN`.

What the planner validates at `resolve_plan` time is narrower — it
catches the two cases that would otherwise surface as a confusing
downstream Spark error, and raises a `ValueError` listing the available
columns:

1. **Unknown column** — a referenced name that is not a column on the
   table at all.
2. **Unreachable column** — a referenced name that *is* a column but is
   a foreign-key, `FakerColumn`, or `seed_from` column. Those are
   applied in a later phase (via `withColumn`), so they don't exist yet
   when the expression evaluates.

```python
ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
ColumnSpec(name="b", gen=ExpressionColumn(expr="unknown_col + 1")),  # ValueError at resolve_plan
```

Note the planner does **not** check ordering among regular columns: a
reference to a regular column that happens to be declared *later* is not
flagged at `resolve_plan` — it fails at Spark evaluation time instead.
Keep referenced regular columns earlier in the list.

Spark builtins (`year(...)`, `concat(...)`, `row_number() over (...)`)
are recognised as function calls, not column references, so they are not
flagged. The plan-time check is about *column* names that resolve to
nothing or to a later-phase column.

## Security note

> Expressions are passed directly to `F.expr()` and can execute
> arbitrary Spark SQL. **Do not use `ExpressionColumn` with untrusted
> plan YAML in multi-tenant environments** — a malicious expression runs
> with the full privileges of the generating session.

## Validation & gotchas

- **Min length 1, no whitespace-only.** `expr` must be a non-empty,
  non-blank string. A whitespace-only `expr` (`"   "`) is rejected at
  plan time rather than failing later as a Spark parse error far from
  the declaration.
- **No declared type on an `ExpressionColumn`.** The engine evaluates
  the SQL as-is with no cast, so a declared `dtype`, `precision`, or
  `scale` would be silently ignored — setting any of them on a
  `ColumnSpec` whose `gen` is an `ExpressionColumn` is rejected at plan
  time. Pin the type *inside* the expression instead:
  `cast(quantity * unit_price as decimal(11,2))` or
  `cast(ts_string as date)`.
- **References resolved at `resolve_plan`, not construction.** The
  whitespace check runs when the model is built; the *column-reference*
  check runs when the planner resolves the table (see above), because
  that's the first point at which the full set of sibling columns and
  their order is known.

## See also

- [constants.md](constants.md) — for a fixed value rather than a
  computed one
- [../concepts/plans-and-tables.md](../concepts/plans-and-tables.md) —
  column declaration order and how tables resolve
- [index.md](index.md) — all twelve strategies
