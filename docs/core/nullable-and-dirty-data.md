# Nullable columns and dirty data

Real source data has holes and noise. To exercise a pipeline's
null-handling, validation, and edge cases, you usually want test data
that *isn't* perfectly clean. Core's primary lever for this is
`null_fraction` on `ColumnSpec`, which injects `NULL`s at generation
time. This page covers nulls and how to approximate other "dirty" data.

## Injecting NULLs: `null_fraction`

Every `ColumnSpec` has two related fields — they are **orthogonal**:

| Field | Type | Default | What it does |
| --- | --- | --- | --- |
| `null_fraction` | `float` | `0.0` | Probability in `[0.0, 1.0]` that a given **row** emits `NULL` for this column. Drives runtime NULL injection. |
| `nullable` | `bool` | `False` | Whether the column is declared **nullable in the Spark schema**. Pure schema metadata; does not itself produce any nulls. |

Setting `null_fraction > 0` does **not** automatically flip `nullable`
to `True` — if you want the resulting Spark column marked nullable in
the schema *as well as* having nulls in the data, set both.

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import FakerColumn

# ~10% of email cells are NULL; column also declared nullable in the schema
ColumnSpec(
    name="email",
    nullable=True,
    null_fraction=0.10,
    gen=FakerColumn(provider="email"),
)
```

The NULL injection is **deterministic** — the same plan and seed null
out the same rows every run (see
[concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md)).

### Granularity floor

The engine's null mask has a granularity of `1/10000`, so
`null_fraction` must be either `0.0` or **at least `0.0001`**. A value
strictly between `0.0` and `0.0001` is rejected at plan time (rather
than silently rounding to zero):

```python
ColumnSpec(name="x", gen=RangeColumn(), null_fraction=1e-6)  # ValueError
```

## Nulls in the DSL

The `datagendg` helpers do **not** forward `null_fraction` / `nullable`
(extra kwargs go to the *strategy*, not the `ColumnSpec`) — the one
exception is [`fk()`](relationships/foreign-keys.md), which has explicit
`nullable=` and `null_fraction=` parameters for optional foreign keys.

For a non-FK column in DSL form, build the column and then set the field
with Pydantic's `model_copy`:

```python
from dbldatagen.core.spec import dsl as datagendg

email = datagendg.faker("email", provider="email").model_copy(
    update={"nullable": True, "null_fraction": 0.10}
)
```

…or just use the schema form (above), which takes the fields directly.

## Nulls on foreign keys

For optional foreign keys, `null_fraction` lives on the
[`ForeignKeyRef`](relationships/foreign-keys.md) (and `fk()` exposes it
directly). Don't set conflicting values in both places — if
`ColumnSpec.null_fraction` and `ForeignKeyRef.null_fraction` are both
non-zero and **differ**, the plan is rejected with an error naming both
values. Set it in one place.

## Nulls in nested columns

`StructColumn` fields each carry their own `null_fraction` (a field can
be null within an otherwise-populated struct), and an `ArrayColumn` with
`null_fraction=1.0` emits a NULL array. See
[column-strategies/nested.md](column-strategies/nested.md).

## Other "dirty" patterns

Core's first-class dirty-data feature is NULL injection. Other kinds of
mess are produced by composing existing strategies:

- **Outliers / out-of-range values** — widen a `RangeColumn`'s bounds,
  or use a heavy-tailed [distribution](distributions.md) (`Zipf`,
  `Exponential`, `LogNormal`) on an integer range so a few extreme
  values appear. To mix a clean range with rare extremes, generate a
  base column and an outlier-flag column, then combine them with an
  [`ExpressionColumn`](column-strategies/expressions.md)
  (`case when ... then ... else ... end`).
- **Duplicates** — a `ValuesColumn` over a small value list, or a
  `SequenceColumn` whose span is smaller than the row count, repeats
  values. `seed_from`
  ([correlated columns](relationships/correlated-columns.md)) makes two
  columns repeat *together*.
- **Malformed strings** — a [`PatternColumn`](column-strategies/text-and-patterns.md)
  with a deliberately wrong-shaped template, or an `ExpressionColumn`
  that concatenates junk onto a clean value.

## See also

- [concepts/plans-and-tables.md](concepts/plans-and-tables.md) — the
  full `ColumnSpec` field set
- [relationships/foreign-keys.md](relationships/foreign-keys.md) —
  nullable / optional foreign keys
- [column-strategies/nested.md](column-strategies/nested.md) — nulls in
  structs and arrays
