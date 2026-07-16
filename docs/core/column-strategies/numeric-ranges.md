# Numeric ranges

`RangeColumn` generates numbers from an inclusive `[min, max]` range —
the workhorse strategy for ages, prices, scores, quantities, and any
other numeric quantity. Samples are continuous by default; set a `step`
to snap them to a lattice, and set a `distribution` to skew them away
from uniform.

## When to use

Reach for a range whenever the column is a number drawn from a bounded
span. Use `integer` for whole numbers, `double` for general-purpose
floats, and `decimal` for fixed-precision financial values. For columns
that can only hold a handful of fixed values, use
[`ValuesColumn`](categorical-values.md) instead.

## Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import RangeColumn, Normal, DataType

# A continuous double in [5.0, 500.0] (the orders.amount column)
ColumnSpec(name="amount", dtype=DataType.DOUBLE,
           gen=RangeColumn(min=5.0, max=500.0))

# An integer age, lattice-snapped and Gaussian, peaked at 40
ColumnSpec(name="age", dtype=DataType.INT,
           gen=RangeColumn(min=18, max=90, step=1,
                           distribution=Normal(mean=40, stddev=12)))
```

## DSL form

```python
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import Normal

datagendg.double("amount", 5.0, 500.0)
datagendg.integer("age", 18, 90, step=1, distribution=Normal(mean=40, stddev=12))
datagendg.decimal("price", 0.0, 9999.99, precision=10, scale=2)
```

The three DSL helpers all build a `RangeColumn` and differ only in the
`dtype` they stamp on the `ColumnSpec`:

| Helper | dtype | Default `[min, max]` |
| --- | --- | --- |
| `integer(name, min, max)` | `INT` | `[0, 100]` |
| `double(name, min, max)` | `DOUBLE` | `[0.0, 1.0]` |
| `decimal(name, min, max, precision, scale)` | `DECIMAL` | `[0.0, 1000.0]` |

Each forwards extra keyword args (`step=`, `distribution=`) straight
through to the underlying `RangeColumn`, and each accepts `seed_from=`.

## Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["range"]` | `"range"` | Discriminator; set automatically. |
| `min` | `float \| int` | `0` | Inclusive lower bound. `int`/`float` may be mixed. |
| `max` | `float \| int` | `100` | Inclusive upper bound; must be `>= min`. |
| `step` | `float \| int \| None` | `None` | When set, output snaps to `{min, min+step, min+2*step, …}` ∩ `[min, max]`. Must be `> 0`. `None` means continuous. |
| `distribution` | `Distribution` | `Uniform()` | Shape of the sampling. `WeightedValues` is **not** accepted here. |

### Decimal precision and scale

`precision` and `scale` live on the `ColumnSpec`, not on `RangeColumn`.
They are only valid when `dtype=DataType.DECIMAL`, and must be set
together. When both are unset on a DECIMAL column the engine falls back
to Spark's `DecimalType()` default of `(10, 0)`. Spark's bounds apply:
`1 <= precision <= 38` and `0 <= scale <= precision`. The validator also
checks that the range *fits* — `max(abs(min), abs(max))` must be below
`10 ** (precision - scale)`, so a `decimal("price", 0, 100000,
precision=4, scale=2)` is rejected at plan time instead of overflowing
deep in a Spark job.

## Validation & gotchas

`RangeColumn`'s `validate_range` enforces, at plan time:

- **`WeightedValues` is rejected.** It weights a discrete value list,
  and a range has no list to weight. To skew a numeric range, use
  `Normal`, `LogNormal`, `Zipf`, or `Exponential`; for weighted
  *categorical* selection, use [`ValuesColumn`](categorical-values.md).
- **No NaN / Inf.** `min`, `max`, and `step` must all be finite. (NaN
  comparisons silently return `False`, which would let every other
  check pass and ship all-NaN output to the engine.)
- **`min <= max`**, and **`step > 0`** when set.
- **Int64 range-size cap.** When both `min` and `max` are `int`, the
  engine computes `range_size = max - min + 1` and passes it to
  `F.lit`, which only accepts signed int64. So `(max - min + 1)` must be
  `< 2**63`; a range spanning the full int64 domain is rejected. Narrow
  the bounds to fit.

Notes on behavior:

- **`Normal` peaks where you ask, in value units.** `Normal(mean=40,
  stddev=12)` centers the bell at 40 (clamped to the range); a bare
  `Normal()` auto-centers on the midpoint with spread `span / 6`. This
  is the one distribution with value-space parameters — and they are
  honored only on numeric `RangeColumn` (rejected on timestamps / FK /
  value lists). See [distributions.md](../distributions.md#normal).
- **Skewed distributions only fully apply to integer ranges.** On an
  integer range (`int` `min`/`max`), `Normal`, `LogNormal`, `Zipf`, and
  `Exponential` all shape the output. On a **float/double** range, only
  `Normal` is honored today; `LogNormal`, `Zipf`, and `Exponential` are
  accepted without error but currently fold into a uniform draw. For a
  skewed float column, shape an integer range and scale it in an
  [expression](expressions.md), or keep integer/decimal bounds. See
  [distributions.md](../distributions.md).
- The range is **inclusive of both ends**. `RangeColumn(min=1, max=10)`
  can emit `1` and `10`.
- With a `step`, output is snapped to the lattice intersected with
  `[min, max]` — values strictly inside the range but off the lattice
  are never produced.

## See also

- [categorical-values.md](categorical-values.md) — when the column is a
  fixed set of values, and where `WeightedValues` *is* supported
- [distributions.md](../distributions.md) — `Uniform`, `Normal`,
  `LogNormal`, `Zipf`, `Exponential` and their parameters
- [index.md](index.md) — all twelve strategies
