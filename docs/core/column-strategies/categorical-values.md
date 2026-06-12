# Categorical values

`ValuesColumn` picks each row's value from an explicit list of allowed
values — the strategy for status flags, country codes, plan tiers, and
any column drawn from a fixed vocabulary. Selection is uniform by
default; pair it with `WeightedValues` to give each value its own
probability mass.

## When to use

Use a values column when the column can only hold a handful of known
values and you want to enumerate them. For numbers from a continuous
range, use [`RangeColumn`](numeric-ranges.md); for free-form realistic
text (names, addresses), use `FakerColumn`.

## Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import ValuesColumn, WeightedValues, DataType

# Uniform pick from four strings (the customers.name column)
ColumnSpec(name="name", dtype=DataType.STRING,
           gen=ValuesColumn(values=["Alice", "Bob", "Carol", "Dave"]))

# Weighted pick — 60% delivered (the orders.status column)
ColumnSpec(name="status", dtype=DataType.STRING,
           gen=ValuesColumn(
               values=["pending", "shipped", "delivered", "cancelled"],
               distribution=WeightedValues(weights={
                   "pending": 0.1, "shipped": 0.2,
                   "delivered": 0.6, "cancelled": 0.1,
               }),
           ))
```

## DSL form

The `text` helper builds a STRING-typed `ValuesColumn`:

```python
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import WeightedValues

datagendg.text("name", ["Alice", "Bob", "Carol", "Dave"])

datagendg.text(
    "status",
    ["pending", "shipped", "delivered", "cancelled"],
    distribution=WeightedValues(weights={
        "pending": 0.1, "shipped": 0.2, "delivered": 0.6, "cancelled": 0.1,
    }),
)
```

`text` forwards extra keyword args (such as `distribution=`) to the
underlying `ValuesColumn` and accepts `seed_from=`. It always stamps
`DataType.STRING`; for a non-string value list, construct the
`ValuesColumn` directly and set `dtype` on the `ColumnSpec`.

## Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["values"]` | `"values"` | Discriminator; set automatically. |
| `values` | `list[Any]` | *(required)* | Non-empty list of allowed values. Element type (`str`, `int`, `bool`, …) is preserved through plan dump/reload. |
| `distribution` | `Distribution` | `Uniform()` | Either `Uniform` (each value equally likely) or `WeightedValues` (explicit per-value probabilities). |

## Weighted selection

Unlike [`RangeColumn`](numeric-ranges.md) and `TimestampColumn` — which
reject `WeightedValues` because they sample from a continuous range with
no list to weight — `ValuesColumn` is exactly where `WeightedValues`
belongs. Weights do not have to sum to `1.0`; the engine normalises
them. The weight keys are matched against the values by `str(value)`, so
each value must have a corresponding weight key.

## Validation & gotchas

`ValuesColumn`'s `validate_values` enforces, at plan time:

- **Non-empty list.** An empty `values` is rejected.
- **No duplicates.** Duplicate entries would silently double a value's
  probability mass under `Uniform` (and collide under `WeightedValues`,
  since the keys are `str(v)`). The error names the offending entries —
  use distinct values, or switch to `WeightedValues` with explicit
  weights.
- **Complete weights (when weighted).** Every value must have a weight
  key (keyed by `str(value)`); a missing key would silently contribute
  zero, and if *every* value were zero the engine would silently fall
  back to uniform. A missing key is rejected.
- **Positive weight mass.** The weights across the values list must sum
  to more than zero, otherwise the cumulative distribution is
  degenerate.

`WeightedValues` itself also validates its `weights` mapping: it must be
non-empty, every weight finite, and every weight `>= 0`.

## See also

- [distributions.md](../distributions.md), and its `WeightedValues`
  section, for the full weighting semantics
- [numeric-ranges.md](numeric-ranges.md) — the range strategy, which
  rejects `WeightedValues`
- [index.md](index.md) — all twelve strategies
