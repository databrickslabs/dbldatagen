# Distributions

A **distribution** shapes *which* values a strategy draws. By default
every strategy that accepts one uses `Uniform` — each value in the
target range (or each item in a value list) is equally likely. Swap in
a different distribution to model realistic skew: a handful of popular
products, a normal cluster of ages, a heavy tail of file sizes.

Distributions live in `dbldatagen.core.spec.schema` and are attached to
a strategy's `distribution` field:

```python
from dbldatagen.core.spec.schema import RangeColumn, Normal

RangeColumn(min=0, max=100, distribution=Normal())  # bell centered on the range
```

In YAML/JSON, a distribution is a small object keyed by a `type:`
discriminator (`uniform`, `normal`, `lognormal`, `zipf`, `exponential`,
`weighted`):

```yaml
gen:
  strategy: range
  min: 0
  max: 100
  distribution:
    type: normal
    mean: 50
    stddev: 15
```

## Where distributions apply

Two families of strategy accept a `distribution`, and they accept
*different* sets:

| Strategy | Accepts | Rejects |
| --- | --- | --- |
| `RangeColumn` (continuous numeric range) | `Uniform`, `Normal` (honors `mean`/`stddev`), `LogNormal`, `Zipf`, `Exponential` | `WeightedValues` |
| `TimestampColumn` (continuous time range) | `Uniform`, `Normal` (bare only †), `LogNormal`, `Zipf`, `Exponential` | `WeightedValues`; `Normal` with explicit `mean`/`stddev` |
| `ValuesColumn` (discrete value list) | `Uniform`, `WeightedValues`, `Normal` (bare only †) | `Normal` with explicit `mean`/`stddev` |
| `ForeignKeyRef` (parent-row index range) | `Uniform`, `Normal` (bare only †), `LogNormal`, `Zipf`, `Exponential` | `WeightedValues`; `Normal` with explicit `mean`/`stddev` |

† **bare only**: `Normal()` (auto-centered) is accepted, but `Normal`
carrying explicit `mean`/`stddev` is rejected at plan time. Those are
value-space parameters honored only on numeric `RangeColumn` (see the
[Normal](#normal) section).

> ## ⚠️ Float ranges silently ignore `Zipf` / `Exponential` / `LogNormal`
>
> This is a **wrong-data trap**, not just a limitation: on a
> **float/double** range, `LogNormal`, `Zipf`, and `Exponential` are
> *accepted without any error* but the float path folds them into a
> **uniform** draw. You ask for a skewed column and get uniform data,
> with no warning.
>
> Only **integer** `RangeColumn`s (integer `min`/`max`) shape all five
> non-weighted distributions. On a float range, only `Uniform` and
> `Normal` actually do anything.
>
> **To get a skewed continuous column:** shape an *integer* range and
> scale it in an [expression](column-strategies/expressions.md)
> (e.g. `RangeColumn(min=0, max=10000, distribution=Zipf(...))` then
> `expr="col / 100.0"`), or keep integer/`decimal` bounds. Applies to
> `double(...)` and any `RangeColumn` with float bounds.

The reason `WeightedValues` is rejected above: it assigns probability
mass to *named discrete values*. A continuous range (`RangeColumn`,
`TimestampColumn`)
or an implicit parent-row index range (`ForeignKeyRef`) has no value
list to weight, so passing `WeightedValues` there raises at plan
construction with a message pointing you at `ValuesColumn` (for
weighted categoricals) or `Zipf` / `Normal` / `LogNormal` /
`Exponential` (to skew a range).

## How to pick

| You want… | Use | Key parameter |
| --- | --- | --- |
| Everything equally likely (the default) | `Uniform` | — |
| A bell curve clustered around a center | `Normal` | `mean`, `stddev` (numeric `RangeColumn` only; auto-centers elsewhere) |
| A heavy right tail of positive quantities | `LogNormal` | `mean`, `stddev` |
| A few "hot" values dominating the rest | `Zipf` | `exponent` |
| Memoryless inter-arrival / decay shape | `Exponential` | `rate` |
| Explicit per-value probabilities (discrete) | `WeightedValues` | `weights` |

---

## Uniform

The default for every strategy that accepts a `distribution`. Each
value in the target range — or each item in a value list — is equally
likely. It has no parameters beyond the discriminator.

```python
from dbldatagen.core.spec.schema import RangeColumn, Uniform

RangeColumn(min=0, max=100)                       # Uniform is implicit
RangeColumn(min=0, max=100, distribution=Uniform())  # explicit, same thing
```

```yaml
distribution:
  type: uniform
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | `"uniform"` | `"uniform"` | Discriminator; set automatically. |

---

## Normal

A Gaussian bell curve over the target range, with values clamped to the
range bounds. `mean` and `stddev` are in the range's **value units**
(e.g. for `RangeColumn(min=0, max=100)`, `mean=40` peaks the bell at 40).

Both fields default to `None`, meaning **auto-center**: the bell sits on
the range midpoint with a spread of `span / 6` (so ~99.7% of values land
inside the range). Pass either field to override it; the other stays
auto.

```python
from dbldatagen.core.spec.schema import RangeColumn, Normal

# Ages clustered around 40, spread of 12 years (clamped to [0, 100]).
RangeColumn(min=0, max=100, distribution=Normal(mean=40, stddev=12))

# Auto-centered: bell on the midpoint 50, spread (100-0)/6.
RangeColumn(min=0, max=100, distribution=Normal())
```

```yaml
distribution:
  type: normal
  mean: 40
  stddev: 12
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | `"normal"` | `"normal"` | Discriminator. |
| `mean` | `float \| None` | `None` | Peak in value units. `None` auto-centers on the range midpoint. Validated as finite when set. |
| `stddev` | `float \| None` | `None` | Spread in value units. `None` uses `span / 6`. Validated as `>= 0` when set. |

> **`mean`/`stddev` only apply to numeric `RangeColumn`.** On
> `TimestampColumn` (epoch-second value space), `ForeignKeyRef`
> (parent-row index), and `ValuesColumn` (list position) there's no
> usable float value space, so passing explicit `mean`/`stddev` there is
> **rejected at plan time**. Use a bare `Normal()` on those hosts (it
> still auto-centers).

---

## LogNormal

The natural log of the samples is normally distributed — a heavy
right tail useful for positive quantities like incomes, file sizes, or
page response times. `mean` and `stddev` describe the *underlying*
normal, not the output.

```python
from dbldatagen.core.spec.schema import RangeColumn, LogNormal

# File sizes: most small, a long tail of large ones.
RangeColumn(min=0, max=1_000_000, distribution=LogNormal(mean=0.0, stddev=1.0))
```

```yaml
distribution:
  type: lognormal
  mean: 0.0
  stddev: 1.0
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | `"lognormal"` | `"lognormal"` | Discriminator. |
| `mean` | `float` | `0.0` | Mean of the underlying normal. Must be in `[-100.0, 100.0]` (the engine computes `exp(mean)` on the driver; the cap keeps it finite). |
| `stddev` | `float` | `1.0` | Standard deviation of the underlying normal. Must be `>= 0`. |

---

## Zipf

A power-law distribution — the classic shape for realistic cardinality
skew, where a small fraction of values (popular customers, hot keys,
viral content) account for most of the mass. This is the default
distribution for foreign keys (see
[relationships/foreign-keys.md](relationships/foreign-keys.md)).

```python
from dbldatagen.core.spec.schema import RangeColumn, Zipf

# Heavily skewed toward the low end of the range.
RangeColumn(min=1, max=1000, distribution=Zipf(exponent=1.2))
```

```yaml
distribution:
  type: zipf
  exponent: 1.2
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | `"zipf"` | `"zipf"` | Discriminator. |
| `exponent` | `float` | `1.5` | Power-law exponent. Must be **strictly greater than `1.0`** — the sampler's inverse power-law CDF only converges above `1`. Smaller values (e.g. `1.05`) give heavier tails / more skew; larger values approach `Uniform` from above. |

If you need milder-than-Zipf skew, the validator's error message
points you at `Normal` / `LogNormal`; for no skew, `Uniform`.

---

## Exponential

Models memoryless inter-arrival times — customer sessions, web
requests, time-to-event. Samples are drawn with rate parameter `rate`
and mapped onto the strategy's target range (the right tail is
clipped to preserve the monotonically-decreasing shape).

```python
from dbldatagen.core.spec.schema import RangeColumn, Exponential

RangeColumn(min=0, max=3600, distribution=Exponential(rate=1.0))
```

```yaml
distribution:
  type: exponential
  rate: 1.0
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | `"exponential"` | `"exponential"` | Discriminator. |
| `rate` | `float` | `1.0` | Rate parameter (lambda); its reciprocal is the mean. Must be **strictly positive**. |

---

## WeightedValues

Explicit per-value probabilities for a **discrete value list**. Pair it
with `ValuesColumn` — it is *not* accepted by `RangeColumn`,
`TimestampColumn`, or `ForeignKeyRef`, because those have no value list
to weight.

Weights do not have to sum to `1.0`; the engine normalizes them. Every
value in the `ValuesColumn.values` list must have a matching weight key
(keyed by `str(value)`), and at least one weight must be positive —
both are checked at plan time so a typo doesn't silently fall back to
uniform.

```python
from dbldatagen.core.spec.schema import ValuesColumn, WeightedValues

ValuesColumn(
    values=["pending", "shipped", "delivered", "cancelled"],
    distribution=WeightedValues(
        weights={
            "pending": 0.1,
            "shipped": 0.2,
            "delivered": 0.6,
            "cancelled": 0.1,
        }
    ),
)
```

In DSL form, pass it through `text(...)`:

```python
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import WeightedValues

datagendg.text(
    "status",
    values=["pending", "shipped", "delivered", "cancelled"],
    distribution=WeightedValues(
        weights={"pending": 0.1, "shipped": 0.2, "delivered": 0.6, "cancelled": 0.1},
    ),
)
```

```yaml
gen:
  strategy: values
  values: [pending, shipped, delivered, cancelled]
  distribution:
    type: weighted
    weights:
      pending: 0.1
      shipped: 0.2
      delivered: 0.6
      cancelled: 0.1
```

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | `"weighted"` | `"weighted"` | Discriminator. |
| `weights` | `dict[str, float]` | — | Map of value (as a string key) to non-negative weight. Must be non-empty; every weight finite and `>= 0`; keys must cover every value in the paired `ValuesColumn`. |

This is exactly the `status` column from the canonical
customers/orders plan — 60% `delivered`, 20% `shipped`, and so on.

## See also

- [column-strategies/index.md](column-strategies/index.md) — the
  strategies that carry a `distribution`
- [relationships/foreign-keys.md](relationships/foreign-keys.md) —
  `Zipf` as the default for FK skew
- [getting-started.md](getting-started.md) — the canonical plan that
  uses `Zipf` and `WeightedValues` together
