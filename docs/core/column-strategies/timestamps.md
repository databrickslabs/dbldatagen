# Timestamps

`TimestampColumn` **samples** timestamps from a `[start, end]` range. It
is not a sequential or monotonic clock — each row gets an independent
draw from the range, optionally skewed by a distribution.

## When to use

- A timestamp column where each row is an independent event time within
  a window — like `signup_date` in the running `customers` table, drawn
  from `2022-01-01` to `2024-12-31`.
- When you want the distribution of times to be non-uniform (more recent
  signups, say), pass a `distribution`.

If you need **sequential or evenly-spaced** timestamps (a row every
hour, a monotonic event stream), `TimestampColumn` is the wrong tool —
it samples, it doesn't step. Compose a `SequenceColumn` (the step index)
with an `ExpressionColumn` (turn the index into a timestamp) instead.
See [../recipes/sequential-timestamps.md](../recipes/sequential-timestamps.md).

## Schema form

```python
from dbldatagen.core import DataGenPlan, TableSpec, ColumnSpec
from dbldatagen.core.spec.schema import TimestampColumn

ColumnSpec(
    name="signup_date",
    gen=TimestampColumn(start="2022-01-01", end="2024-12-31"),
)
```

To skew the sampling, set `distribution` (the default is `Uniform`).
For example, `Normal` clusters signups in the middle of the date range
(it centers the bell on the range midpoint automatically — see the note
below):

```python
from dbldatagen.core.spec.schema import TimestampColumn, Normal

ColumnSpec(
    name="signup_date",
    gen=TimestampColumn(
        start="2022-01-01",
        end="2024-12-31",
        distribution=Normal(),   # auto-centers on the middle of the range
    ),
)
```

> **Use a bare `Normal()` on timestamps.** `Normal`'s `mean` / `stddev`
> are value-space parameters honored only on numeric `RangeColumn`; a
> time range has no usable float value space, so passing explicit
> `mean` / `stddev` on a `TimestampColumn` is **rejected at plan time**.
> `Normal()` auto-centers the bell on the middle of `[start, end]`. See
> [distributions.md](../distributions.md#normal).

## DSL form

```python
from dbldatagen.core.spec import dsl as datagendg

datagendg.timestamp("signup_date", "2022-01-01", "2024-12-31")
```

`timestamp(name, start, end)` builds a `ColumnSpec` with
`DataType.TIMESTAMP` and a `TimestampColumn`. Extra `TimestampColumn`
fields (notably `distribution`) pass straight through as keyword args:

```python
from dbldatagen.core.spec.schema import Normal

datagendg.timestamp(
    "signup_date", "2022-01-01", "2024-12-31",
    distribution=Normal(),   # auto-centers; explicit mean/stddev rejected here
)
```

## Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["timestamp"]` | `"timestamp"` | Discriminator; set automatically. |
| `start` | `str` | — (required) | Inclusive lower bound as an ISO-8601 string: `"YYYY-MM-DD"` or `"YYYY-MM-DD HH:MM:SS"`. No default — you must specify the window. |
| `end` | `str` | — (required) | Inclusive upper bound, same format as `start`; must be `>= start`. |
| `distribution` | `Distribution` | `Uniform()` | Sampling distribution over the range. `WeightedValues` is **not** accepted. |

## Validation & gotchas

- **`start` and `end` are required.** There's no universal default for a
  time range, so both bounds must be given.
- **Both must parse as ISO timestamps.** Accepted forms are
  `"YYYY-MM-DD"` and `"YYYY-MM-DD HH:MM:SS"` (anything
  `datetime.fromisoformat` accepts, including a `T` separator and an
  explicit offset). An unparseable string raises a `ValueError` naming
  the field and the expected format.
- **`start` must be `<= end`.** A reversed range raises
  `ValueError("start (...) must be <= end (...)")`.
- **Timezone-awareness must match on both bounds.** Either both naive
  (e.g. `"2024-01-01T00:00:00"`) or both aware with an explicit offset
  (e.g. `"2024-01-01T00:00:00+00:00"`). Mixing a naive bound with an
  aware one raises a `ValueError` — don't mix regimes.
- **`WeightedValues` is rejected.** It weights a discrete value list,
  but a timestamp range is continuous. Use `Uniform` / `Normal` /
  `LogNormal` / `Zipf` / `Exponential` to skew the distribution instead;
  the error message says exactly this.
- **Output is session-timezone-independent.** The engine routes
  `start`/`end` through UTC epoch before sampling, so the same plan run
  on clusters in different timezones produces byte-identical timestamps.

## See also

- [../recipes/sequential-timestamps.md](../recipes/sequential-timestamps.md)
  — evenly-spaced timestamps via `SequenceColumn` + `ExpressionColumn`
- [sequences-and-ids.md](sequences-and-ids.md) — the `SequenceColumn`
  building block
- [expressions.md](expressions.md) — `ExpressionColumn`, used to turn an
  index into a timestamp
- [../distributions.md](../distributions.md) — every distribution the
  `distribution` field accepts
- [index.md](index.md) — all strategies at a glance
