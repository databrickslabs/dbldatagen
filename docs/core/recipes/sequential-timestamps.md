# Recipe: evenly-spaced (sequential) timestamps

`TimestampColumn` produces *random* timestamps inside a range — great
for things like signup dates, but wrong when you need timestamps at a
fixed interval (one row per second, per hour, per day). For
evenly-spaced timestamps, compose two columns instead:

1. a **`SequenceColumn`** that emits the row index `0, 1, 2, …`, and
2. an **`ExpressionColumn`** that turns that index into a timestamp:
   take a base instant as Unix epoch seconds, add `row_idx * step`, and
   cast the result back to a timestamp.

## The recipe

```python
from dbldatagen.core import generate_table, TableSpec, ColumnSpec
from dbldatagen.core.spec.schema import SequenceColumn, ExpressionColumn

spec = TableSpec(
    name="hourly_ts",
    rows=72,                       # 3 days * 24 hours
    seed=42,
    columns=[
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        ColumnSpec(
            name="ts",
            gen=ExpressionColumn(
                expr="cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 3600 as timestamp)"
            ),
        ),
    ],
)

df = generate_table(spark, spec)
```

This emits one row per hour starting at `2024-01-01 00:00:00`:
`row_idx` runs `0..71`, and `ts` lands on `00:00`, `01:00`, `02:00`, …

The same two columns work as part of a `DataGenPlan` / `generate`; the
standalone `generate_table` form is shown here because the timestamp
columns don't depend on any other table. (When used standalone,
`TableSpec.seed` must be set — `seed=42` above; inside a plan it's
propagated from `plan.seed`.)

### DSL form

The DSL helpers cover both columns:

```python
from dbldatagen.core import generate_table, TableSpec, ColumnSpec
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import SequenceColumn

spec = TableSpec(
    name="hourly_ts",
    rows=72,
    seed=42,
    columns=[
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        datagendg.expression(
            "ts",
            "cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 3600 as timestamp)",
        ),
    ],
)
```

There's no dedicated DSL helper for a bare sequence index, so use
`SequenceColumn(start=0, step=1)` directly (or `datagendg.pk_auto` if
you want a `1`-based key). `datagendg.expression(name, expr)` builds the
`ExpressionColumn`.

## Choosing the spacing

The spacing is just the multiplier on `row_idx`, in **seconds**:

| Interval | Multiplier |
| --- | --- |
| one row per second | `row_idx * 1` |
| one row per hour | `row_idx * 3600` |
| one row per day | `row_idx * 86400` |

So `row_idx * 86400` gives one row per day; `row_idx * 900` gives one
row per 15 minutes. Change `start` / `step` on the `SequenceColumn` if
you want a different index origin or stride.

## Two things that will bite you

### 1. Use a full `HH:mm:ss` literal

Write the base instant as `'2024-01-01 00:00:00'`, **not**
`'2024-01-01'`. Under ANSI mode, Spark's default `unix_timestamp`
format pattern requires the time component — `unix_timestamp('2024-01-01')`
alone raises `CANNOT_PARSE_TIMESTAMP`. Always include the full
`HH:mm:ss`.

### 2. `unix_timestamp` parses in the session time zone

`unix_timestamp(...)` parses its literal in
`spark.sql.session.timeZone`, **not** UTC. That means the absolute
epoch — and therefore the actual instants in the `ts` column — depend
on the cluster's configured session TZ.

If you need UTC-absolute timestamps regardless of where the job runs,
pin the session TZ to UTC around generation:

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
df = generate_table(spark, spec)
```

If your timestamps are meant to be *local* to the cluster's TZ, leave
the session TZ alone — but be aware the same spec then produces
different absolute instants on differently-configured clusters.

## Dropping the helper index column

`row_idx` is scaffolding — the `ExpressionColumn` references it, but you
usually don't want it in the final dataset. Drop it after generation:

```python
df = df.drop("row_idx")
```

The reference has to resolve at generation time, so you can't omit the
column from the spec; drop it from the resulting `DataFrame` instead.

## When to use random timestamps instead

If you want timestamps scattered *randomly* across a range rather than
evenly spaced — signup dates, event times that shouldn't form a perfect
grid — use `TimestampColumn`, which draws each value from `[start, end]`
under a distribution:

```python
from dbldatagen.core.spec.schema import TimestampColumn

ColumnSpec(name="signup_date", gen=TimestampColumn(start="2022-01-01", end="2024-12-31"))
```

`TimestampColumn` output is session-TZ-independent (it works in UTC
epoch seconds), so it doesn't have the time-zone caveat above. See
[column-strategies/timestamps.md](../column-strategies/timestamps.md)
for the random-timestamp strategy in full.

## Next steps

- [column-strategies/timestamps.md](../column-strategies/timestamps.md)
  — `TimestampColumn` (random timestamps)
- [column-strategies/expressions.md](../column-strategies/expressions.md)
  — `ExpressionColumn` rules (what it can reference)
- [api-reference.md](../api-reference.md) — `generate_table`
