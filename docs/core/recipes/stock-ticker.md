# Recipe: a daily stock-ticker (OHLCV) dataset

This recipe rebuilds v0's `basic/stock_ticker` dataset on the
`dbldatagen.core` engine: a daily **open / high / low / close /
volume** time series, one row per `(symbol, trading day)`. It pairs a
small `symbols` dimension with a `prices` fact and derives the OHLC
values so each row is internally consistent (`high >= max(open,
close)`, `low <= min(open, close)`), using `decimal(11,2)` for every
price.

The shape:

```
symbols (dimension)        prices (fact)
  symbol                     row_idx       sequence 0,1,2,...
  sector                     symbol        derived from row_idx
                             post_date     derived from row_idx (daily)
                             open          decimal(11,2)
                             high/low/close  derived from open
                             adj_close     derived from close
                             volume        long, skewed
```

## What this builds

- A **`symbols`** dimension: a stock ticker plus a `sector` label.
  Five symbols here; scale the value list up for more.
- A **`prices`** fact with one row per `(symbol, day)`. Rows are laid
  out on a grid: with `N` symbols, row `i` belongs to symbol
  `i mod N` on day `floor(i / N)`. A `SequenceColumn` emits the row
  index `row_idx`, and `ExpressionColumn`s turn it into a `symbol` and
  a daily `post_date`.
- OHLC prices: an `open` base from a `RangeColumn` (a `Normal`
  distribution centered on a typical share price), then `high` / `low`
  / `close` **derived from `open`** with `greatest` / `least` so they
  stay internally consistent. `adj_close` is `close` minus a small
  dividend. `volume` is a skewed integer range.

### Honest differences from v0

v0 walks each price with a `sin()`-based pseudo-random series seeded
per symbol (a `start_value` + `growth_rate` + `volatility` model). This
recipe **simplifies that walk** into a clear, single-row derivation:
each day's `open` is drawn independently and `high/low/close` are
derived from it. The result is a consistent OHLCV row per day, but it
is *not* a correlated random walk — consecutive days are independent
draws, not a trending series. If you need the trending walk, keep v0's
expression model (it ports to an `ExpressionColumn`; see *Notes /
variations*). Everything else — the daily-per-symbol grid, the
`decimal(11,2)` prices, the `100000..5000000` volume — mirrors v0.

## Build the plan

`N` (the symbol count) appears in several SQL expressions, so define it
once in Python and format it into the expression strings. Keep the
symbol list and `N` in sync.

### Schema form

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec
from dbldatagen.core.spec.schema import (
    ValuesColumn, SequenceColumn, RangeColumn, ExpressionColumn,
    DataType, Normal, Exponential,
)

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
N = len(SYMBOLS)
DAYS = 60
SYMBOLS_SQL = ", ".join(f"'{s}'" for s in SYMBOLS)   # -> 'AAPL', 'MSFT', ...
START_DATE = "2024-01-01"

symbols = TableSpec(
    name="symbols",
    rows=N,
    columns=[
        # One row per symbol: row_idx picks the symbol out of the list.
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        ColumnSpec(
            name="symbol",
            gen=ExpressionColumn(expr=f"element_at(array({SYMBOLS_SQL}), cast(row_idx as int) + 1)"),
        ),
        ColumnSpec(
            name="sector",
            gen=ValuesColumn(values=["Technology", "Consumer", "Semiconductors"]),
        ),
    ],
)

prices = TableSpec(
    name="prices",
    rows=N * DAYS,                       # one row per (symbol, day)
    columns=[
        # Row index 0,1,2,... drives both the symbol and the date.
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        # symbol = SYMBOLS[row_idx mod N]  (element_at is 1-based, index must be INT)
        ColumnSpec(
            name="symbol",
            gen=ExpressionColumn(
                expr=f"element_at(array({SYMBOLS_SQL}), cast(pmod(row_idx, {N}) as int) + 1)"
            ),
        ),
        # post_date = START_DATE + floor(row_idx / N) days (cast in SQL)
        ColumnSpec(
            name="post_date",
            gen=ExpressionColumn(
                expr=f"date_add(DATE'{START_DATE}', cast(floor(row_idx / {N}) as int))"
            ),
        ),
        # open: a per-day base price, bell-centered on ~$150.  RangeColumn
        # is the one price column that honors precision/scale (see note).
        ColumnSpec(
            name="open",
            dtype=DataType.DECIMAL, precision=11, scale=2,
            gen=RangeColumn(min=10, max=400, distribution=Normal(mean=150, stddev=40)),
        ),
        # close: open nudged by a small percentage move.  Derived columns
        # cast explicitly to decimal(11,2) -- see the note below.
        ColumnSpec(
            name="close",
            gen=ExpressionColumn(
                expr="cast(open * (1 + (pmod(row_idx, 7) - 3) * 0.01) as decimal(11,2))"
            ),
        ),
        # high/low bracket the open-close band by ~1%.
        ColumnSpec(
            name="high",
            gen=ExpressionColumn(expr="cast(greatest(open, close) * 1.01 as decimal(11,2))"),
        ),
        ColumnSpec(
            name="low",
            gen=ExpressionColumn(
                expr="cast(greatest(least(open, close) * 0.99, 0.0) as decimal(11,2))"
            ),
        ),
        # adj_close = close minus a small dividend on every 5th row, floored at 0.
        ColumnSpec(
            name="adj_close",
            gen=ExpressionColumn(
                expr=(
                    "cast(greatest(close - case when pmod(row_idx, 5) = 0 "
                    "then 0.02 * close else 0.0 end, 0.0) as decimal(11,2))"
                )
            ),
        ),
        # volume: skewed integer range (most days light, a few heavy).
        ColumnSpec(
            name="volume",
            dtype=DataType.LONG,
            gen=RangeColumn(min=100000, max=5000000, distribution=Exponential(rate=1.0)),
        ),
    ],
)

plan = DataGenPlan(tables=[symbols, prices], seed=42)
```

A few things worth calling out:

- **`row_idx` is scaffolding.** Both tables declare a
  `SequenceColumn(start=0, step=1)` because the engine's internal row
  id isn't exposed to expressions — if you want row-index math, you add
  your own index column and reference it. Drop it after generation
  (`dfs["prices"].drop("row_idx")`) if you don't want it in the output.
- **The symbol mapping is a SQL array literal.** `element_at(array(...),
  cast(pmod(row_idx, N) as int) + 1)` is the deterministic, core-API
  equivalent of v0's `symbol_id` math: row `i` always lands on
  `SYMBOLS[i mod N]`. `element_at` is 1-based (hence `+ 1`) and its
  index argument must be an `INT` — `row_idx` is a `bigint`, so the
  `cast(... as int)` is required, not optional.
- **Daily dates come from `floor(row_idx / N)`.** Row `i` is on day
  `floor(i / N)`; `date_add(DATE'2024-01-01', day)` steps the calendar
  one day at a time, exactly like v0's `days_from_start_date`.
- **OHLC consistency is enforced in SQL.** `high` and `low` are derived
  from `greatest` / `least` of `open` and `close`, so `high >=
  max(open, close)` and `low <= min(open, close)` hold by construction.
  `greatest(..., 0.0)` floors prices at zero like v0 does.
- **`ExpressionColumn` reads only regular columns.** Each derived price
  references `open` / `close` / `row_idx`, all plain (non-FK, non-Faker,
  non-`seed_from`) columns — the planner rejects references to those
  later-phase columns at `resolve_plan` time. Declare the inputs *above*
  the expression too: expressions evaluate in the first projection pass,
  so a forward reference fails at Spark eval time (the planner enforces
  membership and phase, not declaration order).

> **Decimal type on expression columns — cast inside the SQL.** The
> engine applies `precision` / `scale` only to `RangeColumn` (and the
> other value-producing strategies), **not** to `ExpressionColumn` —
> its SQL is evaluated by Spark as-is, with no cast. Because the declared
> fields would be a silent no-op, **setting `precision` / `scale` on a
> `ColumnSpec` whose `gen` is an `ExpressionColumn` is rejected at plan
> time.** To pin a derived price to `decimal(11,2)`, **`cast(... as
> decimal(11,2))` inside the expression string** (as every derived column
> above does) and leave `precision` / `scale` off the `ColumnSpec`. For
> `open`, which is a `RangeColumn`, the `precision=11, scale=2` on the
> `ColumnSpec` is honored.

> **A boolean is not a number.** `pmod(row_idx, 5) = 0` is a `BOOLEAN`;
> multiplying a decimal by it raises a type error. Use
> `case when ... then ... else ... end` (as `adj_close` does) when you
> want a conditional numeric term.

> `Normal` and `Exponential` are imported alongside the other
> strategies above. A skewed distribution (`Exponential` / `Zipf`) only
> takes effect on an **integer** range; on a float range it folds back
> to uniform. `volume` is an integer (`min`/`max` are ints), so the
> skew applies. `Normal(mean=..., stddev=...)` with explicit parameters
> is honored only on a numeric `RangeColumn` — which is exactly where
> `open` uses it.

### DSL form

The `datagendg` helpers collapse the common columns. The derived price
columns still use `datagendg.expression` with the `cast(... as
decimal(11,2))` inside the SQL — there's no helper that both writes an
expression and pins a decimal type, so the cast in the string is the
single source of truth for those:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import SequenceColumn, Normal, Exponential, DataType

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
N = len(SYMBOLS)
DAYS = 60
SYMBOLS_SQL = ", ".join(f"'{s}'" for s in SYMBOLS)
START_DATE = "2024-01-01"

symbols = TableSpec(
    name="symbols",
    rows=N,
    columns=[
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        datagendg.expression(
            "symbol",
            f"element_at(array({SYMBOLS_SQL}), cast(row_idx as int) + 1)",
        ),
        datagendg.text("sector", ["Technology", "Consumer", "Semiconductors"]),
    ],
)

prices = TableSpec(
    name="prices",
    rows=N * DAYS,
    columns=[
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        datagendg.expression(
            "symbol",
            f"element_at(array({SYMBOLS_SQL}), cast(pmod(row_idx, {N}) as int) + 1)",
        ),
        datagendg.expression(
            "post_date",
            f"date_add(DATE'{START_DATE}', cast(floor(row_idx / {N}) as int))",
        ),
        # open is a real RangeColumn, so precision/scale are honored here.
        datagendg.decimal(
            "open", 10, 400,
            precision=11, scale=2,
            distribution=Normal(mean=150, stddev=40),
        ),
        # Derived prices: cast to decimal(11,2) inside the SQL.
        datagendg.expression("close", "cast(open * (1 + (pmod(row_idx, 7) - 3) * 0.01) as decimal(11,2))"),
        datagendg.expression("high", "cast(greatest(open, close) * 1.01 as decimal(11,2))"),
        datagendg.expression("low", "cast(greatest(least(open, close) * 0.99, 0.0) as decimal(11,2))"),
        datagendg.expression(
            "adj_close",
            "cast(greatest(close - case when pmod(row_idx, 5) = 0 then 0.02 * close "
            "else 0.0 end, 0.0) as decimal(11,2))",
        ),
        datagendg.integer("volume", 100000, 5000000, distribution=Exponential(rate=1.0)),
    ],
)

plan = DataGenPlan(tables=[symbols, prices], seed=42)
```

`datagendg.decimal` and `datagendg.integer` carry the `distribution=`
through to the underlying `RangeColumn`. `datagendg.expression(name,
expr)` builds the `ExpressionColumn` — it takes no `dtype` (an
expression's type is always inferred), so the `cast(... as
decimal(11,2))` inside each derived-price expression is what fixes the
output type.

## Generate

```python
dfs = generate(spark, plan)

dfs["symbols"].show()
dfs["prices"].orderBy("symbol", "post_date").show(10)
```

`generate` returns a `dict` keyed by table name. Row counts match the
spec — `N` symbols and `N * DAYS` price rows:

```python
assert dfs["symbols"].count() == N
assert dfs["prices"].count() == N * DAYS
```

A quick consistency check on the OHLC invariants and the decimal type:

```python
from pyspark.sql import functions as F

bad = dfs["prices"].filter(
    (F.col("high") < F.greatest("open", "close"))
    | (F.col("low") > F.least("open", "close"))
)
assert bad.count() == 0          # high/low bracket the open-close band

# every price column lands on decimal(11,2)
types = dict(dfs["prices"].dtypes)
for c in ["open", "close", "high", "low", "adj_close"]:
    assert types[c] == "decimal(11,2)", (c, types[c])
```

If you don't want the helper index column in the output, drop it:

```python
prices_df = dfs["prices"].drop("row_idx")
```

## Notes / variations

- **More symbols / more days.** Add tickers to `SYMBOLS` (and matching
  sectors to the `sector` list), then leave the rest alone — `N` and
  the row count derive from the list. `DAYS` sets the per-symbol history
  length; `rows = N * DAYS` keeps the grid rectangular.
- **A correlated random walk (v0's model).** To recover v0's trending
  series, replace the independent `open` draw with an `ExpressionColumn`
  that walks from a per-symbol base — e.g. add `start_value`,
  `growth_rate`, and `volatility` columns (seeded per symbol via
  `seed_from="symbol"`), then express `open` as
  `start_value + growth_rate * start_value * (day / 365) + volatility *
  start_value * sin(row_idx)`. That ports v0's expression almost
  verbatim; this recipe drops it for clarity (see *Honest differences*
  above).
- **Per-symbol reproducible values.** `seed_from="<column>"` on a
  `ColumnSpec` is the core equivalent of v0's `baseColumn`: two rows
  with the same `seed_from` value get the same generated value. Useful
  for a constant `sector` per symbol, or a per-symbol base price.
- **Volume skew.** `Exponential(rate=1.0)` gives a long right tail (most
  days light, occasional heavy days). Swap to `Zipf(exponent=1.5)` for
  power-law skew, or drop the `distribution` for a flat uniform spread.
  Skew only applies because `volume` is an integer range.
- **Joining back to the dimension.** `prices.symbol` is drawn from the
  same `SYMBOLS` list as `symbols.symbol`, so a join on `symbol`
  attaches `sector` to every price row. This recipe derives the symbol
  by construction rather than via a `ForeignKeyColumn` for two reasons:
  an FK picks parents *randomly* (it wouldn't give the deterministic
  one-row-per-symbol-per-day grid), and a PK column cannot itself be an
  `ExpressionColumn` (the planner only accepts `SequenceColumn` /
  `PatternColumn` / `UUIDColumn` as primary keys), so the `symbols`
  dimension here carries no `PrimaryKey`. For a random child→parent
  mapping with a real FK, see the foreign-key recipe linked below.

## See also

- [recipes/multi-table.md](multi-table.md) — two tables joined by a
  real foreign key (random child→parent mapping)
- [recipes/sequential-timestamps.md](sequential-timestamps.md) —
  evenly-spaced timestamps via `SequenceColumn` + `ExpressionColumn`
- [../persisting-output.md](../persisting-output.md) — writing the
  generated tables to Delta / Unity Catalog
- [../api-reference.md](../api-reference.md) — `generate`,
  `generate_table`, `resolve_plan`
```
