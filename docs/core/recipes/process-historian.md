# Recipe: a process-historian sensor time-series

This recipe recreates v0's `basic/process_historian` dataset with the
`dbldatagen.core` engine: a manufacturing process-historian feed where
each row is one tag reading from one device. The interesting part is
the **per-device correlation** — a given device always reports from the
same plant under the same tag name, even though those facts repeat
across thousands of reading rows. In v0 that was `baseColumn`; in core
it is `seed_from`.

The shape of one table:

```
readings
  device_id          hex-ish device identifier (string)
  plant_id           plant the device lives in   (stable per device_id)
  tag_name           sensor tag the device reports (stable per device_id)
  ts                 reading timestamp (one row per second)
  value              sensor reading (e.g. temperature, ~150 Deg.F)
  engineering_units  unit-of-measure label ("Deg.F"), same on every row
```

## What this builds

A single `readings` table of sensor samples. Five ideas map straight
across from the v0 provider:

- **`device_id`** — v0 generated a hashed `internal_device_id` long and
  formatted it as `0x%09x`. Core builds the hex-ish string directly with
  a `PatternColumn` template (`DEV-{hex:9}`). The `{hex:N}` placeholder
  is capped at width 15 (`MAX_HEX_WIDTH`), so 9 is well within range.
- **`plant_id` / `tag_name`** — v0 picked these from a list with
  `baseColumn="internal_device_id"`, so every row for a device landed on
  the same plant and tag. Core does this with **`seed_from="device_id"`**:
  the column's per-cell seed is derived from `device_id`'s value instead
  of the row position, so two rows sharing a `device_id` always pick the
  same `plant_id` (and the same `tag_name`). This is the direct
  `baseColumn` equivalent — see [the determinism note below](#how-seed_from-reproduces-basecolumn).
- **`ts`** — v0 used `interval="1 second"` over a `[begin, end]` window.
  Core has no fixed-interval timestamp strategy, so we compose the
  standard sequential-timestamp recipe: a `SequenceColumn` row index plus
  an `ExpressionColumn` that turns it into a timestamp.
- **`value`** — a float sensor reading. v0 used a flat
  `[50.0, 60.0]` range; here we make it a touch more realistic with a
  bell curve around a setpoint using `RangeColumn` + `Normal(mean=...,
  stddev=...)`. On a numeric `RangeColumn`, `Normal`'s `mean` / `stddev`
  are in the column's own value units.
- **`engineering_units`** — a constant unit label, exactly v0's
  `expr="'Deg.F'"`. Core uses `ConstantColumn(value="Deg.F")`.

## Build the plan

### Schema form

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec
from dbldatagen.core.spec.schema import (
    PatternColumn, ValuesColumn, SequenceColumn, ExpressionColumn,
    RangeColumn, ConstantColumn, Normal, DataType,
)

PLANTS = ["PLANT-00", "PLANT-01", "PLANT-02", "PLANT-03"]
TAGS = ["INLET_TMP", "OUTLET_TMP", "BEARING_TMP", "MOTOR_TMP"]

readings = TableSpec(
    name="readings",
    rows=5000,
    seed=42,
    columns=[
        # Hex-ish device id, e.g. "DEV-3f9a1c0b2".  {hex:9} is within the
        # width-15 cap; the same template fills random hex per row.
        ColumnSpec(
            name="device_id",
            dtype=DataType.STRING,
            gen=PatternColumn(template="DEV-{hex:9}"),
        ),
        # plant_id and tag_name are STABLE PER DEVICE: seed_from="device_id"
        # derives the pick from the device value, so every reading for a
        # given device_id resolves to the same plant and the same tag.
        # This is v0's baseColumn="internal_device_id".
        ColumnSpec(
            name="plant_id",
            dtype=DataType.STRING,
            gen=ValuesColumn(values=PLANTS),
            seed_from="device_id",
        ),
        ColumnSpec(
            name="tag_name",
            dtype=DataType.STRING,
            gen=ValuesColumn(values=TAGS),
            seed_from="device_id",
        ),
        # Sequential one-row-per-second timestamps: a row index plus an
        # expression that casts (epoch + row_idx * step_seconds) back to ts.
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        ColumnSpec(
            name="ts",
            dtype=DataType.TIMESTAMP,
            gen=ExpressionColumn(
                expr="cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 1 as timestamp)"
            ),
        ),
        # Sensor reading: a temperature that bells around a 150 Deg.F
        # setpoint.  On a numeric RangeColumn, Normal's mean/stddev are in
        # value units (degrees here), and samples are clamped to [min, max].
        ColumnSpec(
            name="value",
            dtype=DataType.FLOAT,
            gen=RangeColumn(min=120.0, max=180.0, distribution=Normal(mean=150.0, stddev=10.0)),
        ),
        # Constant unit-of-measure label on every row (v0's expr="'Deg.F'").
        ColumnSpec(
            name="engineering_units",
            gen=ConstantColumn(value="Deg.F"),
        ),
    ],
)

plan = DataGenPlan(tables=[readings], seed=42)
```

A few rules worth calling out, all enforced at plan time:

- **`seed_from` must name an existing column on the same table.** Here
  both correlated columns point at `device_id`, a plain `PatternColumn`.
  `seed_from` cannot chain (you can't `seed_from` a column that itself
  has `seed_from`), and the referenced column must exist — both are
  validated by `resolve_plan` before Spark runs. (Declaration order
  doesn't matter for `seed_from`: those columns resolve in a later phase
  by name.)
- **`row_idx` is scaffolding.** The `ExpressionColumn` for `ts` has to
  reference a real column, declared earlier (expressions evaluate in the
  first projection pass), so `row_idx` must be in the spec. Drop it from
  the resulting DataFrame after generation (shown below).
- **`Normal(mean=150.0, stddev=10.0)` is only meaningful here because
  the host is a numeric `RangeColumn`.** The same parametrized `Normal`
  on a `ValuesColumn` / `TimestampColumn` / FK is rejected at plan time —
  those have no float value space, so they take a bare `Normal()`.

### DSL form

The `datagendg` helpers collapse most of the boilerplate. `text(...)`
takes `seed_from` directly (it is literally documented as v0's
`baseColumn`), and `pattern` / `expression` / `constant` / `double`
cover the rest. There is no DSL helper for a bare sequence index, so
use `SequenceColumn` directly:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import SequenceColumn, RangeColumn, Normal, DataType

PLANTS = ["PLANT-00", "PLANT-01", "PLANT-02", "PLANT-03"]
TAGS = ["INLET_TMP", "OUTLET_TMP", "BEARING_TMP", "MOTOR_TMP"]

readings = TableSpec(
    name="readings",
    rows=5000,
    seed=42,
    columns=[
        datagendg.pattern("device_id", "DEV-{hex:9}"),
        datagendg.text("plant_id", values=PLANTS, seed_from="device_id"),
        datagendg.text("tag_name", values=TAGS, seed_from="device_id"),
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        datagendg.expression(
            "ts",
            "cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 1 as timestamp)",
            dtype=DataType.TIMESTAMP,
        ),
        # double() is the float migration target; pass the Normal via **kw.
        datagendg.double(
            "value", 120.0, 180.0,
            distribution=Normal(mean=150.0, stddev=10.0),
        ),
        datagendg.constant("engineering_units", "Deg.F"),
    ],
)

plan = DataGenPlan(tables=[readings], seed=42)
```

One honest difference from the schema form: `datagendg.double` builds a
`DataType.DOUBLE` column, whereas v0 (and the schema form above) used a
32-bit `float`. Core has no `float` DSL shorthand; for an exact
`FLOAT`-typed column use the schema form's
`ColumnSpec(..., dtype=DataType.FLOAT, gen=RangeColumn(...))`. The
generated *values* are the same shape either way.

## Generate

```python
dfs = generate(spark, plan)
readings_df = dfs["readings"].drop("row_idx")   # row_idx was scaffolding

readings_df.show(10, truncate=False)
print(readings_df.count())                       # 5000
```

`generate` returns a `dict` keyed by table name. The standalone
`generate_table(spark, readings)` works too (the table has no FK to any
other table); either way `TableSpec.seed` must be set when generating
outside a multi-table plan — `seed=42` above.

### Confirm the per-device correlation

The whole point of `seed_from` is that each device maps to exactly one
plant and one tag. Verify it the same way the engine's own test does —
count distinct `(device_id, plant_id)` pairs against distinct devices:

```python
n_devices = readings_df.select("device_id").distinct().count()
n_device_plant = readings_df.select("device_id", "plant_id").distinct().count()
n_device_tag = readings_df.select("device_id", "tag_name").distinct().count()

assert n_device_plant == n_devices   # one plant per device
assert n_device_tag == n_devices     # one tag per device
```

If `plant_id` varied per row (i.e. you forgot `seed_from`), the distinct
pair count would balloon past the device count — that assertion is the
regression guard.

To actually *see* multiple readings per device (so the correlation is
visible, not just trivially true), narrow the device space below the row
count — e.g. `PatternColumn(template="DEV-{digit:3}")` gives at most 1000
distinct devices across 5000 rows, so each device recurs and reliably
keeps the same plant and tag on every appearance.

## Notes / variations

### How `seed_from` reproduces `baseColumn`

In v0, `baseColumn="internal_device_id"` meant "derive this column's
value from the device id, not the row," so a device's plant and tag were
fixed. Core's `seed_from="device_id"` does exactly that: the per-cell
seed comes from the source column's *value*, so identical source values
always yield identical generated values, regardless of row position. The
correlation is deterministic and survives re-runs (same plan + seed →
byte-identical output). Two columns that both `seed_from` the same source
still differ from each other (each column folds its own name into the
seed), so `plant_id` and `tag_name` aren't locked to the same list
index.

### Random vs. sequential timestamps

This recipe emits one reading per second from a fixed start, matching
v0's `interval="1 second"`. If instead you want timestamps scattered
*randomly* across a window (v0's `random=True`), drop `row_idx` / the
`ts` expression and use a `TimestampColumn` instead:

```python
from dbldatagen.core.spec.schema import TimestampColumn

ColumnSpec(
    name="ts",
    dtype=DataType.TIMESTAMP,
    gen=TimestampColumn(start="2024-01-01 00:00:00", end="2024-02-01 00:00:00"),
)
```

`TimestampColumn` is session-timezone-independent; the sequential
expression form parses its literal in the session time zone (see the
[sequential-timestamps recipe](sequential-timestamps.md) for the full
caveat and how to pin UTC).

### Two-table variation: a devices dimension + a readings fact

v0 squeezed the device, its plant, and its tag into the reading row via
`baseColumn`. If you'd rather model the devices as their own dimension
table and have readings reference it by foreign key, split into two
tables. The device's plant/tag now live on the dimension row (one row
per device), and `readings` carries only the FK plus the time-series
columns:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import SequenceColumn, RangeColumn, Normal, DataType

PLANTS = ["PLANT-00", "PLANT-01", "PLANT-02", "PLANT-03"]
TAGS = ["INLET_TMP", "OUTLET_TMP", "BEARING_TMP", "MOTOR_TMP"]

devices = TableSpec(
    name="devices",
    rows=200,
    primary_key=PrimaryKey(columns=["device_id"]),
    columns=[
        datagendg.pk_pattern("device_id", "DEV-{hex:9}"),
        datagendg.text("plant_id", values=PLANTS),
        datagendg.text("tag_name", values=TAGS),
    ],
)

readings = TableSpec(
    name="readings",
    rows=5000,
    columns=[
        datagendg.pk_auto("reading_id"),
        datagendg.fk("device_id", "devices.device_id"),
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        datagendg.expression(
            "ts",
            "cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 1 as timestamp)",
            dtype=DataType.TIMESTAMP,
        ),
        ColumnSpec(
            name="value",
            dtype=DataType.FLOAT,
            gen=RangeColumn(min=120.0, max=180.0, distribution=Normal(mean=150.0, stddev=10.0)),
        ),
        datagendg.constant("engineering_units", "Deg.F"),
    ],
    primary_key=PrimaryKey(columns=["reading_id"]),
)

plan = DataGenPlan(tables=[devices, readings], seed=42)
dfs = generate(spark, plan)
```

Trade-off: here `plant_id` / `tag_name` live on `devices` and you join
to recover them, instead of carrying them denormalized on every reading.
The FK version also gives you a `Zipf` skew by default (a few devices
emit most of the readings) — see
[relationships/foreign-keys.md](../relationships/foreign-keys.md). The
single-table `seed_from` version above is the closer match to v0's
original denormalized shape.

## See also

- [relationships/correlated-columns.md](../relationships/correlated-columns.md)
  — `seed_from` in depth (the `baseColumn` equivalent)
- [column-strategies/text-and-patterns.md](../column-strategies/text-and-patterns.md)
  — `PatternColumn` placeholders and width caps (`{hex:N}`, `{digit:N}`, ...)
- [column-strategies/numeric-ranges.md](../column-strategies/numeric-ranges.md)
  — `RangeColumn` with `Normal` and other distributions
- [column-strategies/constants.md](../column-strategies/constants.md)
  — `ConstantColumn` for the unit-of-measure label
- [recipes/sequential-timestamps.md](sequential-timestamps.md)
  — the one-row-per-interval timestamp recipe and its time-zone caveat
- [persisting-output.md](../persisting-output.md)
  — writing the generated tables to Delta / files
