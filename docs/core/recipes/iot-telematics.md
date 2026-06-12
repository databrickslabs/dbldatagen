# Recipe: an IoT telematics dataset (GPS device tracking)

This recipe rebuilds v0's `basic/telematics` dataset on the
`dbldatagen.core` engine: a fleet of GPS devices emitting time-series
`lat` / `lon` / `heading` readings, with a WKT `POINT(...)` string per
row. v0 packed it into a single flat table; core models it as a proper
two-table dataset — a `devices` dimension and a `telemetry` fact joined
by a foreign key — which is closer to how real telematics data lands.

The shape:

```
devices (parent)              telemetry (child)
  device_id  PK   <-----------  device_id  FK -> devices.device_id
  vehicle_type                  event_id   PK
  base_lat                      ts          (sequential, 1s interval)
  base_lon                      lat / lon / heading / speed
                                wkt         POINT(lon lat)
```

## What this builds

- **`devices`** — one row per physical device: a `device_id` PK, a
  `vehicle_type` drawn from a small category list, and a `base_lat` /
  `base_lon` giving each device a home region.
- **`telemetry`** — the reading stream: a `device_id` FK back into
  `devices`, a `ts` at a fixed one-second interval, a `lat` / `lon`
  position, a `heading` in `0..359`, a `speed`, and a `wkt` column
  holding the `POINT(lon lat)` well-known-text string.

Two things are worth flagging up front, because they shape the spec:

1. **The per-row index isn't exposed.** To build sequential timestamps
   you need a row counter, so the spec carries an explicit
   `SequenceColumn(start=0, step=1)` named `row_idx` and the `ts`
   expression references it. See
   [sequential-timestamps.md](sequential-timestamps.md).
2. **`ExpressionColumn` can only reference *regular* columns** — never
   FK, Faker, or `seed_from` columns (those are filled in a later phase,
   and the planner rejects such references at `resolve_plan` time).
   Declare a referenced column earlier in the table too: expressions
   evaluate in the first projection pass, so a forward reference to a
   not-yet-projected column fails at Spark eval time (the planner checks
   membership and phase, not declaration order). That constraint decides
   how `lat` / `lon` are generated; see
   [Notes / variations](#notes--variations) for the honest tradeoff
   against v0's per-row jitter.

## Schema form

```python
from dbldatagen.core import (
    generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey, ForeignKeyRef, DataType,
)
from dbldatagen.core.spec.schema import (
    SequenceColumn, ValuesColumn, RangeColumn, ForeignKeyColumn,
    ExpressionColumn, Normal,
)

devices = TableSpec(
    name="devices",
    rows=50,
    primary_key=PrimaryKey(columns=["device_id"]),
    columns=[
        ColumnSpec(name="device_id", gen=SequenceColumn(start=1, step=1)),
        ColumnSpec(
            name="vehicle_type",
            dtype=DataType.STRING,
            gen=ValuesColumn(values=["car", "truck", "van", "motorcycle"]),
        ),
        # A home region per device. Continuous range -> DOUBLE.
        ColumnSpec(name="base_lat", dtype=DataType.DOUBLE, gen=RangeColumn(min=37.0, max=38.0)),
        ColumnSpec(name="base_lon", dtype=DataType.DOUBLE, gen=RangeColumn(min=-123.0, max=-122.0)),
    ],
)

telemetry = TableSpec(
    name="telemetry",
    rows=500,
    primary_key=PrimaryKey(columns=["event_id"]),
    columns=[
        ColumnSpec(name="event_id", gen=SequenceColumn(start=1, step=1)),
        # Row counter for the timestamp expression below (see note 1).
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        ColumnSpec(
            name="device_id",
            gen=ForeignKeyColumn(),
            foreign_key=ForeignKeyRef(ref="devices.device_id"),
        ),
        # Sequential timestamps at a 1-second interval. Full HH:mm:ss is
        # required under ANSI mode -- '2024-01-01' alone raises
        # CANNOT_PARSE_TIMESTAMP.
        ColumnSpec(
            name="ts",
            gen=ExpressionColumn(
                expr="cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 1 as timestamp)"
            ),
        ),
        # Regional lat/lon -- plain RangeColumns so the wkt expression
        # below can reference them (see note 2 and the variations section).
        ColumnSpec(name="lat", dtype=DataType.DOUBLE, gen=RangeColumn(min=37.0, max=38.0)),
        ColumnSpec(name="lon", dtype=DataType.DOUBLE, gen=RangeColumn(min=-123.0, max=-122.0)),
        ColumnSpec(name="heading", dtype=DataType.INT, gen=RangeColumn(min=0, max=359)),
        # Speed clustered around the mid-range via Normal (auto-centered).
        ColumnSpec(
            name="speed",
            dtype=DataType.DOUBLE,
            gen=RangeColumn(min=0.0, max=120.0, distribution=Normal()),
        ),
        # WKT POINT string -- references the regular lat/lon columns above.
        ColumnSpec(
            name="wkt",
            dtype=DataType.STRING,
            gen=ExpressionColumn(expr="concat('POINT(', lon, ' ', lat, ')')"),
        ),
    ],
)

plan = DataGenPlan(tables=[devices, telemetry], seed=42)
```

The pieces that wire it together:

- **`PrimaryKey` on `devices`** marks `device_id` as the key the child
  references — a parent that something FKs into must have a primary key.
- **`ForeignKeyColumn()` + `ForeignKeyRef(ref="devices.device_id")`** on
  the child column is the FK pair (the `gen` marks it as an FK, the ref
  says which `"table.column"` it points at). The FK column's type is
  inferred from the parent PK, so you don't set `dtype` on it. See
  [relationships/foreign-keys.md](../relationships/foreign-keys.md).
- **`row_idx` + the `ts` expression** turn a row counter into
  evenly-spaced timestamps. The multiplier on `row_idx` is the spacing
  in seconds (`* 1` = one row per second; `* 60` = per minute).
- **`wkt`** references the `lat` and `lon` columns declared above it.
  Both are plain `RangeColumn`s — the expression rules require regular
  columns (note 2).

## DSL form

The same plan with the `datagendg` helpers, which collapse the PK / FK /
range boilerplate. The two `SequenceColumn`s without a DSL alias
(`row_idx`) and the FK pair are spelled out where the helper doesn't fit:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey
from dbldatagen.core.spec.schema import SequenceColumn, Normal
from dbldatagen.core.spec import dsl as datagendg

devices = TableSpec(
    name="devices",
    rows=50,
    primary_key=PrimaryKey(columns=["device_id"]),
    columns=[
        datagendg.pk_auto("device_id"),
        datagendg.text("vehicle_type", values=["car", "truck", "van", "motorcycle"]),
        datagendg.double("base_lat", 37.0, 38.0),
        datagendg.double("base_lon", -123.0, -122.0),
    ],
)

telemetry = TableSpec(
    name="telemetry",
    rows=500,
    primary_key=PrimaryKey(columns=["event_id"]),
    columns=[
        datagendg.pk_auto("event_id"),
        # No DSL alias for a bare 0-based sequence index -- use SequenceColumn.
        ColumnSpec(name="row_idx", gen=SequenceColumn(start=0, step=1)),
        datagendg.fk("device_id", "devices.device_id"),
        datagendg.expression(
            "ts",
            "cast(unix_timestamp('2024-01-01 00:00:00') + row_idx * 1 as timestamp)",
        ),
        datagendg.double("lat", 37.0, 38.0),
        datagendg.double("lon", -123.0, -122.0),
        datagendg.integer("heading", 0, 359),
        datagendg.double("speed", 0.0, 120.0, distribution=Normal()),
        datagendg.expression("wkt", "concat('POINT(', lon, ' ', lat, ')')"),
    ],
)

plan = DataGenPlan(tables=[devices, telemetry], seed=42)
```

`datagendg.pk_auto` is the sequence PK, `datagendg.fk` builds the
`ForeignKeyColumn` + `ForeignKeyRef` pair, and `datagendg.double` /
`integer` / `text` / `expression` wrap the matching strategies. One
behaviour difference worth knowing: `datagendg.fk` defaults the sampling
distribution to `Zipf(exponent=1.2)` (a few devices emit most of the
readings), whereas the schema form above leaves `distribution` unset,
which falls back to `Uniform`. Pass `distribution=` to `datagendg.fk` to
match the schema form, or vice versa.

## Generate

```python
dfs = generate(spark, plan)
```

`generate` resolves the plan — ordering `devices` before `telemetry` so
the FK can sample from real parent keys — and returns a `dict` keyed by
table name:

```python
dfs["devices"].count()    # 50
dfs["telemetry"].count()  # 500

dfs["telemetry"].select("device_id", "ts", "lat", "lon", "heading", "speed", "wkt").show(3, truncate=False)
```

A sample row looks like:

```
device_id | ts                  | lat               | lon                 | heading | speed   | wkt
2         | 2024-01-01 00:00:00 | 37.93506415408908 | -122.52686029778711 | 304     | 54.043… | POINT(-122.52686029778711 37.93506415408908)
```

The same plan and seed produce byte-identical data on every run; vary
`seed` at construction time for different data. See
[concepts/determinism-and-seeds.md](../concepts/determinism-and-seeds.md).

## Notes / variations

### Per-device home location vs. a WKT column — the honest tradeoff

v0 gave each device a stable `base_lat` / `base_lon` (keyed off
`device_id`), then jittered each reading around that base with a per-row
`rand()`, and finally built the WKT from the jittered values. Core
**cannot reproduce all three at once**, because of the expression rule
in note 2. You pick one of two faithful approximations:

**Option A — regional sampling + WKT (used above).** `lat` / `lon` are
plain `RangeColumn`s sampled across a regional bounding box. They vary
per row (like v0's jitter) and the `wkt` `ExpressionColumn` can reference
them. What you lose: the readings are *not* correlated to a device's
home region — a reading's position is independent of which `device_id`
it carries. This is the version that satisfies the "WKT from lat/lon"
requirement and is what the runnable spec above does.

**Option B — per-device home location via `seed_from`.** If correlating
position to the device matters more than the WKT column, use
`seed_from="device_id"` so every reading from the same device gets the
*same* home `lat` / `lon`:

```python
# In the telemetry table, replace the plain lat/lon with:
datagendg.double("home_lat", 37.0, 38.0, seed_from="device_id"),
datagendg.double("home_lon", -123.0, -122.0, seed_from="device_id"),
```

`seed_from` derives the cell's seed from another column's value, so the
same `device_id` always yields the same `home_lat` — exactly one home
location per device (verifiable: `select("device_id", "home_lat").distinct()`
has one row per device). This is the direct successor to v0's
`baseColumn`. See
[relationships/correlated-columns.md](../relationships/correlated-columns.md).

What you lose with Option B: you **cannot** then build a `wkt`
`ExpressionColumn` from `home_lat` / `home_lon`, because `seed_from`
columns are applied in a later phase than expressions — the planner
rejects `concat('POINT(', home_lon, ' ', home_lat, ')')` at
`resolve_plan` time with a "FK / Faker / seed_from columns applied in a
later phase" error. If you need both a per-device location *and* a WKT
string, build the WKT downstream on the generated `DataFrame`:

```python
from pyspark.sql import functions as F

dfs["telemetry"] = dfs["telemetry"].withColumn(
    "wkt", F.concat(F.lit("POINT("), F.col("home_lon"), F.lit(" "), F.col("home_lat"), F.lit(")"))
)
```

There is no core construct that gives you per-row `rand()` jitter
*around* a per-device base the way v0 did — `seed_from` makes the value
constant per device, not jittered. The two options above are the closest
faithful core approaches; pick based on whether device-correlation or
the inline WKT column matters more for your use case.

### Dropping the `row_idx` scaffold

`row_idx` exists only so the `ts` expression has a counter to reference;
you usually don't want it in the output. Drop it after generation:

```python
dfs["telemetry"] = dfs["telemetry"].drop("row_idx")
```

The reference has to resolve at generation time, so you can't omit the
column from the spec — drop it from the resulting `DataFrame` instead.

### Timestamp spacing and time zone

The multiplier on `row_idx` is the interval in seconds: `* 1` per
second, `* 60` per minute, `* 3600` per hour. `unix_timestamp(...)`
parses its literal in `spark.sql.session.timeZone`, not UTC — pin the
session TZ to UTC (`spark.conf.set("spark.sql.session.timeZone", "UTC")`)
if you need runner-independent absolute timestamps. Full details and the
random-timestamp alternative (`TimestampColumn`) are in
[sequential-timestamps.md](sequential-timestamps.md).

### Skewing the readings per device

`datagendg.fk` defaults to `Zipf`, so a few devices already dominate the
reading stream. To skew `speed` toward the low end (most readings near
idle, a long tail of fast ones), swap `Normal()` for a different
distribution — `LogNormal` or `Exponential` on the `RangeColumn`. See
[column-strategies/numeric-ranges.md](../column-strategies/numeric-ranges.md).

## See also

- [column-strategies/numeric-ranges.md](../column-strategies/numeric-ranges.md)
  — `RangeColumn`, distributions
- [column-strategies/sequences-and-ids.md](../column-strategies/sequences-and-ids.md)
  — `SequenceColumn` for the row counter and PKs
- [column-strategies/categorical-values.md](../column-strategies/categorical-values.md)
  — `ValuesColumn` for `vehicle_type`
- [column-strategies/expressions.md](../column-strategies/expressions.md)
  — `ExpressionColumn` rules (what it can and can't reference)
- [column-strategies/timestamps.md](../column-strategies/timestamps.md)
  — `TimestampColumn` for random (non-sequential) timestamps
- [relationships/foreign-keys.md](../relationships/foreign-keys.md)
  — FK distributions, nullable FKs, `null_fraction`
- [relationships/correlated-columns.md](../relationships/correlated-columns.md)
  — `seed_from` for per-device home locations (Option B)
- [recipes/sequential-timestamps.md](sequential-timestamps.md)
  — evenly-spaced timestamps in full
- [persisting-output.md](../persisting-output.md)
  — writing the generated tables to Delta / Unity Catalog
