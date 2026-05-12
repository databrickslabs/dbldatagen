# Migrating from dbldatagen v0 to core

## Overview

`dbldatagen.core` is a new synthetic data engine that lives alongside the existing v0 API. Both work independently -- you can use v0 and core in the same project without conflicts. Most v0 capabilities have a direct or indirect equivalent in core; this guide maps each one and calls out the handful of true differences (e.g. FK references and struct columns are core-only; Beta/Gamma distributions and `uniqueValues` are v0-only today). CDC support is shipping in a follow-up PR.

## Installation

```bash
# v0 only (existing behavior)
pip install dbldatagen

# v0 + core engine
pip install dbldatagen[core]

# v0 + core + all optional extras
pip install dbldatagen[core-dev]

# Individual core extras
pip install dbldatagen[core-faker]   # Faker-based text generation
```

## Recommended import style

The entry-point classes and functions are PascalCase and imported flat -- they don't shadow anything in the Python ecosystem:

```python
from dbldatagen.core import DataGenPlan, TableSpec, PrimaryKey, ColumnSpec, generate
```

The lowercase DSL factory helpers live in `dbldatagen.core.spec.dsl`. They are **not** re-exported from `dbldatagen.core` -- several of those names (`decimal`, `array`, `struct`) shadow stdlib modules and `faker` collides with the PyPI package, so flat-importing them would poison user code. Import the dsl module under a short alias instead:

```python
from dbldatagen.core.spec import dsl as dg

dg.pk_auto("id")
dg.integer("age", 0, 99)
dg.decimal("price", precision=10, scale=2)
dg.faker("name", provider="name")
```

This mirrors the established PySpark convention (`import pyspark.sql.functions as F` then `F.col(...)`). Every example below uses the `dg.` prefix; direct imports (`from dbldatagen.core.spec.dsl import integer`) continue to work if you prefer them.

## Manual Conversion Reference

### Table Setup

```python
# v0
dg_v0 = (
    DataGenerator(sparkSession=spark, name="orders", rows=10000, randomSeed=42)
    .withIdOutput()
    .withColumn(...)
)
df = dg_v0.build()

# core
from dbldatagen.core import DataGenPlan, PrimaryKey, TableSpec, generate

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="orders",
            rows=10000,
            columns=[...],
            primary_key=PrimaryKey(columns=["id"]),
        )
    ],
)
result = generate(spark, plan)
df = result["orders"]
```

### Column Types

#### Integer/Long Range

```python
# v0
.withColumn("age", IntegerType(), minValue=18, maxValue=90)

# core (schema)
from dbldatagen.core.spec.schema import ColumnSpec, DataType, RangeColumn
ColumnSpec(name="age", dtype=DataType.INT, gen=RangeColumn(min=18, max=90))

# core (DSL shorthand)
from dbldatagen.core.spec import dsl as dg
dg.integer("age", min=18, max=90)
```

#### Float/Double Range

```python
# v0
.withColumn("amount", DoubleType(), minValue=10.0, maxValue=500.0)

# core
from dbldatagen.core.spec import dsl as dg
dg.double("amount", min=10.0, max=500.0)
```

`dg.double()` maps to `DataType.DOUBLE`.  Use `dg.decimal(name,
precision=P, scale=S)` only for fixed-precision financial values (the
earlier MIGRATION doc suggested `decimal` here — wrong, it produced a
different on-disk type).

#### String Values (Discrete)

```python
# v0
.withColumn("status", StringType(), values=["active", "inactive", "pending"])

# core
from dbldatagen.core.spec import dsl as dg
dg.text("status", values=["active", "inactive", "pending"])
```

#### Weighted Values

```python
# v0
.withColumn("tier", StringType(),
            values=["free", "basic", "premium"],
            weights=[70, 20, 10])

# core
from dbldatagen.core.spec.schema import ColumnSpec, DataType, ValuesColumn, WeightedValues
ColumnSpec(
    name="tier",
    dtype=DataType.STRING,
    gen=ValuesColumn(
        values=["free", "basic", "premium"],
        distribution=WeightedValues(weights={"free": 70, "basic": 20, "premium": 10})
    )
)
```

Note: v0 uses a list of weights (position-matched). core uses a dict (name-matched).

#### Timestamp/Date Range

```python
# v0
.withColumn("created_at", TimestampType(),
            begin="2020-01-01 00:00:00", end="2025-12-31 23:59:59")

# core
from dbldatagen.core.spec import dsl as dg
dg.timestamp("created_at", start="2020-01-01", end="2025-12-31")
```

#### Boolean

```python
# v0
.withColumn("is_active", BooleanType())

# core
from dbldatagen.core.spec.schema import ColumnSpec, DataType, ValuesColumn
ColumnSpec(name="is_active", dtype=DataType.BOOLEAN, gen=ValuesColumn(values=[True, False]))
```

#### SQL Expression

```python
# v0
.withColumn("total", DoubleType(), expr="quantity * unit_price")

# core
from dbldatagen.core.spec import dsl as dg
from dbldatagen.core.spec.schema import DataType
dg.expression("total", "quantity * unit_price", dtype=DataType.DOUBLE)
```

#### Template / Pattern

```python
# v0 (character-class placeholders -- e.g. `d` = random decimal digit)
.withColumn("code", StringType(), template=r"ORD-dddd")

# core (named placeholders with explicit length)
from dbldatagen.core.spec import dsl as dg
dg.pattern("code", template="ORD-{digit:4}")
```

v0 placeholders are single literal chars: `d/D` (decimal), `a/A` (alpha),
`x/X` (hex), `k/K` (alphanumeric), `\\w/\\W` (word), `\\n/\\N` (number).
core placeholders are named with explicit length: `{digit:N}`, `{alpha:N}`,
`{hex:N}`, `{seq}`, `{uuid}`.

#### Prefix/Suffix

```python
# v0
.withColumn("sku", StringType(), template=r"SKU-dddddd")

# core
dg.pattern("sku", template="SKU-{digit:6}")
```

### Primary Keys

v0 always materialises an implicit seed column (default name `id`) via
`spark.range`; `withIdOutput()` keeps it in the output. UUID and patterned
keys are built as ordinary columns derived from that seed. core makes the
key explicit and lifts that information to plan level so foreign-key
generation can target it.

```python
# v0 -- sequential integer id
DataGenerator(sparkSession=spark, name="orders", rows=1000).withIdOutput()

# v0 -- UUID id (computed from the seed column)
(
    DataGenerator(sparkSession=spark, name="orders", rows=1000)
    .withColumn("id", StringType(), expr="uuid()", omit=False)
)

# v0 -- patterned id (template applied to the seed column)
(
    DataGenerator(sparkSession=spark, name="orders", rows=1000)
    .withColumn("id", StringType(), template=r"ORD-dddddd")
)

# core -- explicit primary key, declared once on the column
from dbldatagen.core.spec import dsl as dg

dg.pk_auto("id")                          # Sequential integer (1, 2, 3, ...)
dg.pk_uuid("id")                          # Deterministic UUID
dg.pk_pattern("id", "ORD-{digit:6}")      # Patterned string
```

### Nullable Columns

```python
# v0
.withColumn("email", StringType(), percentNulls=0.2)

# core
ColumnSpec(name="email", dtype=DataType.STRING, gen=..., nullable=True, null_fraction=0.2)
```

### Correlated Columns (baseColumn -> seed_from)

```python
# v0 -- same device_id always produces same country
.withColumn("device_id", IntegerType(), minValue=1, maxValue=50)
.withColumn("country", StringType(),
            baseColumn="device_id",
            values=["US", "DE", "JP", "BR"])

# core -- seed_from is the direct equivalent
from dbldatagen.core.spec import dsl as dg

dg.integer("device_id", min=1, max=50)
dg.text("country", values=["US", "DE", "JP", "BR"], seed_from="device_id")
```

### Multiple Identical Columns (numColumns)

```python
# v0 -- generates feature_0, feature_1, feature_2
.withColumn("feature", IntegerType(), minValue=0, maxValue=100, numColumns=3)

# core -- define each column explicitly
from dbldatagen.core.spec import dsl as dg

dg.integer("feature_0", min=0, max=100)
dg.integer("feature_1", min=0, max=100)
dg.integer("feature_2", min=0, max=100)
```

Expand `numColumns` manually when migrating. This is a deliberate design
choice in core (every column spec is explicit, named, and individually
validated), not a side-effect of template validation. If a generator
needs many similar columns, build the list with a Python comprehension
and splat it into `TableSpec.columns`.

### Unique Values

```python
# v0 -- constrain to exactly 50 distinct values
.withColumn("category", IntegerType(), minValue=1, maxValue=1000, uniqueValues=50)

# core -- no direct equivalent today; the closest workaround is to adjust
# the range to match the desired cardinality:
#   max = min + (N-1) * step
dg.integer("category", min=1, max=50)
```

`uniqueValues=N` with a wider range is a known follow-up for core; the
range-narrowing workaround above keeps the resulting cardinality but
loses the original `[min, max]` envelope.

### Distributions

```python
# v0
from dbldatagen.distributions import Normal
.withColumn("score", DoubleType(), minValue=0, maxValue=100, distribution=Normal(50, 10))

# core
from dbldatagen.core.spec.schema import Normal, RangeColumn, ColumnSpec, DataType
ColumnSpec(
    name="score",
    dtype=DataType.DOUBLE,
    gen=RangeColumn(min=0, max=100, distribution=Normal(mean=50, stddev=10))
)
```

Available in both: `Normal`, `Exponential`
v0 only: `Beta`, `Gamma` (tracked as follow-ups for core)
core only: `LogNormal`, `Zipf`, `WeightedValues`

## Core-Only Features

These have no v0 equivalent today.

### Foreign Keys (cross-table references)

v0's `baseColumn` only correlates two columns inside the same table.
core lifts this to plan level so a child column can draw from a parent
table's primary key, with topology validated up front:

```python
from dbldatagen.core import DataGenPlan, PrimaryKey, TableSpec
from dbldatagen.core.spec import dsl as dg

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=100,
            columns=[dg.pk_auto("id"), dg.text("name", values=["Alice", "Bob", "Carol"])],
            primary_key=PrimaryKey(columns=["id"]),
        ),
        TableSpec(
            name="orders",
            rows=1000,
            columns=[dg.pk_auto("id"), dg.fk("customer_id", ref="customers.id"), dg.decimal("amount", min=10, max=500)],
            primary_key=PrimaryKey(columns=["id"]),
        ),
    ]
)
```

### Nested Struct Columns

v0 can pass through a `StructType` defined elsewhere but does not
declare struct fields in the column spec itself. core defines the
nested fields inline:

```python
from dbldatagen.core.spec import dsl as dg

dg.struct("address", [
    dg.text("city", ["Austin", "NYC", "LA"]),
    dg.text("state", ["TX", "NY", "CA"]),
    dg.integer("zip", min=10000, max=99999),
])
```

### CDC (Change Data Capture)

CDC support is shipping in a follow-up PR.

## Different Surface for the Same Capability

v0 supports each of these — core just exposes them through a different
API. Listed here for orientation when porting a v0 generator.

### Array Columns

```python
# v0 -- combine N columns into an array via numColumns + structType
.withColumn("tags", StringType(),
            values=["sale", "new", "popular"],
            numColumns=4,
            structType="array")

# core -- declare the element generator and length bounds directly
from dbldatagen.core.spec import dsl as dg
from dbldatagen.core.spec.schema import ValuesColumn

dg.array("tags",
         ValuesColumn(values=["sale", "new", "popular"]),
         min_length=1, max_length=4)
```

### Faker / Custom Text Providers

```python
# v0 -- FakerText wraps a faker callable; PyfuncText wraps any callable
from dbldatagen.text_generator_plugins import FakerText
.withColumn("email", StringType(),
            text=FakerText(lambda fake: fake.email(), rootProperty="faker"))

# core -- name the provider directly on a faker() column
from dbldatagen.core.spec import dsl as dg

dg.faker("email", provider="email")
dg.faker("full_name", provider="name")
```

Requires: `pip install dbldatagen[core-faker]`

## v0-Only Features (Today)

These v0 features do not yet have a core equivalent. They are tracked
as follow-ups for core; the recommended interim workarounds are:

| v0 Feature | Workaround in core |
|-----------|----------------|
| `uniqueValues=N` (constrain cardinality below the natural range) | Narrow the range so cardinality matches: `max = min + (N-1) * step` |
| `format` (printf-style `%05d`) | `dg.pattern("col", template="{digit:5}")` or `dg.expression("col", "format_string(...)")` |
| `text=ILText(...)` (Lorem Ipsum) | `dg.faker("col", provider="paragraph")` |
| `withConstraint(SqlExpr(...))` (post-hoc filtering) | Design generation so the rule holds by construction; core does not filter rows after generation |
| `Beta` / `Gamma` distributions | Approximate with `Normal` or `LogNormal` for now |
