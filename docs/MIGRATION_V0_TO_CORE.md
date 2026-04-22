# Migrating from dbldatagen v0 to core

## Overview

`dbldatagen.core` is a next-generation synthetic data engine that lives alongside the existing v0 API. Both work independently -- you can use v0 and core in the same project without conflicts.

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

## Manual Conversion Reference

### Table Setup

```python
# v0
dg = DataGenerator(spark, rows=10000, name="orders", randomSeed=42)
df = dg.build()

# core
from dbldatagen.core import generate
from dbldatagen.core.spec.schema import DataGenPlan, TableSpec, PrimaryKey

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(name="orders", rows=10000, columns=[...], primary_key=PrimaryKey(columns=["id"]))
    ]
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
from dbldatagen.core.spec.dsl import integer
integer("age", min=18, max=90)
```

#### Float/Double Range

```python
# v0
.withColumn("amount", DoubleType(), minValue=10.0, maxValue=500.0)

# core
from dbldatagen.core.spec.dsl import decimal
decimal("amount", min=10.0, max=500.0)
```

#### String Values (Discrete)

```python
# v0
.withColumn("status", StringType(), values=["active", "inactive", "pending"])

# core
from dbldatagen.core.spec.dsl import text
text("status", values=["active", "inactive", "pending"])
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
from dbldatagen.core.spec.dsl import timestamp
timestamp("created_at", start="2020-01-01", end="2025-12-31")
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
from dbldatagen.core.spec.dsl import expression
expression("total", "quantity * unit_price", dtype=DataType.DOUBLE)
```

#### Template / Pattern

```python
# v0 (regex-style)
.withColumn("code", StringType(), template=r"ORD-\d{4}")

# core (placeholder-style)
from dbldatagen.core.spec.dsl import pattern
pattern("code", template="ORD-{digit:4}")
```

core template placeholders: `{digit:N}`, `{alpha:N}`, `{hex:N}`, `{seq}`, `{uuid}`

#### Prefix/Suffix

```python
# v0
.withColumn("sku", StringType(), prefix="SKU-")

# core
pattern("sku", template="SKU-{digit:6}")
```

### Primary Keys

```python
# v0 (implicit -- the 'id' column is auto-generated)
dg = DataGenerator(spark, rows=1000)

# core (explicit)
from dbldatagen.core.spec.dsl import pk_auto, pk_uuid, pk_pattern

pk_auto("id")                          # Sequential integer (1, 2, 3, ...)
pk_uuid("id")                          # Deterministic UUID
pk_pattern("id", "ORD-{digit:6}")      # Patterned string
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
integer("device_id", min=1, max=50)
text("country", values=["US", "DE", "JP", "BR"], seed_from="device_id")
```

### Multiple Identical Columns (numColumns)

```python
# v0 -- generates feature_0, feature_1, feature_2
.withColumn("feature", IntegerType(), minValue=0, maxValue=100, numColumns=3)

# core -- define each column explicitly
integer("feature_0", min=0, max=100)
integer("feature_1", min=0, max=100)
integer("feature_2", min=0, max=100)
```

The automatic converter expands `numColumns` for you.

### Unique Values

```python
# v0 -- constrain to exactly 50 distinct values
.withColumn("category", IntegerType(), minValue=1, maxValue=1000, uniqueValues=50)

# core -- adjust range to match cardinality: max = min + (N-1) * step
integer("category", min=1, max=50)
```

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
v0 only: `Beta`, `Gamma`
core only: `LogNormal`, `Zipf`, `WeightedValues`

## Core-Only Features (No v0 Equivalent)

These features are new in core and have no v0 equivalent:

### Foreign Keys

```python
from dbldatagen.core.spec.dsl import pk_auto, fk, integer
from dbldatagen.core.spec.schema import DataGenPlan, TableSpec, PrimaryKey

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=100,
            columns=[pk_auto("id"), text("name", values=["Alice", "Bob", "Carol"])],
            primary_key=PrimaryKey(columns=["id"]),
        ),
        TableSpec(
            name="orders",
            rows=1000,
            columns=[pk_auto("id"), fk("customer_id", ref="customers.id"), decimal("amount", min=10, max=500)],
            primary_key=PrimaryKey(columns=["id"]),
        ),
    ]
)
```

### Nested Types (Struct, Array)

```python
from dbldatagen.core.spec.dsl import struct, array, text, integer

struct("address", [
    text("city", ["Austin", "NYC", "LA"]),
    text("state", ["TX", "NY", "CA"]),
    integer("zip", min=10000, max=99999),
])

array("tags", ValuesColumn(values=["sale", "new", "popular"]), min_length=1, max_length=4)
```

### Faker Integration

```python
from dbldatagen.core.spec.dsl import faker

faker("email", provider="email")
faker("full_name", provider="name")
faker("address", provider="street_address")
faker("phone", provider="phone_number")
```

Requires: `pip install dbldatagen[core-faker]`

### CDC (Change Data Capture)

```python
from dbldatagen.core.engine.cdc import generate_cdc

stream = generate_cdc(spark, plan, num_batches=10)
initial_df = stream.initial["orders"]
batch_1 = stream.batches[0]["orders"]  # Contains _op column: I/U/D
```

## Features Not Supported in Core

These v0 features have no core equivalent:

| v0 Feature | Recommendation |
|-----------|----------------|
| `format` (printf-style `%05d`) | Use `PatternColumn(template="{digit:5}")` or `ExpressionColumn(expr="format_string(...)")` |
| `text=ILText(...)` (Lorem Ipsum) | Use `FakerColumn(provider="paragraph")` |
| `withConstraint(SqlExpr(...))` | Design generation to naturally satisfy rules; core doesn't do post-hoc filtering |
| `Beta` / `Gamma` distributions | Use `Normal` or `LogNormal` as approximation |

## What the Converter Handles

When using `from_data_generator()`, here's what converts automatically:

| v0 Feature | Core Result | Status |
|-----------|-----------|--------|
| `minValue`/`maxValue` | `RangeColumn(min, max)` | Automatic |
| `values` | `ValuesColumn(values)` | Automatic |
| `values` + `weights` | `ValuesColumn` + `WeightedValues` | Automatic |
| `expr` | `ExpressionColumn` | Automatic |
| `template` | `PatternColumn` | Automatic |
| `prefix`/`suffix` | `PatternColumn` | Automatic |
| `begin`/`end` | `TimestampColumn` | Automatic |
| `BooleanType` | `ValuesColumn([True, False])` | Automatic |
| `percentNulls` | `null_fraction` | Automatic |
| `baseColumn` | `seed_from` | Automatic |
| `uniqueValues` | Adjusted range | Automatic (with warning) |
| `numColumns`/`numFeatures` | Expanded to N columns | Automatic |
| `distribution=Normal(...)` | `Normal(...)` | Automatic |
| `distribution=Exponential(...)` | `Exponential(...)` | Automatic |
| `format` | -- | Warning emitted |
| `text=ILText(...)` | Placeholder pattern | Warning emitted |
| Constraints | -- | Warning emitted |
| `Beta`/`Gamma` distributions | -- | Warning emitted |
