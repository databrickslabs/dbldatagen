# Migrating from dbldatagen v0 to v1

## Overview

`dbldatagen.v1` is a next-generation synthetic data engine that lives alongside the existing v0 API. Both work independently -- you can use v0 and v1 in the same project without conflicts.

## Installation

```bash
# v0 only (existing behavior)
pip install dbldatagen

# v0 + v1 core
pip install dbldatagen[v1]

# v0 + v1 + all optional extras
pip install dbldatagen[v1-dev]

# Individual v1 extras
pip install dbldatagen[v1-faker]   # Faker-based text generation
pip install dbldatagen[v1-csv]     # CSV schema inference
pip install dbldatagen[v1-jdbc]    # Database schema extraction
pip install dbldatagen[v1-sql]     # SQL query parsing
```

## Quick Start: Automatic Conversion

The fastest migration path is the built-in converter:

```python
from dbldatagen import DataGenerator
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType
from dbldatagen.v1.compat import from_data_generator
from dbldatagen.v1 import generate

# Your existing v0 code
dg = (
    DataGenerator(spark, rows=10000, name="orders")
    .withColumn("order_id", LongType(), minValue=1, maxValue=10000)
    .withColumn("status", StringType(), values=["pending", "shipped", "delivered"])
    .withColumn("amount", DoubleType(), minValue=10.0, maxValue=500.0)
    .withColumn("priority", StringType(), values=["low", "med", "high"], weights=[60, 30, 10])
)

# Convert to v1
plan = from_data_generator(dg)

# Generate with v1 engine
result = generate(spark, plan)
df = result["orders"]
```

The converter handles most common patterns automatically and emits warnings for anything it can't convert.

## Manual Conversion Reference

### Table Setup

```python
# v0
dg = DataGenerator(spark, rows=10000, name="orders", randomSeed=42)
df = dg.build()

# v1
from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey

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

# v1 (schema)
from dbldatagen.v1.schema import ColumnSpec, DataType, RangeColumn
ColumnSpec(name="age", dtype=DataType.INT, gen=RangeColumn(min=18, max=90))

# v1 (DSL shorthand)
from dbldatagen.v1.dsl import integer
integer("age", min=18, max=90)
```

#### Float/Double Range

```python
# v0
.withColumn("amount", DoubleType(), minValue=10.0, maxValue=500.0)

# v1
from dbldatagen.v1.dsl import decimal
decimal("amount", min=10.0, max=500.0)
```

#### String Values (Discrete)

```python
# v0
.withColumn("status", StringType(), values=["active", "inactive", "pending"])

# v1
from dbldatagen.v1.dsl import text
text("status", values=["active", "inactive", "pending"])
```

#### Weighted Values

```python
# v0
.withColumn("tier", StringType(),
            values=["free", "basic", "premium"],
            weights=[70, 20, 10])

# v1
from dbldatagen.v1.schema import ColumnSpec, DataType, ValuesColumn, WeightedValues
ColumnSpec(
    name="tier",
    dtype=DataType.STRING,
    gen=ValuesColumn(
        values=["free", "basic", "premium"],
        distribution=WeightedValues(weights={"free": 70, "basic": 20, "premium": 10})
    )
)
```

Note: v0 uses a list of weights (position-matched). v1 uses a dict (name-matched).

#### Timestamp/Date Range

```python
# v0
.withColumn("created_at", TimestampType(),
            begin="2020-01-01 00:00:00", end="2025-12-31 23:59:59")

# v1
from dbldatagen.v1.dsl import timestamp
timestamp("created_at", start="2020-01-01", end="2025-12-31")
```

#### Boolean

```python
# v0
.withColumn("is_active", BooleanType())

# v1
from dbldatagen.v1.schema import ColumnSpec, DataType, ValuesColumn
ColumnSpec(name="is_active", dtype=DataType.BOOLEAN, gen=ValuesColumn(values=[True, False]))
```

#### SQL Expression

```python
# v0
.withColumn("total", DoubleType(), expr="quantity * unit_price")

# v1
from dbldatagen.v1.dsl import expression
expression("total", "quantity * unit_price", dtype=DataType.DOUBLE)
```

#### Template / Pattern

```python
# v0 (regex-style)
.withColumn("code", StringType(), template=r"ORD-\d{4}")

# v1 (placeholder-style)
from dbldatagen.v1.dsl import pattern
pattern("code", template="ORD-{digit:4}")
```

v1 template placeholders: `{digit:N}`, `{alpha:N}`, `{hex:N}`, `{seq}`, `{uuid}`

#### Prefix/Suffix

```python
# v0
.withColumn("sku", StringType(), prefix="SKU-")

# v1
pattern("sku", template="SKU-{digit:6}")
```

### Primary Keys

```python
# v0 (implicit -- the 'id' column is auto-generated)
dg = DataGenerator(spark, rows=1000)

# v1 (explicit)
from dbldatagen.v1.dsl import pk_auto, pk_uuid, pk_pattern

pk_auto("id")                          # Sequential integer (1, 2, 3, ...)
pk_uuid("id")                          # Deterministic UUID
pk_pattern("id", "ORD-{digit:6}")      # Patterned string
```

### Nullable Columns

```python
# v0
.withColumn("email", StringType(), percentNulls=0.2)

# v1
ColumnSpec(name="email", dtype=DataType.STRING, gen=..., nullable=True, null_fraction=0.2)
```

### Correlated Columns (baseColumn -> seed_from)

```python
# v0 -- same device_id always produces same country
.withColumn("device_id", IntegerType(), minValue=1, maxValue=50)
.withColumn("country", StringType(),
            baseColumn="device_id",
            values=["US", "DE", "JP", "BR"])

# v1 -- seed_from is the direct equivalent
integer("device_id", min=1, max=50)
text("country", values=["US", "DE", "JP", "BR"], seed_from="device_id")
```

### Multiple Identical Columns (numColumns)

```python
# v0 -- generates feature_0, feature_1, feature_2
.withColumn("feature", IntegerType(), minValue=0, maxValue=100, numColumns=3)

# v1 -- define each column explicitly
integer("feature_0", min=0, max=100)
integer("feature_1", min=0, max=100)
integer("feature_2", min=0, max=100)
```

The automatic converter expands `numColumns` for you.

### Unique Values

```python
# v0 -- constrain to exactly 50 distinct values
.withColumn("category", IntegerType(), minValue=1, maxValue=1000, uniqueValues=50)

# v1 -- adjust range to match cardinality: max = min + (N-1) * step
integer("category", min=1, max=50)
```

### Distributions

```python
# v0
from dbldatagen.distributions import Normal
.withColumn("score", DoubleType(), minValue=0, maxValue=100, distribution=Normal(50, 10))

# v1
from dbldatagen.v1.schema import Normal, RangeColumn, ColumnSpec, DataType
ColumnSpec(
    name="score",
    dtype=DataType.DOUBLE,
    gen=RangeColumn(min=0, max=100, distribution=Normal(mean=50, stddev=10))
)
```

Available in both: `Normal`, `Exponential`
v0 only: `Beta`, `Gamma`
v1 only: `LogNormal`, `Zipf`, `WeightedValues`

## v1-Only Features (No v0 Equivalent)

These features are new in v1 and have no v0 equivalent:

### Foreign Keys

```python
from dbldatagen.v1.dsl import pk_auto, fk, integer
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey

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
from dbldatagen.v1.dsl import struct, array, text, integer

struct("address", [
    text("city", ["Austin", "NYC", "LA"]),
    text("state", ["TX", "NY", "CA"]),
    integer("zip", min=10000, max=99999),
])

array("tags", ValuesColumn(values=["sale", "new", "popular"]), min_length=1, max_length=4)
```

### Faker Integration

```python
from dbldatagen.v1.dsl import faker

faker("email", provider="email")
faker("full_name", provider="name")
faker("address", provider="street_address")
faker("phone", provider="phone_number")
```

Requires: `pip install dbldatagen[v1-faker]`

### CDC (Change Data Capture)

```python
from dbldatagen.v1.cdc import generate_cdc

stream = generate_cdc(spark, plan, num_batches=10)
initial_df = stream.initial["orders"]
batch_1 = stream.batches[0]["orders"]  # Contains _op column: I/U/D
```

### Streaming

```python
from dbldatagen.v1 import generate_stream

sdf = generate_stream(spark, table_spec, rows_per_second=100)
# Returns a Spark Structured Streaming DataFrame
```

### Schema Inference from SQL

```python
from dbldatagen.v1.connectors import extract_from_sql_query

plan = extract_from_sql_query("SELECT id, name, amount FROM customers JOIN orders ON ...")
```

Requires: `pip install dbldatagen[v1-sql]`

## Features Not Supported in v1

These v0 features have no v1 equivalent:

| v0 Feature | Recommendation |
|-----------|----------------|
| `format` (printf-style `%05d`) | Use `PatternColumn(template="{digit:5}")` or `ExpressionColumn(expr="format_string(...)")` |
| `text=ILText(...)` (Lorem Ipsum) | Use `FakerColumn(provider="paragraph")` |
| `withConstraint(SqlExpr(...))` | Design generation to naturally satisfy rules; v1 doesn't do post-hoc filtering |
| `Beta` / `Gamma` distributions | Use `Normal` or `LogNormal` as approximation |

## What the Converter Handles

When using `from_data_generator()`, here's what converts automatically:

| v0 Feature | v1 Result | Status |
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
