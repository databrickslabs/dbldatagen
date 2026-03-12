---
sidebar_position: 2
title: "Column Strategies"
---

# Column Strategies

> **TL;DR:** dbldatagen.v1 offers 11 column generation strategies covering everything from numeric ranges and categorical values to nested JSON structures. Each strategy is a Pydantic model that gets compiled into optimized Spark SQL or pandas UDF code. Choose based on your data type: `RangeColumn` for numbers, `ValuesColumn` for categories, `FakerColumn` for realistic text, `PatternColumn` for formatted strings, and more.

## Overview

Each column in dbldatagen.v1 uses a **generation strategy** that defines how values are produced. Strategies are Pydantic models with a discriminator field (`strategy`) forming a tagged union.

All strategies support:
- **Deterministic generation** — same seed + row ID = same value
- **Partition independence** — generates correctly regardless of cluster size
- **Distribution control** — where applicable (range, values, timestamps)

## Performance Tiers

Strategies fall into two performance tiers:

| Tier | Implementation | Performance | Examples |
|------|----------------|-------------|----------|
| **Tier 1** | Pure Spark SQL expressions | ~100M+ rows/sec/core | Range, Values, Pattern, Sequence, UUID, Expression, Timestamp, Constant, Struct, Array |
| **Tier 2** | Pandas UDF (vectorized Python) | ~10-50M rows/sec/core | Faker, Feistel cipher PKs |

**Tier 1** strategies compile to native Spark SQL and run at maximum speed. **Tier 2** strategies use `pandas_udf` for complex logic but are still highly optimized.

## Strategy 1: RangeColumn

Generate numeric values within a range `[min, max]`.

```python
from dbldatagen.v1 import integer, decimal
from dbldatagen.v1.schema import Normal, ColumnSpec, DataType, RangeColumn

# Integer range: 18-90
integer("age", min=18, max=90)

# Decimal range: 0.99-999.99
decimal("price", min=0.99, max=999.99)

# With custom distribution (bell curve)
integer("score", min=0, max=100, distribution=Normal(mean=75, stddev=15))

# Full model syntax
ColumnSpec(
    name="quantity",
    dtype=DataType.INT,
    gen=RangeColumn(min=1, max=100, distribution=Normal(mean=50, stddev=20))
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min` | `float \| int` | `0` | Minimum value (inclusive) |
| `max` | `float \| int` | `100` | Maximum value (inclusive) |
| `step` | `float \| int \| None` | `None` | Step size (for discrete ranges) |
| `distribution` | `Distribution` | `Uniform()` | Value distribution (see [Distributions](distributions.md)) |

**Supported types:** `INT`, `LONG`, `FLOAT`, `DOUBLE`, `DECIMAL`

**Performance:** Tier 1 (pure SQL)

**When to use:**
- Numeric columns: age, price, quantity, score
- Timestamps (as Unix epochs, though prefer `TimestampColumn`)
- Any bounded numeric data

## Strategy 2: ValuesColumn

Pick from an explicit list of allowed values.

```python
from dbldatagen.v1 import text
from dbldatagen.v1.schema import WeightedValues, ColumnSpec, DataType, ValuesColumn

# Uniform selection
text("status", values=["active", "inactive", "suspended"])

# Weighted selection
text("tier", values=["bronze", "silver", "gold"],
     distribution=WeightedValues(weights={"bronze": 0.70, "silver": 0.25, "gold": 0.05}))

# Full model syntax
ColumnSpec(
    name="color",
    dtype=DataType.STRING,
    gen=ValuesColumn(values=["red", "green", "blue", "yellow"])
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `values` | `list[Any]` | required | List of allowed values |
| `distribution` | `Distribution` | `Uniform()` | Selection distribution |

**Supported types:** Any (strings, integers, booleans, etc.)

**Performance:** Tier 1 (pure SQL for small lists)

**When to use:**
- Categorical columns: status, category, type, enum values
- Boolean flags (use `values=[True, False]`)
- Small finite sets with known values

**Note:** For very large value lists (10K+ items), consider using `RangeColumn` with a hash-to-index mapping instead.

## Strategy 3: FakerColumn

Generate realistic text data using [Faker](https://faker.readthedocs.io/) providers.

```python
from dbldatagen.v1 import faker
from dbldatagen.v1.schema import ColumnSpec, DataType, FakerColumn

# Names
faker("name", "name")

# Emails
faker("email", "email")

# Addresses
faker("address", "street_address")

# Phone numbers with locale
faker("phone", "phone_number", locale="ja_JP")

# Dates with parameters
faker("dob", "date_of_birth", minimum_age=18, maximum_age=80)

# Full model syntax
ColumnSpec(
    name="city",
    dtype=DataType.STRING,
    gen=FakerColumn(provider="city", locale="de_DE")
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `provider` | `str` | required | Faker provider method name |
| `kwargs` | `dict[str, Any]` | `{}` | Keyword arguments for provider |
| `locale` | `str \| None` | `None` | Locale (e.g., "en_US", "de_DE", "ja_JP") |

**Supported providers:** Any [Faker provider](https://faker.readthedocs.io/en/master/providers.html) (100+ built-in)

**Performance:** Tier 2 (uses 10K pre-generated pool for speed)

**How it works:**
1. Driver-side: Generate 10,000 values using Faker with deterministic seed
2. Executor-side: Select from pool using `hash(column_seed, row_id) % 10000`

This gives 200x speedup vs per-row Faker instantiation while maintaining determinism.

**When to use:**
- Realistic names, emails, addresses, phone numbers
- Dates, credit card numbers, company names, URLs
- Localized data (over 50 locales supported)

**Dependency:** Requires `pip install "dbldatagen[v1-faker]"` or `pip install faker`

### Popular Faker Providers

| Provider | Example Output | Use Case |
|----------|---------------|----------|
| `name` | "Alice Johnson" | Person names |
| `email` | "alice@example.com" | Email addresses |
| `street_address` | "123 Main St" | Street addresses |
| `city` | "New York" | City names |
| `phone_number` | "(555) 123-4567" | Phone numbers |
| `date_of_birth` | "1985-03-15" | Birthdates |
| `company` | "Acme Corp" | Company names |
| `url` | "https://example.com" | URLs |
| `credit_card_number` | "4532-1234-5678-9012" | CC numbers (fake) |
| `ipv4` | "192.168.1.1" | IP addresses |

See [Faker documentation](https://faker.readthedocs.io/en/master/providers.html) for full list.

## Strategy 4: PatternColumn

Generate strings from a template with placeholders.

```python
from dbldatagen.v1 import pattern, pk_pattern
from dbldatagen.v1.schema import ColumnSpec, DataType, PatternColumn

# Order IDs: ORD-3847-KMX
pattern("order_id", "ORD-{digit:4}-{alpha:3}")

# SKUs: SKU-a3f1b2-00042
pattern("sku", "SKU-{hex:6}-{seq:05d}")

# License plates: CA-1234-AB
pattern("license", "CA-{digit:4}-{alpha:2}")

# Full model syntax
ColumnSpec(
    name="tracking_code",
    dtype=DataType.STRING,
    gen=PatternColumn(template="TRACK-{digit:8}-{alpha:2}")
)
```

### Placeholder Reference

| Placeholder | Description | Example Template | Example Output |
|-------------|-------------|------------------|----------------|
| `{seq}` | Row sequence number | `"ID-{seq}"` | `ID-1`, `ID-2` |
| `{seq:05d}` | Zero-padded sequence | `"{seq:08d}"` | `00000001`, `00000002` |
| `{digit:N}` | N random digits | `"{digit:4}"` | `3847`, `9012` |
| `{alpha:N}` | N random uppercase letters | `"{alpha:3}"` | `KMX`, `PLQ` |
| `{hex:N}` | N random hex chars (lowercase) | `"{hex:6}"` | `a3f1b2`, `c4d5e6` |
| `{uuid}` | Full deterministic UUID | `"{uuid}"` | `a3f1b2c4-d5e6-...` |

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `template` | `str` | required | Pattern template with placeholders |

**Supported types:** `STRING`

**Performance:** Tier 1 (pure Spark SQL — `F.concat` with bitwise operations)

**When to use:**
- Human-readable IDs (order numbers, tracking codes)
- Formatted identifiers (SKUs, license plates)
- Primary keys with specific formats (see [Primary Keys](primary-keys.md))

## Strategy 5: SequenceColumn

Monotonically increasing integer sequence.

```python
from dbldatagen.v1 import pk_auto
from dbldatagen.v1.schema import ColumnSpec, DataType, SequenceColumn

# Default: 1, 2, 3, ...
pk_auto("id")

# Custom start/step: 1000, 1010, 1020, ...
ColumnSpec(
    name="order_number",
    dtype=DataType.LONG,
    gen=SequenceColumn(start=1000, step=10)
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start` | `int` | `1` | Starting value |
| `step` | `int` | `1` | Increment per row |

**Formula:** `value = start + row_id * step`

**Supported types:** `INT`, `LONG`

**Performance:** Tier 1 (pure SQL, single arithmetic expression)

**When to use:**
- Auto-incrementing primary keys (most common use case)
- Monotonic counters
- Any sequential numeric data

## Strategy 6: UUIDColumn

Deterministic UUID generation (128-bit hex string).

```python
from dbldatagen.v1 import pk_uuid
from dbldatagen.v1.schema import ColumnSpec, DataType, UUIDColumn

# Standard UUID format: "a3f1b2c4-d5e6-f7a8-b9c0-d1e2f3a4b5c6"
pk_uuid("event_id")

# Full model syntax
ColumnSpec(name="session_id", dtype=DataType.STRING, gen=UUIDColumn())
```

### Fields

No fields — UUID is derived from `xxhash64(seed, id)` + `xxhash64(seed+1, id)`.

**Supported types:** `STRING`

**Performance:** Tier 1 (pure SQL, two hash calls + hex formatting)

**When to use:**
- Globally unique identifiers
- Event tracking (clickstream, logs)
- Distributed systems requiring UUID format

## Strategy 7: ExpressionColumn

Arbitrary Spark SQL expression referencing sibling columns.

```python
from dbldatagen.v1 import expression
from dbldatagen.v1.schema import ColumnSpec, DataType, ExpressionColumn

# Computed total
expression("total", "quantity * unit_price")

# String concatenation
expression("full_name", "concat(first_name, ' ', last_name)")

# Conditional logic
expression("adult_flag", "CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END")

# Date math
expression("order_ship_date", "date_add(order_date, 3)")

# Full model syntax
ColumnSpec(
    name="discounted_price",
    dtype=DataType.DOUBLE,
    gen=ExpressionColumn(expr="price * 0.9")
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `expr` | `str` | required | Spark SQL expression |

**Supported types:** Any (inferred from expression)

**Performance:** Tier 1 (pure SQL, compiled to Catalyst plan)

**When to use:**
- Computed columns derived from other columns
- Business logic (discounts, tax calculations)
- Transformations (concatenation, date math, conditional logic)

**Important:** Expression columns are evaluated AFTER base columns are generated. They can reference any previously defined column in the same table.

## Strategy 8: TimestampColumn

Generate timestamps or dates within a range.

```python
from dbldatagen.v1 import timestamp
from dbldatagen.v1.schema import Normal, ColumnSpec, DataType, TimestampColumn

# Timestamp range: 2023-01-01 to 2025-12-31
timestamp("created_at", start="2023-01-01", end="2025-12-31")

# Date column (set dtype=DATE)
ColumnSpec(
    name="order_date",
    dtype=DataType.DATE,
    gen=TimestampColumn(start="2024-01-01", end="2024-12-31")
)

# Clustered around middle (bell curve)
timestamp("event_time", start="2024-01-01", end="2024-12-31",
          distribution=Normal(mean=0.5, stddev=0.2))

# Full model syntax
ColumnSpec(
    name="event_timestamp",
    dtype=DataType.TIMESTAMP,
    gen=TimestampColumn(start="2024-06-01", end="2024-06-30")
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start` | `str` | `"2020-01-01"` | Start date/time (ISO format) |
| `end` | `str` | `"2025-12-31"` | End date/time (ISO format) |
| `distribution` | `Distribution` | `Uniform()` | Temporal distribution |

**Supported types:** `TIMESTAMP`, `DATE`

**Performance:** Tier 1 (pure SQL, Unix epoch arithmetic)

**Date format:** ISO 8601 strings: `"YYYY-MM-DD"` or `"YYYY-MM-DD HH:MM:SS"`

**When to use:**
- Event timestamps (orders, logins, clicks)
- Date columns (birthdays, hire dates)
- Time-series data with specific ranges

## Strategy 9: ConstantColumn

Every row gets the same value.

```python
from dbldatagen.v1.schema import ColumnSpec, DataType, ConstantColumn

# String constant
ColumnSpec(name="status", dtype=DataType.STRING, gen=ConstantColumn(value="active"))

# Integer constant
ColumnSpec(name="version", dtype=DataType.INT, gen=ConstantColumn(value=1))

# Boolean constant
ColumnSpec(name="is_verified", dtype=DataType.BOOLEAN, gen=ConstantColumn(value=True))
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `value` | `Any` | required | Constant value for all rows |

**Supported types:** Any

**Performance:** Tier 1 (pure SQL, literal value)

**When to use:**
- Default values (e.g., all rows have `status="pending"`)
- Version fields (e.g., `schema_version=2`)
- Boolean flags (e.g., `is_test=False`)
- Placeholder columns (though rarely needed)

**Note:** `fk()` uses `ConstantColumn(value=None)` as an internal placeholder before FK resolution.

## Strategy 10: StructColumn

Group child columns into a Spark struct (nested JSON object).

```python
from dbldatagen.v1 import struct, faker, integer, text
from dbldatagen.v1.schema import ColumnSpec, ValuesColumn, RangeColumn, StructColumn

# Nested address object
address = struct("address", fields=[
    ColumnSpec(name="street", gen=ValuesColumn(values=["123 Main St", "456 Oak Ave"])),
    ColumnSpec(name="city", gen=ValuesColumn(values=["New York", "Boston", "Chicago"])),
    ColumnSpec(name="state", gen=ValuesColumn(values=["NY", "MA", "IL"])),
    ColumnSpec(name="zip", gen=RangeColumn(min=10000, max=99999)),
])

# Full model syntax
ColumnSpec(
    name="user_profile",
    gen=StructColumn(fields=[
        ColumnSpec(name="bio", gen=ValuesColumn(values=["Engineer", "Designer"])),
        ColumnSpec(name="years_experience", gen=RangeColumn(min=0, max=40)),
    ])
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `fields` | `list[ColumnSpec]` | required | Child column specifications |

**Supported types:** Inferred as `StructType` with child field types

**Performance:** Tier 1 (pure Spark SQL — `F.struct()` with recursive column dispatch)

**JSON output:**
```json
{
  "address": {
    "street": "123 Main St",
    "city": "Boston",
    "state": "MA",
    "zip": 54321
  }
}
```

**When to use:**
- JSON columns with nested objects
- Grouped related fields (address, contact info, metadata)
- Complex document structures (e.g., for NoSQL ingestion)

**Nesting:** Struct fields can themselves be structs, enabling arbitrary nesting depth.

## Strategy 11: ArrayColumn

Generate variable-length arrays from an inner strategy.

```python
from dbldatagen.v1 import array
from dbldatagen.v1.schema import ValuesColumn, RangeColumn, ColumnSpec, ArrayColumn

# Array of tags (1-4 items)
tags = array("tags",
    element=ValuesColumn(values=["sale", "new", "premium", "eco", "clearance"]),
    min_length=1,
    max_length=4,
)

# Array of integers
scores = array("scores",
    element=RangeColumn(min=0, max=100),
    min_length=3,
    max_length=10,
)

# Full model syntax
ColumnSpec(
    name="categories",
    gen=ArrayColumn(
        element=ValuesColumn(values=["electronics", "home", "toys"]),
        min_length=1,
        max_length=3,
    )
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `element` | `ColumnStrategy` | required | Strategy for array elements |
| `min_length` | `int` | `1` | Minimum array length |
| `max_length` | `int` | `5` | Maximum array length |

**Supported types:** Inferred as `ArrayType` with element type

**Performance:** Tier 1 (pure Spark SQL — `F.array()` + `F.slice()` for variable length)

**JSON output:**
```json
{
  "tags": ["sale", "premium", "eco"]
}
```

**How it works:**
1. Array length per row is deterministically chosen between `min_length` and `max_length`
2. Each element is generated with a unique seed offset (for variety)
3. Elements can use any strategy (range, values, faker, even nested structs/arrays)

**When to use:**
- Multi-valued columns (tags, categories, features)
- JSON arrays
- Variable-length lists of related items

**Nested arrays:** You can nest arrays inside structs or arrays inside arrays for complex structures:

```python
# Array of struct objects
from dbldatagen.v1.schema import StructColumn

items = array("order_items",
    element=StructColumn(fields=[
        ColumnSpec(name="product_id", gen=RangeColumn(min=1, max=10000)),
        ColumnSpec(name="quantity", gen=RangeColumn(min=1, max=10)),
    ]),
    min_length=1,
    max_length=5,
)
```

**JSON output:**
```json
{
  "order_items": [
    {"product_id": 4523, "quantity": 2},
    {"product_id": 891, "quantity": 1},
    {"product_id": 7234, "quantity": 3}
  ]
}
```

## Strategy Summary Table

| Strategy | Use Case | Data Types | Distribution | Performance |
|----------|----------|-----------|--------------|-------------|
| **RangeColumn** | Numeric ranges | INT, LONG, FLOAT, DOUBLE, DECIMAL | ✅ Yes | Tier 1 |
| **ValuesColumn** | Categorical selection | Any | ✅ Yes | Tier 1 |
| **FakerColumn** | Realistic text (names, emails) | STRING, DATE | ❌ No | Tier 2 |
| **PatternColumn** | Formatted strings | STRING | ❌ No | Tier 1 |
| **SequenceColumn** | Auto-increment | INT, LONG | ❌ No | Tier 1 |
| **UUIDColumn** | Unique identifiers | STRING | ❌ No | Tier 1 |
| **ExpressionColumn** | Computed columns | Any | ❌ No | Tier 1 |
| **TimestampColumn** | Date/time ranges | TIMESTAMP, DATE | ✅ Yes | Tier 1 |
| **ConstantColumn** | Fixed values | Any | ❌ No | Tier 1 |
| **StructColumn** | Nested objects | STRUCT | ❌ No | Tier 1 |
| **ArrayColumn** | Variable-length arrays | ARRAY | ❌ No | Tier 1 |

## Combining Strategies

Build rich schemas by combining strategies:

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, faker, integer, text, decimal, timestamp, expression, struct, array
from dbldatagen.v1.schema import ColumnSpec, ValuesColumn, RangeColumn

products = TableSpec(
    name="products",
    rows="100K",
    columns=[
        pk_auto("product_id"),                                     # Strategy 5: Sequence
        faker("name", "product_name"),                             # Strategy 3: Faker
        text("category", values=["electronics", "home", "toys"]),  # Strategy 2: Values
        decimal("base_price", min=9.99, max=2999.99),              # Strategy 1: Range
        expression("final_price", "base_price * 0.9"),             # Strategy 7: Expression
        timestamp("launched_at", start="2020-01-01", end="2024-12-31"),  # Strategy 8: Timestamp
        array("tags",                                              # Strategy 11: Array
            element=ValuesColumn(values=["sale", "new", "popular"]),
            min_length=1, max_length=3,
        ),
        struct("dimensions", fields=[                              # Strategy 10: Struct
            ColumnSpec(name="length", gen=RangeColumn(min=1, max=100)),
            ColumnSpec(name="width", gen=RangeColumn(min=1, max=100)),
            ColumnSpec(name="height", gen=RangeColumn(min=1, max=100)),
        ]),
    ],
    primary_key=PrimaryKey(columns=["product_id"]),
)

plan = DataGenPlan(tables=[products])
```

## Next Steps

- Control value distributions with [Distributions](distributions.md) (normal, Zipf, exponential, weighted)
- Create relationships with [Foreign Keys](foreign-keys.md)
- Generate change streams with [CDC](../cdc/overview.md)
