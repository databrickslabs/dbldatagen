---
sidebar_position: 1
title: "Plans, Tables & Columns"
---

# Tables and Columns

> **TL;DR:** Use `DataGenPlan` to define a complete schema with multiple tables. Each `TableSpec` specifies rows, columns, and a primary key. Each `ColumnSpec` pairs a column name with a generation strategy. Row counts support shorthand like "10K" or "1M". Seeds cascade from global → table → column → cell for deterministic generation.

## DataGenPlan

The top-level plan that describes all tables you want to generate. Tables are processed in dependency order when foreign keys are present (parents first).

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec

plan = DataGenPlan(
    tables=[customers, orders, order_items],
    seed=42,                    # Global seed for deterministic generation
    default_locale="en_US",     # Default locale for Faker columns
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tables` | `list[TableSpec]` | required | List of table specifications |
| `seed` | `int` | `42` | Global random seed |
| `default_locale` | `str` | `"en_US"` | Default Faker locale (e.g., "de_DE", "ja_JP") |

Tables without an explicit `seed` are automatically assigned `global_seed + table_index`.

## TableSpec

Defines one table with its columns, row count, and optional primary key.

```python
from dbldatagen.v1.schema import TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, integer, text

customers = TableSpec(
    name="customers",
    rows="5M",                                  # String shorthand for 5,000,000
    columns=[
        pk_auto("customer_id"),
        text("name", values=["Alice", "Bob", "Carol"]),
        integer("age", min=18, max=90),
    ],
    primary_key=PrimaryKey(columns=["customer_id"]),
    seed=None,                                  # Inherits from plan
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Table name |
| `columns` | `list[ColumnSpec]` | required | Column definitions |
| `rows` | `int \| str` | required | Row count (see shorthand below) |
| `primary_key` | `PrimaryKey \| None` | `None` | Primary key definition |
| `seed` | `int \| None` | `None` | Per-table seed override |

### Row Count Shorthand

Row counts accept convenient string formats:

| Format | Equivalent Integer | Example |
|--------|-------------------|---------|
| `"1K"` | 1,000 | `rows="10K"` → 10,000 |
| `"1M"` | 1,000,000 | `rows="2.5M"` → 2,500,000 |
| `"1B"` | 1,000,000,000 | `rows="1B"` → 1,000,000,000 |

Fractional values work: `"2.5M"` = 2,500,000.

## ColumnSpec

Defines one column with its name, data type, and generation strategy.

```python
from dbldatagen.v1.schema import ColumnSpec, DataType, RangeColumn

age_column = ColumnSpec(
    name="age",
    dtype=DataType.INT,
    gen=RangeColumn(min=18, max=90),
    nullable=False,
    null_fraction=0.0,
)
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `dtype` | `DataType \| None` | `None` | Spark data type (inferred from strategy if omitted) |
| `gen` | `ColumnStrategy` | required | Generation strategy (see [Column Strategies](column-strategies.md)) |
| `nullable` | `bool` | `False` | Allow NULL values |
| `null_fraction` | `float` | `0.0` | Fraction of rows to set NULL (0.0-1.0) |
| `foreign_key` | `ForeignKeyRef \| None` | `None` | Foreign key relationship (see [Foreign Keys](foreign-keys.md)) |

**Note:** Setting `null_fraction > 0` automatically sets `nullable = True`.

## DataType Enum

Maps to Spark SQL types:

| Value | Spark Type | Python Type |
|-------|-----------|-------------|
| `DataType.INT` | `IntegerType` | `int` |
| `DataType.LONG` | `LongType` | `int` |
| `DataType.FLOAT` | `FloatType` | `float` |
| `DataType.DOUBLE` | `DoubleType` | `float` |
| `DataType.STRING` | `StringType` | `str` |
| `DataType.BOOLEAN` | `BooleanType` | `bool` |
| `DataType.DATE` | `DateType` | `datetime.date` |
| `DataType.TIMESTAMP` | `TimestampType` | `datetime.datetime` |
| `DataType.DECIMAL` | `DecimalType` | `decimal.Decimal` |

**Convenience alias:** `DataType.INTEGER` is the same as `DataType.INT`.

## Seed Hierarchy

dbldatagen.v1 uses a four-level seed hierarchy for deterministic generation:

```
global_seed (DataGenPlan.seed, default: 42)
  └── table_seed = global_seed + table_index (or explicit TableSpec.seed)
        └── column_seed = hash(table_seed, table_name, column_name)
              └── cell_seed = xxhash64(column_seed, row_id)
```

Each level can be overridden:

```python
# Global seed
plan = DataGenPlan(seed=12345, tables=[...])

# Per-table seed override
customers = TableSpec(name="customers", rows="1M", seed=99999, columns=[...])
```

**Why this matters:** The same seed + config always produces identical data, regardless of cluster size or partition count. See [Architecture](../reference/architecture.md) for details.

## Complete Example

Here's a complete plan with multiple tables:

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, pk_pattern, integer, text, decimal, fk

# Define tables
products = TableSpec(
    name="products",
    rows="100K",
    columns=[
        pk_auto("product_id"),
        text("name", values=["Widget", "Gadget", "Doohickey"]),
        decimal("price", min=0.99, max=999.99),
        text("category", values=["electronics", "home", "toys"]),
    ],
    primary_key=PrimaryKey(columns=["product_id"]),
)

customers = TableSpec(
    name="customers",
    rows="1M",
    columns=[
        pk_pattern("customer_id", "CUST-{digit:8}"),
        text("name", values=["Alice", "Bob", "Carol", "Dave"]),
        integer("age", min=18, max=90),
        text("country", values=["US", "CA", "UK", "DE"]),
    ],
    primary_key=PrimaryKey(columns=["customer_id"]),
)

orders = TableSpec(
    name="orders",
    rows="5M",
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        fk("product_id", "products.product_id"),
        integer("quantity", min=1, max=10),
        decimal("unit_price", min=0.99, max=999.99),
    ],
    primary_key=PrimaryKey(columns=["order_id"]),
)

# Create plan and generate
plan = DataGenPlan(
    tables=[products, customers, orders],
    seed=42,
)

# Generate all tables
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("dbldatagen").getOrCreate()
dfs = generate(spark, plan)

# Access individual DataFrames
products_df = dfs["products"]
customers_df = dfs["customers"]
orders_df = dfs["orders"]

orders_df.show()
```

**Output:**
```
+--------+------------+----------+--------+----------+
|order_id|customer_id|product_id|quantity|unit_price|
+--------+------------+----------+--------+----------+
|       1|CUST-00123456|         1|       3|     45.99|
|       2|CUST-00789012|        23|       1|    199.99|
|       3|CUST-00123456|         7|       2|     12.50|
...
```

## Next Steps

- Learn about [Primary Key strategies](primary-keys.md) (sequential, UUID, pattern, random unique)
- Configure [Foreign Key relationships](foreign-keys.md) with distributions and nullable options
- Explore all [Column Strategies](column-strategies.md) (range, values, faker, pattern, etc.)
- Control value distributions with [Distributions](distributions.md) (uniform, normal, Zipf, etc.)
