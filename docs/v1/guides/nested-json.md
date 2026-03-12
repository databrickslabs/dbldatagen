---
sidebar_position: 3
title: "Nested JSON"
description: "Generate nested objects and variable-length arrays for rich JSON output - e-commerce orders with line items and addresses"
keywords: [dbldatagen, nested json, struct, array, json output, complex types]
---

# Nested JSON: Structs and Arrays

> **TL;DR:** Generate tables with nested objects (structs) and variable-length arrays for rich JSON output. Perfect for document databases, event streams, and APIs that consume nested JSON.

## E-commerce Orders with Nested Data

A complete example showing nested manufacturer info, physical dimensions, tags, and ratings arrays.

:::info What This Demonstrates
- **Struct columns** for nested objects (`manufacturer`, `dimensions`)
- **Array columns** for variable-length lists (`tags`, `recent_ratings`)
- JSON output with natural nesting
- Accessing nested fields in Spark SQL
:::

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, struct, array, integer, decimal, text, timestamp,
)
from dbldatagen.v1.schema import ColumnSpec, RangeColumn, ValuesColumn

products = TableSpec(
    name="products",
    rows=500,
    primary_key=PrimaryKey(columns=["product_id"]),
    columns=[
        pk_auto("product_id"),
        text("name", values=["Laptop", "Headphones", "Keyboard", "Monitor"]),
        decimal("price", min=9.99, max=1999.99),

        # Nested struct: manufacturer info
        struct("manufacturer", fields=[
            ColumnSpec(name="company", gen=ValuesColumn(
                values=["Acme Corp", "TechWorks", "GadgetCo"])),
            ColumnSpec(name="country", gen=ValuesColumn(
                values=["US", "CN", "DE", "JP"])),
            ColumnSpec(name="year_founded", gen=RangeColumn(min=1970, max=2020)),
        ]),

        # Nested struct: physical dimensions
        struct("dimensions", fields=[
            ColumnSpec(name="length_cm", gen=RangeColumn(min=5.0, max=100.0)),
            ColumnSpec(name="width_cm", gen=RangeColumn(min=3.0, max=60.0)),
            ColumnSpec(name="weight_grams", dtype="int",
                       gen=RangeColumn(min=50, max=5000)),
        ]),

        # Array of tags
        array("tags",
            element=ValuesColumn(
                values=["bestseller", "new", "sale", "premium", "eco-friendly"]),
            min_length=1, max_length=4,
        ),

        # Array of recent ratings
        array("recent_ratings",
            element=RangeColumn(min=1, max=5),
            min_length=3, max_length=5,
        ),

        timestamp("listed_at", start="2023-01-01", end="2025-12-31"),
    ],
)

plan = DataGenPlan(tables=[products], seed=700)
dfs = generate(spark, plan)

# JSON output with nested structures
dfs["products"].write.mode("overwrite").json("/tmp/products_json")

# Access nested fields
dfs["products"].select(
    "product_id", "name",
    "manufacturer.company",
    "dimensions.weight_grams",
    "tags",
).show(5, truncate=40)
```

**Sample JSON Output:**

```json
{
  "product_id": 1,
  "name": "Laptop",
  "price": 1299.99,
  "manufacturer": {"company": "TechWorks", "country": "JP", "year_founded": 2005},
  "dimensions": {"length_cm": 35.2, "width_cm": 24.1, "weight_grams": 1800},
  "tags": ["bestseller", "premium"],
  "recent_ratings": [4, 5, 3, 5],
  "listed_at": "2024-06-15T10:22:31.000Z"
}
```

---

## Understanding Struct Columns

### Definition

A `struct()` groups child fields into a Spark `StructType`, producing nested JSON objects:

```python
struct("address", fields=[
    ColumnSpec(name="street", gen=ValuesColumn(values=["123 Main St", "456 Oak Ave"])),
    ColumnSpec(name="city", gen=ValuesColumn(values=["NYC", "LA", "Chicago"])),
    ColumnSpec(name="zip", gen=RangeColumn(min=10000, max=99999)),
])
```

Each field is a full `ColumnSpec` — it can use any generation strategy, including:
- Range columns (numeric, timestamps)
- Value selection (text, categorical)
- Faker providers
- **Nested structs** (structs within structs)

### Accessing Nested Fields

Use dot notation in Spark SQL:

```python
df.select("address.city", "address.zip").show()
df.filter(F.col("address.city") == "NYC")
df.groupBy("address.state").count()
```

### Nullable Fields

Each child field can be independently nullable:

```python
struct("contact", fields=[
    ColumnSpec(name="email", gen=FakerColumn(provider="email")),
    ColumnSpec(name="phone", gen=FakerColumn(provider="phone_number"),
               nullable=True, null_fraction=0.3),  # 30% NULL
])
```

---

## Understanding Array Columns

### Definition

An `array()` generates a variable-length list from an inner strategy:

```python
array("tags",
    element=ValuesColumn(values=["sale", "new", "premium", "eco-friendly"]),
    min_length=1,
    max_length=4,
)
```

Each row gets a random array length between `min_length` and `max_length`. Elements are generated with unique seed offsets to ensure variety within each array.

### Element Strategies

The `element` parameter can be any column strategy **except** primary keys and foreign keys:

```python
# Array of integers
array("scores", element=RangeColumn(min=0, max=100), min_length=5, max_length=10)

# Array of timestamps
array("login_times", element=TimestampColumn(start="2025-01-01", end="2025-12-31"),
      min_length=1, max_length=20)

# Array of nested structs (see next section)
array("line_items", element=StructColumn(fields=[...]), min_length=1, max_length=10)
```

### Accessing Array Elements

Use Spark SQL array functions:

```python
# Array length
df.selectExpr("size(tags) as num_tags")

# First element
df.selectExpr("tags[0] as first_tag")

# Explode to rows
df.selectExpr("product_id", "explode(tags) as tag")

# Filter arrays
df.filter(F.array_contains("tags", "sale"))
```

---

## Advanced: Arrays of Structs

Model complex nested relationships like order line items:

```python
from dbldatagen.v1.schema import StructColumn

orders = TableSpec(
    name="orders",
    rows=1000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        timestamp("ordered_at", start="2025-01-01", end="2025-12-31"),

        # Array of line items (each item is a struct)
        array("line_items",
            element=StructColumn(fields=[
                ColumnSpec(name="product_id", gen=RangeColumn(min=1, max=10000)),
                ColumnSpec(name="quantity", gen=RangeColumn(min=1, max=10)),
                ColumnSpec(name="unit_price", gen=RangeColumn(min=1.99, max=999.99)),
            ]),
            min_length=1, max_length=5,
        ),

        # Nested shipping address
        struct("shipping_address", fields=[
            ColumnSpec(name="street", gen=FakerColumn(provider="street_address")),
            ColumnSpec(name="city", gen=FakerColumn(provider="city")),
            ColumnSpec(name="state", gen=FakerColumn(provider="state_abbr")),
            ColumnSpec(name="zip", gen=FakerColumn(provider="zipcode")),
        ]),
    ],
)

plan = DataGenPlan(tables=[orders], seed=800)
dfs = generate(spark, plan)
```

**Sample JSON Output:**

```json
{
  "order_id": 1,
  "ordered_at": "2025-03-15T14:22:31.000Z",
  "line_items": [
    {"product_id": 42, "quantity": 2, "unit_price": 49.99},
    {"product_id": 103, "quantity": 1, "unit_price": 299.99},
    {"product_id": 7, "quantity": 5, "unit_price": 9.99}
  ],
  "shipping_address": {
    "street": "123 Main St",
    "city": "Austin",
    "state": "TX",
    "zip": "78701"
  }
}
```

### Querying Arrays of Structs

```python
# Explode line items
dfs["orders"].selectExpr(
    "order_id",
    "explode(line_items) as item"
).selectExpr(
    "order_id",
    "item.product_id",
    "item.quantity",
    "item.unit_price",
    "item.quantity * item.unit_price as line_total"
).show()

# Total revenue per order
dfs["orders"].selectExpr(
    "order_id",
    "aggregate(line_items, 0.0, (acc, x) -> acc + x.quantity * x.unit_price) as total"
).show()
```

---

## YAML Equivalent

All nested structures can be defined in YAML:

```yaml
seed: 700
tables:
  - name: products
    rows: 500
    primary_key: {columns: [product_id]}
    columns:
      - name: product_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: manufacturer
        gen:
          strategy: struct
          fields:
            - name: company
              dtype: string
              gen:
                strategy: values
                values: [Acme Corp, TechWorks, GadgetCo]
            - name: country
              dtype: string
              gen:
                strategy: values
                values: [US, CN, DE, JP]
            - name: year_founded
              dtype: int
              gen:
                strategy: range
                min: 1970
                max: 2020

      - name: tags
        gen:
          strategy: array
          element:
            strategy: values
            values: [bestseller, new, sale, premium, eco-friendly]
          min_length: 1
          max_length: 4
```

**Loading YAML:**

```python
import yaml
from dbldatagen.v1.schema import DataGenPlan

raw = yaml.safe_load(open("plan.yml").read())
plan = DataGenPlan.model_validate(raw)
dfs = generate(spark, plan)
```

---

## Writing Nested JSON

### Option 1: JSON Files

```python
# Single-line JSON (one object per line)
dfs["products"].write.mode("overwrite").json("/tmp/products.json")

# Pretty-printed JSON (for readability, not for downstream processing)
dfs["products"].coalesce(1).write.mode("overwrite") \
    .option("pretty", "true") \
    .json("/tmp/products_pretty.json")
```

### Option 2: Parquet (Preserves Schema)

```python
# Parquet natively supports nested structs and arrays
dfs["products"].write.mode("overwrite").parquet("/tmp/products.parquet")

# Read back with full schema
df = spark.read.parquet("/tmp/products.parquet")
df.printSchema()
```

### Option 3: Delta Tables

```python
dfs["products"].write.mode("overwrite").format("delta") \
    .saveAsTable("catalog.products")
```

---

## CDC with Nested Types

Nested structs and arrays work seamlessly with CDC:

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

base = DataGenPlan(tables=[products], seed=700)

plan = cdc_plan(
    base,
    num_batches=5,
    format="delta_cdf",
    products=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)),
)

stream = generate_cdc(spark, plan)

# Initial snapshot includes nested structures
stream.initial["products"].select("product_id", "manufacturer.company", "tags").show()

# CDC batches preserve nesting
stream.batches[0]["products"].select(
    "product_id", "tags", "_change_type"
).show()
```

**Update Behavior:**

When a row is updated, the **entire struct** is replaced (full-row replacement for updates). Individual struct fields do not update independently. This matches real-world CDC behavior where nested objects are typically treated as atomic units.

---

## Performance Considerations

### Struct Columns

- **Planning cost:** O(1) per struct (compiled to single Spark SQL struct expression)
- **Execution cost:** Same as flat columns (no UDF overhead)
- **Recommended:** Use structs liberally for logical grouping

### Array Columns

- **Planning cost:** O(max_length) — generates `max_length` elements per row
- **Execution cost:** Proportional to `max_length`
- **Recommendation:** Keep `max_length ≤ 20` for large tables (>10M rows)
- **Large arrays:** If you need arrays with 100+ elements, consider denormalizing to a separate table with FK relationships instead

### Nested Depth

- **Maximum depth:** Unlimited (structs can nest arbitrarily deep)
- **Practical limit:** 3-4 levels (beyond that, consider data model refactoring)

---

## Common Patterns

### Pattern 1: Event Payloads

```python
events = TableSpec(
    name="events",
    rows="10M",
    columns=[
        pk_auto("event_id"),
        text("event_type", values=["click", "view", "purchase"]),
        timestamp("event_time", start="2025-01-01", end="2025-12-31"),

        struct("user", fields=[
            ColumnSpec(name="user_id", gen=RangeColumn(min=1, max=100000)),
            ColumnSpec(name="session_id", gen=UUIDColumn()),
        ]),

        struct("context", fields=[
            ColumnSpec(name="ip", gen=PatternColumn(template="{digit:3}.{digit:3}.{digit:3}.{digit:3}")),
            ColumnSpec(name="user_agent", gen=ValuesColumn(values=["Chrome", "Firefox", "Safari"])),
        ]),

        # Variable metadata
        array("tags", element=ValuesColumn(values=["promo", "featured", "sale"]),
              min_length=0, max_length=3),
    ],
)
```

### Pattern 2: Hierarchical Addresses

```python
struct("address", fields=[
    ColumnSpec(name="street", gen=FakerColumn(provider="street_address")),
    ColumnSpec(name="city", gen=FakerColumn(provider="city")),

    # Nested country/region info
    ColumnSpec(name="location", gen=StructColumn(fields=[
        ColumnSpec(name="country", gen=ValuesColumn(values=["US", "CA", "UK"])),
        ColumnSpec(name="region", gen=ValuesColumn(values=["North", "South", "East", "West"])),
    ])),
])
```

### Pattern 3: Audit Metadata

```python
struct("audit", fields=[
    ColumnSpec(name="created_at", gen=TimestampColumn(start="2024-01-01", end="2025-12-31")),
    ColumnSpec(name="created_by", gen=RangeColumn(min=1, max=1000)),
    ColumnSpec(name="updated_at", gen=TimestampColumn(start="2024-01-01", end="2025-12-31"),
               nullable=True, null_fraction=0.5),
    ColumnSpec(name="updated_by", gen=RangeColumn(min=1, max=1000),
               nullable=True, null_fraction=0.5),
])
```

---

## Next Steps

- See [CDC examples](./cdc-recipes.md) to add change tracking to nested tables
- See [API Reference](../reference/api.md) for full struct/array API documentation
- See [Schema Models](../reference/schema-models.md) for `StructColumn` and `ArrayColumn` field details
