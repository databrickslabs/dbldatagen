---
sidebar_position: 8
title: "Nested Types"
---

# Nested Types (Structs and Arrays)

> **TL;DR:** dbldatagen.v1 supports struct columns (nested objects) and array columns (variable-length lists) to generate rich JSON output. Define nested schemas in Python or YAML, and Spark's JSON writer handles serialization automatically. Nested types work in both regular generation and CDC batches.

Real-world JSON data has nested structures - address objects with street/city/zip fields, arrays of tags, order line items, and more. dbldatagen.v1 provides first-class support for generating these patterns.

## Struct Columns

A struct column groups multiple child fields into a single nested object. In Spark, this is a `StructType` column. When written as JSON, it produces a nested object.

### Basic Struct Example

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, struct, faker, integer
from dbldatagen.v1.schema import ColumnSpec, ValuesColumn, RangeColumn

users = TableSpec(
    name="users",
    rows=1000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("username", "user_name"),

        # Nested address object
        struct("address", fields=[
            ColumnSpec(name="street", gen=ValuesColumn(
                values=["123 Main St", "456 Oak Ave", "789 Pine Rd"])),
            ColumnSpec(name="city", gen=ValuesColumn(
                values=["New York", "Los Angeles", "Chicago", "Houston"])),
            ColumnSpec(name="state", gen=ValuesColumn(
                values=["NY", "CA", "IL", "TX"])),
            ColumnSpec(name="zip", gen=RangeColumn(min=10000, max=99999)),
        ]),
    ],
)

plan = DataGenPlan(tables=[users], seed=42)
dfs = generate(spark, plan)

# Write as JSON - nested struct serializes naturally
dfs["users"].write.mode("overwrite").json("/tmp/users")
```

**Output JSON:**
```json
{
  "user_id": 1,
  "username": "jdoe42",
  "address": {
    "street": "123 Main St",
    "city": "New York",
    "state": "NY",
    "zip": 10001
  }
}
```

### Accessing Nested Fields

Use dot notation in Spark SQL to access nested struct fields:

```python
dfs["users"].select(
    "user_id",
    "username",
    "address.city",
    "address.zip"
).show(5)
```

### Nested Struct Fields with Faker

Struct fields can use any generation strategy, including Faker:

```python
from dbldatagen.v1 import struct, faker
from dbldatagen.v1.schema import ColumnSpec

columns=[
    pk_auto("customer_id"),

    struct("contact_info", fields=[
        ColumnSpec(name="name", gen=faker("name", "name").gen),
        ColumnSpec(name="email", gen=faker("email", "email").gen),
        ColumnSpec(name="phone", gen=faker("phone", "phone_number").gen),
    ]),
]
```

**Output:**
```json
{
  "customer_id": 1,
  "contact_info": {
    "name": "Jennifer Martinez",
    "email": "jmartinez@example.com",
    "phone": "555-123-4567"
  }
}
```

## Array Columns

An array column generates a variable-length list of values. Each element is generated using the same strategy but with a different seed offset for variety.

### Basic Array Example

```python
from dbldatagen.v1 import array, pk_auto
from dbldatagen.v1.schema import ValuesColumn

products = TableSpec(
    name="products",
    rows=500,
    primary_key=PrimaryKey(columns=["product_id"]),
    columns=[
        pk_auto("product_id"),
        text("name", values=["Laptop", "Mouse", "Keyboard", "Monitor"]),

        # Array of tags - each row gets 1-4 tags
        array("tags",
            element=ValuesColumn(
                values=["sale", "new", "premium", "eco-friendly", "bestseller"]),
            min_length=1,
            max_length=4,
        ),
    ],
)

plan = DataGenPlan(tables=[products], seed=42)
dfs = generate(spark, plan)
```

**Output JSON:**
```json
{
  "product_id": 1,
  "name": "Laptop",
  "tags": ["sale", "premium", "bestseller"]
}
```

### Array of Numbers

Arrays can contain any type, including numbers:

```python
from dbldatagen.v1 import array
from dbldatagen.v1.schema import RangeColumn

columns=[
    pk_auto("review_id"),

    # Array of 3-5 ratings (1-5 stars each)
    array("recent_ratings",
        element=RangeColumn(min=1, max=5),
        min_length=3,
        max_length=5,
    ),
]
```

**Output:**
```json
{
  "review_id": 1,
  "recent_ratings": [4, 5, 3, 5]
}
```

### Variable Array Length

Each row gets a random array length between `min_length` and `max_length`. The length is deterministic per row (same seed always produces the same length).

```python
array("tags",
    element=ValuesColumn(values=["A", "B", "C", "D"]),
    min_length=1,    # Some rows get 1 tag
    max_length=10,   # Some rows get 10 tags
)
```

## Recursive Nesting

You can nest structs within structs, arrays within structs, and structs within arrays to create arbitrarily complex hierarchical data.

### Array of Structs

Model line items, order history, or any one-to-many relationship within a row:

```python
from dbldatagen.v1 import struct, array, pk_auto, decimal, integer
from dbldatagen.v1.schema import ColumnSpec, ValuesColumn, RangeColumn

orders = TableSpec(
    name="orders",
    rows=1000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        faker("customer_name", "name"),

        # Array of line items (each item is a struct)
        array("line_items",
            element=ColumnSpec(
                name="item",  # name is required but not used in output
                gen=StructColumn(fields=[
                    ColumnSpec(name="product_name", gen=ValuesColumn(
                        values=["Laptop", "Mouse", "Keyboard", "Monitor"])),
                    ColumnSpec(name="quantity", dtype="int",
                               gen=RangeColumn(min=1, max=5)),
                    ColumnSpec(name="unit_price", dtype="double",
                               gen=RangeColumn(min=9.99, max=999.99)),
                ]),
            ).gen,
            min_length=1,
            max_length=5,
        ),
    ],
)
```

**Output JSON:**
```json
{
  "order_id": 1,
  "customer_name": "Jennifer Martinez",
  "line_items": [
    {"product_name": "Laptop", "quantity": 1, "unit_price": 899.99},
    {"product_name": "Mouse", "quantity": 2, "unit_price": 24.99}
  ]
}
```

### Struct with Array Fields

```python
columns=[
    pk_auto("product_id"),
    text("name", values=["Laptop", "Tablet"]),

    # Nested struct containing arrays
    struct("metadata", fields=[
        ColumnSpec(name="tags", gen=ArrayColumn(
            element=ValuesColumn(values=["sale", "new", "premium"]),
            min_length=1, max_length=3).gen),
        ColumnSpec(name="ratings", gen=ArrayColumn(
            element=RangeColumn(min=1, max=5),
            min_length=5, max_length=10).gen),
    ]),
]
```

**Output JSON:**
```json
{
  "product_id": 1,
  "name": "Laptop",
  "metadata": {
    "tags": ["sale", "premium"],
    "ratings": [4, 5, 3, 5, 4, 5, 3, 4]
  }
}
```

### Deeply Nested Structs

```python
columns=[
    pk_auto("product_id"),

    struct("manufacturer", fields=[
        ColumnSpec(name="company", gen=ValuesColumn(
            values=["Acme Corp", "TechWorks", "GadgetCo"])),
        ColumnSpec(name="country", gen=ValuesColumn(
            values=["US", "CN", "DE", "JP"])),

        # Nested contact info struct
        ColumnSpec(name="contact", gen=StructColumn(fields=[
            ColumnSpec(name="email", gen=faker("email", "email").gen),
            ColumnSpec(name="phone", gen=faker("phone", "phone_number").gen),
        ])),
    ]),
]
```

**Output JSON:**
```json
{
  "product_id": 1,
  "manufacturer": {
    "company": "Acme Corp",
    "country": "US",
    "contact": {
      "email": "contact@acmecorp.com",
      "phone": "555-123-4567"
    }
  }
}
```

## Complete E-Commerce Example

Here's a realistic example with multiple levels of nesting:

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, struct, array, faker, decimal, integer, text, timestamp,
)
from dbldatagen.v1.schema import ColumnSpec, StructColumn, ArrayColumn, ValuesColumn, RangeColumn

orders = TableSpec(
    name="orders",
    rows=10000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        faker("customer_name", "name"),
        faker("customer_email", "email"),

        # Shipping address (struct)
        struct("shipping_address", fields=[
            ColumnSpec(name="street", gen=faker("street", "street_address").gen),
            ColumnSpec(name="city", gen=faker("city", "city").gen),
            ColumnSpec(name="state", gen=faker("state", "state_abbr").gen),
            ColumnSpec(name="zip", gen=faker("zip", "zipcode").gen),
        ]),

        # Line items (array of structs)
        array("line_items",
            element=ColumnSpec(
                name="item",
                gen=StructColumn(fields=[
                    ColumnSpec(name="product_name", gen=ValuesColumn(
                        values=["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"])),
                    ColumnSpec(name="sku", gen=ValuesColumn(
                        values=["SKU-100", "SKU-200", "SKU-300", "SKU-400", "SKU-500"])),
                    ColumnSpec(name="quantity", dtype="int", gen=RangeColumn(min=1, max=5)),
                    ColumnSpec(name="unit_price", dtype="double", gen=RangeColumn(min=9.99, max=999.99)),
                ]),
            ).gen,
            min_length=1,
            max_length=5,
        ),

        decimal("tax", min=0.0, max=100.0),
        decimal("shipping", min=0.0, max=50.0),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
        timestamp("ordered_at", start="2024-01-01", end="2025-12-31"),
    ],
)

plan = DataGenPlan(tables=[orders], seed=42)
dfs = generate(spark, plan)

# Write as JSON
dfs["orders"].write.mode("overwrite").json("/tmp/orders")
```

## YAML Format for Nested Types

Nested types can be defined in YAML plans:

### Struct in YAML

```yaml
seed: 100
tables:
  - name: products
    rows: 500
    primary_key: {columns: [product_id]}
    columns:
      - name: product_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: name
        dtype: string
        gen:
          strategy: values
          values: [Laptop, Mouse, Keyboard]

      # Nested struct
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
              gen: {strategy: range, min: 1970, max: 2020}
```

### Array in YAML

```yaml
columns:
  - name: product_id
    dtype: long
    gen: {strategy: sequence, start: 1, step: 1}

  # Array column
  - name: tags
    gen:
      strategy: array
      element:
        strategy: values
        values: [sale, new, premium, eco-friendly]
      min_length: 1
      max_length: 4
```

### Array of Structs in YAML

```yaml
columns:
  - name: line_items
    gen:
      strategy: array
      element:
        strategy: struct
        fields:
          - name: product_name
            dtype: string
            gen:
              strategy: values
              values: [Laptop, Mouse, Keyboard]
          - name: quantity
            dtype: int
            gen: {strategy: range, min: 1, max: 5}
          - name: unit_price
            dtype: double
            gen: {strategy: range, min: 9.99, max: 999.99}
      min_length: 1
      max_length: 5
```

## Writing Nested JSON

Spark's JSON writer handles nested types automatically. No special configuration needed:

```python
# Write with nested structs/arrays
dfs["orders"].write.mode("overwrite").json("/tmp/orders")

# Works with partitioning too
dfs["orders"].write.partitionBy("status").mode("overwrite").json("/tmp/orders")

# Single file output (for small datasets)
dfs["orders"].coalesce(1).write.mode("overwrite").json("/tmp/orders")
```

## CDC Compatibility

Nested types work seamlessly in CDC batches. The CDC generator uses the same column expression builder, so structs and arrays are automatically supported:

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Base plan with nested types
base = DataGenPlan(tables=[orders], seed=42)

# CDC plan
plan = cdc_plan(
    base,
    num_batches=5,
    format="delta_cdf",
    orders=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)),
)

stream = generate_cdc(spark, plan)

# Initial snapshot has nested types
stream.initial["orders"].show()

# Batches have nested types too
stream.batches[0]["orders"].show()

# Write CDC batches as JSON
stream.batches[0]["orders"].write.mode("overwrite").json("/tmp/orders_batch_0")
```

## Performance Considerations

Nested types are **Tier 1** operations (pure Spark SQL expressions):

- **Struct columns:** Zero overhead - just wraps child expressions in `F.struct()`
- **Array columns:** Minimal overhead - generates N elements and uses `F.array()` + `F.slice()`

Performance is essentially the same as flat columns with the same underlying types.

## Related Documentation

- [Faker Guide](./faker.md) - Use Faker in struct fields
- [Nullable Columns Guide](./nullable-columns.md) - Make struct/array columns nullable
- [Writing Output Guide](../data-generation/writing-output.md) - Write nested JSON to storage
- [Examples - Nested JSON](../guides/nested-json.md) - See complete nested examples
