---
sidebar_position: 2
title: "Star Schema"
description: "Complete e-commerce star schema with fact and dimension tables, multiple FK relationships, and referential integrity validation"
keywords: [dbldatagen, star schema, fact table, dimension tables, e-commerce, foreign keys, zipf distribution]
---

# Star Schema: E-commerce Analytics

> **TL;DR:** Build a realistic 4-table e-commerce schema (products, customers, orders, order_items) with multiple FK relationships, Zipf-distributed product popularity, and post-generation integrity validation.

## The Schema

A realistic e-commerce star schema with 4 tables and 3 FK relationships:

```
products (10K rows)
  └── order_items.product_id (30M rows)
        └── order_items.order_id
              └── orders.order_id (10M rows)
                    └── orders.customer_id
                          └── customers.customer_id (1M rows)
```

:::info What This Demonstrates
- Multi-table FK relationships spanning 3 levels
- Zipf distribution for realistic "hot products" (power-law sales)
- Normal distribution for product pricing
- UUID primary keys
- Computed columns with `expression()`
- Post-generation validation with `validate_referential_integrity()`
:::

## Full Implementation

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, pk_pattern, pk_uuid, fk,
    faker, integer, decimal, text, timestamp, expression,
)
from dbldatagen.v1.schema import Normal, Zipf

products = TableSpec(
    name="products",
    rows=10_000,
    primary_key=PrimaryKey(columns=["product_id"]),
    columns=[
        pk_pattern("product_id", "PROD-{digit:6}"),
        faker("product_name", "catch_phrase"),
        text("category", values=[
            "Electronics", "Clothing", "Home", "Books",
            "Sports", "Toys", "Food", "Health",
        ]),
        decimal("unit_price", min=1.99, max=999.99,
                distribution=Normal(mean=49.99, stddev=30.0)),
        integer("stock_qty", min=0, max=5000),
    ],
)

customers = TableSpec(
    name="customers",
    rows="1M",
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        faker("first_name", "first_name"),
        faker("last_name", "last_name"),
        faker("email", "email"),
        faker("address", "street_address"),
        faker("city", "city"),
        faker("state", "state_abbr"),
        faker("zip_code", "zipcode"),
        timestamp("registered_at", start="2020-01-01", end="2025-06-30"),
    ],
)

orders = TableSpec(
    name="orders",
    rows="10M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_uuid("order_id"),
        fk("customer_id", "customers.customer_id"),
        timestamp("ordered_at", start="2023-06-01", end="2025-12-31"),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
)

order_items = TableSpec(
    name="order_items",
    rows="30M",
    primary_key=PrimaryKey(columns=["item_id"]),
    columns=[
        pk_auto("item_id"),
        fk("order_id", "orders.order_id"),
        fk("product_id", "products.product_id",
           distribution=Zipf(exponent=1.3)),  # some products more popular
        integer("quantity", min=1, max=10),
        decimal("unit_price", min=1.99, max=999.99),
        expression("line_total", "quantity * unit_price"),
    ],
)

plan = DataGenPlan(
    tables=[products, customers, orders, order_items],
    seed=12345,
    default_locale="en_US",
)

dfs = generate(spark, plan)

# All FK joins work
print(f"Products:    {dfs['products'].count():>12,}")
print(f"Customers:   {dfs['customers'].count():>12,}")
print(f"Orders:      {dfs['orders'].count():>12,}")
print(f"Order Items: {dfs['order_items'].count():>12,}")
```

**Output:**
```
Products:           10,000
Customers:       1,000,000
Orders:         10,000,000
Order Items:    30,000,000
```

---

## Understanding the FK Relationships

### 1. `orders.customer_id` → `customers.customer_id`

Every order belongs to exactly one customer. The default `Zipf(exponent=1.2)` distribution means some customers are "power users" with many orders, while most have only a few.

```python
# Top 10 customers by order count
dfs["orders"].groupBy("customer_id") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10) \
    .show()
```

### 2. `order_items.order_id` → `orders.order_id`

Each order can have multiple line items (1-N relationship). Order items reference the parent order via UUID.

### 3. `order_items.product_id` → `products.product_id`

This FK uses **Zipf(exponent=1.3)** — a higher exponent than the default. This creates "hot products" where a small subset of products appear in most orders (bestsellers), while the long tail barely sells.

```python
# Top 10 bestselling products
dfs["order_items"].groupBy("product_id") \
    .agg(F.count("*").alias("orders"), F.sum("quantity").alias("units")) \
    .orderBy("orders", ascending=False) \
    .limit(10) \
    .join(dfs["products"], "product_id") \
    .select("product_id", "product_name", "category", "orders", "units") \
    .show(truncate=False)
```

---

## Post-Generation Validation

Verify that all FK relationships are valid using the built-in validator:

```python
from dbldatagen.v1.validation import validate_referential_integrity

errors = validate_referential_integrity(dfs, plan)
if errors:
    for e in errors:
        print(f"ERROR: {e}")
else:
    print("✓ All FK relationships are valid!")
```

This validator uses left anti joins to detect orphan FK values. It's optional but useful for testing and debugging complex schemas.

**Expected output:**
```
✓ All FK relationships are valid!
```

---

## Querying the Star Schema

### Revenue by Category

```python
dfs["order_items"] \
    .join(dfs["products"], "product_id") \
    .groupBy("category") \
    .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue")) \
    .orderBy("revenue", ascending=False) \
    .show()
```

### Customer Lifetime Value (Top 10)

```python
dfs["order_items"] \
    .join(dfs["orders"], "order_id") \
    .groupBy("customer_id") \
    .agg(F.sum("line_total").alias("lifetime_value")) \
    .orderBy("lifetime_value", ascending=False) \
    .limit(10) \
    .join(dfs["customers"], "customer_id") \
    .select("customer_id", "first_name", "last_name", "email", "lifetime_value") \
    .show(truncate=False)
```

### Orders per Month

```python
dfs["orders"] \
    .withColumn("month", F.date_trunc("month", "ordered_at")) \
    .groupBy("month") \
    .count() \
    .orderBy("month") \
    .show(24)
```

---

## Next Steps

- **More skew**: See [Skewed Data examples](./skewed-data.md) for extreme Zipf distributions and cybersecurity attack patterns
- **Change tracking**: See [CDC examples](./cdc-recipes.md) to add inserts/updates/deletes over time
- **Nested data**: See [Nested JSON examples](./nested-json.md) to add structs and arrays

---

## Full Code with Validation

```python
from dbldatagen.v1 import generate, DataGenPlan
from dbldatagen.v1.validation import validate_referential_integrity

# ... (table definitions from above)

plan = DataGenPlan(tables=[products, customers, orders, order_items], seed=12345)
dfs = generate(spark, plan)

# Validate integrity
errors = validate_referential_integrity(dfs, plan)
assert errors == [], f"Integrity errors: {errors}"

# Write to storage
for name, df in dfs.items():
    df.write.mode("overwrite").format("delta").saveAsTable(f"ecommerce.{name}")
```
