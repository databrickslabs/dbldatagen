---
sidebar_position: 1
title: "Basic: Your First Table"
description: "Simple synthetic data generation examples - single tables and two-table relationships"
keywords: [dbldatagen, basic examples, faker, foreign keys, simple tables]
---

# Basic Examples

> **TL;DR:** Start here to learn the basics. Generate a single user table with Faker and timestamps, then build a two-table schema with foreign key relationships.

## 1. Simple User Table

A single table with mixed column types. No primary or foreign keys — just data generation.

:::info What This Demonstrates
- Basic column types: Faker, integers, text, timestamps
- Normal distribution for realistic age values
- Creating a complete table with one PrimaryKey
- The `generate()` entry point returns a dict of DataFrames
:::

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, faker, integer, text, timestamp
from dbldatagen.v1.schema import Normal

users = TableSpec(
    name="users",
    rows="1M",
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("first_name", "first_name"),
        faker("last_name", "last_name"),
        faker("email", "email"),
        integer("age", min=18, max=90, distribution=Normal(mean=0.5, stddev=0.2)),
        text("tier", values=["free", "pro", "enterprise"]),
        timestamp("created_at", start="2022-01-01", end="2025-06-30"),
    ],
)

plan = DataGenPlan(tables=[users], seed=42)
dfs = generate(spark, plan)

dfs["users"].show(5)
# +-------+----------+---------+--------------------+---+----+-------------------+
# |user_id|first_name|last_name|               email|age|tier|         created_at|
# +-------+----------+---------+--------------------+---+----+-------------------+
# |      1|     James|   Miller|james.miller@exam..| 34| pro|2023-08-14 10:22:31|
# |      2|     Sarah|  Johnson|sarah.j@example.c..| 42|free|2024-01-07 15:44:12|
# ...
```

**Key Points:**
- `rows="1M"` — human-readable shorthand for 1,000,000 rows
- `pk_auto()` — auto-incrementing integer primary key
- `faker()` columns use [Faker providers](https://faker.readthedocs.io/en/master/providers.html)
- `distribution=Normal(mean=0.5, stddev=0.2)` — ages cluster around the midpoint (54) with realistic spread
- `generate()` returns `dict[str, DataFrame]` keyed by table name

---

## 2. Two Tables with FK Relationship

Customers and orders. Every order references a valid customer.

:::info What This Demonstrates
- Foreign key relationships via `fk()`
- Pattern-based primary keys (`CUST-00000001`)
- Decimal columns for monetary values
- How FKs guarantee referential integrity (every child joins to a parent)
:::

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_pattern, pk_auto, fk, faker, decimal, text, timestamp,
)

customers = TableSpec(
    name="customers",
    rows="500K",
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_pattern("customer_id", "CUST-{digit:8}"),
        faker("name", "name"),
        faker("email", "email"),
        faker("city", "city"),
        text("segment", values=["retail", "wholesale", "government"]),
    ],
)

orders = TableSpec(
    name="orders",
    rows="5M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        timestamp("ordered_at", start="2023-01-01", end="2025-12-31"),
        decimal("total_amount", min=9.99, max=4999.99),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
)

plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)

# Verify FK integrity -- every order joins to a customer
join_count = dfs["orders"].join(dfs["customers"], "customer_id", "inner").count()
assert join_count == dfs["orders"].count()  # True!
```

**Key Points:**
- `pk_pattern()` — template-based PK generation (`{digit:8}` = 8 random digits)
- `fk("customer_id", "customers.customer_id")` — creates a foreign key relationship
- FK values are **guaranteed** to reference valid parent PKs — no orphan rows
- Default FK distribution is `Zipf(exponent=1.2)` for realistic parent selection skew
- Tables are generated in dependency order (customers first, then orders)

**Next Steps:**
- See [Star Schema example](./star-schema.md) for multi-table relationships
- See [Skewed Data example](./skewed-data.md) to model realistic distribution patterns
