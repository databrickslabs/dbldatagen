---
sidebar_position: 6
title: "Liquid Clustering"
description: "Generate synthetic data to learn and benchmark Delta Lake Liquid Clustering — hands-on guide with performance experiments"
keywords: [dbldatagen, liquid clustering, delta lake, performance, benchmark, optimize, file pruning, databricks]
---

# Liquid Clustering: Hands-On Performance Guide

> **TL;DR:** Generate a 10M-row retail fact table, write it to Delta with and without Liquid Clustering, run the same queries against both, and measure the difference in file pruning and query speed.

## What is Liquid Clustering?

Liquid Clustering is a Delta Lake feature that automatically organizes data files so queries that filter on the clustering columns can skip irrelevant files. It replaces the older `ZORDER BY` approach with several advantages:

- **Change columns anytime** — `ALTER TABLE ... CLUSTER BY (new_cols)` takes effect on the next `OPTIMIZE`, no full rewrite needed.
- **Handles mixed cardinality** — uses Hilbert space-filling curves internally, so combining a low-cardinality column (region) with a high-cardinality column (timestamp) works well.
- **Incremental** — `OPTIMIZE` only re-clusters new/changed data, not the whole table.

The key question is always: **which columns should I cluster by?** This guide gives you a dataset and experiments to answer that empirically.

---

## The Dataset

A retail analytics star schema with columns at different cardinalities — ideal for testing which clustering choices matter.

```
stores (200 rows)           products (5K rows)       customers (100K rows)
   └── transactions.store_id    └── transactions.product_id   └── transactions.customer_id

transactions (10M rows)
  txn_id          — PK, auto-increment
  store_id        — FK, 200 distinct values (low cardinality)
  product_id      — FK, 5K distinct values (medium cardinality, Zipf-skewed)
  customer_id     — FK, 100K distinct values (high cardinality, Zipf-skewed)
  quantity        — int, 1-20
  amount          — double, 1.99-4999.99
  payment_method  — 5 distinct values (very low cardinality)
  channel         — 3 distinct values (very low cardinality)
  region          — 6 distinct values (denormalized, direct filter target)
  category        — 10 distinct values (denormalized, direct filter target)
  txn_date        — timestamp, 2023-01-01 to 2025-12-31
```

:::tip Why denormalize region and category?
Liquid Clustering only prunes files based on columns *in the scanned table*. If `region` only exists in `stores`, a query filtering `WHERE region = 'Northeast'` requires a join first — the optimizer can't prune transaction files before the join. By denormalizing `region` and `category` directly into `transactions`, `CLUSTER BY (txn_date, region)` enables direct file pruning without any joins.
:::

:::info Why this schema works for benchmarking
The `transactions` table has columns spanning the full cardinality spectrum. This lets you test clustering on different column combinations and see exactly when it helps vs. when it makes no difference.
:::

---

## Step 1: Generate the Data

### Option A: YAML Plan (Databricks Notebook)

Upload `examples/yaml/11_liquid_clustering.yml` and run:

```python
from dbldatagen.v1 import generate, DataGenPlan
import yaml

with open("/path/to/11_liquid_clustering.yml") as f:
    plan = DataGenPlan(**yaml.safe_load(f))

dfs = generate(spark, plan)

# Write dimension tables as regular Delta
for name in ["stores", "products", "customers"]:
    dfs[name].write.mode("overwrite").format("delta").saveAsTable(f"benchmark.{name}")
```

### Option B: Python API

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, pk_pattern, fk, faker, integer, decimal, text, timestamp,
)
from dbldatagen.v1.schema import Zipf

plan = DataGenPlan(
    seed=2000,
    tables=[
        TableSpec(name="stores", rows=200,
                  primary_key=PrimaryKey(columns=["store_id"]),
                  columns=[
                      pk_pattern("store_id", "STORE-{seq:4}"),
                      faker("store_name", "company"),
                      faker("city", "city"),
                      text("region", ["Northeast", "Southeast", "Midwest",
                                       "Southwest", "West", "Northwest"]),
                      text("store_type", ["flagship", "standard", "outlet", "express"]),
                  ]),
        TableSpec(name="products", rows=5_000,
                  primary_key=PrimaryKey(columns=["product_id"]),
                  columns=[
                      pk_pattern("product_id", "PROD-{seq:6}"),
                      faker("product_name", "catch_phrase"),
                      text("category", ["Electronics", "Clothing", "Home", "Books",
                                         "Sports", "Food", "Toys", "Health",
                                         "Beauty", "Garden"]),
                      faker("brand", "company"),
                      decimal("unit_price", min=1.99, max=999.99),
                  ]),
        TableSpec(name="customers", rows=100_000,
                  primary_key=PrimaryKey(columns=["customer_id"]),
                  columns=[
                      pk_pattern("customer_id", "CUST-{seq:7}"),
                      faker("name", "name"),
                      faker("email", "email"),
                      faker("state", "state_abbr"),
                      text("tier", ["bronze", "silver", "gold", "platinum"]),
                  ]),
        TableSpec(name="transactions", rows=10_000_000,
                  primary_key=PrimaryKey(columns=["txn_id"]),
                  columns=[
                      pk_auto("txn_id"),
                      fk("store_id", "stores.store_id"),
                      fk("product_id", "products.product_id",
                         distribution=Zipf(exponent=1.3)),
                      fk("customer_id", "customers.customer_id",
                         distribution=Zipf(exponent=1.5)),
                      integer("quantity", min=1, max=20),
                      decimal("amount", min=1.99, max=4999.99),
                      text("payment_method", ["credit_card", "debit_card", "cash",
                                               "digital_wallet", "gift_card"]),
                      text("channel", ["in_store", "online", "mobile"]),
                      text("region", ["Northeast", "Southeast", "Midwest",
                                       "Southwest", "West", "Northwest"]),
                      text("category", ["Electronics", "Clothing", "Home", "Books",
                                         "Sports", "Food", "Toys", "Health",
                                         "Beauty", "Garden"]),
                      timestamp("txn_date", start="2023-01-01", end="2025-12-31"),
                  ]),
    ],
)

dfs = generate(spark, plan)
```

---

## Step 2: Create Unclustered vs. Clustered Tables

Write the same 10M-row fact table twice — once without clustering, once with.

```sql
-- Unclustered baseline
CREATE TABLE benchmark.txn_unclustered
USING DELTA
AS SELECT * FROM transactions_temp_view;

-- Clustered on txn_date + region
CREATE TABLE benchmark.txn_clustered
USING DELTA
CLUSTER BY (txn_date, region)
AS SELECT * FROM transactions_temp_view;
```

Or in PySpark:

```python
# Write unclustered
dfs["transactions"].write.mode("overwrite").format("delta") \
    .saveAsTable("benchmark.txn_unclustered")

# Write clustered
spark.sql("CREATE TABLE benchmark.txn_clustered USING DELTA CLUSTER BY (txn_date, region) AS SELECT * FROM benchmark.txn_unclustered")
```

Now trigger the clustering layout:

```sql
OPTIMIZE benchmark.txn_clustered;
```

---

## Step 3: Compare File Layout

```sql
DESCRIBE DETAIL benchmark.txn_unclustered;
DESCRIBE DETAIL benchmark.txn_clustered;
```

| Metric | Unclustered | Clustered (after OPTIMIZE) |
|---|---|---|
| `numFiles` | Many small files | Fewer, larger files |
| `clusteringColumns` | `[]` | `[txn_date, store_id]` |

The clustered table should have fewer files because `OPTIMIZE` compacts and co-locates rows with similar `txn_date` and `store_id` values.

---

## Step 4: Run Benchmark Queries

Run each query against both tables and compare the metrics from the **Query Profile** (SQL Warehouse) or **Spark UI** (notebook cluster).

### Query 1: Time-Range Filter

This query benefits heavily from clustering on `txn_date` because it filters a narrow date range out of 3 years of data.

```sql
-- Against unclustered
SELECT COUNT(*), SUM(amount)
FROM benchmark.txn_unclustered
WHERE txn_date BETWEEN '2025-06-01' AND '2025-06-30';

-- Against clustered
SELECT COUNT(*), SUM(amount)
FROM benchmark.txn_clustered
WHERE txn_date BETWEEN '2025-06-01' AND '2025-06-30';
```

**What to check in Query Profile:**
- `files read` vs `files pruned` on the Scan operator
- Total `bytes scanned`
- Query duration

**Expected result:** The clustered table should prune 80-95% of files because only a few files contain June 2025 data.

### Query 2: Region + Time Filter (Two-Column Predicate)

This is where Liquid Clustering really shines — it can prune on both dimensions simultaneously. Because `region` is denormalized directly into `transactions`, no join is needed and the optimizer can prune files on both columns.

```sql
-- Against unclustered
SELECT region, COUNT(*), SUM(amount)
FROM benchmark.txn_unclustered
WHERE region = 'Northeast'
  AND txn_date BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY region;

-- Against clustered
SELECT region, COUNT(*), SUM(amount)
FROM benchmark.txn_clustered
WHERE region = 'Northeast'
  AND txn_date BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY region;
```

**Expected result:** Even more pruning than Query 1 because both clustering columns are filtered.

### Query 3: Channel Filter (Very Low Cardinality)

This query filters on `channel` which is NOT a clustering column.

```sql
SELECT channel, COUNT(*), SUM(amount)
FROM benchmark.txn_unclustered
WHERE channel = 'online'
GROUP BY channel;

SELECT channel, COUNT(*), SUM(amount)
FROM benchmark.txn_clustered
WHERE channel = 'online'
GROUP BY channel;
```

**Expected result:** No significant difference — clustering on `txn_date, region` doesn't help filter on `channel`. This demonstrates that clustering only helps queries that filter on the clustering columns.

### Query 4: Customer Lookup (High Cardinality, Not Clustered)

```sql
SELECT *
FROM benchmark.txn_unclustered
WHERE customer_id = 'CUST-0000042';

SELECT *
FROM benchmark.txn_clustered
WHERE customer_id = 'CUST-0000042';
```

**Expected result:** No improvement — `customer_id` is not in the clustering key. This motivates Experiment 2 below.

---

## Step 5: Experiment with Different Clustering Columns

### Experiment 1: Cluster by `customer_id`

```sql
CREATE TABLE benchmark.txn_by_customer
USING DELTA
CLUSTER BY (customer_id)
AS SELECT * FROM benchmark.txn_unclustered;

OPTIMIZE benchmark.txn_by_customer;
```

Re-run Query 4 — you should now see massive file pruning for customer lookups.

But re-run Query 1 — time-range filtering gets worse because the table is no longer organized by date.

**Lesson:** Clustering optimizes for specific query patterns. You can't optimize for everything.

### Experiment 2: Cluster by `txn_date, customer_id`

```sql
CREATE TABLE benchmark.txn_by_date_customer
USING DELTA
CLUSTER BY (txn_date, customer_id)
AS SELECT * FROM benchmark.txn_unclustered;

OPTIMIZE benchmark.txn_by_date_customer;
```

Re-run both Query 1 and Query 4. You may see moderate improvement on both, but not as good as single-column clustering for either query alone.

**Lesson:** Each additional clustering column dilutes the effectiveness per column. 1-2 columns is the sweet spot.

### Experiment 3: Change Clustering Columns In-Place

This is the "liquid" part — you can change columns without rewriting data.

```sql
-- Start with date clustering
ALTER TABLE benchmark.txn_clustered CLUSTER BY (txn_date);
OPTIMIZE benchmark.txn_clustered;
-- Test Query 1... great performance!

-- Switch to customer clustering
ALTER TABLE benchmark.txn_clustered CLUSTER BY (customer_id);
OPTIMIZE benchmark.txn_clustered;
-- Test Query 4... now this is fast!

-- Remove clustering entirely
ALTER TABLE benchmark.txn_clustered CLUSTER BY NONE;
```

---

## Step 6: Measure with System Tables

Track your experiments using Databricks system tables.

### Query Duration Over Time

```sql
SELECT
  statement_text,
  total_duration_ms,
  read_bytes,
  read_files,
  pruned_files,
  ROUND(pruned_files / (read_files + pruned_files) * 100, 1) AS prune_pct,
  start_time
FROM system.query.history
WHERE statement_text LIKE '%benchmark.txn_%'
  AND start_time > CURRENT_TIMESTAMP - INTERVAL 1 HOUR
ORDER BY start_time;
```

### Table Storage Comparison

```sql
SELECT
  table_name,
  data_size_in_bytes / 1e6 AS size_mb,
  -- Check DESCRIBE DETAIL for file counts
FROM system.information_schema.tables
WHERE table_schema = 'benchmark'
  AND table_name LIKE 'txn_%';
```

---

## Choosing Clustering Columns: Decision Framework

| Column Property | Clustering Benefit | Example in This Dataset |
|---|---|---|
| **High cardinality + common range filter** | Excellent | `txn_date` — most analytical queries filter by date range |
| **Low-medium cardinality + common equality filter** | Good | `region` (6 values), `category` (10 values) — dashboard drill-downs |
| **Medium cardinality + FK join key** | Good | `store_id` (200 values), `product_id` (5K values) |
| **High cardinality + point lookup** | Good (if queries filter on it) | `customer_id` (100K) — customer-level analytics |
| **Very low cardinality** | Minimal | `channel` (3 values), `payment_method` (5 values) — not enough distinct values to create meaningful file boundaries |
| **Columns never filtered on** | None | `quantity`, `amount` — typically aggregated, not filtered |

**Rules of thumb:**
1. Pick 1-2 columns that appear in `WHERE` clauses of your most important queries.
2. Prefer columns with at least 100+ distinct values.
3. Timestamp columns are almost always a good first choice for analytical workloads.
4. If you have a multi-tenant system, `tenant_id` + `event_date` is a common winning combo.

---

## Scaling Up

For a more realistic benchmark, increase the transaction count:

```yaml
tables:
  - name: transactions
    rows: 100000000    # 100M rows
```

At 100M rows on Databricks serverless, the difference between clustered and unclustered becomes dramatic — expect 5-10x reduction in bytes scanned for well-matched queries.

---

## Next Steps

- **CDC for incremental loads**: Use [CDC mode](../cdc/overview.md) to simulate ongoing transaction inserts and test how `OPTIMIZE` handles incremental clustering.
- **Streaming**: Use [streaming mode](../data-generation/streaming.md) for continuous transaction generation to test streaming + periodic `OPTIMIZE`.
- **More distributions**: See [Distributions guide](../core-concepts/distributions.md) to add Normal or LogNormal price distributions for more realistic data.
