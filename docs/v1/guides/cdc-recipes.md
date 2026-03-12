---
sidebar_position: 4
title: "CDC Recipes"
description: "Change Data Capture examples - generate inserts, updates, deletes across multiple batches with all 4 output formats"
keywords: [dbldatagen, cdc, change data capture, delta cdf, debezium, sql server, batch independence]
---

# CDC Recipes: Change Data Capture

> **TL;DR:** Generate realistic change streams (inserts, updates, deletes) across multiple batches. Supports Delta CDF, Debezium, SQL Server CDC, and raw formats. Batches are independent — generate batch 5 without materializing batches 1-4.

## 1. Simple Change Stream

Generate an initial snapshot followed by batches of inserts, updates, and deletes.

:::info What This Demonstrates
- Basic CDC workflow: `generate_cdc()` returns initial snapshot + batches
- Operation weights via `ops(insert, update, delete)`
- Batch size as fraction of initial rows (`batch_size=0.2` = 20% per batch)
- Raw format with `_op` column (I/U/UB/D)
:::

```python
from dbldatagen.v1 import (
    DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, faker, integer, text, timestamp,
)
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define the base table
users = TableSpec(
    name="users",
    rows=100,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("name", "name"),
        text("status", values=["active", "inactive", "pending"]),
        integer("score", min=0, max=100),
        timestamp("updated_at", start="2024-01-01", end="2025-12-31"),
    ],
)

base = DataGenPlan(tables=[users], seed=42)

# Configure CDC
plan = cdc_plan(
    base,
    num_batches=3,
    format="raw",
    users=cdc_config(batch_size=0.2, operations=ops(3, 5, 2)),
)

stream = generate_cdc(spark, plan)

# Initial snapshot (all inserts)
stream.initial["users"].show(5)

# Batch 1 changes (inserts, updates, deletes with _op column)
stream.batches[0]["users"].show(10)
# +-------+----+-------+-----+-------------------+---+
# |user_id|name|status |score|         updated_at|_op|
# +-------+----+-------+-----+-------------------+---+
# |    101|... |active |   42|2024-07-...        |  I|  <- insert
# |     15|... |pending|   88|2024-03-...        | UB|  <- update (before)
# |     15|... |active |   91|2024-11-...        |  U|  <- update (after)
# |     23|... |inactive|  67|2024-02-...        |  D|  <- delete
# ...
```

**Key Points:**
- `ops(3, 5, 2)` = 3 parts insert, 5 parts update, 2 parts delete (relative weights)
- `batch_size=0.2` = each batch affects ~20 rows (20% of 100 initial rows)
- Updates emit **two rows**: before-image (UB) and after-image (U)
- Raw format uses `_op` column: `I` (insert), `U` (update after), `UB` (update before), `D` (delete)

---

## 2. Delta CDF Format with FK

Generate CDC data with Delta Lake CDF format for tables with foreign key relationships.

:::info What This Demonstrates
- Delta CDF format with `_change_type` column
- FK parent delete guard (accounts have deletes auto-disabled)
- Multiple batches with different operation mixes per table
:::

```python
from dbldatagen.v1 import (
    DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, fk, faker, decimal, text, timestamp,
)
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

accounts = TableSpec(
    name="accounts",
    rows=100,
    primary_key=PrimaryKey(columns=["account_id"]),
    columns=[
        pk_auto("account_id"),
        text("account_type", values=["checking", "savings", "credit"]),
        text("status", values=["active", "suspended", "closed"]),
        timestamp("opened_at", start="2020-01-01", end="2025-01-01"),
    ],
)

transactions = TableSpec(
    name="transactions",
    rows=1000,
    primary_key=PrimaryKey(columns=["txn_id"]),
    columns=[
        pk_auto("txn_id"),
        fk("account_id", "accounts.account_id"),
        text("txn_type", values=["deposit", "withdrawal", "transfer"]),
        decimal("amount", min=1.0, max=10000.0),
        timestamp("txn_date", start="2024-01-01", end="2025-12-31"),
    ],
)

base = DataGenPlan(tables=[accounts, transactions], seed=500)

plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",                                    # Delta Lake CDF format
    accounts=cdc_config(batch_size=0.05, operations=ops(1, 8, 1)),
    transactions=cdc_config(batch_size=0.15, operations=ops(4, 4, 2)),
)

stream = generate_cdc(spark, plan)

# Delta CDF format uses _change_type column
stream.batches[0]["transactions"].select(
    "txn_id", "account_id", "amount", "_change_type"
).show(5)
# +------+----------+------+------------------+
# |txn_id|account_id|amount|      _change_type|
# +------+----------+------+------------------+
# |  1001|        42| 500.0|            insert|
# |   156|        12| 200.0|  update_preimage|
# |   156|        12| 350.0| update_postimage|
# |   789|        55| 100.0|            delete|
# ...

# Note: accounts have deletes auto-disabled (FK parent guard)
# because transactions reference accounts.account_id
```

**FK Parent Delete Guard:**

When a table is referenced by foreign keys (like `accounts` referenced by `transactions.account_id`), the CDC engine **automatically disables deletes** for that table to maintain referential integrity. You'll see this in the logs:

```
INFO: Disabling deletes for 'accounts' (has FK dependents)
```

This prevents orphan FK violations in child tables.

---

## 3. Batch Independence and State Verification

Generate any batch independently and verify pipeline output.

:::info What This Demonstrates
- `generate_cdc_batch()` for generating a single batch without materializing prior batches
- `generate_expected_state()` for computing the expected table state after N batches
- Testing downstream pipelines by comparing against expected state
:::

```python
from dbldatagen.v1.cdc import generate_cdc_batch, generate_expected_state

# Generate batch 5 directly, without materialising batches 1-4
batch_5 = generate_cdc_batch(spark, plan, batch_id=5, format="raw")
batch_5["transactions"].show()

# Get expected state of transactions after applying all 5 batches
expected = generate_expected_state(spark, plan, "transactions", batch_id=5)

# Compare against your pipeline's output
pipeline_output = spark.table("my_pipeline.transactions")
diff = expected.subtract(pipeline_output).count()
assert diff == 0, f"Pipeline output differs: {diff} rows"
```

**How This Works:**

The CDC engine uses **metadata replay** to compute table state at any batch without generating DataFrames for prior batches. It tracks:
- Live row IDs (which rows exist)
- Next insert ID (counter for new rows)
- Row versions (how many times each row has been updated)

This allows you to:
1. Generate batch N independently for testing
2. Verify downstream pipeline correctness by comparing against expected state
3. Re-run batches with different seeds/configs without regenerating everything

---

## 4. All Four Output Formats

CDC supports 4 output formats matching common CDC systems:

```python
from dbldatagen.v1.cdc import generate_cdc

# Raw format: _op column with I/U/UB/D
stream_raw = generate_cdc(spark, plan, format="raw")

# Delta CDF: _change_type column matching Delta Lake CDF
stream_delta = generate_cdc(spark, plan, format="delta_cdf")

# SQL Server CDC: __$operation column with numeric codes
stream_sql = generate_cdc(spark, plan, format="sql_server")

# Debezium: op column with c/u/d
stream_deb = generate_cdc(spark, plan, format="debezium")
```

### Format Details

| Format | Column | Insert | Update Before | Update After | Delete |
|--------|--------|--------|---------------|--------------|--------|
| **raw** | `_op` | `I` | `UB` | `U` | `D` |
| **delta_cdf** | `_change_type` | `insert` | `update_preimage` | `update_postimage` | `delete` |
| **sql_server** | `__$operation` | `2` | `3` | `4` | `1` |
| **debezium** | `op` | `c` | (dropped) | `u` | `d` |

**Note:** Debezium format drops before-images (only keeps after-images for updates).

---

## 5. CDC Presets

Pre-configured operation mixes for common use cases:

```python
from dbldatagen.v1.cdc_dsl import (
    cdc_plan, cdc_config,
    append_only_config,   # inserts only (event logs, audit trails)
    high_churn_config,    # heavy updates + deletes (stress testing)
    scd2_config,          # moderate updates, rare deletes (SCD2 testing)
)

plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    events=append_only_config(batch_size=0.2),    # event log: inserts only
    accounts=scd2_config(batch_size=0.05),         # dimension: mostly updates
    temp_data=high_churn_config(batch_size=0.3),   # stress test
)
```

### Preset Details

| Preset | Insert | Update | Delete | Use Case |
|--------|--------|--------|--------|----------|
| **append_only** | 10 | 0 | 0 | Event logs, audit trails, time-series |
| **scd2** | 2 | 7 | 1 | Slowly changing dimensions |
| **high_churn** | 2 | 5 | 3 | Stress testing, high-velocity tables |

---

## 6. Writing CDC to Delta Tables

Apply CDC batches to Delta tables with MERGE operations:

```python
from delta.tables import DeltaTable

# Write initial snapshot
stream.initial["accounts"].write.format("delta").mode("overwrite").saveAsTable("accounts")

# Apply batches
for batch_id, batch_dfs in enumerate(stream.batches, start=1):
    batch_df = batch_dfs["accounts"]

    target = DeltaTable.forName(spark, "accounts")

    # Separate inserts, updates, deletes
    inserts = batch_df.filter(F.col("_change_type") == "insert")
    updates = batch_df.filter(F.col("_change_type") == "update_postimage")
    deletes = batch_df.filter(F.col("_change_type") == "delete")

    # Apply deletes
    if deletes.count() > 0:
        target.alias("target").merge(
            deletes.alias("source"),
            "target.account_id = source.account_id"
        ).whenMatchedDelete().execute()

    # Apply updates
    if updates.count() > 0:
        target.alias("target").merge(
            updates.alias("source"),
            "target.account_id = source.account_id"
        ).whenMatchedUpdateAll().execute()

    # Apply inserts
    if inserts.count() > 0:
        inserts.write.format("delta").mode("append").saveAsTable("accounts")

    print(f"Applied batch {batch_id}")
```

---

## 7. Temporal Clustering with Distribution

Model realistic event timing patterns using distributions on timestamps:

```python
events = TableSpec(
    name="events",
    rows="10M",
    primary_key=PrimaryKey(columns=["event_id"]),
    columns=[
        pk_auto("event_id"),
        text("event_type", values=["login", "view", "purchase"]),
        # Cluster events in the middle 50% of time range (simulating peak hours)
        timestamp("event_time", start="2025-01-01", end="2025-12-31",
                  distribution=Normal(mean=0.5, stddev=0.15)),
    ],
)

base = DataGenPlan(tables=[events], seed=100)

plan = cdc_plan(
    base,
    num_batches=20,
    format="raw",
    events=append_only_config(batch_size=0.1),  # append-only event log
)

stream = generate_cdc(spark, plan)

# Events per month -- should peak mid-year
stream.initial["events"].withColumn("month", F.month("event_time")) \
    .groupBy("month").count() \
    .orderBy("month") \
    .show()
```

---

## 8. Column Mutations on Updates

Control which columns change when rows are updated:

```python
from dbldatagen.v1.cdc_dsl import mutations

plan = cdc_plan(
    base,
    num_batches=5,
    users=cdc_config(
        batch_size=0.1,
        operations=ops(2, 8, 0),  # update-heavy
        mutations_spec=mutations(
            columns=["status", "score"],  # only these columns mutate
            fraction=0.5,                 # 50% of eligible columns change per update
        ),
    ),
)
```

**Mutation Rules:**
- If `columns` is `None`, all non-PK / non-FK columns are eligible
- `fraction` controls what proportion of eligible columns change per row
- PK and FK columns **never** mutate (integrity constraint)

---

## 9. Multi-Table CDC with Dependencies

CDC respects FK relationships and generates tables in dependency order:

```python
plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    orders=cdc_config(batch_size=0.10, operations=ops(5, 4, 1)),
    order_items=cdc_config(batch_size=0.15, operations=ops(6, 3, 1)),
)

stream = generate_cdc(spark, plan)

# Tables are in dependency order: customers, orders, order_items
for table_name in stream.initial.keys():
    print(f"Initial {table_name}: {stream.initial[table_name].count()} rows")

for batch_id, batch_dfs in enumerate(stream.batches, start=1):
    print(f"\nBatch {batch_id}:")
    for table_name in batch_dfs.keys():
        print(f"  {table_name}: {batch_dfs[table_name].count()} changes")
```

**FK Parent Delete Guard:**

Parent tables (referenced by FKs) automatically have deletes disabled to prevent orphan FK violations. In this example:
- `customers` — deletes disabled (referenced by `orders.customer_id`)
- `orders` — deletes disabled (referenced by `order_items.order_id`)
- `order_items` — deletes enabled (no FK dependents)

---

## Next Steps

- See [Architecture](../reference/architecture.md) for CDC engine internals (state replay, batch independence)
- See [API Reference](../reference/api.md) for full CDC API documentation
- See [Nested JSON examples](./nested-json.md) to add structs and arrays to CDC tables
