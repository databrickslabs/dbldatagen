---
sidebar_position: 4
title: "Batch Independence"
---

# Batch Independence

> **TL;DR:** Generate batch N directly without materializing batches 1..N-1. State replay uses metadata (seeds, weights, row counts) instead of DataFrames to compute which rows are live, which are deleted, and what their current values are. This enables parallel batch generation, selective retry, and spot-checking at any batch ID.

## Key Concept: Generate Batch N Without Prior Batches

Traditional CDC systems require applying all prior changes to compute the current state. dbldatagen.v1 CDC is different:

```python
from dbldatagen.v1.cdc import generate_cdc_batch

# Generate batch 5 directly, without generating batches 1-4
batch_5 = generate_cdc_batch(spark, plan, batch_id=5)
```

**How is this possible?**

Every operation in every batch is a deterministic function of:
1. **Global seed** — The plan's random seed
2. **Batch ID** — Which batch we're generating
3. **Operation weights** — Insert/update/delete fractions
4. **Initial row count** — How many rows existed at batch 0

Given these inputs, we can compute:
- Which rows are inserted in batch N
- Which existing rows are updated in batch N
- Which existing rows are deleted in batch N
- The current value of any column for any live row

This computation happens **without materializing any DataFrames** from prior batches. It's purely metadata-based.

---

## How Stateless Row Lifecycle Works

### The "Three Clocks" Model

Instead of tracking mutable state, the CDC engine assigns each row a deterministic lifecycle from its identity seed alone:

```python
birth_tick(k)   # Row k is born at this batch
death_tick(k)   # Row k is deleted at this batch
update_due(k,t) # Whether row k is due for an update at batch t
is_alive(k, t)  # True if birth_tick(k) <= t < death_tick(k)
```

These are **pure functions** — no state accumulation, no iteration over prior batches.

### Computing Batch N Directly

To generate batch N, the engine:

1. **Computes insert range** — which row indices are born at batch N (`insert_range(N)`)
2. **Scans for deletes** — which alive rows have `death_tick(k) == N` (`delete_indices_at_batch_fast(N)`)
3. **Scans for updates** — which alive rows have `update_due(k, N) == True` (`update_indices_at_batch(N)`)

Each scan is O(batch_size), not O(total_rows). The engine generates **only** the rows involved in batch N.

### Example: Row Lifecycle

For a table with 1000 initial rows, `min_life=3`, inserts_per_batch=50:

```
Row 0:     born at batch 0, death_tick = 5   → alive batches 0-4, deleted at 5
Row 1000:  born at batch 1, death_tick = 8   → alive batches 1-7, deleted at 8
Row 1050:  born at batch 2, death_tick = 4   → alive batches 2-3, deleted at 4
```

At batch 3: `is_alive(0, 3) = True`, `is_alive(1050, 3) = True`, `is_alive(1050, 4) = False`

**No DataFrames from batches 1..N-1 are ever created.** The entire lifecycle is mathematical.

---

## Using `generate_cdc_batch()`

### Function Signature

```python
def generate_cdc_batch(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    batch_id: int,
    format: str | CDCFormat | None = None,
) -> dict[str, DataFrame]
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_base` | `CDCPlan \| DataGenPlan` | Your CDC plan |
| `batch_id` | `int` | Which batch to generate (1-based, 1 = first change batch) |
| `format` | `str \| CDCFormat \| None` | Override format: `"raw"`, `"delta_cdf"`, `"sql_server"`, `"debezium"` |

### Returns

Dictionary of `{table_name: DataFrame}` containing CDC changes for batch `batch_id`.

### Example: Direct Batch Generation

```python
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker
from dbldatagen.v1.cdc import generate_cdc_batch
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define plan
users = TableSpec(
    name="users",
    rows=10000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[pk_auto("user_id"), faker("name", "name")],
)

base = DataGenPlan(tables=[users], seed=42)
plan = cdc_plan(base, num_batches=100, users=cdc_config(batch_size=0.05, operations=ops(3, 5, 2)))

# Generate batch 50 directly (without batches 1-49)
batch_50 = generate_cdc_batch(spark, plan, batch_id=50)

batch_50["users"].show()
# +-------+----+---+
# |user_id|name|_op|
# +-------+----+---+
# |  12345|John|  I|  <- insert
# |   4567|Jane| UB|  <- update before
# |   4567|Jane|  U|  <- update after
# |   8901|Bob |  D|  <- delete
# +-------+----+---+
```

---

## Use Cases

### 1. Parallel Batch Generation

Generate all batches in parallel instead of sequentially:

```python
from pyspark.sql import SparkSession
from dbldatagen.v1.cdc import generate_cdc_batch

spark = SparkSession.builder.appName("parallel-cdc").getOrCreate()

num_batches = 100

# Generate batches 1-100 in parallel (if running on a cluster)
def generate_batch_wrapper(batch_id):
    return (batch_id, generate_cdc_batch(spark, plan, batch_id))

# Using Python multiprocessing or Spark native parallelism
batches = [generate_cdc_batch(spark, plan, i) for i in range(1, num_batches + 1)]

# Write each batch
for batch_id, batch in enumerate(batches, start=1):
    batch["orders"].write.mode("overwrite").parquet(f"/data/cdc/orders/batch_{batch_id}")
```

**Performance:** Generating 100 batches in parallel can be 10-100x faster than sequential generation for large tables.

### 2. Retry Failed Batches

If batch 47 failed to write to storage, regenerate only that batch:

```python
# Original generation failed at batch 47
try:
    for batch_id in range(1, 101):
        batch = generate_cdc_batch(spark, plan, batch_id)
        batch["orders"].write.mode("overwrite").parquet(f"/data/cdc/orders/batch_{batch_id}")
except Exception as e:
    print(f"Failed at batch 47: {e}")

# Later: regenerate only batch 47
batch_47 = generate_cdc_batch(spark, plan, batch_id=47)
batch_47["orders"].write.mode("overwrite").parquet("/data/cdc/orders/batch_47")
```

### 3. Spot-Check Long-Term Behavior

Verify your pipeline handles batch 1000 correctly without generating all 999 prior batches:

```python
# Jump to batch 1000 to test edge cases
batch_1000 = generate_cdc_batch(spark, plan, batch_id=1000)
print(f"Batch 1000 has {batch_1000['orders'].count()} changes")

# Verify pipeline handles it
batch_1000["orders"].write.mode("append").format("delta").save("/pipeline/orders")
```

### 4. Testing Pipeline Backfill

Simulate catching up from batch 1 to batch 50 by generating each batch on-demand:

```python
def backfill_pipeline(start_batch, end_batch):
    for batch_id in range(start_batch, end_batch + 1):
        batch = generate_cdc_batch(spark, plan, batch_id)
        apply_batch_to_pipeline(batch)

backfill_pipeline(1, 50)
```

---

## FK Parent Delete Guard (Cascade Safety)

When a table has foreign key dependents, deletes are automatically disabled to prevent orphaning child rows. This is called **cascade_safe** mode.

### Why This Matters for Batch Independence

State replay must use the **same configuration** as generation. If generation disables deletes for FK parents, state replay must also disable deletes.

```python
# Example: orders.customer_id references customers.customer_id

plan = cdc_plan(
    base,
    num_batches=10,
    customers=cdc_config(operations=ops(2, 7, 1)),  # delete weight = 1
    orders=cdc_config(operations=ops(3, 5, 2)),
)

# During generation:
# - customers: deletes are DISABLED (has FK dependents in orders)
# - orders: deletes are ENABLED

# During state replay for batch 5:
# - customers: deletes are DISABLED (consistent with generation)
# - orders: deletes are ENABLED
```

**Result:** State computation and generation use the same rules, ensuring consistency.

### Viewing FK Parent Guard Status

```python
from dbldatagen.v1.cdc import generate_cdc

stream = generate_cdc(spark, plan, format="raw")

# Check if customers had any deletes in batch 1
customers_batch_1 = stream.batches[0]["customers"]
delete_count = customers_batch_1.filter("_op = 'D'").count()
print(f"Customers deletes in batch 1: {delete_count}")  # Should be 0 if FK parent
```

---

## Practical Example: Generate Batches 1, 5, and 10

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, fk, faker, decimal
from dbldatagen.v1.cdc import generate_cdc_batch
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("batch-independence").getOrCreate()

# Define schema
customers = TableSpec(
    name="customers",
    rows=1000,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[pk_auto("customer_id"), faker("name", "name")],
)

orders = TableSpec(
    name="orders",
    rows=5000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        decimal("amount", min=10.0, max=1000.0),
    ],
)

base = DataGenPlan(tables=[customers, orders], seed=42)
plan = cdc_plan(
    base,
    num_batches=10,
    customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    orders=cdc_config(batch_size=0.10, operations=ops(4, 4, 2)),
)

# Generate only batches 1, 5, and 10 (skip 2-4, 6-9)
for batch_id in [1, 5, 10]:
    batch = generate_cdc_batch(spark, plan, batch_id=batch_id, format="delta_cdf")

    print(f"\n=== Batch {batch_id} ===")
    print(f"Customers changes: {batch['customers'].count()}")
    print(f"Orders changes: {batch['orders'].count()}")

    # Write to storage
    batch["customers"].write.mode("overwrite").parquet(f"/data/cdc/customers/batch_{batch_id}")
    batch["orders"].write.mode("overwrite").parquet(f"/data/cdc/orders/batch_{batch_id}")

# Verify state at batch 10 (includes effects of batches 1-10)
from dbldatagen.v1.cdc import generate_expected_state

expected_customers = generate_expected_state(spark, plan, "customers", batch_id=10)
expected_orders = generate_expected_state(spark, plan, "orders", batch_id=10)

print(f"\nExpected state at batch 10:")
print(f"Customers: {expected_customers.count()} live rows")
print(f"Orders: {expected_orders.count()} live rows")
```

---

## Performance Characteristics

### Batch Generation Complexity

| Operation | Time Complexity | Memory |
|-----------|----------------|---------|
| Generate batch N | O(batch_size) | O(batch_size) |
| Compute row lifecycle (Three Clocks) | O(1) per row | O(1) |
| Full CDC stream (1..N) | O(N * batch_size) | O(batch_size) per chunk |
| Fused multi-batch (bulk) | O(total_rows) | O(chunk_size * batch_size) |

**Key insight:** The Three Clocks model makes batch generation truly O(batch_size) with no iterative state replay. Each row's lifecycle is computed in O(1) from its identity seed.

### When to Use Batch Independence

| Scenario | Best Approach |
|----------|---------------|
| Need all batches sequentially | Use `generate_cdc()` |
| Need batches 1-100 in parallel | Use `generate_cdc_batch()` in parallel |
| Need to retry batch 50 | Use `generate_cdc_batch(batch_id=50)` |
| Need batches 1, 10, 100 only | Use `generate_cdc_batch()` 3 times |
| Need expected state at batch N | Use `generate_expected_state()` |

---

## Limitations and Edge Cases

### 1. Three Clocks Precision

The stateless model uses modular arithmetic for row lifecycle. For very large batch counts (>10,000) combined with extreme operation weight ratios, small floating-point rounding in period computation can accumulate. In practice this is negligible.

### 2. FK Parent Guard Must Be Consistent

If you manually override FK parent delete behavior, ensure state replay uses the same config. This is automatic when using `generate_cdc_batch()` with the same plan.

### 3. Mutation Spec Must Be Deterministic

If you use custom `MutationSpec` with specific columns, ensure the columns exist in all batches.

---

## Complete Example: Parallel + Expected State

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker
from dbldatagen.v1.cdc import generate_cdc_batch, generate_expected_state
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("parallel-batch-independence").getOrCreate()

# Define schema
users = TableSpec(
    name="users",
    rows=100000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[pk_auto("user_id"), faker("name", "name")],
)

base = DataGenPlan(tables=[users], seed=42)
plan = cdc_plan(base, num_batches=50, users=cdc_config(batch_size="5K", operations=ops(3, 5, 2)))

# Generate batches 1-50 (could be parallelized)
batches = {}
for batch_id in range(1, 51):
    batches[batch_id] = generate_cdc_batch(spark, plan, batch_id=batch_id, format="raw")
    print(f"Generated batch {batch_id}: {batches[batch_id]['users'].count()} changes")

# Write batches to storage
for batch_id, batch in batches.items():
    batch["users"].write.mode("overwrite").parquet(f"/data/cdc/users/batch_{batch_id}")

# Verify expected state at batch 50
expected_state = generate_expected_state(spark, plan, "users", batch_id=50)
print(f"\nExpected live rows at batch 50: {expected_state.count()}")

# Compare with naive computation
naive_live = 100000  # initial
for batch_id in range(1, 51):
    batch = spark.read.parquet(f"/data/cdc/users/batch_{batch_id}")
    inserts = batch.filter("_op = 'I'").count()
    deletes = batch.filter("_op = 'D'").count()
    naive_live += (inserts - deletes)

print(f"Naive live row count: {naive_live}")
print(f"Match: {expected_state.count() == naive_live}")
```

---

**Related:** [CDC Overview](./overview.md) | [Expected State](./expected-state.md) | [API Reference](../reference/api.md)
