---
sidebar_position: 5
title: "Expected State Verification"
---

# Expected State

> **TL;DR:** `generate_expected_state()` computes what your table should look like after applying N batches of CDC. Use it to verify that your ETL pipeline correctly applied all inserts, updates, and deletes. Perfect for testing Delta Live Tables, SCD2 implementations, and CDC consumers.

## What is Expected State?

When you apply CDC changes to a table, each batch modifies the table's contents:
- **Inserts** add new rows
- **Updates** modify existing rows
- **Deletes** remove rows

After applying batches 1..N, the table should contain a specific set of rows with specific values. **Expected state** is that exact set of rows.

dbldatagen.v1 can compute expected state **without materializing the intermediate batches**. It replays all changes through batch N using metadata, then generates the final live rows.

---

## Function Signature

```python
def generate_expected_state(
    spark: SparkSession,
    plan_or_stream: CDCPlan | CDCStream | DataGenPlan,
    table_name: str,
    batch_id: int,
) -> DataFrame
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_stream` | `CDCPlan \| CDCStream \| DataGenPlan` | Your CDC plan or stream |
| `table_name` | `str` | Name of the table to compute state for |
| `batch_id` | `int` | Target batch (0 = initial, 1+ = after applying that batch) |

### Returns

A DataFrame containing all **live rows** at batch `batch_id`, with their **current values**.

---

## How It Works

### Step 1: Replay Metadata for Batches 1..N

For each batch:
1. Compute which rows were inserted
2. Track which rows were deleted
3. Track which rows were updated and which columns mutated

**No DataFrames are materialized.** Only row IDs and mutation flags are tracked.

### Step 2: Determine Live Rows

After replaying all batches, compute the set of live row IDs:

```
live_rows = (initial_rows + all_inserts) - all_deletes
```

### Step 3: Generate Current Values

For each live row, generate its **current** column values:
- If the row was never updated, use its initial generation seed
- If the row was updated, use the seed from the **last** update batch

This produces a DataFrame with:
- All live rows (no deleted rows)
- Current column values (reflecting all updates)
- No CDC metadata columns (just the data columns)

---

## Use Case: Verify Pipeline Output

The primary use case is **testing your CDC pipeline**.

### Pattern: Generate → Apply → Compare

```python
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state

# 1. Generate CDC stream
stream = generate_cdc(spark, plan)

# 2. Apply changes to your pipeline
my_pipeline.load_initial(stream.initial["orders"])
for batch in stream.batches:
    my_pipeline.apply_cdc(batch["orders"])

# 3. Get expected state
expected = generate_expected_state(spark, stream, "orders", batch_id=len(stream.batches))

# 4. Get actual pipeline output
actual = spark.table("my_pipeline.orders")

# 5. Compare
diff = expected.subtract(actual)
assert diff.count() == 0, f"Pipeline output differs: {diff.count()} rows"
```

If your pipeline correctly applied all CDC changes, `expected` and `actual` will be identical.

---

## Testing Delta CDF Consumers

Verify that your Delta Lake CDF consumer produces the correct final state.

### Example: Delta CDF Pipeline Test

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker, decimal
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("delta-cdf-test").getOrCreate()

# Define schema
products = TableSpec(
    name="products",
    rows=1000,
    primary_key=PrimaryKey(columns=["product_id"]),
    columns=[
        pk_auto("product_id"),
        faker("name", "catch_phrase"),
        decimal("price", min=1.0, max=1000.0),
    ],
)

base = DataGenPlan(tables=[products], seed=42)
plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    products=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)),
)

# Generate CDC
stream = generate_cdc(spark, plan)

# Write to Delta with CDF enabled
stream.initial["products"] \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("/delta/products")

# Apply change batches
for batch in stream.batches:
    batch["products"] \
        .write \
        .format("delta") \
        .mode("append") \
        .save("/delta/products")

# Read final state from Delta
delta_output = spark.read.format("delta").load("/delta/products")

# Get expected state
expected = generate_expected_state(spark, stream, "products", batch_id=10)

# Compare
assert delta_output.count() == expected.count(), "Row count mismatch"

# Check data equality (order-insensitive)
diff_expected_minus_actual = expected.subtract(delta_output)
diff_actual_minus_expected = delta_output.subtract(expected)

assert diff_expected_minus_actual.count() == 0, "Expected has rows not in Delta output"
assert diff_actual_minus_expected.count() == 0, "Delta output has rows not in expected"

print("Delta CDF pipeline output is correct!")
```

---

## Testing SCD2 Implementations

Verify that your Type 2 Slowly Changing Dimension logic produces correct current records.

### Example: SCD2 Pipeline Test

```python
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state
from dbldatagen.v1.cdc_dsl import cdc_plan, scd2_config

# SCD2 plan: heavy updates, rare deletes
plan = cdc_plan(
    base,
    num_batches=20,
    format="delta_cdf",
    customers=scd2_config(batch_size=0.05),
)

stream = generate_cdc(spark, plan)

# Apply to SCD2 pipeline (your implementation)
scd2_pipeline.load_initial(stream.initial["customers"])
for batch_id, batch in enumerate(stream.batches, start=1):
    scd2_pipeline.apply_scd2_batch(batch["customers"], batch_id)

# Get current (active) records from SCD2 table
scd2_current = spark.table("scd2_pipeline.customers") \
    .filter("is_current = true") \
    .select("customer_id", "name", "tier")  # Drop SCD2 metadata

# Get expected current state
expected = generate_expected_state(spark, stream, "customers", batch_id=20) \
    .select("customer_id", "name", "tier")

# Compare
assert scd2_current.subtract(expected).count() == 0
assert expected.subtract(scd2_current).count() == 0

print("SCD2 current records match expected state!")
```

---

## Incremental Verification

Test your pipeline's correctness at each batch, not just the final state.

```python
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state

stream = generate_cdc(spark, plan)

# Apply batches incrementally and verify after each one
my_pipeline.load_initial(stream.initial["orders"])

for batch_id, batch in enumerate(stream.batches, start=1):
    # Apply this batch
    my_pipeline.apply_cdc(batch["orders"])

    # Verify correctness at this batch
    expected = generate_expected_state(spark, stream, "orders", batch_id=batch_id)
    actual = spark.table("my_pipeline.orders")

    expected_count = expected.count()
    actual_count = actual.count()

    assert expected_count == actual_count, \
        f"Batch {batch_id}: row count mismatch ({expected_count} vs {actual_count})"

    # Optional: full data comparison (expensive for large tables)
    # assert expected.subtract(actual).count() == 0

    print(f"Batch {batch_id} verified: {actual_count} live rows")
```

---

## Spot-Checking Long-Running Pipelines

Verify state at batch 100 without generating/applying batches 1-99:

```python
from dbldatagen.v1.cdc import generate_expected_state

# Your pipeline has applied 100 batches of CDC
# Verify correctness at batch 100 without regenerating all prior batches

expected_at_100 = generate_expected_state(spark, plan, "orders", batch_id=100)
actual_at_100 = spark.table("my_pipeline.orders")

assert expected_at_100.count() == actual_at_100.count()
print(f"Pipeline state at batch 100: {actual_at_100.count()} rows")
```

---

## Practical Testing Pattern

A complete testing workflow for CDC pipelines.

### Setup: Define Plan and Generate CDC

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("cdc-test").getOrCreate()

# Define schema
users = TableSpec(
    name="users",
    rows=10000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("name", "name"),
        faker("email", "email"),
    ],
)

base = DataGenPlan(tables=[users], seed=42)
plan = cdc_plan(base, num_batches=50, users=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)))

# Generate CDC
stream = generate_cdc(spark, plan, format="delta_cdf")
```

### Test 1: Initial Snapshot

```python
# Apply initial snapshot
stream.initial["users"].write.format("delta").mode("overwrite").save("/delta/users")

# Verify initial state
expected_initial = generate_expected_state(spark, stream, "users", batch_id=0)
actual_initial = spark.read.format("delta").load("/delta/users")

assert expected_initial.count() == actual_initial.count()
print(f"Initial snapshot: {actual_initial.count()} rows")
```

### Test 2: Apply First 10 Batches

```python
# Apply batches 1-10
for batch_id in range(1, 11):
    stream.batches[batch_id - 1]["users"] \
        .write.format("delta").mode("append").save("/delta/users")

# Verify state at batch 10
expected_at_10 = generate_expected_state(spark, stream, "users", batch_id=10)
actual_at_10 = spark.read.format("delta").load("/delta/users")

# Use MERGE or DELETE WHERE to apply deletes in Delta
# (depends on your pipeline implementation)

# Compare counts
print(f"Expected live rows at batch 10: {expected_at_10.count()}")
print(f"Actual rows in Delta: {actual_at_10.count()}")
```

### Test 3: Apply All 50 Batches

```python
# Apply remaining batches
for batch_id in range(11, 51):
    stream.batches[batch_id - 1]["users"] \
        .write.format("delta").mode("append").save("/delta/users")

# Verify final state
expected_final = generate_expected_state(spark, stream, "users", batch_id=50)
actual_final = spark.read.format("delta").load("/delta/users")

# Full comparison
diff_expected = expected_final.subtract(actual_final)
diff_actual = actual_final.subtract(expected_final)

assert diff_expected.count() == 0, "Expected has rows not in actual"
assert diff_actual.count() == 0, "Actual has rows not in expected"

print("All 50 batches applied correctly!")
```

---

## Understanding State at Batch 0

`batch_id=0` represents the **initial state** (before any changes):

```python
# Generate initial snapshot
stream = generate_cdc(spark, plan)

# These are equivalent
initial_from_stream = stream.initial["orders"]
initial_from_state = generate_expected_state(spark, plan, "orders", batch_id=0)

assert initial_from_stream.count() == initial_from_state.count()
```

---

## Row Count Evolution Example

Trace how row count changes across batches:

```python
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state

stream = generate_cdc(spark, plan)

print("Batch | Live Rows")
print("------|----------")

# Initial
state_0 = generate_expected_state(spark, stream, "orders", batch_id=0)
print(f"    0 | {state_0.count()}")

# Batches 1-N
for batch_id in range(1, len(stream.batches) + 1):
    state = generate_expected_state(spark, stream, "orders", batch_id=batch_id)
    print(f"{batch_id:5d} | {state.count()}")

# Example output:
# Batch | Live Rows
# ------|----------
#     0 |     10000
#     1 |     10080  (+100 inserts, -20 deletes = +80)
#     2 |     10150  (+90 inserts, -20 deletes = +70)
#   ...
```

---

## Foreign Key Verification

Expected state respects FK relationships. If a parent table has deletes disabled (FK parent guard), the expected state will reflect that.

```python
# orders.customer_id references customers.customer_id
# CDC disables deletes on customers (FK parent guard)

plan = cdc_plan(
    base,
    num_batches=10,
    customers=cdc_config(operations=ops(2, 7, 1)),  # delete weight ignored
    orders=cdc_config(operations=ops(3, 5, 2)),
)

stream = generate_cdc(spark, plan)

# Expected state for customers: no rows deleted
expected_customers = generate_expected_state(spark, stream, "customers", batch_id=10)
initial_customers = stream.initial["customers"].count()
print(f"Initial customers: {initial_customers}")
print(f"Final customers: {expected_customers.count()}")
# Final >= initial (only inserts and updates, no deletes)

# Expected state for orders: deletes allowed
expected_orders = generate_expected_state(spark, stream, "orders", batch_id=10)
initial_orders = stream.initial["orders"].count()
print(f"Initial orders: {initial_orders}")
print(f"Final orders: {expected_orders.count()}")
# Final can be < initial (deletes are applied)
```

---

## Complete Example: End-to-End Test

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, fk, faker, decimal
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("expected-state-test").getOrCreate()

# Define schema
accounts = TableSpec(
    name="accounts",
    rows=5000,
    primary_key=PrimaryKey(columns=["account_id"]),
    columns=[
        pk_auto("account_id"),
        faker("name", "name"),
    ],
)

transactions = TableSpec(
    name="transactions",
    rows=50000,
    primary_key=PrimaryKey(columns=["txn_id"]),
    columns=[
        pk_auto("txn_id"),
        fk("account_id", "accounts.account_id"),
        decimal("amount", min=1.0, max=10000.0),
    ],
)

base = DataGenPlan(tables=[accounts, transactions], seed=42)
plan = cdc_plan(
    base,
    num_batches=10,
    format="raw",
    accounts=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    transactions=cdc_config(batch_size=0.10, operations=ops(4, 4, 2)),
)

# Generate CDC
stream = generate_cdc(spark, plan)

# Write to Parquet (simulating pipeline storage)
stream.initial["accounts"].write.mode("overwrite").parquet("/pipeline/accounts")
stream.initial["transactions"].write.mode("overwrite").parquet("/pipeline/transactions")

for batch_id, batch in enumerate(stream.batches, start=1):
    batch["accounts"].write.mode("append").parquet("/pipeline/accounts")
    batch["transactions"].write.mode("append").parquet("/pipeline/transactions")

# Verify expected state at batch 10
expected_accounts = generate_expected_state(spark, stream, "accounts", batch_id=10)
expected_transactions = generate_expected_state(spark, stream, "transactions", batch_id=10)

# Read actual pipeline output (would include your CDC application logic)
actual_accounts = spark.read.parquet("/pipeline/accounts")
actual_transactions = spark.read.parquet("/pipeline/transactions")

print(f"Expected accounts: {expected_accounts.count()}")
print(f"Actual accounts: {actual_accounts.count()}")
print(f"Expected transactions: {expected_transactions.count()}")
print(f"Actual transactions: {actual_transactions.count()}")

# In a real test, apply your CDC logic (MERGE, DELETE) then compare
# For this example, we assume a naive append-only pipeline
```

---

**Related:** [CDC Overview](./overview.md) | [Batch Independence](./batch-independence.md) | [API Reference](../reference/api.md)
