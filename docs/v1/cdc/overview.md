---
sidebar_position: 1
title: "CDC Overview"
---

# CDC Overview

> **TL;DR:** dbldatagen.v1 CDC generates deterministic Change Data Capture streams: an initial snapshot followed by batches of inserts, updates, and deletes. Perfect for testing Delta Lake CDF pipelines, Debezium consumers, and SCD2 implementations without manually crafting synthetic change events.

## What is CDC in dbldatagen.v1?

CDC (Change Data Capture) generation produces realistic change streams that simulate how production databases evolve over time. Instead of generating static tables, you get:

1. **Initial snapshot** — The full table at time T₀ (all rows as inserts)
2. **Batch 1..N** — Deterministic sequences of insert/update/delete operations

Each batch represents a delta of changes that occurred since the previous batch. This mirrors real-world CDC systems like:
- Delta Lake Change Data Feed (CDF)
- Debezium connectors
- SQL Server CDC
- Database replication logs

## Quick Start

Define your base table plan, wrap it in a CDC plan, and generate the stream:

```python
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker, text
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define base table schema
users = TableSpec(
    name="users",
    rows=1000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("name", "name"),
        text("status", values=["active", "inactive", "pending"]),
    ],
)

base = DataGenPlan(tables=[users], seed=42)

# Configure CDC: 5 batches, 20% of rows change per batch
# Operation mix: 30% inserts, 50% updates, 20% deletes
plan = cdc_plan(
    base,
    num_batches=5,
    format="raw",
    users=cdc_config(batch_size=0.2, operations=ops(3, 5, 2)),
)

# Generate the stream
stream = generate_cdc(spark, plan)

# Access the data
initial = stream.initial["users"]       # DataFrame with all 1000 rows
batch_1 = stream.batches[0]["users"]     # DataFrame with ~200 change rows
batch_2 = stream.batches[1]["users"]     # Next batch of changes
```

## The CDCStream Dataclass

`generate_cdc()` returns a `CDCStream` object with three attributes:

```python
from dbldatagen.v1.cdc import CDCStream

@dataclass
class CDCStream:
    initial: dict[str, DataFrame]                      # Full initial snapshots
    batches: _LazyBatchList | list[dict[str, DataFrame]]  # Change batches (lazy, generated on access)
    plan: CDCPlan | None                               # The plan used (None for bulk streams)
```

**Note:** `batches` is a lazy list — batches are generated on demand when accessed by index and cached for reuse. This means memory usage stays low even for hundreds of batches.

**Access pattern:**
```python
# Initial data
for table_name, df in stream.initial.items():
    df.write.mode("overwrite").parquet(f"/data/initial/{table_name}")

# Change batches
for batch_id, batch_dict in enumerate(stream.batches, start=1):
    for table_name, df in batch_dict.items():
        df.write.mode("append").parquet(f"/data/cdc/{table_name}/batch={batch_id}")
```

## When to Use CDC Generation

### 1. Testing Delta Lake CDF Pipelines

If you consume Delta Lake Change Data Feed, CDC generation gives you realistic test data:

```python
# Generate CDC in Delta CDF format
stream = generate_cdc(spark, plan, format="delta_cdf")

# Write to Delta with CDF enabled
stream.initial["orders"].write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("/data/delta/orders")

# Apply changes
for batch in stream.batches:
    batch["orders"].write.format("delta").mode("append").save("/data/delta/orders")

# Now your pipeline can read CDF and you can verify correctness
```

### 2. Testing Debezium Consumers

Generate Debezium-formatted CDC events to test your Kafka consumers:

```python
stream = generate_cdc(spark, plan, format="debezium")

# Write to Kafka or files
for batch_id, batch in enumerate(stream.batches, start=1):
    batch["products"].write \
        .format("kafka") \
        .option("topic", "dbserver1.inventory.products") \
        .save()
```

### 3. Testing SCD Type 2 Implementations

Generate realistic update patterns to validate your slowly changing dimension logic:

```python
from dbldatagen.v1.cdc_dsl import scd2_config

# Heavy updates, rare inserts/deletes
plan = cdc_plan(
    base,
    num_batches=10,
    customers=scd2_config(batch_size=0.05),  # 5% changes per batch
)
```

### 4. Stress Testing ETL Pipelines

Test how your pipeline handles high-churn scenarios:

```python
from dbldatagen.v1.cdc_dsl import high_churn_config

# 60% updates, 30% deletes, 10% inserts
plan = cdc_plan(
    base,
    num_batches=20,
    transactions=high_churn_config(batch_size=0.3),  # 30% per batch
)
```

### 5. Testing Append-Only Pipelines

Generate insert-only streams for event logs and audit trails:

```python
from dbldatagen.v1.cdc_dsl import append_only_config

plan = cdc_plan(
    base,
    num_batches=10,
    events=append_only_config(batch_size=0.1),  # inserts only
)
```

## Key Features

### Deterministic by Default

Same plan + same seed = identical CDC stream, every time. This enables:
- **Reproducible tests** — Your CI pipeline generates the same data on every run
- **Debugging** — Reproduce failures locally with the exact same data
- **Data diffs** — Compare pipeline outputs across code changes

### Referential Integrity Maintained

Foreign key relationships are preserved across batches. When a parent table has FK dependents:
- **Deletes are automatically disabled** on the parent (cascade_safe mode)
- **Inserts preserve FK validity** — new child rows reference existing parents
- **Updates maintain relationships** — FK columns don't mutate by default

```python
# orders.customer_id references customers.customer_id
# CDC will NOT delete from customers (could orphan orders)
plan = cdc_plan(
    base,
    num_batches=5,
    customers=cdc_config(operations=ops(2, 7, 1)),  # delete weight is ignored
    orders=cdc_config(operations=ops(3, 5, 2)),
)
```

### Multiple Output Formats

One CDC plan, four different output formats. Switch formats without changing your plan:

```python
# Same plan, different consumers
stream_raw = generate_cdc(spark, plan, format="raw")
stream_delta = generate_cdc(spark, plan, format="delta_cdf")
stream_sql = generate_cdc(spark, plan, format="sql_server")
stream_deb = generate_cdc(spark, plan, format="debezium")
```

See [CDC Formats](./formats.md) for details on each format's schema.

### Batch Independence

Generate any batch directly without materializing prior batches. Useful for:
- **Parallel batch generation** — Generate batches 1-10 in parallel
- **Retry failed batches** — Regenerate batch 7 without regenerating 1-6
- **Spot-checking** — Jump to batch 100 to verify long-term behavior

See [Batch Independence](./batch-independence.md) for details.

### Expected State Verification

Generate the expected table state at any batch to verify your pipeline:

```python
from dbldatagen.v1.cdc import generate_expected_state

# What should the table look like after applying 5 batches of CDC?
expected = generate_expected_state(spark, plan, "orders", batch_id=5)

# Compare against your pipeline's output
pipeline_output = spark.table("my_pipeline.orders")
assert expected.subtract(pipeline_output).count() == 0
```

See [Expected State](./expected-state.md) for testing patterns.

## CDC vs. Static Generation

| Feature | Static (`generate()`) | CDC (`generate_cdc()`) |
|---------|----------------------|------------------------|
| Output | Single snapshot per table | Initial + N batches of changes |
| Use case | Populating test databases | Testing CDC pipelines |
| Operation types | Inserts only (implicitly) | Inserts, updates, deletes (explicit) |
| Metadata columns | None | `_op`, `_change_type`, etc. (format-specific) |
| Time dimension | Single point in time | Temporal sequence with configurable intervals |
| FK handling | Guaranteed valid at generation | Maintained across batches |

## Next Steps

- **[Configuration](./configuration.md)** — Detailed guide to `CDCPlan`, `CDCTableConfig`, operation weights, and batch sizing
- **[Formats](./formats.md)** — Schema reference for all four CDC output formats
- **[Batch Independence](./batch-independence.md)** — Generate any batch without materializing prior batches
- **[Expected State](./expected-state.md)** — Verify pipeline correctness with state replay

## Complete Example

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, fk, faker, decimal
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("cdc-example").getOrCreate()

# Define schema
customers = TableSpec(
    name="customers",
    rows=1000,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        faker("name", "name"),
        faker("email", "email"),
    ],
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

# Configure CDC: 10 batches
plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    orders=cdc_config(batch_size=0.10, operations=ops(4, 4, 2)),
)

# Generate
stream = generate_cdc(spark, plan)

# Write initial snapshot
stream.initial["customers"].write.format("delta").save("/data/customers")
stream.initial["orders"].write.format("delta").save("/data/orders")

# Write change batches
for batch_id, batch in enumerate(stream.batches, start=1):
    batch["customers"].write.format("delta").mode("append").save("/data/customers")
    batch["orders"].write.format("delta").mode("append").save("/data/orders")

# Verify correctness
expected_customers = generate_expected_state(spark, plan, "customers", batch_id=10)
expected_orders = generate_expected_state(spark, plan, "orders", batch_id=10)

print(f"Expected customers: {expected_customers.count()}")
print(f"Expected orders: {expected_orders.count()}")
```

---

**Related:** [API Reference](../reference/api.md) | [CDC Configuration](./configuration.md) | [CDC Recipes](../guides/cdc-recipes.md)
