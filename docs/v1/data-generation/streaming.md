---
sidebar_position: 3
title: "Streaming"
---

# Streaming

> **TL;DR:** Use `generate_stream()` for a native Spark Structured Streaming source that emits synthetic rows continuously. Use CDC batches written to Delta tables for multi-table change streams with referential integrity. Both approaches work with Structured Streaming consumers, watermarks, and trigger modes.

dbldatagen.v1 supports two streaming approaches:

1. **Native streaming** (`generate_stream()`) — a streaming DataFrame backed by Spark's `rate` source. Supports FK columns via `parent_specs`. Best for: event streams, load testing, streaming pipeline smoke tests.
2. **CDC-to-Delta streaming** — generate CDC batches, write to Delta, consume with `readStream`. Best for: multi-table schemas with FK integrity, SCD Type 2 testing, realistic change feeds.

---

## Native Streaming with `generate_stream()`

`generate_stream()` returns a Spark Structured Streaming DataFrame that emits rows continuously at a configurable rate. It uses Spark's built-in `rate` source internally but applies all of dbldatagen.v1's column generation on top.

### Quick Start

```python
from dbldatagen.v1 import TableSpec, PrimaryKey, generate_stream
from dbldatagen.v1.dsl import pk_auto, faker, integer, text, timestamp

spec = TableSpec(
    name="events",
    rows=0,  # ignored in streaming — the stream is unbounded
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        faker("user", "user_name"),
        integer("score", min=0, max=100),
        text("level", values=["bronze", "silver", "gold", "platinum"]),
        timestamp("event_time", start="2024-01-01", end="2025-12-31"),
    ],
    seed=42,
)

sdf = generate_stream(spark, spec, rows_per_second=500)
assert sdf.isStreaming

# Write to console for quick inspection
query = sdf.writeStream.format("console").start()
query.awaitTermination(10)
query.stop()
```

### Write to Delta for Downstream Consumers

```python
sdf = generate_stream(spark, spec, rows_per_second=1000)

query = sdf.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/events") \
    .toTable("catalog.schema.raw_events")
```

Downstream streaming pipelines consume the Delta table with `spark.readStream.format("delta")` as usual.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rows_per_second` | int | 1000 | How many rows the rate source emits per second |
| `num_partitions` | int | None | Number of streaming partitions (Spark default if omitted) |
| `parent_specs` | dict[str, TableSpec] | None | Parent table definitions for FK resolution (see below) |

### Supported Column Types

| Strategy | Streaming? | Why |
|----------|-----------|-----|
| `RangeColumn` | Yes | Pure Spark SQL expression on row id |
| `ValuesColumn` | Yes | Pure Spark SQL expression |
| `PatternColumn` | Yes | Pure Spark SQL expression |
| `SequenceColumn` | Yes | `id * step + start` — works with unbounded ids |
| `UUIDColumn` | Yes | `xxhash64(seed, id)` — deterministic per row |
| `TimestampColumn` | Yes | Epoch arithmetic on cell seed |
| `ConstantColumn` | Yes | `lit(value)` |
| `ExpressionColumn` | Yes | `expr(sql)` |
| `FakerColumn` | Yes | `pandas_udf` works in Structured Streaming |
| `StructColumn` | Yes | Recursive — all child strategies supported |
| `ArrayColumn` | Yes | Recursive — all element strategies supported |
| FK columns | Yes | Requires `parent_specs` with parent table definitions |
| Feistel PK (random-unique) | **No** | Requires a fixed domain size N |

If you use an unsupported strategy, `generate_stream()` raises a clear `ValueError` at call time — not a runtime failure during streaming.

### Streaming with Foreign Keys

FK columns work in streaming when you provide `parent_specs` — a dict mapping parent table names to their `TableSpec` definitions. Parent tables are not materialised; only their PK metadata (row count, PK strategy, seed) is used to generate valid FK values.

```python
from dbldatagen.v1 import TableSpec, PrimaryKey, generate_stream
from dbldatagen.v1.dsl import pk_auto, pk_uuid, fk, faker, integer, text

# Define parent tables (batch specs — rows defines the FK domain)
customers = TableSpec(
    name="customers",
    rows=10_000,
    primary_key=PrimaryKey(columns=["id"]),
    columns=[pk_auto("id"), faker("name", "name")],
    seed=42,
)

products = TableSpec(
    name="products",
    rows=500,
    primary_key=PrimaryKey(columns=["sku"]),
    columns=[pk_uuid("sku"), text("category", values=["A", "B", "C"])],
    seed=42,
)

# Define streaming child table with FK columns
orders = TableSpec(
    name="orders",
    rows=0,  # ignored in streaming
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        fk("customer_id", "customers.id"),
        fk("product_sku", "products.sku"),
        integer("amount", min=5, max=5000),
    ],
    seed=42,
)

# Stream with FK resolution
sdf = generate_stream(
    spark,
    orders,
    rows_per_second=500,
    parent_specs={"customers": customers, "products": products},
)

# Every customer_id is a valid PK from customers (1..10000)
# Every product_sku is a valid UUID PK from products
query = sdf.writeStream.format("console").start()
query.awaitTermination(10)
query.stop()
```

**How it works:** The parent `TableSpec.rows` defines the FK domain size (N_parent). FK values are generated by sampling a parent row index via the configured distribution, then reconstructing the parent PK value using the same function the parent table would use. This guarantees referential integrity without materialising the parent table.

**FK distributions** work the same as batch mode:

```python
from dbldatagen.v1.schema import Zipf, Uniform

fk("customer_id", "customers.id", distribution=Zipf(exponent=2.0))  # power users
fk("product_sku", "products.sku", distribution=Uniform())            # even spread
fk("referrer_id", "users.id", null_fraction=0.7)                     # 70% NULL
```

### How It Works Internally

```
spark.readStream.format("rate")
        │
        │  produces: (timestamp: TimestampType, value: LongType)
        │            value is monotonically increasing (like spark.range id)
        │
        ▼
rename "value" → "_synth_row_id"
        │
        ▼
apply column expressions (same builders as batch)
        │  cell_seed = xxhash64(column_seed, _synth_row_id)
        │  range, values, pattern, timestamp, etc.
        │
        ▼
apply UDF columns (Faker) via withColumn
        │
        ▼
drop "_synth_row_id"
        │
        ▼
streaming DataFrame with user-defined columns
```

The key insight: **every column builder in dbldatagen.v1 operates on `(column_seed, id_col)` — none of them need the total row count.** The `rate` source's `value` column serves exactly the same role as `spark.range()`'s `id`, so all Spark SQL expressions and pandas UDFs work identically in both batch and streaming modes.

The only features that need N (total row count) are:
- **Feistel cipher** — the bijective permutation is defined on `[0, N)`, so N must be known. In streaming, N is infinite.
- **FK resolution** — `apply_distribution(seed, N_parent)` maps to a parent row index in `[0, N_parent)`. Without parent metadata, FK values can't be generated.

Both of these raise `ValueError` at call time rather than producing silently wrong data.

### Determinism

For a given `seed` and row id (the `value` from the rate source), the generated column values are always the same. Two runs of the same streaming query with the same seed produce identical values for identical row ids.

However, the rate source assigns ids based on timing, so the *set of rows in a given time window* may vary between runs. Determinism is per-row (same id → same data), not per-run (same duration → same total rows).

---

## CDC-to-Delta Streaming

For multi-table schemas with foreign keys, use CDC batches written to Delta tables. This pattern matches real-world data pipelines where change feeds land in staging tables and downstream consumers read them as streams.

```
dbldatagen.v1 CDC batches
        |
        v
  Delta tables (append / merge)
        |
        v
  spark.readStream.format("delta")
        |
        v
  Your streaming pipeline
```

## Pattern 1: Append CDC Batches to Delta

The simplest approach — generate CDC batches and append them to a Delta table.

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1.dsl import pk_auto, fk, integer, text, timestamp, faker
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define schema
plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=10_000,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[
                pk_auto("customer_id"),
                faker("name", provider="name"),
                text("status", values=["active", "inactive", "churned"]),
            ],
        ),
        TableSpec(
            name="orders",
            rows=50_000,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                pk_auto("order_id"),
                fk("customer_id", ref="customers.customer_id"),
                integer("amount", min=5, max=5000),
                text("status", values=["pending", "shipped", "delivered", "returned"]),
                timestamp("order_date", start="2024-01-01", end="2025-12-31"),
            ],
        ),
    ],
)

# Generate CDC stream
cdc = cdc_plan(
    plan,
    num_batches=10,
    format="delta_cdf",
    orders=cdc_config(batch_size=1000, operations=ops(insert=3, update=5, delete=2)),
)
stream = generate_cdc(spark, cdc)

# Write initial snapshot
stream.initial["orders"].write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("catalog.schema.raw_orders")

# Append CDC batches — each append triggers downstream streams
for i, batch in enumerate(stream.batches):
    batch["orders"].write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("catalog.schema.raw_orders")
    print(f"Batch {i + 1}: appended {batch['orders'].count()} change rows")
```

### Consume with Structured Streaming

```python
# Read the Delta table as a stream
stream_df = spark.readStream \
    .format("delta") \
    .table("catalog.schema.raw_orders")

# Continuous aggregation
counts = stream_df.groupBy("status").count()

query = counts.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/order_status") \
    .toTable("catalog.schema.order_status_counts")
```

## Pattern 2: Delta Change Data Feed

The `delta_cdf` format produces columns that match Delta's [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html) schema exactly: `_change_type`, `_commit_version`, and `_commit_timestamp`.

This lets you test pipelines that consume Delta CDF without needing a real production feed.

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

cdc = cdc_plan(
    plan,
    num_batches=5,
    format="delta_cdf",
    batch_interval_seconds=3600,  # 1 hour between batches
    start_timestamp="2025-01-01T00:00:00Z",
    orders=cdc_config(batch_size=0.1, operations=ops(insert=3, update=5, delete=2)),
)
stream = generate_cdc(spark, cdc)

# Write initial load to a CDF-enabled Delta table
stream.initial["orders"].drop("_change_type", "_commit_version", "_commit_timestamp") \
    .write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable("catalog.schema.orders_cdf")

# Write change batches
for batch in stream.batches:
    df = batch["orders"]
    # Merge changes into the target table
    df.createOrReplaceTempView("batch_changes")
    spark.sql("""
        MERGE INTO catalog.schema.orders_cdf AS target
        USING (
            SELECT * FROM batch_changes
            WHERE _change_type IN ('insert', 'update_postimage')
        ) AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # Handle deletes
    spark.sql("""
        MERGE INTO catalog.schema.orders_cdf AS target
        USING (
            SELECT * FROM batch_changes WHERE _change_type = 'delete'
        ) AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN DELETE
    """)
```

### Read the Change Feed as a Stream

```python
# Downstream pipeline reads the CDF stream
cdf_stream = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table("catalog.schema.orders_cdf")

# Filter to just inserts and updates
new_and_updated = cdf_stream.filter(
    "_change_type IN ('insert', 'update_postimage')"
)

query = new_and_updated.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/orders_cdf") \
    .toTable("catalog.schema.orders_silver")
```

## Pattern 3: On-Demand Batch Generation

Use `generate_cdc_batch()` to generate individual batches without materializing the entire stream. This is useful for simulating a long-running stream where you only need specific batches.

```python
from dbldatagen.v1.cdc import generate_cdc_batch
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

cdc = cdc_plan(
    plan,
    num_batches=1000,  # plan supports up to 1000 batches
    format="delta_cdf",
    orders=cdc_config(batch_size=500, operations=ops(insert=3, update=5, delete=2)),
)

# Generate only batch 500 — no need to compute batches 1-499
# State is replayed via metadata, not DataFrames
batch_500 = generate_cdc_batch(spark, cdc, batch_id=500)
batch_500["orders"].show()

# Simulate a live feed: generate batches one at a time
import time

for batch_id in range(1, 101):
    batch = generate_cdc_batch(spark, cdc, batch_id=batch_id)
    batch["orders"].write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("catalog.schema.raw_orders")
    print(f"Wrote batch {batch_id}")
    time.sleep(5)  # simulate arrival interval
```

## Pattern 4: Watermarking and Windowed Aggregations

CDC batches have deterministic timestamps controlled by `batch_interval_seconds` and `start_timestamp`. Use these for reproducible time-windowed streaming tests.

```python
from pyspark.sql.functions import window

cdc = cdc_plan(
    plan,
    num_batches=24,
    format="delta_cdf",
    batch_interval_seconds=3600,       # 1 batch per hour
    start_timestamp="2025-06-01T00:00:00Z",
    orders=cdc_config(batch_size=1000),
)

# Generate and write all batches
stream = generate_cdc(spark, cdc)
for batch in stream.batches:
    batch["orders"].write.format("delta").mode("append") \
        .saveAsTable("catalog.schema.raw_orders")

# Streaming consumer with watermark
stream_df = spark.readStream \
    .format("delta") \
    .table("catalog.schema.raw_orders")

windowed = stream_df \
    .withWatermark("_commit_timestamp", "2 hours") \
    .groupBy(
        window("_commit_timestamp", "1 hour"),
        "status",
    ) \
    .count()

query = windowed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/hourly_orders") \
    .toTable("catalog.schema.hourly_order_counts")
```

## Pattern 5: Verify Streaming Pipeline Correctness

Use `generate_expected_state()` to verify that your streaming pipeline correctly applies all CDC changes.

```python
from dbldatagen.v1.cdc import generate_cdc, generate_expected_state

cdc = cdc_plan(
    plan,
    num_batches=10,
    format="delta_cdf",
    orders=cdc_config(batch_size=0.1, operations=ops(insert=3, update=5, delete=2)),
)

# Generate and write all batches through your pipeline
stream = generate_cdc(spark, cdc)
stream.initial["orders"].write.format("delta").mode("overwrite") \
    .saveAsTable("catalog.schema.orders_raw")
for batch in stream.batches:
    batch["orders"].write.format("delta").mode("append") \
        .saveAsTable("catalog.schema.orders_raw")

# ... your streaming pipeline processes the data into a target table ...

# Verify: what should the table look like after batch 10?
expected = generate_expected_state(spark, cdc, "orders", batch_id=10)
actual = spark.table("catalog.schema.orders_target")

# Compare
missing = expected.subtract(actual)
extra = actual.subtract(expected)

assert missing.count() == 0, f"Missing {missing.count()} rows"
assert extra.count() == 0, f"Extra {extra.count()} rows"
print("Pipeline output matches expected state!")
```

## Pattern 6: Trigger Modes

Control how your streaming consumer processes batches.

```python
stream_df = spark.readStream.format("delta").table("catalog.schema.raw_orders")
result = stream_df.groupBy("status").count()

# Process all available data at once (good for testing)
query = result.writeStream \
    .trigger(availableNow=True) \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/trigger_test") \
    .toTable("catalog.schema.order_counts")

# Wait for completion
query.awaitTermination()

# Or: process every 30 seconds (simulates production polling)
query = result.writeStream \
    .trigger(processingTime="30 seconds") \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/trigger_test") \
    .toTable("catalog.schema.order_counts")
```

## Presets for Common Streaming Scenarios

The CDC DSL includes presets that map to common streaming patterns.

```python
from dbldatagen.v1.cdc_dsl import (
    cdc_plan, cdc_config,
    append_only_config,    # inserts only — event streams, logs
    high_churn_config,     # heavy updates/deletes — SCD2 stress test
    scd2_config,           # moderate updates — dimension tables
)

cdc = cdc_plan(
    plan,
    num_batches=50,
    format="delta_cdf",
    batch_interval_seconds=300,  # 5 min between batches

    # Event log: append-only, 500 new rows per batch
    orders=append_only_config(batch_size=500),

    # Customer dimension: moderate updates, for SCD2 testing
    customers=scd2_config(batch_size=0.05),
)
```

| Preset | Insert | Update | Delete | Use Case |
|--------|--------|--------|--------|----------|
| `append_only_config` | 1 | 0 | 0 | Event streams, logs, clickstream |
| `scd2_config` | 1 | 8 | 1 | Dimension tables, SCD Type 2 |
| `high_churn_config` | 1 | 6 | 3 | Stress testing merges and deletes |

## Databricks Notebook Example

A complete runnable notebook is included in the repository at [`notebook_examples/streaming_to_delta.py`](https://github.com/anup-kalburgi/dbldatagen/blob/main/notebook_examples/streaming_to_delta.py). It demonstrates:

1. Schema definition with FK relationships
2. Initial snapshot written to Delta
3. Simulated arriving batches via append
4. Streaming consumer with continuous aggregation
5. Result verification

Import it into a Databricks workspace with **File > Import** and run on a cluster with `%pip install "dbldatagen[v1-faker]"`.

## Output Format Reference

Each format produces different metadata columns — pick the one that matches your downstream consumer.

| Format | Metadata Columns | Values |
|--------|-----------------|--------|
| `delta_cdf` | `_change_type`, `_commit_version`, `_commit_timestamp` | `insert`, `update_postimage`, `update_preimage`, `delete` |
| `raw` | `_op`, `_batch_id`, `_ts` | `I`, `U`, `UB`, `D` |
| `debezium` | `op`, `ts_ms` | `c`, `u`, `d` |
| `sql_server` | `__$operation`, `__$start_lsn`, `__$seqval` | `1` (del), `2` (ins), `3` (upd before), `4` (upd after) |

See [CDC Formats](../cdc/formats.md) for full details on each format's schema.

## Related Documentation

- [CDC Overview](../cdc/overview.md) - Core CDC concepts and quick start
- [CDC Configuration](../cdc/configuration.md) - CDCPlan, operation weights, batch sizing
- [Batch Independence](../cdc/batch-independence.md) - How on-demand batch generation works
- [Expected State](../cdc/expected-state.md) - Verifying pipeline correctness
- [Writing Output](./writing-output.md) - Parquet, Delta, and JSON output patterns
