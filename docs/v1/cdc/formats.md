---
sidebar_position: 2
title: "CDC Formats"
---

# CDC Formats

> **TL;DR:** dbldatagen.v1 supports four CDC output formats, each matching a real-world CDC system's schema. Switch formats by setting `format="delta_cdf"` (or `raw`, `sql_server`, `debezium`) in your `CDCPlan`. All formats contain the same logical operations; only the metadata columns differ.

## Overview

Each CDC format represents operations (insert, update, delete) using different metadata columns and encoding schemes. The data columns remain identical across formats; only the CDC metadata changes.

| Format | System | Metadata Columns | Use Case |
|--------|--------|------------------|----------|
| `raw` | dbldatagen.v1 native | `_op`, `_batch_id`, `_ts` | Internal testing, custom pipelines |
| `delta_cdf` | Delta Lake CDF | `_change_type`, `_commit_version`, `_commit_timestamp` | Delta Lake consumers, Databricks |
| `sql_server` | SQL Server CDC | `__$operation`, `__$start_lsn`, `__$seqval` | SQL Server replication, SSIS |
| `debezium` | Debezium | `op`, `ts_ms` | Kafka Connect, event streaming |

---

## Format 1: `raw` (Default)

The native dbldatagen.v1 format with explicit operation codes.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `_op` | `string` | Operation code: `I`, `U`, `UB`, `D` |
| `_batch_id` | `long` | Batch sequence number (0 = initial, 1+ = changes) |
| `_ts` | `timestamp` | Simulated timestamp for this operation |
| *(data columns)* | *(varies)* | User-defined table columns |

### Operation Codes

| Code | Meaning | Description |
|------|---------|-------------|
| `I` | Insert | New row created |
| `U` | Update (after) | Row after update |
| `UB` | Update (before) | Row before update |
| `D` | Delete | Row deleted |

### Update Semantics

Updates generate **two rows** in `raw` format:
1. **Before-image** (`_op = 'UB'`) — The row's state before the update
2. **After-image** (`_op = 'U'`) — The row's state after the update

This enables full audit trails and temporal queries.

### Example

```python
from dbldatagen.v1.cdc import generate_cdc

stream = generate_cdc(spark, plan, format="raw")
stream.batches[0]["users"].show()

# +-------+----+-------+---------+------+--------------------+
# |user_id|name| status| _op     |_batch_id|         _ts      |
# +-------+----+-------+---------+------+--------------------+
# |    101|John|active |       I |     1|2025-01-01 01:00:00|  <- insert
# |     42|Jane|pending|      UB |     1|2025-01-01 01:00:00|  <- update before
# |     42|Jane|active |       U |     1|2025-01-01 01:00:00|  <- update after
# |     15|Bob |inactive|      D |     1|2025-01-01 01:00:00|  <- delete
# +-------+----+-------+---------+------+--------------------+
```

### When to Use

- Custom CDC pipelines
- Internal testing
- Maximum transparency (explicit before/after images)
- Converting to other formats downstream

---

## Format 2: `delta_cdf` (Delta Lake CDF)

Matches Delta Lake's Change Data Feed format.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `_change_type` | `string` | Change type: `insert`, `update_postimage`, `update_preimage`, `delete` |
| `_commit_version` | `long` | Delta table version (= batch ID) |
| `_commit_timestamp` | `timestamp` | Transaction timestamp |
| *(data columns)* | *(varies)* | User-defined table columns |

### Change Types

| Value | Meaning | Equivalent to `raw` |
|-------|---------|---------------------|
| `insert` | New row | `_op = 'I'` |
| `update_postimage` | Row after update | `_op = 'U'` |
| `update_preimage` | Row before update | `_op = 'UB'` |
| `delete` | Row deleted | `_op = 'D'` |

### Example

```python
stream = generate_cdc(spark, plan, format="delta_cdf")
stream.batches[0]["users"].show()

# +-------+----+-------+------------------+---------------+--------------------+
# |user_id|name| status|    _change_type  |_commit_version|  _commit_timestamp |
# +-------+----+-------+------------------+---------------+--------------------+
# |    101|John|active |           insert|             1|2025-01-01 01:00:00|
# |     42|Jane|pending|  update_preimage|             1|2025-01-01 01:00:00|
# |     42|Jane|active | update_postimage|             1|2025-01-01 01:00:00|
# |     15|Bob |inactive|          delete|             1|2025-01-01 01:00:00|
# +-------+----+-------+------------------+---------------+--------------------+
```

### Integration with Delta Lake

Write CDC batches to a Delta table with CDF enabled:

```python
from dbldatagen.v1.cdc import generate_cdc

# Generate CDC in Delta format
stream = generate_cdc(spark, plan, format="delta_cdf")

# Write initial snapshot
stream.initial["orders"] \
    .write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("/delta/orders")

# Apply change batches
for batch in stream.batches:
    batch["orders"] \
        .write \
        .format("delta") \
        .mode("append") \
        .save("/delta/orders")

# Read CDF from Delta
cdf = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table("delta.`/delta/orders`")

cdf.show()
```

### When to Use

- Testing Delta Lake CDF consumers
- Databricks pipelines
- Delta Live Tables
- Unity Catalog change tracking
- Streaming CDC from Delta

---

## Format 3: `sql_server` (SQL Server CDC)

Matches Microsoft SQL Server's CDC format.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `__$operation` | `int` | Operation code: `1`, `2`, `3`, `4` |
| `__$start_lsn` | `string` | Log Sequence Number (synthetic hex string) |
| `__$seqval` | `string` | Sequence value within LSN (synthetic hex string) |
| *(data columns)* | *(varies)* | User-defined table columns |

### Operation Codes

| Code | Meaning | Equivalent to `raw` |
|------|---------|---------------------|
| `1` | Delete | `_op = 'D'` |
| `2` | Insert | `_op = 'I'` |
| `3` | Update (before) | `_op = 'UB'` |
| `4` | Update (after) | `_op = 'U'` |

### LSN and Sequence Values

- `__$start_lsn` — Zero-padded hex representation of batch ID
- `__$seqval` — Synthetic sequence value derived from batch ID and row monotonic ID

These values are deterministic and match SQL Server CDC's structure, but are synthetic (not real LSNs).

### Example

```python
stream = generate_cdc(spark, plan, format="sql_server")
stream.batches[0]["users"].show()

# +-------+----+-------+-------------+--------------------+--------------------+
# |user_id|name| status|__$operation|      __$start_lsn  |       __$seqval    |
# +-------+----+-------+-------------+--------------------+--------------------+
# |    101|John|active |           2|00000000000000000001|8F3A12B4C5D6E7F8...|
# |     42|Jane|pending|           3|00000000000000000001|1A2B3C4D5E6F7A8B...|
# |     42|Jane|active |           4|00000000000000000001|9F8E7D6C5B4A3928...|
# |     15|Bob |inactive|          1|00000000000000000001|2C3D4E5F6A7B8C9D...|
# +-------+----+-------+-------------+--------------------+--------------------+
```

### When to Use

- Testing SQL Server CDC replication
- SQL Server Integration Services (SSIS)
- Azure Data Factory with SQL Server CDC source
- Change tracking pipelines that expect SQL Server format

---

## Format 4: `debezium` (Kafka Connect Debezium)

Matches Debezium's flattened CDC event structure.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `op` | `string` | Operation code: `c`, `u`, `d` |
| `ts_ms` | `long` | Event timestamp in milliseconds since epoch |
| *(data columns)* | *(varies)* | User-defined table columns |

### Operation Codes

| Code | Meaning | Equivalent to `raw` |
|------|---------|---------------------|
| `c` | Create (insert) | `_op = 'I'` |
| `u` | Update | `_op = 'U'` (after-image only) |
| `d` | Delete | `_op = 'D'` |

### Update Semantics

Unlike `raw` and `delta_cdf`, Debezium format **does not include separate before-images** for updates. Only the after-image is included in `op='u'` rows.

This matches Debezium's default behavior when `include.schema.changes=false` and no nested `before`/`after` structs are used.

### Example

```python
stream = generate_cdc(spark, plan, format="debezium")
stream.batches[0]["users"].show()

# +-------+----+-------+---+-------------------+
# |user_id|name| status| op|             ts_ms |
# +-------+----+-------+---+-------------------+
# |    101|John|active | c |      1704067200000|  <- create (insert)
# |     42|Jane|active | u |      1704067200000|  <- update (after-image only)
# |     15|Bob |inactive| d|      1704067200000|  <- delete
# +-------+----+-------+---+-------------------+
```

### Integration with Kafka

Write CDC events to Kafka topics:

```python
from dbldatagen.v1.cdc import generate_cdc

stream = generate_cdc(spark, plan, format="debezium")

for batch_id, batch in enumerate(stream.batches, start=1):
    batch["products"] \
        .selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "dbserver1.inventory.products") \
        .save()
```

### When to Use

- Testing Debezium consumers
- Kafka Connect pipelines
- Streaming CDC to Kafka
- Event-driven architectures
- Microservices consuming database changes

---

## Comparing Formats Side-by-Side

```python
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker, text
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define a simple table
users = TableSpec(
    name="users",
    rows=100,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("name", "name"),
        text("status", values=["active", "inactive"]),
    ],
)

base = DataGenPlan(tables=[users], seed=42)
plan = cdc_plan(base, num_batches=1, users=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)))

# Generate in all 4 formats
stream_raw = generate_cdc(spark, plan, format="raw")
stream_delta = generate_cdc(spark, plan, format="delta_cdf")
stream_sql = generate_cdc(spark, plan, format="sql_server")
stream_deb = generate_cdc(spark, plan, format="debezium")

# Compare batch 1 metadata columns
print("=== RAW FORMAT ===")
stream_raw.batches[0]["users"].select("user_id", "_op", "_batch_id").show(5)

print("=== DELTA CDF FORMAT ===")
stream_delta.batches[0]["users"].select("user_id", "_change_type", "_commit_version").show(5)

print("=== SQL SERVER FORMAT ===")
stream_sql.batches[0]["users"].select("user_id", "__$operation").show(5)

print("=== DEBEZIUM FORMAT ===")
stream_deb.batches[0]["users"].select("user_id", "op", "ts_ms").show(5)
```

---

## Format Conversion

The same logical CDC stream can be output in any format. To convert between formats, regenerate with a different `format` parameter:

```python
# Generate in raw format
stream_raw = generate_cdc(spark, plan, format="raw")

# Regenerate the SAME stream in Delta format (same seed, same operations)
stream_delta = generate_cdc(spark, plan, format="delta_cdf")

# Data columns are identical; only metadata differs
assert (
    stream_raw.batches[0]["users"].select("user_id", "name", "status").collect()
    == stream_delta.batches[0]["users"].select("user_id", "name", "status").collect()
)
```

---

## Format Selection Guide

### Use `raw` when:
- Building custom CDC pipelines
- Needing maximum transparency (explicit before/after)
- Prototyping or testing CDC logic
- Converting to other formats downstream

### Use `delta_cdf` when:
- Working with Delta Lake tables
- Using Databricks or Spark Structured Streaming
- Testing Delta Live Tables
- Consuming Unity Catalog change feeds

### Use `sql_server` when:
- Testing SQL Server replication
- Using Azure Data Factory with SQL Server CDC
- Working with SSIS packages
- Migrating from SQL Server CDC to another system

### Use `debezium` when:
- Testing Kafka Connect Debezium consumers
- Building event-driven microservices
- Streaming CDC events to Kafka
- Working with Debezium-compatible systems

---

## Complete Example: All Formats

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker, decimal
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

spark = SparkSession.builder.appName("cdc-formats").getOrCreate()

# Define schema
orders = TableSpec(
    name="orders",
    rows=1000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        faker("customer_name", "name"),
        decimal("amount", min=10.0, max=1000.0),
    ],
)

base = DataGenPlan(tables=[orders], seed=42)
plan = cdc_plan(base, num_batches=5, orders=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)))

# Generate in all formats
formats = ["raw", "delta_cdf", "sql_server", "debezium"]
for fmt in formats:
    stream = generate_cdc(spark, plan, format=fmt)

    # Write initial snapshot
    stream.initial["orders"].write.mode("overwrite").parquet(f"/data/cdc/{fmt}/initial/orders")

    # Write batches
    for batch_id, batch in enumerate(stream.batches, start=1):
        batch["orders"].write.mode("overwrite").parquet(f"/data/cdc/{fmt}/batch_{batch_id}/orders")

    print(f"Generated {fmt} format: {len(stream.batches)} batches")

# Compare batch 1 in all formats
print("\n=== BATCH 1 COMPARISON ===")
for fmt in formats:
    df = spark.read.parquet(f"/data/cdc/{fmt}/batch_1/orders")
    print(f"{fmt:15s}: {df.count():6d} rows, columns: {df.columns}")
```

---

**Related:** [CDC Overview](./overview.md) | [CDC Configuration](./configuration.md) | [API Reference](../reference/api.md)
