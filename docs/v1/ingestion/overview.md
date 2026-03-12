---
sidebar_position: 1
title: "Ingestion Simulation"
---

# Ingest Pipeline

> **TL;DR:** The ingest pipeline wraps synthetic data generation and CDC into a single abstraction that mimics how data arrives in a lakehouse: an initial full load followed by periodic change batches (inserts, updates, deletes). One call to `write_ingest_to_delta` generates everything and writes it to Delta tables.

## What Is the Ingest Pipeline?

Many data engineering workloads need to test a realistic multi-batch ingestion pattern:

1. **Day 0** — Full initial snapshot lands in a Delta table
2. **Days 1–N** — Daily/hourly incremental files arrive with new rows, changed rows, and deletions

dbldatagen.v1's ingest pipeline generates exactly this pattern, fully deterministically, at any scale:

```python
from dbldatagen.v1.ingest import generate_ingest, write_ingest_to_delta
from dbldatagen.v1.ingest_schema import IngestPlan, IngestTableConfig, IngestMode, IngestStrategy
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, integer, text, timestamp

# Define base schema
orders_table = TableSpec(
    name="orders",
    rows="10M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        integer("customer_id", min=1, max=500_000),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
        timestamp("created_at", start="2024-01-01", end="2025-12-31"),
    ],
)

base_plan = DataGenPlan(tables=[orders_table], seed=42)

# Generate and write: 10M initial rows + 5 daily batches
write_ingest_to_delta(spark, base_plan, catalog="my_catalog", schema="my_schema")
# Creates: my_catalog.my_schema.orders
#   version 0: 10M rows (initial snapshot)
#   versions 1-5: daily incremental batches (60% insert / 30% update / 10% delete)
```

---

## Core Concepts

### IngestMode: INCREMENTAL vs SNAPSHOT

| Mode | Batch content | Use case |
|------|--------------|----------|
| `INCREMENTAL` | Only new + changed rows | Simulates CDC files, append-only staging layers |
| `SNAPSHOT` | All live rows at batch time | Simulates daily full-dumps, SCD Type 1 overwrites |

INCREMENTAL is the default and most common pattern. SNAPSHOT is used when the upstream system sends a complete extract each time.

```python
# INCREMENTAL (default): batch contains only what changed
stream = generate_ingest(spark, base_plan, mode="incremental")

# SNAPSHOT: batch contains all live rows at that point in time
stream = generate_ingest(spark, base_plan, mode="snapshot")
```

### IngestStrategy: SYNTHETIC vs STATELESS vs DELTA

The strategy controls *how* change rows are selected:

| Strategy | Description | Best for |
|----------|-------------|----------|
| `SYNTHETIC` | Wraps the stateful CDC engine. Full state replay from metadata. | Small-medium tables, complete CDC fidelity |
| `STATELESS` | "Three Clocks" engine — computes each row's lifecycle from its identity seed. No state accumulation. | Large tables (1B+ rows), highest throughput |
| `DELTA` | Reads the actual Delta table to select rows for update. Requires a live Delta table. | Maximum realism, join-based update selection |

**STATELESS** is recommended for large-scale workloads. It is O(batch_size) — computing batch N requires no knowledge of batches 1..N-1.

```python
# SYNTHETIC (default)
stream = generate_ingest(spark, base_plan, strategy="synthetic")

# STATELESS — best for 100M+ rows
stream = generate_ingest(spark, base_plan, strategy="stateless")
```

---

## Quick Start Patterns

### Pattern 1: One-Shot Write to Delta

The simplest path — define a plan and write everything to Delta:

```python
from dbldatagen.v1.ingest import write_ingest_to_delta
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, integer, text

plan = DataGenPlan(
    tables=[
        TableSpec(
            name="transactions",
            rows="50M",
            primary_key=PrimaryKey(columns=["txn_id"]),
            columns=[
                pk_auto("txn_id"),
                integer("account_id", min=1, max=1_000_000),
                integer("amount_cents", min=100, max=1_000_000),
                text("type", values=["debit", "credit", "transfer"]),
            ],
        )
    ],
    seed=42,
)

# 50M initial + 5 daily batches, default 60/30/10 I/U/D split
tables = write_ingest_to_delta(spark, plan, catalog="my_catalog", schema="staging")
print(tables)  # {"transactions": "my_catalog.staging.transactions"}
```

### Pattern 2: Access Individual Batches

Use `generate_ingest` for programmatic access to each batch DataFrame:

```python
from dbldatagen.v1.ingest import generate_ingest

stream = generate_ingest(spark, plan, num_batches=10)

# Initial snapshot
initial_df = stream.initial["transactions"]
print(f"Initial: {initial_df.count()} rows")

# Access any batch (lazy — only materialised on access)
batch_1 = stream.batches[0]["transactions"]  # batches[0] = batch_id 1
batch_5 = stream.batches[4]["transactions"]  # batches[4] = batch_id 5

# Iterate all batches
for i, batch in enumerate(stream.batches):
    df = batch["transactions"]
    print(f"Batch {i+1}: {df.count()} rows")
```

Batches are **lazy** — accessing `stream.batches[4]` does not materialise batches 0–3.

### Pattern 3: Generate a Single Batch Independently

For testing or replaying a specific batch without the full stream:

```python
from dbldatagen.v1.ingest import generate_ingest_batch

# Generate batch 5 in isolation — no prior batches needed
batch_5 = generate_ingest_batch(spark, plan, batch_id=5)
df = batch_5["transactions"]
```

This is useful for testing idempotency, debugging a specific batch, or generating batches in parallel.

### Pattern 4: Per-Table Configuration

Override batch size and operation mix per table:

```python
from dbldatagen.v1.ingest_schema import IngestPlan, IngestTableConfig

plan = IngestPlan(
    base_plan=base_plan,
    num_batches=30,
    strategy="stateless",
    mode="incremental",
    table_configs={
        "orders": IngestTableConfig(
            batch_size=0.05,          # 5% of initial rows per batch
            insert_fraction=0.7,
            update_fraction=0.3,
            delete_fraction=0.0,      # No deletes for this table
        ),
    },
)
```

---

## Model Reference

### `IngestPlan`

The top-level configuration model.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_plan` | `DataGenPlan` | required | The schema and row counts for all tables |
| `num_batches` | `int` | `5` | Number of change batches to generate |
| `mode` | `IngestMode` | `INCREMENTAL` | `"incremental"` or `"snapshot"` |
| `strategy` | `IngestStrategy` | `SYNTHETIC` | `"synthetic"`, `"stateless"`, or `"delta"` |
| `table_configs` | `dict[str, IngestTableConfig]` | `{}` | Per-table overrides (unspecified tables use `default_config`) |
| `default_config` | `IngestTableConfig` | see below | Config applied to all tables without an explicit override |
| `batch_interval_seconds` | `int` | `86400` | Simulated time between batches (1 day). Controls timestamp metadata columns. |
| `start_timestamp` | `str` | `"2025-01-01T00:00:00Z"` | Simulated start time for batch 0 |
| `ingest_tables` | `list[str]` | `[]` | Tables to include in ingest output. Empty = all tables in `base_plan`. |
| `include_batch_id` | `bool` | `True` | Add `_batch_id` column to output |
| `include_load_timestamp` | `bool` | `True` | Add `_ingest_ts` column to output |

### `IngestTableConfig`

Per-table operation mix and batch sizing.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | `int \| float \| str` | `0.1` | Rows per batch. Float = fraction of initial rows (e.g. `0.1` = 10%). String shorthand supported: `"1M"`, `"500K"`. |
| `insert_fraction` | `float` | `0.6` | Fraction of batch rows that are inserts |
| `update_fraction` | `float` | `0.3` | Fraction of batch rows that are updates |
| `delete_fraction` | `float` | `0.1` | Fraction of batch rows that are deletes |
| `min_life` | `int` | `1` | Minimum number of batches a row must live before it can be deleted |
| `update_window` | `int \| None` | `None` | Max age (in batches) of rows eligible for updates. `None` = all live rows. |
| `mutations` | `MutationSpec` | `MutationSpec()` | Controls column-level mutations on updated rows |

**Constraint:** `insert_fraction + update_fraction + delete_fraction` must sum to ~1.0 (within 0.05 tolerance).

**Batch size examples:**
```python
batch_size=0.1      # 10% of initial rows per batch
batch_size=50_000   # Fixed 50K rows per batch
batch_size="1M"     # Fixed 1M rows per batch
batch_size="500K"   # Fixed 500K rows per batch
```

---

## Output Schema

Each batch DataFrame includes your defined columns plus optional metadata:

| Column | Type | Added when | Description |
|--------|------|------------|-------------|
| _(your columns)_ | varies | always | All columns from `TableSpec.columns` |
| `_batch_id` | `int` | `include_batch_id=True` | Batch number (0 = initial, 1+ = change batches) |
| `_op_type` | `string` | INCREMENTAL mode | Operation: `"I"` insert, `"U"` update, `"D"` delete |
| `_ingest_ts` | `timestamp` | `include_load_timestamp=True` | Simulated ingest timestamp for this batch |

In SNAPSHOT mode, `_op_type` is not present because the entire live table is emitted.

---

## API Reference

### `generate_ingest`

```python
from dbldatagen.v1.ingest import generate_ingest

stream = generate_ingest(
    spark,
    plan_or_base,       # IngestPlan or DataGenPlan
    *,
    num_batches=None,   # override plan.num_batches
    mode=None,          # override plan.mode: "incremental" | "snapshot"
    strategy=None,      # override plan.strategy: "synthetic" | "stateless"
) -> IngestStream
```

Returns an `IngestStream`:
- `stream.initial` — `dict[str, DataFrame]` — full snapshot at batch 0
- `stream.batches` — lazy list-like, `stream.batches[i]` returns `dict[str, DataFrame]` for batch `i+1`
- `stream.plan` — the `IngestPlan` used

### `generate_ingest_batch`

```python
from dbldatagen.v1.ingest import generate_ingest_batch

batch = generate_ingest_batch(
    spark,
    plan_or_base,   # IngestPlan or DataGenPlan
    batch_id,       # 1-indexed: batch_id=1 is the first change batch
    *,
    mode=None,
) -> dict[str, DataFrame]
```

Generates a single batch without materialising the full stream. Batch IDs are 1-indexed (`batch_id=1` = first change, `batch_id=5` = fifth change).

### `write_ingest_to_delta`

```python
from dbldatagen.v1.ingest import write_ingest_to_delta

tables = write_ingest_to_delta(
    spark,
    plan_or_base,    # IngestPlan or DataGenPlan
    *,
    catalog,         # UC catalog name
    schema,          # UC schema name
    num_batches=None,
    mode=None,
    strategy=None,
    chunk_size=None, # batches per Delta commit (default=1)
) -> dict[str, str]
```

Writes the full ingest stream to Delta tables in Unity Catalog:
- Initial snapshot → `{catalog}.{schema}.{table}` (version 0, overwrite)
- Each batch → appended as a new Delta version

Returns `{table_name: fully_qualified_uc_path}`.

**`chunk_size` parameter:** Controls how many batches are unioned into a single Spark job and Delta commit. Default `chunk_size=1` creates one Delta version per batch. `chunk_size=num_batches` unions all batches into one write — significantly faster for large-scale jobs because it reduces driver overhead and the number of Delta transactions.

```python
# One Delta version per batch (default — good for auditability)
write_ingest_to_delta(spark, plan, catalog="my_cat", schema="stg")

# All batches in one Delta commit (best for throughput at scale)
write_ingest_to_delta(spark, plan, catalog="my_cat", schema="stg", chunk_size=5)
```

### `detect_changes`

Compare two DataFrames (e.g., yesterday's snapshot vs today's) and produce a CDC-style change set:

```python
from dbldatagen.v1.ingest import detect_changes

changes = detect_changes(
    spark,
    before_df,         # Snapshot at time T
    after_df,          # Snapshot at time T+1
    key_columns,       # ["order_id"] — primary key columns
    data_columns=None, # Columns to compare for update detection (None = all non-key cols)
) -> dict[str, DataFrame]
```

Returns a dict with four keys:
- `changes["inserts"]` — rows in `after_df` not in `before_df`
- `changes["updates"]` — rows in both but with changed data columns
- `changes["deletes"]` — rows in `before_df` not in `after_df`
- `changes["unchanged"]` — rows present in both with identical data

```python
# Example: detect what changed between two generated snapshots
stream = generate_ingest(spark, plan, mode="snapshot", num_batches=2)
snap_0 = stream.initial["orders"]
snap_1 = stream.batches[0]["orders"]

changes = detect_changes(spark, snap_0, snap_1, key_columns=["order_id"])
print(f"New orders:     {changes['inserts'].count()}")
print(f"Updated orders: {changes['updates'].count()}")
print(f"Deleted orders: {changes['deletes'].count()}")
```

---

## Strategy Deep Dive

### SYNTHETIC Strategy

Uses the **stateful CDC engine** underneath. The engine maintains a driver-side `TableState` that tracks which rows are alive, their last-write batch, and deletion history. For each batch it deterministically computes which rows to insert, update, or delete by replaying state from metadata (no DataFrame materialisation).

**Characteristics:**
- Full CDC fidelity — operation weights are respected precisely
- Driver-side state: 2 bytes per row (int16 array), 10M row threshold for memory-mapped files
- Batch-independent: `generate_ingest_batch(spark, plan, batch_id=N)` replays state from metadata
- Suitable for tables up to ~100M initial rows without hitting driver memory limits

### STATELESS Strategy

Uses the **"Three Clocks"** engine from `engine/cdc_stateless.py`. Each row's entire lifecycle is determined by three deterministic clocks derived from its identity seed alone:

```
birth_tick(k)   = batch when row k is first inserted
death_tick(k)   = birth_tick(k) + min_life + hash(k) % lifespan_range
update_due(k,t) = (t - birth_tick(k)) % update_period == 0
is_alive(k, t)  = birth_tick(k) <= t < death_tick(k)
```

To generate batch N, the engine performs three range scans:
1. **Inserts** — `spark.range(first_new_row, last_new_row)` — rows born at batch N
2. **Updates** — `spark.range(0, initial_rows, update_period)` — rows due for update at batch N
3. **Deletes** — `spark.range(death_indices_at_N)` — rows whose death_tick = N

**Characteristics:**
- O(batch_size) — no driver-side state, no state replay
- Any batch can be generated in isolation with O(1) setup
- Best throughput at scale (1B+ initial rows)
- Slight difference from SYNTHETIC in exact operation semantics — update selection is purely modular, not random

### DELTA Strategy

The DELTA strategy reads the **actual Delta table** to select rows for update. It is the most realistic strategy for testing pipelines that care about which specific rows change.

Since it requires an existing Delta table, it is typically used after an initial snapshot has been written:

```python
# Step 1: write initial snapshot
write_ingest_to_delta(spark, base_plan, catalog="my_cat", schema="stg",
                       num_batches=0)

# Step 2: run batches with DELTA strategy
write_ingest_to_delta(spark, base_plan, catalog="my_cat", schema="stg",
                       strategy="delta", num_batches=5)
```

---

## Complete Example: Multi-Table Ingest

A three-table schema with per-table config, STATELESS strategy, and optimised Delta writes:

```python
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, pk_pattern
from dbldatagen.v1 import fk, integer, decimal, text, timestamp
from dbldatagen.v1.ingest import write_ingest_to_delta
from dbldatagen.v1.ingest_schema import IngestPlan, IngestTableConfig

# Schema
customers = TableSpec(
    name="customers",
    rows="1M",
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        text("tier", values=["bronze", "silver", "gold", "platinum"]),
        timestamp("signup_date", start="2020-01-01", end="2024-12-31"),
    ],
)

orders = TableSpec(
    name="orders",
    rows="10M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        decimal("amount", min=5.0, max=9999.99),
        text("status", values=["pending", "processing", "shipped", "delivered", "cancelled"]),
        timestamp("order_ts", start="2024-01-01", end="2025-06-30"),
    ],
)

base_plan = DataGenPlan(tables=[customers, orders], seed=2025)

# Per-table config
ingest_plan = IngestPlan(
    base_plan=base_plan,
    num_batches=30,
    strategy="stateless",  # Best for 10M+ rows
    mode="incremental",
    table_configs={
        "customers": IngestTableConfig(
            batch_size=0.02,           # 2% of 1M = 20K rows/batch
            insert_fraction=0.5,
            update_fraction=0.5,
            delete_fraction=0.0,       # Customers never deleted
        ),
        "orders": IngestTableConfig(
            batch_size=0.1,            # 10% of 10M = 1M rows/batch
            insert_fraction=0.75,
            update_fraction=0.25,
            delete_fraction=0.0,
        ),
    },
)

# Write everything to Delta
# chunk_size=30 unions all 30 batches into one Spark job → fastest write
spark_conf = {
    "spark.default.parallelism": "320",
    "spark.sql.files.maxRecordsPerFile": "10000000",
    "spark.databricks.delta.optimizeWrite.enabled": "false",
}
spark.conf.update(spark_conf)

tables = write_ingest_to_delta(
    spark, ingest_plan,
    catalog="my_catalog",
    schema="staging",
    chunk_size=30,
)

print(tables)
# {
#   "customers": "my_catalog.staging.customers",
#   "orders":    "my_catalog.staging.orders"
# }
```

---

## Performance Considerations

### chunk_size

The most impactful parameter for write throughput. With `chunk_size=1` (default), each batch is a separate Spark job — fine for small tables but creates significant driver overhead at large scale.

| chunk_size | Delta versions | Spark jobs | Best for |
|---|---|---|---|
| `1` | 1 per batch | N batches | Auditability, streaming use cases |
| `num_batches` | 1 total (batch) | 1 | Maximum throughput |

### STATELESS vs SYNTHETIC at Scale

| Metric | SYNTHETIC | STATELESS |
|---|---|---|
| Driver memory | O(initial_rows) | O(1) |
| Batch generation | Stateful replay | Pure function |
| Independent batch access | Yes (with replay) | Yes (O(1)) |
| Throughput at 1B rows | Good | Best |

For tables with 100M+ initial rows, STATELESS is strongly recommended. See [Scaling Guide](../data-generation/scaling.md) for cluster configuration.

### Parallelism and Write Config

For large ingest workloads on classic compute:

```python
spark.conf.set("spark.default.parallelism", "320")             # = workers × cores
spark.conf.set("spark.sql.files.maxRecordsPerFile", "10000000") # Fewer concurrent writes
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")  # Skip shuffle
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
```

See [Scaling Guide](../data-generation/scaling.md) for a full breakdown of these settings and their measured impact.

---

## See Also

- [CDC Guide](../cdc/overview.md) — lower-level CDC API with more format options
- [Scaling Guide](../data-generation/scaling.md) — configuration for 100B+ row workloads
- [Architecture](../reference/architecture.md) — Three Clocks engine internals
- [API Reference](../reference/api.md) — full function signatures
