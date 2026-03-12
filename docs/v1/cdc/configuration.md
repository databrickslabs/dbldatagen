---
sidebar_position: 3
title: "Configuration"
---

# CDC Configuration

> **TL;DR:** Control every aspect of CDC generation with `CDCPlan`, `CDCTableConfig`, and `OperationWeights`. Configure operation mix (insert/update/delete ratios), batch sizes (absolute or fractional), column mutations, and per-table overrides. Use presets for common patterns like append-only logs or SCD2 dimensions.

## CDCPlan Model

The top-level CDC plan wraps your base `DataGenPlan` and adds change semantics.

```python
from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, CDCFormat, OperationWeights

plan = CDCPlan(
    base_plan=base,                         # Your DataGenPlan
    num_batches=10,                         # Number of change batches
    format=CDCFormat.DELTA_CDF,             # Output format
    table_configs={                         # Per-table overrides
        "customers": CDCTableConfig(...),
        "orders": CDCTableConfig(...),
    },
    default_config=CDCTableConfig(),        # Default for unconfigured tables
    batch_interval_seconds=3600,            # Simulated time between batches
    start_timestamp="2025-01-01T00:00:00Z", # Simulated start time
    cdc_tables=["customers", "orders"],     # Tables to include (empty = all)
)
```

### Fields Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_plan` | `DataGenPlan` | *required* | Base schema for initial data generation |
| `num_batches` | `int` | `5` | Number of change batches to generate |
| `table_configs` | `dict[str, CDCTableConfig]` | `{}` | Per-table CDC configuration overrides |
| `default_config` | `CDCTableConfig` | `CDCTableConfig()` | Fallback config for tables not in `table_configs` |
| `format` | `CDCFormat` | `CDCFormat.RAW` | Output format: `raw`, `delta_cdf`, `sql_server`, `debezium` |
| `batch_interval_seconds` | `int` | `3600` | Simulated seconds between batches (affects `_ts` columns) |
| `start_timestamp` | `str` | `"2025-01-01T00:00:00Z"` | ISO timestamp for batch 0 |
| `cdc_tables` | `list[str]` | `[]` | Tables to include in CDC (empty list means all tables) |

### Example: Full Configuration

```python
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, faker
from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, CDCFormat, OperationWeights

# Base schema
base = DataGenPlan(
    tables=[
        TableSpec(name="users", rows=1000, primary_key=PrimaryKey(columns=["id"]),
                  columns=[pk_auto("id"), faker("name", "name")]),
    ],
    seed=42,
)

# CDC plan with explicit config
plan = CDCPlan(
    base_plan=base,
    num_batches=10,
    format=CDCFormat.DELTA_CDF,
    table_configs={
        "users": CDCTableConfig(
            operations=OperationWeights(insert=2, update=7, delete=1),
            batch_size=0.1,  # 10% of rows per batch
        ),
    },
    batch_interval_seconds=86400,  # 1 day between batches
    start_timestamp="2024-01-01T00:00:00Z",
)
```

---

## CDCTableConfig

Per-table configuration controlling how CDC batches are generated.

```python
from dbldatagen.v1.cdc_schema import CDCTableConfig, OperationWeights, MutationSpec

config = CDCTableConfig(
    operations=OperationWeights(insert=3, update=5, delete=2),
    batch_size=0.1,       # 10% of initial rows per batch
    mutations=MutationSpec(columns=None, fraction=0.5),
)
```

### Fields Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `operations` | `OperationWeights` | `OperationWeights()` | Relative weights for insert/update/delete operations |
| `batch_size` | `int \| float \| str` | `0.1` | Rows to change per batch (see below) |
| `mutations` | `MutationSpec` | `MutationSpec()` | Controls which columns mutate on updates |

---

## OperationWeights

Controls the mix of insert, update, and delete operations per batch. Weights are **relative** and automatically normalized to fractions.

```python
from dbldatagen.v1.cdc_schema import OperationWeights

weights = OperationWeights(insert=3, update=5, delete=2)
# Normalized fractions: 30% insert, 50% update, 20% delete
```

### Fields Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `insert` | `float` | `3.0` | Relative weight for insert operations |
| `update` | `float` | `5.0` | Relative weight for update operations |
| `delete` | `float` | `2.0` | Relative weight for delete operations |

### How Weights are Normalized

```python
weights = OperationWeights(insert=3, update=5, delete=2)
total = 3 + 5 + 2  # = 10
fractions = (3/10, 5/10, 2/10)  # (0.3, 0.5, 0.2)

# Access normalized fractions
i_frac, u_frac, d_frac = weights.fractions
# i_frac = 0.3, u_frac = 0.5, d_frac = 0.2
```

If `batch_size = 100`, then:
- **30 inserts** (new rows with IDs beyond initial range)
- **50 updates** (existing rows get new values)
- **20 deletes** (existing rows are marked deleted)

### Common Operation Patterns

```python
# Equal mix
OperationWeights(insert=1, update=1, delete=1)  # 33.3% each

# Insert-heavy (initial population)
OperationWeights(insert=8, update=1, delete=1)  # 80% inserts, 10% updates, 10% deletes

# Update-heavy (SCD2, dimension tables)
OperationWeights(insert=1, update=8, delete=1)  # 10% inserts, 80% updates, 10% deletes

# Delete-heavy (cleanup scenarios)
OperationWeights(insert=1, update=1, delete=8)  # 10% inserts, 10% updates, 80% deletes

# Append-only (event logs)
OperationWeights(insert=1, update=0, delete=0)  # 100% inserts
```

---

## Batch Size Semantics

`batch_size` controls how many rows change per batch. Three formats are supported:

### 1. Float (0.0 - 1.0): Fraction of Initial Rows

```python
CDCTableConfig(batch_size=0.1)  # 10% of initial row count per batch
```

If the table has 10,000 initial rows:
- Batch 1: 1,000 row changes
- Batch 2: 1,000 row changes
- ...

### 2. Integer: Absolute Row Count

```python
CDCTableConfig(batch_size=500)  # Exactly 500 rows change per batch
```

Every batch has exactly 500 changes, regardless of initial table size.

### 3. String: Shorthand Notation

```python
CDCTableConfig(batch_size="10K")   # 10,000 rows per batch
CDCTableConfig(batch_size="2.5M")  # 2,500,000 rows per batch
CDCTableConfig(batch_size="1B")    # 1,000,000,000 rows per batch
```

Supported suffixes:
- `K` = 1,000
- `M` = 1,000,000
- `B` = 1,000,000,000

Fractional values work: `"2.5K"` = 2,500

### Batch Size Examples

```python
# Table with 50,000 initial rows

# Fractional
CDCTableConfig(batch_size=0.05)   # 2,500 rows per batch (5%)
CDCTableConfig(batch_size=0.2)    # 10,000 rows per batch (20%)

# Absolute
CDCTableConfig(batch_size=1000)   # 1,000 rows per batch
CDCTableConfig(batch_size=25000)  # 25,000 rows per batch

# Shorthand
CDCTableConfig(batch_size="5K")   # 5,000 rows per batch
CDCTableConfig(batch_size="100K") # 100,000 rows per batch
```

---

## MutationSpec

Controls which columns mutate during update operations.

```python
from dbldatagen.v1.cdc_schema import MutationSpec

# Default: 50% of non-PK/non-FK columns mutate per update
MutationSpec()

# Specify exact columns
MutationSpec(columns=["status", "updated_at", "score"], fraction=1.0)

# All eligible columns mutate
MutationSpec(columns=None, fraction=1.0)

# Only 20% of eligible columns mutate per update
MutationSpec(columns=None, fraction=0.2)
```

### Fields Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `columns` | `list[str] \| None` | `None` | Specific columns to mutate. `None` means all non-PK/non-FK columns |
| `fraction` | `float` | `0.5` | Fraction of eligible columns that mutate per update row |

### Mutation Rules

1. **Primary key columns never mutate** — PK is the row's identity
2. **Foreign key columns never mutate by default** — preserves referential integrity
3. **If `columns` is `None`**: All non-PK/non-FK columns are eligible
4. **If `columns` is a list**: Only those columns are eligible
5. **`fraction` controls how many eligible columns change per row**

### Examples

```python
# Table: users(user_id, name, email, status, score, updated_at)
# PK: user_id

# Default: 50% of {name, email, status, score, updated_at} mutate per update
MutationSpec()

# Only status and updated_at mutate, always
MutationSpec(columns=["status", "updated_at"], fraction=1.0)

# All non-PK columns mutate
MutationSpec(columns=None, fraction=1.0)

# Only 30% of non-PK columns mutate per update
MutationSpec(columns=None, fraction=0.3)
```

---

## Per-Table Config Overrides

You can configure each table independently using the `table_configs` dictionary:

```python
from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights

plan = CDCPlan(
    base_plan=base,
    num_batches=10,
    table_configs={
        # Event log: append-only
        "events": CDCTableConfig(
            operations=OperationWeights(insert=1, update=0, delete=0),
            batch_size=0.2,  # 20% growth per batch
        ),
        # Dimension table: heavy updates
        "customers": CDCTableConfig(
            operations=OperationWeights(insert=1, update=8, delete=1),
            batch_size=0.05,  # 5% changes per batch
        ),
        # Fact table: balanced mix
        "orders": CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size="10K",  # 10,000 rows per batch
        ),
    },
    default_config=CDCTableConfig(  # Any table not in table_configs uses this
        operations=OperationWeights(insert=3, update=5, delete=2),
        batch_size=0.1,
    ),
)
```

---

## Configuration Presets

Common patterns are provided as convenience functions.

### Append-Only (Event Logs, Audit Trails)

```python
from dbldatagen.v1.cdc_dsl import append_only_config

config = append_only_config(batch_size=0.1)
# Equivalent to:
# CDCTableConfig(
#     operations=OperationWeights(insert=1, update=0, delete=0),
#     batch_size=0.1,
# )
```

Use for tables where rows are never modified after insert:
- Event logs
- Audit trails
- Time-series data
- Immutable fact tables

### High Churn (Stress Testing)

```python
from dbldatagen.v1.cdc_dsl import high_churn_config

config = high_churn_config(batch_size=0.2)
# Equivalent to:
# CDCTableConfig(
#     operations=OperationWeights(insert=1, update=6, delete=3),
#     batch_size=0.2,
# )
```

Use for stress-testing your pipeline with heavy update/delete activity:
- Performance benchmarking
- SCD2 implementation testing
- Tombstone/deletion handling

### SCD2 (Slowly Changing Dimensions)

```python
from dbldatagen.v1.cdc_dsl import scd2_config

config = scd2_config(batch_size=0.1)
# Equivalent to:
# CDCTableConfig(
#     operations=OperationWeights(insert=1, update=8, delete=1),
#     batch_size=0.1,
# )
```

Use for dimension tables with frequent updates and rare deletes:
- Customer dimensions
- Product catalogs
- Reference data

### Using Presets in a Plan

```python
from dbldatagen.v1.cdc_dsl import cdc_plan, append_only_config, scd2_config, high_churn_config

plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    events=append_only_config(batch_size=0.15),
    customers=scd2_config(batch_size=0.05),
    temp_data=high_churn_config(batch_size=0.3),
)
```

---

## DSL Helpers

Convenience functions make configuration more readable.

### `cdc_plan()`

Build a `CDCPlan` with per-table configs as keyword arguments:

```python
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    orders=cdc_config(batch_size=0.10, operations=ops(4, 4, 2)),
)
```

### `cdc_config()`

Build a `CDCTableConfig` with convenient defaults:

```python
from dbldatagen.v1.cdc_dsl import cdc_config, ops, mutations

config = cdc_config(
    batch_size=0.1,
    operations=ops(3, 5, 2),
    mutations_spec=mutations(columns=["status", "updated_at"], fraction=1.0),
)
```

### `ops()`

Shorthand for `OperationWeights`:

```python
from dbldatagen.v1.cdc_dsl import ops

ops(3, 5, 2)  # OperationWeights(insert=3, update=5, delete=2)
ops(insert=1, update=0, delete=0)  # append-only
```

### `mutations()`

Shorthand for `MutationSpec`:

```python
from dbldatagen.v1.cdc_dsl import mutations

mutations()  # Default: all non-PK/FK columns, 50% mutate
mutations(columns=["status"], fraction=1.0)
mutations(fraction=0.8)  # 80% of eligible columns mutate
```

---

## Complete Configuration Example

```python
from pyspark.sql import SparkSession
from dbldatagen.v1 import DataGenPlan, TableSpec, PrimaryKey, pk_auto, fk, faker, text, decimal
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops, append_only_config, scd2_config

spark = SparkSession.builder.appName("cdc-config").getOrCreate()

# Base schema
customers = TableSpec(
    name="customers",
    rows=10000,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        faker("name", "name"),
        text("tier", values=["bronze", "silver", "gold"]),
    ],
)

orders = TableSpec(
    name="orders",
    rows=50000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        decimal("amount", min=10.0, max=1000.0),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
)

events = TableSpec(
    name="events",
    rows=100000,
    primary_key=PrimaryKey(columns=["event_id"]),
    columns=[
        pk_auto("event_id"),
        text("event_type", values=["page_view", "click", "purchase"]),
    ],
)

base = DataGenPlan(tables=[customers, orders, events], seed=42)

# Configure CDC with different patterns per table
plan = cdc_plan(
    base,
    num_batches=20,
    format="delta_cdf",
    batch_interval_seconds=3600,  # 1 hour between batches
    # Dimension: SCD2 pattern, moderate updates
    customers=scd2_config(batch_size=0.05),
    # Fact: balanced mix, 10K rows per batch
    orders=cdc_config(
        batch_size="10K",
        operations=ops(4, 4, 2),  # 40% inserts, 40% updates, 20% deletes
    ),
    # Event log: append-only, 15% growth per batch
    events=append_only_config(batch_size=0.15),
)

# Generate
stream = generate_cdc(spark, plan)

# Verify configuration
print(f"Initial customers: {stream.initial['customers'].count()}")
print(f"Batch 1 customer changes: {stream.batches[0]['customers'].count()}")
print(f"Batch 1 order changes: {stream.batches[0]['orders'].count()}")
print(f"Batch 1 event inserts: {stream.batches[0]['events'].count()}")
```

---

**Related:** [CDC Overview](./overview.md) | [CDC Formats](./formats.md) | [Schema Models](../reference/schema-models.md)
