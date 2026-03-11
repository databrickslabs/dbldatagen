---
title: CDC Generation
sidebar_position: 4
---

# CDC (Change Data Capture) Generation

`dbldatagen.v1` supports generating realistic CDC streams — sequences of inserts, updates, and deletes that simulate how data changes over time.

## Quick Start

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1.dsl import pk_auto, text, decimal

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="orders",
            rows=1000,
            columns=[
                pk_auto("id"),
                text("status", values=["pending", "shipped", "delivered"]),
                decimal("amount", min=10.0, max=500.0),
            ],
            primary_key=PrimaryKey(columns=["id"]),
        )
    ],
)

stream = generate_cdc(spark, plan, num_batches=5)

# Initial snapshot
initial_df = stream.initial["orders"]

# CDC batches — each contains an _op column with values: I (insert), U (update), D (delete)
for i, batch in enumerate(stream.batches):
    print(f"Batch {i}: {batch['orders'].count()} changes")
```

## CDC Modes

### Stateful CDC

Maintains internal state to produce realistic change sequences. Rows inserted in earlier batches can be updated or deleted in later batches.

```python
stream = generate_cdc(spark, plan, num_batches=10, mode="stateful")
```

### Stateless CDC

Each batch is generated independently without tracking state. Useful for high-volume testing where exact row-level consistency isn't required.

```python
from dbldatagen.v1.cdc import generate_cdc_stateless

batches = generate_cdc_stateless(
    spark, plan,
    num_batches=10,
    insert_ratio=0.5,
    update_ratio=0.3,
    delete_ratio=0.2,
)
```

## SCD Type 2 Support

Generate slowly-changing dimension data with effective dates:

```python
from dbldatagen.v1.cdc import generate_scd2

scd2_df = generate_scd2(
    spark, plan,
    num_versions=5,
    effective_from_col="valid_from",
    effective_to_col="valid_to",
    current_flag_col="is_current",
)
```

## CDC Output Schema

Each CDC batch DataFrame includes these metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| `_op` | string | Operation: `I` (insert), `U` (update), `D` (delete) |
| `_ts` | timestamp | Change timestamp |
| `_batch` | integer | Batch sequence number |

All original table columns are preserved alongside the metadata columns.
