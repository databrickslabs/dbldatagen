---
sidebar_position: 1
title: "Overview"
---

# dbldatagen.v1

> **TL;DR:** `dbldatagen.v1` generates realistic tabular test data on Apache Spark with guaranteed referential integrity, deterministic output, and CDC support. Define schemas with Pydantic, get DataFrames with working FK relationships, realistic distributions, and nested types.

## What is dbldatagen.v1?

`dbldatagen.v1` generates realistic tabular test data on Apache Spark. You define your schema with Pydantic models, and it produces DataFrames with:

- **Deterministic output** -- same config, same seed, same data. Every time. Regardless of cluster size.
- **Primary keys** -- sequential, UUID, pattern-formatted, or random-unique integers via Feistel cipher.
- **Foreign keys with guaranteed referential integrity** -- FK values are valid parent PKs by construction, not by coincidence. Joins, filters, and aggregations work out of the box.
- **Realistic distributions** -- uniform, normal, Zipf, exponential, weighted. Control the shape of your data.
- **Nested types** -- struct columns (nested objects) and array columns for rich JSON output.
- **CDC (Change Data Capture)** -- generate initial snapshots + batches of inserts, updates, and deletes. Four output formats: raw, Delta CDF, SQL Server, Debezium.
- **Faker integration** -- names, addresses, emails via a high-performance pool strategy.
- **Multiple output formats** -- write as Parquet, Delta, or JSON (nested structs/arrays serialize naturally).
- **Spark Connect native** -- designed for Spark 4+ where Connect is the default. No RDD or SparkContext required.

## Quick Start

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, pk_pattern, fk, faker, integer, decimal, text, timestamp,
)

customers = TableSpec(
    name="customers",
    rows="500K",
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_pattern("customer_id", "CUST-{digit:8}"),
        faker("name", "name"),
        faker("email", "email"),
        text("segment", values=["retail", "wholesale", "enterprise"]),
    ],
)

orders = TableSpec(
    name="orders",
    rows="5M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        timestamp("ordered_at", start="2023-01-01", end="2025-12-31"),
        decimal("total", min=9.99, max=4999.99),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
)

plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)

# dfs["customers"] and dfs["orders"] are Spark DataFrames
# Every order's customer_id exists in the customers table -- guaranteed.
dfs["orders"].join(dfs["customers"], "customer_id").count()  # == dfs["orders"].count()

# Write as JSON (nested structs/arrays serialize naturally)
for name, df in dfs.items():
    df.write.mode("overwrite").json(f"/tmp/synth/{name}")
```

### CDC Quick Start

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

cdc = cdc_plan(
    base=plan,
    num_batches=5,
    format="delta_cdf",
    orders=cdc_config(batch_size=0.1, operations=ops(insert=3, update=5, delete=2)),
)

stream = generate_cdc(spark, cdc)
stream.initial["orders"].show()      # full initial snapshot
stream.batches[0]["orders"].show()   # batch 1 changes (with _change_type column)
```

## Documentation

- **[Getting Started](./installation.md)** -- Installation, quickstart, and Databricks setup
- **[Core Concepts](../core-concepts/tables-and-columns.md)** -- Step-by-step guides for common workflows
  - [Ingest Pipeline](../ingestion/overview.md) -- `write_ingest_to_delta`, incremental vs snapshot, STATELESS strategy
  - [Scaling to 100B+ Rows](../data-generation/scaling.md) -- Cluster config, benchmarks, bottleneck analysis
- **[CDC](../cdc/overview.md)** -- Change Data Capture patterns and formats
- **[Connectors](../connectors/sql.md)** -- JDBC, CSV, and reverse engineering from existing databases
- **[Examples](../guides/basic.md)** -- Practical recipes for star schemas, nested JSON, YAML plans
- **[Reference](../reference/api.md)** -- API reference, architecture deep-dives, and concepts

## Installation

```bash
pip install "dbldatagen[v1]"

# With Faker support (optional):
pip install "dbldatagen[v1-faker]"

# With all optional dependencies:
pip install "dbldatagen[v1-dev]"
```

See [Installation](./installation.md) for detailed instructions on optional extras and Databricks setup.

## YAML Plans

Define plans as YAML files and run them with the included runner:

```bash
python examples/run_from_yaml.py examples/yaml/01_simple.yml
python examples/run_from_yaml.py examples/yaml/05_cdc_medium.yml
```

See the [YAML Plans guide](../data-generation/yaml-plans.md) for the full set of YAML examples.
