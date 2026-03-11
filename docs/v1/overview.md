---
title: V1 Overview
sidebar_position: 1
---

# dbldatagen.v1 Overview

`dbldatagen.v1` is a next-generation synthetic data engine that lives alongside the existing V0 API. Both work independently — you can use V0 and V1 in the same project without conflicts.

## What's New in V1

| Feature | Description |
|---------|-------------|
| **Pydantic Schema DSL** | Define tables, columns, distributions, and nested types with validated models |
| **CDC Generation** | Stateful and stateless change-data-capture with SCD Type 2 support |
| **Multi-Table Plans** | Generate related tables with foreign key relationships in a single plan |
| **Connectors** | Import schemas from SQL queries, CSV files, and JDBC connections |
| **Streaming & Ingest** | Micro-batch streaming and bulk ingest simulation |
| **Faker Integration** | Use Faker providers for realistic text generation |
| **Compatibility Layer** | `from_data_generator()` converts V0 specs to V1 for gradual migration |

## Installation

```bash
# V0 + V1 core
pip install dbldatagen[v1]

# V0 + V1 + all optional extras
pip install dbldatagen[v1-dev]

# Individual extras
pip install dbldatagen[v1-faker]   # Faker-based text generation
pip install dbldatagen[v1-csv]     # CSV schema inference
pip install dbldatagen[v1-jdbc]    # Database schema extraction
pip install dbldatagen[v1-sql]     # SQL query parsing
```

## Quick Example

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1.dsl import pk_auto, text, decimal, timestamp

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="orders",
            rows=100_000,
            columns=[
                pk_auto("id"),
                text("status", values=["pending", "shipped", "delivered"]),
                decimal("amount", min=10.0, max=500.0),
                timestamp("created_at", start="2020-01-01", end="2025-12-31"),
            ],
            primary_key=PrimaryKey(columns=["id"]),
        )
    ],
)

result = generate(spark, plan)
df = result["orders"]
df.show()
```

## Next Steps

- [Migration Guide](./migration) — convert existing V0 code to V1
- [DSL Reference](./dsl) — column definition helpers
- [CDC Generation](./cdc) — change data capture
- [Connectors](./connectors) — import schemas from external sources
