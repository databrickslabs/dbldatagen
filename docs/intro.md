---
title: Introduction
sidebar_position: 1
slug: /
---

# Databricks Labs Data Generator

The **Databricks Labs Data Generator** (`dbldatagen`) provides a convenient way to generate large volumes of synthetic data from within a Databricks notebook or a regular Spark application.

By defining a data generation spec — either in conjunction with an existing schema or by creating one on the fly — you can control how synthetic data is generated.

## Quick Example

```python
import dbldatagen as dg

data_gen = (
    dg.DataGenerator(spark, name="test_data", rows=1_000_000)
    .withColumn("id", "long", uniqueValues=1_000_000)
    .withColumn("name", "string", values=["Alice", "Bob", "Carol"])
    .withColumn("amount", "double", minValue=1.0, maxValue=1000.0)
    .withColumn("date", "date", begin="2020-01-01", end="2024-12-31")
)

df = data_gen.build()
df.show()
```

## Documentation

- **[V0 Docs & API Reference](/dbldatagen/public_docs/)** — current stable documentation
- **[V1 Docs](/docs/v1/getting-started/overview)** — next-generation engine (preview)
- **[Migration Guide](/docs/v1/reference/migration)** — upgrading from V0 to V1
