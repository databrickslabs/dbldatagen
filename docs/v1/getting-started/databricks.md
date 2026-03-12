---
sidebar_position: 4
title: "Databricks Setup"
---

# Running on Databricks

> **TL;DR:** Install with `%pip install dbldatagen[v1-faker]` in a Databricks notebook. Use serverless compute for best performance. Write to Unity Catalog with `df.write.saveAsTable("catalog.schema.table")`. No-code UI available via the companion app.

## Notebook Setup

Install `dbldatagen.v1` directly in a Databricks notebook:

### From PyPI (when published)

```python
%pip install dbldatagen[v1-faker]
dbutils.library.restartPython()
```

### From Wheel File (Volumes)

If you have a wheel file uploaded to Databricks Volumes:

```python
%pip install /Volumes/catalog/schema/volume/dbldatagen-0.3.21-py3-none-any.whl[v1-faker]
dbutils.library.restartPython()
```

### From Git Repository

```python
%pip install git+https://github.com/databrickslabs/dbldatagen.git
dbutils.library.restartPython()
```

**Note:** You do NOT need the `[spark]` extra on Databricks because PySpark is provided by the runtime.

## Basic Example

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, fk, faker, text, timestamp, decimal,
)

# Define tables
customers = TableSpec(
    name="customers",
    rows="100K",
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        faker("name", "name"),
        faker("email", "email"),
        text("segment", values=["retail", "wholesale", "enterprise"]),
    ],
)

orders = TableSpec(
    name="orders",
    rows="1M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        timestamp("ordered_at", start="2023-01-01", end="2025-12-31"),
        decimal("total", min=9.99, max=4999.99),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
)

# Generate
plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)  # 'spark' is automatically available in Databricks notebooks

# Display results
dfs["customers"].display()
dfs["orders"].display()
```

## Using with Serverless Compute

`dbldatagen.v1` is fully compatible with Databricks serverless compute:

1. **Create a serverless SQL warehouse or compute cluster** in your Databricks workspace
2. **Run the notebook** -- `dbldatagen.v1` uses Spark Connect by default (no RDD or SparkContext required)
3. **Scale automatically** -- serverless clusters scale based on workload

Serverless is recommended for:
- Large-scale data generation (millions to billions of rows)
- Cost optimization (pay only for what you use)
- Fast startup times

## Writing to Unity Catalog

Write generated DataFrames directly to Unity Catalog tables:

### Delta Tables

```python
# Generate data
dfs = generate(spark, plan)

# Write to Unity Catalog
dfs["customers"].write.format("delta").mode("overwrite").saveAsTable("catalog.schema.customers")
dfs["orders"].write.format("delta").mode("overwrite").saveAsTable("catalog.schema.orders")
```

### Parquet Files

```python
# Write to Volumes as Parquet
dfs["customers"].write.mode("overwrite").parquet("/Volumes/catalog/schema/volume/customers")
dfs["orders"].write.mode("overwrite").parquet("/Volumes/catalog/schema/volume/orders")

# Register as external tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog.schema.customers
    USING PARQUET
    LOCATION '/Volumes/catalog/schema/volume/customers'
""")
```

### JSON Output (for nested types)

`dbldatagen.v1` supports nested types (`struct`, `array`) that serialize naturally to JSON:

```python
from dbldatagen.v1 import struct, array, integer

# Table with nested columns
products = TableSpec(
    name="products",
    rows="10K",
    primary_key=PrimaryKey(columns=["product_id"]),
    columns=[
        pk_auto("product_id"),
        text("name", values=["Widget", "Gadget", "Doohickey"]),
        struct("metadata", fields=[
            text("color", values=["red", "green", "blue"]),
            decimal("weight", min=0.1, max=100.0),
        ]),
        array("tags", element=text(values=["new", "sale", "featured"]), length=3),
    ],
)

plan = DataGenPlan(tables=[products], seed=42)
dfs = generate(spark, plan)

# Write as JSON
dfs["products"].write.mode("overwrite").json("/Volumes/catalog/schema/volume/products")
```

## Using with Databricks Connect (Local Development)

Develop locally and run on Databricks using Databricks Connect:

```python
# Local development setup
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Use dbldatagen.v1 as normal
from dbldatagen.v1 import generate, DataGenPlan, TableSpec
# ... define plan ...
dfs = generate(spark, plan)
```

See [Databricks Connect documentation](https://docs.databricks.com/dev-tools/databricks-connect.html) for setup instructions.

## CDC on Databricks

Generate CDC streams and write to Delta tables with Change Data Feed enabled:

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define CDC plan
cdc = cdc_plan(
    base=plan,
    num_batches=10,
    format="delta_cdf",
    customers=cdc_config(batch_size=0.05, operations=ops(insert=3, update=5, delete=2)),
    orders=cdc_config(batch_size=0.1, operations=ops(insert=5, update=3, delete=1)),
)

# Generate CDC stream
stream = generate_cdc(spark, cdc)

# Write initial snapshot to Delta table
stream.initial["customers"].write.format("delta").mode("overwrite").saveAsTable("catalog.schema.customers")
stream.initial["orders"].write.format("delta").mode("overwrite").saveAsTable("catalog.schema.orders")

# Enable Change Data Feed on tables
spark.sql("ALTER TABLE catalog.schema.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql("ALTER TABLE catalog.schema.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Apply CDC batches using MERGE
for i, batch in enumerate(stream.batches):
    # For customers
    batch["customers"].createOrReplaceTempView("customers_changes")
    spark.sql("""
        MERGE INTO catalog.schema.customers AS target
        USING customers_changes AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED AND source._change_type = 'update_postimage' THEN UPDATE SET *
        WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
        WHEN NOT MATCHED AND source._change_type = 'insert' THEN INSERT *
    """)

    # For orders (similar pattern)
    batch["orders"].createOrReplaceTempView("orders_changes")
    spark.sql("""
        MERGE INTO catalog.schema.orders AS target
        USING orders_changes AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source._change_type = 'update_postimage' THEN UPDATE SET *
        WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
        WHEN NOT MATCHED AND source._change_type = 'insert' THEN INSERT *
    """)

    print(f"Applied batch {i+1}/{len(stream.batches)}")
```

## Performance Tips

### Optimize for Large-Scale Generation

1. **Use serverless compute** for elastic scaling
2. **Repartition output** for balanced file sizes:
   ```python
   dfs["orders"].repartition(200).write.saveAsTable("catalog.schema.orders")
   ```
3. **Adjust Faker pool size** for better concurrency:
   ```python
   from dbldatagen.v1.engine.columns.faker_pool import set_faker_pool_size
   set_faker_pool_size(100)  # Default is 50
   ```
4. **Use pattern PKs instead of UUID** when possible (faster generation)
5. **Avoid unnecessary sorting** -- output order is deterministic but not sorted by PK

### Monitor Resource Usage

Use Databricks metrics to monitor:
- Executor memory usage (Faker pools increase memory)
- Shuffle read/write (FK generation uses minimal shuffle)
- Task runtime (should be balanced across partitions)

## Next Steps

- **[CDC Documentation](../cdc/overview.md)** -- Learn about CDC formats and patterns
- **[Examples](../guides/basic.md)** -- Star schemas, nested JSON, YAML plans
- **[Architecture](../reference/architecture.md)** -- Technical deep-dive into the engine
