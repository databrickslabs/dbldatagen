---
sidebar_position: 4
title: "Writing Output"
---

# Writing Output

> **TL;DR:** Use standard Spark DataFrame write operations to persist generated data. Parquet and Delta are recommended for production. JSON works seamlessly with nested types. Unity Catalog tables are supported. Multi-table and CDC batch patterns are provided below.

After generating data, use Spark's DataFrameWriter API to persist it to storage. dbldatagen.v1 produces standard Spark DataFrames, so all output formats and features work out of the box.

## Writing Parquet

Parquet is the recommended format for large datasets. It's columnar, compressed, and optimized for analytics:

```python
from dbldatagen.v1 import generate, DataGenPlan

plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)

# Write single table
dfs["customers"].write.mode("overwrite").parquet("/data/customers")

# Write all tables
for name, df in dfs.items():
    df.write.mode("overwrite").parquet(f"/data/{name}")
```

### Write Modes

```python
# Overwrite existing data
df.write.mode("overwrite").parquet("/data/customers")

# Append to existing data
df.write.mode("append").parquet("/data/customers")

# Error if data exists (default)
df.write.mode("error").parquet("/data/customers")

# Ignore if data exists
df.write.mode("ignore").parquet("/data/customers")
```

### Controlling Output File Count

```python
# Single output file (for small datasets)
df.coalesce(1).write.mode("overwrite").parquet("/data/customers")

# Specific number of files
df.repartition(10).write.mode("overwrite").parquet("/data/customers")

# Let Spark decide (default - one file per partition)
df.write.mode("overwrite").parquet("/data/customers")
```

## Writing Delta Lake

Delta Lake provides ACID transactions, schema enforcement, and time travel:

```python
# Write as Delta
dfs["customers"].write.format("delta").mode("overwrite").save("/data/customers_delta")

# Write all tables as Delta
for name, df in dfs.items():
    df.write.format("delta").mode("overwrite").save(f"/data/{name}_delta")
```

### Delta Write Options

```python
# Overwrite schema
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/data/customers")

# Optimize write with Z-ordering
df.write.format("delta") \
    .mode("overwrite") \
    .option("dataChange", "false") \
    .save("/data/customers")

# Partition overwrite mode
df.write.format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .save("/data/customers")
```

### Delta Merge (Upsert)

```python
from delta.tables import DeltaTable

# First write
dfs["customers"].write.format("delta").mode("overwrite").save("/data/customers")

# Later: merge new data
new_customers = generate(spark, plan)["customers"]

delta_table = DeltaTable.forPath(spark, "/data/customers")
delta_table.alias("target").merge(
    new_customers.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

## Writing to Unity Catalog

Write directly to Unity Catalog managed tables:

```python
# Write to Unity Catalog table
dfs["customers"].write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("catalog.schema.customers")

# Write all tables to Unity Catalog
for name, df in dfs.items():
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"catalog.schema.{name}")
```

### Unity Catalog Options

```python
# Create table with specific location
df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "s3://bucket/path/customers") \
    .saveAsTable("catalog.schema.customers")

# Set table properties
df.write.format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("catalog.schema.customers")

# Create external table
df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "/mnt/external/customers") \
    .saveAsTable("catalog.schema.customers_external")
```

## Writing JSON

JSON output is ideal for nested data structures. Spark serializes structs and arrays naturally:

```python
# Write as JSON
dfs["orders"].write.mode("overwrite").json("/data/orders_json")

# Single-line JSON (one JSON object per line)
dfs["orders"].write.mode("overwrite").json("/data/orders_json")

# Pretty-printed JSON (not recommended for large datasets)
dfs["orders"].write.option("pretty", "true").mode("overwrite").json("/data/orders_json")
```

### JSON with Nested Types

Nested structs and arrays serialize naturally to JSON:

```python
from dbldatagen.v1 import struct, array
from dbldatagen.v1.schema import ColumnSpec, StructColumn, ArrayColumn, ValuesColumn, RangeColumn

products = TableSpec(
    name="products",
    rows=1000,
    columns=[
        pk_auto("product_id"),
        text("name", values=["Laptop", "Mouse"]),

        # Nested struct
        struct("manufacturer", fields=[
            ColumnSpec(name="company", gen=ValuesColumn(values=["Acme", "TechCo"])),
            ColumnSpec(name="country", gen=ValuesColumn(values=["US", "CN"])),
        ]),

        # Array
        array("tags",
            element=ValuesColumn(values=["sale", "new", "premium"]),
            min_length=1, max_length=3),
    ],
)

plan = DataGenPlan(tables=[products], seed=42)
dfs = generate(spark, plan)

# Write nested JSON
dfs["products"].write.mode("overwrite").json("/data/products_json")
```

**Output file (`/data/products_json/part-00000.json`):**
```json
{"product_id":1,"name":"Laptop","manufacturer":{"company":"Acme","country":"US"},"tags":["sale","premium"]}
{"product_id":2,"name":"Mouse","manufacturer":{"company":"TechCo","country":"CN"},"tags":["new"]}
```

### Reading JSON Back

```python
# Read JSON back into DataFrame
df = spark.read.json("/data/products_json")

# Access nested fields
df.select("product_id", "manufacturer.company", "tags").show()
```

## Writing CSV

CSV is useful for simple tabular data and Excel compatibility:

```python
# Write as CSV
dfs["customers"].write.mode("overwrite").csv("/data/customers_csv")

# CSV with header row
dfs["customers"].write.mode("overwrite") \
    .option("header", "true") \
    .csv("/data/customers_csv")

# CSV with custom delimiter
dfs["customers"].write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv("/data/customers_csv")

# Single CSV file
dfs["customers"].coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("/data/customers_csv")
```

**Note:** CSV doesn't support nested types. Structs and arrays are serialized as strings, which may not be desirable. Use JSON or Parquet for nested data.

## Partitioning

Partition data by one or more columns to improve query performance:

```python
# Partition by date
dfs["orders"].write.partitionBy("order_date") \
    .mode("overwrite") \
    .parquet("/data/orders")

# Partition by multiple columns
dfs["orders"].write.partitionBy("order_date", "status") \
    .mode("overwrite") \
    .parquet("/data/orders")

# Partition with Delta
dfs["orders"].write.format("delta") \
    .partitionBy("order_date") \
    .mode("overwrite") \
    .save("/data/orders_delta")
```

### Partition Pruning

When reading partitioned data, Spark prunes partitions based on filter predicates:

```python
# Write partitioned data
dfs["orders"].write.partitionBy("order_date") \
    .mode("overwrite") \
    .parquet("/data/orders")

# Read with partition pruning - only reads 2025-01-15 partition
df = spark.read.parquet("/data/orders") \
    .filter("order_date = '2025-01-15'")
```

### Choosing Partition Columns

Good partition columns have:
- **Low cardinality** - typically 10-10,000 distinct values
- **Frequently filtered** - columns used in WHERE clauses
- **Uniform distribution** - similar data volume per partition

```python
# Good: date columns (365 partitions/year)
write.partitionBy("order_date")

# Good: category columns with moderate cardinality
write.partitionBy("product_category")

# Bad: high cardinality ID columns (millions of partitions)
write.partitionBy("customer_id")  # Don't do this
```

## Multi-Table Output Pattern

Write all tables from a plan with consistent directory structure:

```python
from dbldatagen.v1 import generate

plan = DataGenPlan(tables=[customers, orders, products, order_items], seed=42)
dfs = generate(spark, plan)

output_base = "/data/test_dataset"

# Write all as Parquet
for name, df in dfs.items():
    df.write.mode("overwrite").parquet(f"{output_base}/{name}")

# Write all as Delta
for name, df in dfs.items():
    df.write.format("delta").mode("overwrite").save(f"{output_base}/{name}_delta")

# Write all to Unity Catalog
catalog = "dev"
schema = "test_data"
for name, df in dfs.items():
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{name}")
```

### Multiple Formats Simultaneously

```python
output_base = "/data/test_dataset"

for name, df in dfs.items():
    # Parquet for analytics
    df.write.mode("overwrite").parquet(f"{output_base}/parquet/{name}")

    # JSON for inspection and nested types
    df.write.mode("overwrite").json(f"{output_base}/json/{name}")

    # Delta for ACID and time travel
    df.write.format("delta").mode("overwrite").save(f"{output_base}/delta/{name}")
```

## CDC Batch Output Pattern

Write CDC initial snapshot and batches separately:

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

base = DataGenPlan(tables=[accounts, transactions], seed=500)

plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    accounts=cdc_config(batch_size=0.05, operations=ops(1, 8, 1)),
    transactions=cdc_config(batch_size=0.15, operations=ops(4, 4, 2)),
)

stream = generate_cdc(spark, plan)

output_base = "/data/cdc_dataset"

# Write initial snapshot
for name, df in stream.initial.items():
    df.write.format("delta").mode("overwrite").save(f"{output_base}/{name}/initial")

# Write each batch
for batch_id, batch in enumerate(stream.batches):
    for name, df in batch.items():
        df.write.format("delta").mode("overwrite").save(
            f"{output_base}/{name}/batch_{batch_id}"
        )
```

### CDC to Delta with CDF

Write CDC batches as Delta tables with Change Data Feed enabled:

```python
# Write initial
for name, df in stream.initial.items():
    df.write.format("delta") \
        .mode("overwrite") \
        .option("delta.enableChangeDataFeed", "true") \
        .save(f"/data/{name}")

# Apply batches using Delta merge
from delta.tables import DeltaTable
from pyspark.sql import functions as F

for batch_id, batch in enumerate(stream.batches):
    for name, df in batch.items():
        delta_table = DeltaTable.forPath(spark, f"/data/{name}")

        # Process inserts, updates, deletes from CDC batch
        inserts = df.filter(F.col("_change_type") == "insert")
        updates = df.filter(F.col("_change_type") == "update_postimage")
        deletes = df.filter(F.col("_change_type") == "delete")

        # Apply operations (simplified - full implementation would handle all cases)
        if inserts.count() > 0:
            delta_table.alias("target").merge(
                inserts.alias("source"),
                "target.id = source.id"
            ).whenNotMatchedInsertAll().execute()
```

### CDC to Separate Staging Tables

Write each batch to a staging table for pipeline processing:

```python
catalog = "dev"
schema = "cdc_staging"

# Write initial
for name, df in stream.initial.items():
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.{name}_initial"
    )

# Write batches
for batch_id, batch in enumerate(stream.batches):
    for name, df in batch.items():
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{catalog}.{schema}.{name}_batch_{batch_id}"
        )
```

## Cloud Storage Paths

### AWS S3

```python
df.write.mode("overwrite").parquet("s3://bucket/path/customers")
df.write.format("delta").mode("overwrite").save("s3://bucket/path/customers_delta")
```

### Azure ADLS

```python
df.write.mode("overwrite").parquet("abfss://container@account.dfs.core.windows.net/customers")
df.write.format("delta").mode("overwrite").save("abfss://container@account.dfs.core.windows.net/customers_delta")
```

### GCS

```python
df.write.mode("overwrite").parquet("gs://bucket/path/customers")
df.write.format("delta").mode("overwrite").save("gs://bucket/path/customers_delta")
```

### Databricks DBFS

```python
df.write.mode("overwrite").parquet("dbfs:/mnt/data/customers")
df.write.mode("overwrite").parquet("/dbfs/mnt/data/customers")
```

## Compression

Control compression codec for better performance or smaller files:

### Parquet Compression

```python
# Snappy (default - good balance)
df.write.mode("overwrite").parquet("/data/customers")

# Gzip (smaller files, slower)
df.write.option("compression", "gzip").mode("overwrite").parquet("/data/customers")

# LZ4 (faster, larger files)
df.write.option("compression", "lz4").mode("overwrite").parquet("/data/customers")

# Uncompressed (fastest write, largest files)
df.write.option("compression", "uncompressed").mode("overwrite").parquet("/data/customers")
```

### Delta Compression

```python
# Snappy (default)
df.write.format("delta").mode("overwrite").save("/data/customers")

# Zstd (better compression)
df.write.format("delta") \
    .option("compression", "zstd") \
    .mode("overwrite") \
    .save("/data/customers")
```

## Performance Tips

### Coalesce Before Write

Reduce small file count:

```python
# Bad: 200 partitions = 200 small files
df.write.mode("overwrite").parquet("/data/customers")

# Good: 10 partitions = 10 larger files
df.coalesce(10).write.mode("overwrite").parquet("/data/customers")
```

### Repartition by Partition Column

Improve write performance for partitioned tables:

```python
# Repartition by partition column before write
df.repartition("order_date") \
    .write.partitionBy("order_date") \
    .mode("overwrite") \
    .parquet("/data/orders")
```

### Adaptive Query Execution (AQE)

Enable AQE for better automatic optimization:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# AQE will automatically coalesce partitions during write
df.write.mode("overwrite").parquet("/data/customers")
```

### Delta Optimize

After writing Delta tables, optimize for better read performance:

```python
# Write
df.write.format("delta").mode("overwrite").save("/data/customers")

# Optimize
spark.sql("OPTIMIZE delta.`/data/customers`")

# Optimize with Z-ordering
spark.sql("OPTIMIZE delta.`/data/customers` ZORDER BY (customer_id)")
```

## Validation After Write

Verify data was written correctly:

```python
# Generate and write
dfs = generate(spark, plan)
dfs["customers"].write.mode("overwrite").parquet("/data/customers")

# Read back and validate
df_read = spark.read.parquet("/data/customers")

# Count check
assert df_read.count() == dfs["customers"].count()

# Schema check
assert df_read.schema == dfs["customers"].schema

# Sample data check
dfs["customers"].show(5)
df_read.show(5)
```

## Related Documentation

- [YAML Plans Guide](./yaml-plans.md) - Define plans for repeatable output
- [Nested Types Guide](../core-concepts/nested-types.md) - Write nested JSON
- [Examples](../guides/basic.md) - See output patterns in complete examples
- [Architecture](../reference/architecture.md) - Understand data generation
