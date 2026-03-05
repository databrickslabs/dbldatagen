# dbldatagen v1: What's New

## Overview

dbldatagen v1 is a ground-up rethink of synthetic data generation for Spark. It ships as `dbldatagen.v1` alongside the existing API -- no breaking changes to v0 users. Install with `pip install dbldatagen[v1]`.

---

## Foreign Keys with Referential Integrity

Define relationships between tables and v1 guarantees every FK value exists in the parent. Supports Zipf-skewed distributions, nullable FKs, and multi-hop joins.

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.dsl import pk_auto, fk, text, decimal
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=500,
            columns=[pk_auto("id"), text("name", values=["Alice", "Bob", "Carol"])],
            primary_key=PrimaryKey(columns=["id"]),
        ),
        TableSpec(
            name="orders",
            rows=5000,
            columns=[
                pk_auto("id"),
                fk("customer_id", ref="customers.id"),
                decimal("amount", min=10.0, max=500.0),
            ],
            primary_key=PrimaryKey(columns=["id"]),
        ),
    ],
)

dfs = generate(spark, plan)
# dfs["orders"] JOIN dfs["customers"] -- zero orphans guaranteed
```

---

## CDC (Change Data Capture) Generation

Generate realistic Insert/Update/Delete streams with configurable churn rates. Each batch contains an `_op` column (`I`, `U`, `D`) and maintains referential integrity across batches. Tested at 500M-3B row scale.

```python
from dbldatagen.v1.cdc import generate_cdc

stream = generate_cdc(spark, plan, num_batches=10)

# Initial snapshot
initial = stream.initial["orders"]

# Batch 1: mix of inserts, updates, deletes
batch = stream.batches[0]["orders"]
batch.groupBy("_op").count().show()
# +---+-----+
# |_op|count|
# +---+-----+
# |  I|  150|
# |  U|   80|
# |  D|   20|
# +---+-----+
```

---

## Multi-Table Plans

One `DataGenPlan` defines an entire star schema -- tables, row counts, FK relationships, and generation order. v0 was one table at a time.

```python
plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(name="regions", rows=10, columns=[pk_auto("id"), text("name", values=["NA", "EU", "APAC"])], primary_key=PrimaryKey(columns=["id"])),
        TableSpec(name="customers", rows=500, columns=[pk_auto("id"), fk("region_id", ref="regions.id")], primary_key=PrimaryKey(columns=["id"])),
        TableSpec(name="products", rows=100, columns=[pk_auto("id"), decimal("price", min=5.0, max=200.0)], primary_key=PrimaryKey(columns=["id"])),
        TableSpec(name="orders", rows=5000, columns=[pk_auto("id"), fk("customer_id", ref="customers.id"), fk("product_id", ref="products.id")], primary_key=PrimaryKey(columns=["id"])),
    ],
)

dfs = generate(spark, plan)
# 4 DataFrames, all FK relationships valid, generated in dependency order
```

---

## Nested Types

Native support for `struct` and `array` columns. No workarounds needed for generating JSON-like nested data.

```python
from dbldatagen.v1.dsl import pk_auto, struct, array, text, integer
from dbldatagen.v1.schema import ValuesColumn

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="items",
            rows=1000,
            columns=[
                pk_auto("id"),
                struct("address", [
                    text("city", ["Austin", "NYC", "LA", "Chicago"]),
                    text("state", ["TX", "NY", "CA", "IL"]),
                    integer("zip", min=10000, max=99999),
                ]),
                array("tags", ValuesColumn(values=["sale", "new", "popular"]), min_length=1, max_length=4),
            ],
        ),
    ],
)

dfs = generate(spark, plan)
dfs["items"].select("address.city", "tags").show(3, truncate=False)
# +-------+----------------------+
# |city   |tags                  |
# +-------+----------------------+
# |Austin |[sale, new]           |
# |NYC    |[popular, sale, new]  |
# |LA     |[sale]                |
# +-------+----------------------+
```

---

## Deterministic at Any Scale

Generation is partition-independent. Same seed + same spec = identical output regardless of cluster size, partition count, or execution order. Uses xxhash64 seed derivation per column per row.

```python
# Run 1: 4-node cluster, 200 partitions
dfs1 = generate(spark, plan)

# Run 2: single node, 2 partitions
dfs2 = generate(spark, plan)

# Identical output
assert dfs1["orders"].collect() == dfs2["orders"].collect()
```

---

## Streaming Support

`generate_stream()` returns a Spark Structured Streaming DataFrame backed by a rate source. FK resolution works in streaming mode too.

```python
from dbldatagen.v1 import generate_stream
from dbldatagen.v1.schema import TableSpec, ColumnSpec, DataType, RangeColumn, SequenceColumn, PrimaryKey

spec = TableSpec(
    name="events",
    rows=0,
    columns=[
        ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
        ColumnSpec(name="score", dtype=DataType.INT, gen=RangeColumn(min=0, max=100)),
    ],
    primary_key=PrimaryKey(columns=["id"]),
    seed=42,
)

sdf = generate_stream(spark, spec, rows_per_second=1000)
# sdf.isStreaming == True
# Write to Delta, Kafka, memory sink, etc.
```

---

## Schema Inference from SQL, CSV, and JDBC

Point v1 at a SQL query, CSV file, or database connection and it infers table schemas, FK relationships, and column types automatically.

```python
# From SQL query
from dbldatagen.v1.connectors import extract_from_sql_query

plan = extract_from_sql_query("""
    SELECT o.order_id, o.amount, c.name, c.email
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
""", dialect="spark")

dfs = generate(spark, plan)

# From CSV files
from dbldatagen.v1.connectors.csv import extract_from_csv

plan = extract_from_csv(["users.csv", "orders.csv"], default_rows=10000)
dfs = generate(spark, plan)

# From a database
from dbldatagen.v1.connectors import extract_from_database

plan = extract_from_database("sqlite:///my.db", tables=["customers", "orders"])
dfs = generate(spark, plan)
```

---

## Faker Integration

Generate realistic names, emails, addresses, phone numbers, and 200+ other data types via a pool-based pandas_udf. Spark Connect compatible.

```python
from dbldatagen.v1.dsl import pk_auto, faker

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="contacts",
            rows=10000,
            columns=[
                pk_auto("id"),
                faker("full_name", provider="name"),
                faker("email", provider="email"),
                faker("phone", provider="phone_number"),
                faker("address", provider="street_address"),
                faker("city", provider="city"),
                faker("company", provider="company"),
            ],
            primary_key=PrimaryKey(columns=["id"]),
        ),
    ],
)

dfs = generate(spark, plan)
dfs["contacts"].show(3, truncate=False)
# +---+----------------+------------------------+--------------+--------------------+----------+-----------+
# |id |full_name       |email                   |phone         |address             |city      |company    |
# +---+----------------+------------------------+--------------+--------------------+----------+-----------+
# |1  |Jennifer Roberts|david42@example.net     |001-555-0123  |742 Evergreen Terrace|Portland |Acme Corp  |
# |2  |Michael Chen    |mchen@example.org       |555-0456      |123 Main St         |Austin   |Globex Inc |
# |3  |Sarah Williams  |swilliams@example.com   |555-0789      |456 Oak Ave         |Denver   |Initech    |
# +---+----------------+------------------------+--------------+--------------------+----------+-----------+
```

Requires: `pip install dbldatagen[v1-faker]`

---

## Declarative YAML/JSON Plans

Define your entire data model in YAML or JSON. Check it into git, share across teams, generate without writing code.

```yaml
# plan.yml
seed: 42
tables:
  - name: customers
    rows: 1000
    primary_key:
      columns: [customer_id]
    columns:
      - name: customer_id
        gen: {strategy: sequence, start: 1, step: 1}
      - name: name
        gen: {strategy: faker, provider: name}
      - name: email
        gen: {strategy: faker, provider: email}

  - name: orders
    rows: 10000
    primary_key:
      columns: [order_id]
    columns:
      - name: order_id
        gen: {strategy: sequence, start: 1, step: 1}
      - name: customer_id
        gen: {strategy: constant, value: null}
        foreign_key: {ref: customers.customer_id}
      - name: amount
        dtype: DOUBLE
        gen: {strategy: range, min: 10.0, max: 500.0}
```

```python
import yaml
from dbldatagen.v1.schema import DataGenPlan
from dbldatagen.v1 import generate

plan = DataGenPlan.model_validate(yaml.safe_load(open("plan.yml")))
dfs = generate(spark, plan)
```

---

## Ingest Simulation

Generate realistic incremental loads with insert/update/delete batches for testing Delta Lake pipelines.

```python
from dbldatagen.v1 import generate_ingest

stream = generate_ingest(spark, plan, num_batches=30)

for batch_id, batch in enumerate(stream.batches):
    df = batch["transactions"]
    # df has _action column: "I" (insert), "U" (update), "D" (delete)
    df.write.mode("append").format("delta").save(f"/tmp/transactions")
```

---

## Automatic v0 to v1 Migration

Existing v0 code converts with one function call. The converter handles ranges, values, weights, expressions, templates, timestamps, nulls, baseColumn, numColumns, uniqueValues, and distributions.

```python
from dbldatagen import DataGenerator
from pyspark.sql.types import IntegerType, StringType, DoubleType
from dbldatagen.v1.compat import from_data_generator
from dbldatagen.v1 import generate

# Existing v0 code
dg = (
    DataGenerator(spark, rows=10000, name="events")
    .withColumn("event_id", IntegerType(), minValue=1, maxValue=100000)
    .withColumn("user_id", IntegerType(), minValue=1, maxValue=5000, baseColumn="event_id")
    .withColumn("category", StringType(), values=["click", "view", "purchase"], weights=[60, 30, 10])
    .withColumn("value", DoubleType(), minValue=0.0, maxValue=100.0)
)

# One line to convert
plan = from_data_generator(dg)

# Generate with v1 engine
result = generate(spark, plan)
df = result["events"]
```

Unsupported features emit clear warnings:
```
UserWarning: Column 'x': format='%05d' has no v1 equivalent.
Consider PatternColumn or ExpressionColumn with format_string().
```

See `docs/MIGRATION_V0_TO_V1.md` for the complete migration reference.
