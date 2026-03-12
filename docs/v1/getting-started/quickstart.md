---
sidebar_position: 3
title: "Quickstart"
---

# Quickstart

> **TL;DR:** Define tables with `TableSpec`, add columns with DSL functions (`pk_auto`, `fk`, `faker`, etc.), create a `DataGenPlan`, and call `generate(spark, plan)`. Or use YAML plans for configuration-driven workflows.

## Python API Quickstart

This example generates customers and orders with guaranteed referential integrity.

```python
from dbldatagen.v1 import (
    generate,
    DataGenPlan,
    TableSpec,
    PrimaryKey,
    pk_auto,
    pk_pattern,
    fk,
    faker,
    integer,
    decimal,
    text,
    timestamp,
)

# Define customers table
customers = TableSpec(
    name="customers",
    rows="500K",  # 500,000 rows
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_pattern("customer_id", "CUST-{digit:8}"),  # CUST-00000001, CUST-00000002, ...
        faker("name", "name"),                         # Realistic names
        faker("email", "email"),                       # Realistic emails
        text("segment", values=["retail", "wholesale", "enterprise"]),
    ],
)

# Define orders table with foreign key to customers
orders = TableSpec(
    name="orders",
    rows="5M",  # 5,000,000 rows
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),                           # 1, 2, 3, ...
        fk("customer_id", "customers.customer_id"),    # Valid customer_id from customers table
        timestamp("ordered_at", start="2023-01-01", end="2025-12-31"),
        decimal("total", min=9.99, max=4999.99),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
)

# Create plan and generate
plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)  # Returns dict[str, DataFrame]

# Use the DataFrames
dfs["customers"].show()
dfs["orders"].show()

# Verify referential integrity -- every order's customer_id exists in customers
dfs["orders"].join(dfs["customers"], "customer_id").count()  # == dfs["orders"].count()

# Write as Parquet
dfs["customers"].write.mode("overwrite").parquet("/tmp/synth/customers")
dfs["orders"].write.mode("overwrite").parquet("/tmp/synth/orders")
```

### Key Concepts

- **`TableSpec`**: defines a table with name, row count, primary key, and columns
- **`pk_auto("id")`**: auto-incrementing integer primary key (1, 2, 3, ...)
- **`pk_pattern("id", "CUST-{digit:8}")`**: pattern-formatted primary key (CUST-00000001, ...)
- **`fk("customer_id", "customers.customer_id")`**: foreign key with guaranteed referential integrity
- **`faker("name", "name")`**: realistic text using Faker providers
- **`text("status", values=[...])`**: categorical column with uniform distribution
- **`DataGenPlan`**: combines multiple tables with a seed for determinism

## YAML Quickstart

For configuration-driven workflows, define plans as YAML files:

### `example_plan.yml`

```yaml
seed: 42
tables:
  - name: customers
    rows: 500K
    primary_key:
      columns: [customer_id]
    columns:
      - name: customer_id
        type: pattern
        format: "CUST-{digit:8}"
      - name: name
        type: faker
        provider: name
      - name: email
        type: faker
        provider: email
      - name: segment
        type: text
        values: [retail, wholesale, enterprise]

  - name: orders
    rows: 5M
    primary_key:
      columns: [order_id]
    columns:
      - name: order_id
        type: auto
      - name: customer_id
        type: foreign_key
        ref: customers.customer_id
      - name: ordered_at
        type: timestamp
        start: "2023-01-01"
        end: "2025-12-31"
      - name: total
        type: decimal
        min: 9.99
        max: 4999.99
      - name: status
        type: text
        values: [pending, shipped, delivered, cancelled]
```

### Load and generate from YAML

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.connectors.strategy import load_plan_from_yaml

# Load YAML plan
plan = load_plan_from_yaml("example_plan.yml")

# Generate
dfs = generate(spark, plan)
dfs["customers"].show()
dfs["orders"].show()
```

Or use the included runner script:

```bash
python examples/run_from_yaml.py example_plan.yml
```

## CDC Quickstart

Generate Change Data Capture (CDC) streams with initial snapshots and incremental batches:

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey, pk_auto, text, decimal
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

# Define base plan (same as before)
customers = TableSpec(
    name="customers",
    rows="10K",
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        text("name", values=["Alice", "Bob", "Charlie", "Diana"]),
        decimal("balance", min=0, max=10000),
    ],
)

base_plan = DataGenPlan(tables=[customers], seed=42)

# Define CDC plan
cdc = cdc_plan(
    base=base_plan,
    num_batches=5,                          # Generate 5 batches of changes
    format="delta_cdf",                     # Output format: Delta Change Data Feed
    customers=cdc_config(
        batch_size=0.1,                     # 10% of rows change per batch
        operations=ops(insert=3, update=5, delete=2),  # Weighted operations
    ),
)

# Generate CDC stream
stream = generate_cdc(spark, cdc)

# Initial snapshot (full table)
stream.initial["customers"].show()
# +---+-------+-------+
# | id|   name|balance|
# +---+-------+-------+
# |  1|  Alice| 2345.67|
# |  2|    Bob| 1234.56|
# ...

# Batch 1 changes (with _change_type column)
stream.batches[0]["customers"].show()
# +---+-------+-------+------------+
# | id|   name|balance|_change_type|
# +---+-------+-------+------------+
# | 57|Charlie| 3456.78|      insert|
# |  3|  Alice| 5678.90|      update|
# | 12|   null|   null |      delete|
# ...

# Write batches to separate directories
stream.initial["customers"].write.mode("overwrite").parquet("/tmp/cdc/customers/snapshot")
for i, batch in enumerate(stream.batches):
    batch["customers"].write.mode("overwrite").parquet(f"/tmp/cdc/customers/batch_{i+1}")
```

### CDC Formats

`dbldatagen.v1` supports four CDC output formats:

| Format | Description | `_change_type` Values |
|--------|-------------|----------------------|
| `raw` | Basic format with change type column | `insert`, `update`, `delete` |
| `delta_cdf` | Delta Change Data Feed format | `insert`, `update_postimage`, `delete` |
| `sql_server` | SQL Server CDC format | `I`, `UN`, `D` (with `__$operation` column) |
| `debezium` | Debezium-style CDC | `c` (create), `u` (update), `d` (delete) |

See [CDC documentation](../cdc/overview.md) for details on each format.

## Next Steps

- **[Running on Databricks](./databricks.md)** -- Databricks-specific setup and patterns
- **[Architecture](../reference/architecture.md)** -- Deep dive into determinism, distributions, FK algorithm
- **[Examples](../guides/basic.md)** -- Star schemas, nested JSON, YAML plans
- **[API Reference](../reference/api.md)** -- Complete API documentation
