---
sidebar_position: 3
title: "CSV Schema Inference"
---

# CSV Connector

> **TL;DR:** Use `extract_from_csv()` to read one or more CSV files, automatically infer column types and primary keys, and produce a `DataGenPlan`. Perfect for expanding small sample datasets into larger test data.

The CSV connector uses [pandas](https://pandas.pydata.org/) to read CSV files, infer column types from data, and produce synthetic data plans. It's the fastest way to turn a small sample CSV into a large test dataset.

## Installation

```bash
pip install "dbldatagen[v1-csv]"
```

This installs `pandas>=2.0` and `pyyaml>=6.0`.

## Quick Start

```python
from dbldatagen.v1.connectors.csv import extract_from_csv
from dbldatagen.v1 import generate

# Extract schema from a single CSV file
plan = extract_from_csv(
    "sample_customers.csv",
    default_rows=10_000  # generate 10K rows instead of original 100
)

# Generate synthetic data
dfs = generate(spark, plan)
customers_df = dfs["sample_customers"]

# Write expanded dataset
customers_df.write.mode("overwrite").parquet("synthetic_customers.parquet")
```

## `extract_from_csv()` Function

The main convenience function:

```python
from dbldatagen.v1.connectors.csv import extract_from_csv

plan = extract_from_csv(
    paths="customers.csv",  # single file or list
    default_rows=1000,      # row count for generated data
    # Any pandas read_csv options:
    sep=",",
    header=0,
    encoding="utf-8"
)
```

**Parameters:**
- `paths` (str | Path | list): One or more CSV file paths. Each file becomes a separate table in the plan.
- `default_rows` (int): Row count to assign each `TableSpec` (default: 1000)
- `**pandas_kwargs`: Forwarded to `pd.read_csv()` (e.g., `sep`, `header`, `encoding`, `dtype`, `parse_dates`)

**Returns:** `DataGenPlan`

### Single File

```python
plan = extract_from_csv("data/users.csv", default_rows=5000)
# Plan contains 1 table named "users"
```

### Multiple Files

```python
plan = extract_from_csv(
    paths=[
        "data/customers.csv",
        "data/products.csv",
        "data/orders.csv"
    ],
    default_rows=10_000
)
# Plan contains 3 tables: customers, products, orders
```

### Custom pandas Options

```python
plan = extract_from_csv(
    "data.csv",
    sep=";",              # semicolon-delimited
    encoding="latin1",    # non-UTF8
    parse_dates=["created_at", "updated_at"],  # parse as datetime
    dtype={"zip_code": str}  # force string type
)
```

## `CSVConnector` Class

For more control, use the `CSVConnector` class:

```python
from dbldatagen.v1.connectors.csv import CSVConnector

connector = CSVConnector(
    paths=["file1.csv", "file2.csv"],
    default_rows=10_000,
    sep=",",
    encoding="utf-8"
)

# Extract as DataGenPlan
plan = connector.extract()

# Or export directly to YAML
connector.to_yaml("output_plan.yaml")
```

**Class Methods:**
- `extract() -> DataGenPlan`: Read each CSV and return a complete plan
- `to_yaml(output_path: str) -> None`: Extract and write the plan as YAML

## How Type Inference Works

The connector reads the full CSV file using pandas and infers types from actual data:

### Stage 1: pandas dtype Detection

pandas automatically infers column types during `read_csv()`:

| pandas dtype | dbldatagen.v1 DataType | Example values |
|--------------|---------------------|----------------|
| `int64` | `LONG` | `1`, `42`, `-100` |
| `float64` | `DOUBLE` | `3.14`, `99.99`, `NaN` |
| `bool` | `BOOLEAN` | `True`, `False` |
| `datetime64[ns]` | `TIMESTAMP` | `2024-01-15 10:30:00` |
| `object` (string) | `STRING` | `"hello"`, `"foo@bar.com"` |

### Stage 2: Boolean String Detection

If pandas infers `object` dtype, the connector checks if all non-null values are `"true"` / `"false"` (case-insensitive):

```csv
is_active
true
false
True
FALSE
```

Detected as `BOOLEAN` (not `STRING`).

### Stage 3: Compute Statistics

For each column, extracts:
- **Nullable**: `True` if any `NaN` values
- **Unique**: `True` if all non-null values are unique
- **Distinct count**: Number of unique values
- **Sample values**: First 50 non-null values

### Stage 4: Strategy Selection

Uses the same `select_strategy()` logic as the JDBC connector:

1. **Primary key** (unique + non-null + "id" in name) → `SequenceColumn()`
2. **Low cardinality** (&lt;10 distinct, cardinality &lt;1% of row count) → `WeightedChoice` with sampled values
3. **Boolean** → `BooleanColumn(p=0.7)`
4. **Timestamp** → `TimestampColumn` with inferred date range
5. **Numeric** → `UniformColumn` with min/max from samples
6. **String** → Name-based inference (email → faker, phone → faker) or `StringColumn`

**Example:**
```csv
customer_id,email,age,status
1,alice@example.com,28,active
2,bob@example.com,35,active
3,charlie@example.com,42,inactive
```

**Inferred:**
- `customer_id`: `LONG`, `SequenceColumn()` (unique + "id" in name)
- `email`: `STRING`, `FakerColumn(provider="email")` (name pattern)
- `age`: `LONG`, `UniformColumn(min=28, max=42)`
- `status`: `STRING`, `WeightedChoice(values=["active", "inactive"], weights=[2, 1])` (low cardinality: 2 distinct)

## Auto-PK Detection

The connector attempts to detect a primary key using this heuristic:

1. Find the **first** column that is:
   - Unique (all values distinct)
   - Non-null (no `NaN` values)
   - Has `"id"` in its name (case-insensitive)

2. If found, mark it as the PK in the plan

3. If not found, no PK is set (you can add one manually in YAML)

**Examples:**
```csv
# Detected: "id" column
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com

# Detected: "customer_id" column
customer_id,name,email
100,Alice,alice@example.com
101,Bob,bob@example.com

# Not detected: "id" has duplicates
id,product_id,quantity
1,SKU001,10
1,SKU002,5

# Not detected: unique but no "id" in name
uuid,name,email
a1b2c3,Alice,alice@example.com
d4e5f6,Bob,bob@example.com
```

## Complete Example: Expanding a Sample Dataset

You have a 100-row sample CSV of customers and want to generate 10,000 realistic rows:

### Input: `sample_customers.csv`

```csv
id,name,email,age,city,balance,is_active
1,Alice Smith,alice@example.com,28,San Francisco,1250.50,true
2,Bob Jones,bob@example.com,35,New York,3400.00,true
3,Charlie Brown,charlie@example.com,42,Chicago,890.25,false
...
```

### Code

```python
from pyspark.sql import SparkSession
from dbldatagen.v1.connectors.csv import extract_from_csv
from dbldatagen.v1 import generate

spark = SparkSession.builder.getOrCreate()

# Extract schema from 100-row sample
plan = extract_from_csv(
    "sample_customers.csv",
    default_rows=10_000  # expand to 10K rows
)

print("Extracted schema:")
for table in plan.tables:
    print(f"\nTable: {table.name} ({table.rows} rows)")
    for col in table.columns:
        print(f"  {col.name}: {col.dtype} ({col.gen.type if hasattr(col.gen, 'type') else type(col.gen).__name__})")

# Output:
# Extracted schema:
#
# Table: sample_customers (10000 rows)
#   id: long (SequenceColumn)
#   name: string (FakerColumn provider=name)
#   email: string (FakerColumn provider=email)
#   age: long (UniformColumn min=28, max=42)
#   city: string (WeightedChoice values=['San Francisco', 'New York', 'Chicago'])
#   balance: double (UniformColumn min=890.25, max=3400.0)
#   is_active: boolean (BooleanColumn p=0.67)

# Generate 10K rows
dfs = generate(spark, plan)
customers_df = dfs["sample_customers"]

customers_df.show(5)
# +---+----------------+----------------------+---+---------------+--------+---------+
# | id|            name|                 email|age|           city| balance|is_active|
# +---+----------------+----------------------+---+---------------+--------+---------+
# |  1|    Michael Reed| michaelreed@example..|29|   San Francisco|1123.45|     true|
# |  2|     Sarah Lopez|    sarahlopez@exampl..|38|       New York|2890.12|     true|
# |  3|      David Chen|     davidchen@exampl..|31|        Chicago| 945.00|    false|
# |  4|   Jennifer Wang|  jenniferwang@exampl..|40|   San Francisco|3200.75|     true|
# |  5|Christopher White|christopherwhite@exam..|33|       New York|1050.30|     true|
# +---+----------------+----------------------+---+---------------+--------+---------+

# Write expanded dataset
customers_df.write.mode("overwrite").csv("expanded_customers.csv", header=True)
```

## Multi-Table Example

Generate a multi-table dataset from separate CSV files:

### Input Files

**`customers.csv`:**
```csv
customer_id,name,email,city
1,Alice,alice@example.com,SF
2,Bob,bob@example.com,NYC
```

**`orders.csv`:**
```csv
order_id,customer_id,total,status
101,1,250.00,completed
102,1,150.00,completed
103,2,500.00,pending
```

### Code

```python
from dbldatagen.v1.connectors.csv import extract_from_csv
from dbldatagen.v1 import generate

# Extract schemas from both files
plan = extract_from_csv(
    paths=["customers.csv", "orders.csv"],
    default_rows=5000
)

# IMPORTANT: CSV connector cannot detect foreign keys!
# You must add them manually:

from dbldatagen.v1.schema import ForeignKeyRef, Zipf, ConstantColumn

for table in plan.tables:
    if table.name == "orders":
        for col in table.columns:
            if col.name == "customer_id":
                # Replace generated strategy with FK
                col.gen = ConstantColumn(value=None)
                col.foreign_key = ForeignKeyRef(
                    ref="customers.customer_id",
                    distribution=Zipf(exponent=1.2),
                    nullable=False
                )

# Generate with FK relationship
dfs = generate(spark, plan)

# Verify FK integrity
orders_df = dfs["orders"]
customers_df = dfs["customers"]

print("Order customer_id range:",
      orders_df.agg({"customer_id": "min"}).first()[0],
      "to",
      orders_df.agg({"customer_id": "max"}).first()[0])

print("Customer customer_id range:",
      customers_df.agg({"customer_id": "min"}).first()[0],
      "to",
      customers_df.agg({"customer_id": "max"}).first()[0])
```

## Customizing the Plan

The extracted plan is fully editable:

### Export to YAML

```python
connector = CSVConnector("data.csv", default_rows=10_000)
connector.to_yaml("plan.yaml")

# Edit plan.yaml, then load:
from dbldatagen.v1 import load_plan, generate

plan = load_plan("plan.yaml")
dfs = generate(spark, plan)
```

### Override strategies in code

```python
from dbldatagen.v1.schema import FakerColumn, UniformColumn

plan = extract_from_csv("customers.csv")

for table in plan.tables:
    for col in table.columns:
        # Force all emails to be unique
        if col.name == "email":
            col.gen = FakerColumn(provider="email")
            col.unique = True

        # Expand age range
        if col.name == "age":
            col.gen = UniformColumn(min=18, max=80, dtype="int")
```

## Handling Special Cases

### Empty CSV (headers only)

If the CSV has no data rows, the connector infers all columns as `STRING`:

```csv
id,name,email
```

```python
plan = extract_from_csv("empty.csv")
# All columns: dtype=STRING, gen=StringColumn()
```

**Recommendation:** Provide at least 10-20 sample rows for accurate inference.

### Missing Values

pandas represents missing values as `NaN`. The connector marks columns with `NaN` as `nullable=True`:

```csv
id,name,email
1,Alice,alice@example.com
2,Bob,
3,Charlie,charlie@example.com
```

Inferred `email` column: `nullable=True`

### Mixed-Type Columns

If a column has inconsistent types (e.g., `1`, `2.5`, `"three"`), pandas infers `object` dtype → dbldatagen.v1 `STRING`:

```csv
value
1
2.5
three
```

Inferred: `dtype=STRING`

**Recommendation:** Clean the CSV or use pandas `dtype=` parameter to force a type:

```python
plan = extract_from_csv(
    "data.csv",
    dtype={"value": float}  # coerce to float, "three" becomes NaN
)
```

### Large CSV Files

For CSV files larger than available RAM, use pandas chunking:

```python
import pandas as pd
from dbldatagen.v1.connectors.csv import CSVConnector

# Read only first 1000 rows for schema inference
df_sample = pd.read_csv("huge_file.csv", nrows=1000)
df_sample.to_csv("sample.csv", index=False)

# Infer schema from sample
plan = extract_from_csv("sample.csv", default_rows=100_000)
```

### Special Characters in Filenames

If the CSV filename has special characters, the table name is derived from the stem (filename without extension):

```python
plan = extract_from_csv("data-2024-01-15.csv")
# Table name: "data-2024-01-15"

plan = extract_from_csv("/path/to/my_customers (backup).csv")
# Table name: "my_customers (backup)"
```

To override the table name, edit the plan:

```python
plan = extract_from_csv("weird-name.csv")
plan.tables[0].name = "customers"
```

## Limitations

### No Foreign Key Detection

The CSV connector cannot detect foreign key relationships between files. You must add them manually:

```python
plan = extract_from_csv(["customers.csv", "orders.csv"])

# Add FK: orders.customer_id → customers.id
from dbldatagen.v1.schema import ForeignKeyRef, Zipf, ConstantColumn

for table in plan.tables:
    if table.name == "orders":
        for col in table.columns:
            if col.name == "customer_id":
                col.gen = ConstantColumn(value=None)
                col.foreign_key = ForeignKeyRef(
                    ref="customers.id",
                    distribution=Zipf(exponent=1.2)
                )
```

**Alternative:** Use the [SQL Connector](./sql.md) to infer FKs from a SQL query with JOINs.

### No Schema-Level Constraints

The connector cannot detect:
- Unique constraints (except for PK detection)
- Check constraints (e.g., `age >= 18`)
- Default values
- Computed columns

These must be added manually in the YAML plan.

### Limited Date Inference

If pandas doesn't auto-detect datetime columns, use `parse_dates`:

```python
plan = extract_from_csv(
    "events.csv",
    parse_dates=["created_at", "updated_at"]
)
```

Without `parse_dates`, datetime columns are inferred as `STRING`.

## Troubleshooting

### "No columns to parse from file"

The CSV file is empty or malformed. Check:
- File has a header row
- Delimiter is correct (use `sep=` parameter)
- File encoding is correct (use `encoding=` parameter)

### Type inference is wrong

Override pandas dtype:

```python
plan = extract_from_csv(
    "data.csv",
    dtype={
        "zip_code": str,    # force string (prevent leading zero loss)
        "price": float,     # force float
        "is_active": bool   # force boolean
    }
)
```

### Primary key not detected

Add it manually:

```python
from dbldatagen.v1.schema import PrimaryKey, SequenceColumn

plan = extract_from_csv("data.csv")
table = plan.tables[0]

# Add PK
table.primary_key = PrimaryKey(columns=["id"])

# Update column strategy
for col in table.columns:
    if col.name == "id":
        col.gen = SequenceColumn()
```

### "ModuleNotFoundError: No module named 'pandas'"

Install the CSV extra:

```bash
pip install "dbldatagen[v1-csv]"
```

## Comparison with Other Connectors

| Feature | CSV | JDBC | SQL |
|---------|-----|------|-----|
| **Input** | CSV files | Live database | SQL query text |
| **FK detection** | None | From DB constraints | From JOINs |
| **Type inference** | pandas dtypes + samples | Exact (from DB schema) | Heuristic (from names + context) |
| **Data sampling** | Full file read | Optional (first 100 rows) | No |
| **Setup complexity** | Low (just files) | Medium (needs driver + credentials) | Low (just SQL text) |
| **Use case** | Expand sample datasets | Clone production schema | Bootstrap test data for queries |

## See Also

- [JDBC Connector](./jdbc.md) — Extract from live databases
- [SQL Connector](./sql.md) — Extract from SQL queries
- [YAML Plans Guide](../data-generation/yaml-plans.md) — Customize generated plans
- [Column Strategies Guide](../core-concepts/column-strategies.md) — All generation strategies
- [Writing Output Guide](../data-generation/writing-output.md) — Export to CSV, Parquet, Delta, etc.
