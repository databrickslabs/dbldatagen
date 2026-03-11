---
title: Connectors
sidebar_position: 5
---

# V1 Connectors

Connectors let you import schemas from external sources and automatically generate `DataGenPlan` specs from them.

## SQL Query Inference

Extract a generation plan from a SQL query. The connector parses column names and types from the query structure.

```python
from dbldatagen.v1.connectors import extract_from_sql_query

plan = extract_from_sql_query("""
    SELECT id, name, email, created_at, amount
    FROM customers
    JOIN orders ON customers.id = orders.customer_id
""")
```

Requires: `pip install dbldatagen[v1-sql]`

## CSV Schema Inference

Infer a generation plan from a CSV file by sampling its contents:

```python
from dbldatagen.v1.connectors import extract_from_csv

plan = extract_from_csv("/path/to/sample.csv", sample_rows=1000)
```

The connector infers:
- Column names and types from the data
- Value ranges for numeric columns
- Discrete value sets for low-cardinality string columns
- Date/timestamp ranges

Requires: `pip install dbldatagen[v1-csv]`

## JDBC Schema Extraction

Extract schema from a database table via JDBC:

```python
from dbldatagen.v1.connectors import extract_from_jdbc

plan = extract_from_jdbc(
    url="jdbc:postgresql://host:5432/mydb",
    table="customers",
    properties={"user": "admin", "password": "..."},
)
```

Requires: `pip install dbldatagen[v1-jdbc]`

## Customizing Inferred Plans

All connectors return a `DataGenPlan` that you can modify before generation:

```python
plan = extract_from_csv("data.csv")

# Adjust row count
plan.tables[0].rows = 1_000_000

# Override a column's generation strategy
from dbldatagen.v1.dsl import faker
plan.tables[0].columns[2] = faker("email", provider="email")

# Generate
from dbldatagen.v1 import generate
result = generate(spark, plan)
```
