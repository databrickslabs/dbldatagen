---
sidebar_position: 1
title: "SQL Schema Extraction"
---

# SQL Connector

> **TL;DR:** Parse SQL queries or DDL statements, infer table schemas (columns, types, primary keys, foreign keys), and produce a `DataGenPlan` — no database connection required. Powered by [sqlglot](https://github.com/tobymao/sqlglot).

## Installation

```bash
pip install "dbldatagen[v1-sql]"
```

Requires the `sqlglot` and `pyyaml` extras.

## Quick Start

```python
from dbldatagen.v1.connectors.sql import extract_from_sql, sql_generate

# Parse SQL and get a DataGenPlan
plan = extract_from_sql("""
    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(200)
    );

    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        amount DECIMAL(10, 2),
        created_at TIMESTAMP
    );
""")

# Or one-shot: parse + generate + register temp views
dfs = sql_generate(spark, sql, row_counts={"customers": 10000, "orders": 50000})
# Now you can run: spark.sql("SELECT * FROM customers JOIN orders ...")
```

## API Reference

### `extract_from_sql(sql, *, dialect=None, row_counts=None, seed=42) -> DataGenPlan`

Parse SQL and build a `DataGenPlan`.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sql` | `str` | required | One or more SQL statements (DDL, SELECT, CTE, multi-statement) |
| `dialect` | `str \| None` | `None` | SQL dialect hint: `"spark"`, `"bigquery"`, `"snowflake"`, `"tsql"`, `"postgres"`, `"mysql"`, etc. |
| `row_counts` | `dict[str, int \| str] \| None` | `None` | Override row counts per table, e.g. `{"customers": "10K"}` |
| `seed` | `int` | `42` | Global seed for deterministic generation |

**Returns:** `DataGenPlan`

### `sql_generate(spark, sql, *, dialect=None, row_counts=None, seed=42, register_temp_views=True) -> dict[str, DataFrame]`

One-shot: parse SQL, generate data, optionally register as temp views so the original SQL query is runnable.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark` | `SparkSession` | required | Active Spark session |
| `sql` | `str` | required | SQL statements |
| `dialect` | `str \| None` | `None` | SQL dialect hint |
| `row_counts` | `dict \| None` | `None` | Override row counts per table |
| `seed` | `int` | `42` | Global seed |
| `register_temp_views` | `bool` | `True` | Register generated tables as Spark temp views |

**Returns:** `dict[str, DataFrame]` keyed by table name.

### `sql_to_yaml(sql, *, dialect=None, row_counts=None, seed=42) -> str`

Parse SQL and return the equivalent YAML plan string.

**Returns:** YAML string representing the inferred `DataGenPlan`.

## Schema Inference

The SQL connector uses a multi-stage pipeline:

1. **Parse** (`parser.py`) — sqlglot-based parsing of DDL, SELECT, CTEs, and multi-statement SQL
2. **Infer** (`inference.py`) — PK/FK inference from column names and JOIN patterns
3. **Build** (`plan_builder.py`) — construct `DataGenPlan` with appropriate column strategies

### Primary Key Inference

- Columns named `id` or `<table>_id` are inferred as PKs
- Tables without a natural PK get a synthesized `_synth_id` column
- Explicit `PRIMARY KEY` constraints in DDL are respected

### Foreign Key Inference

- `JOIN ON child.col = parent.id` patterns are detected and produce `ForeignKeyRef`
- Explicit `REFERENCES` constraints in DDL are respected

### Row Count Defaults

- Parent tables: 1,000 rows (configurable via `row_counts`)
- Child tables: `parent_rows * 5` (5x multiplier)
- Tables are generated in topological order (parents first)

## Supported SQL Features

- `CREATE TABLE` with column types, constraints, PKs, FKs
- `SELECT` with JOINs (FK inference from join conditions)
- CTEs (`WITH ... AS`)
- Multi-statement SQL (multiple `CREATE TABLE` + `SELECT`)
- Dialect-specific syntax via sqlglot's dialect parameter

## Example: Complex Schema

```python
sql = """
CREATE TABLE regions (
    region_id INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE stores (
    store_id INT PRIMARY KEY,
    region_id INT REFERENCES regions(region_id),
    name VARCHAR(100),
    opened_date DATE
);

CREATE TABLE sales (
    sale_id INT PRIMARY KEY,
    store_id INT REFERENCES stores(store_id),
    amount DECIMAL(10,2),
    sale_date TIMESTAMP
);

SELECT s.*, st.name as store_name, r.name as region_name
FROM sales s
JOIN stores st ON s.store_id = st.store_id
JOIN regions r ON st.region_id = r.region_id
WHERE s.sale_date > '2024-01-01';
"""

plan = extract_from_sql(sql, row_counts={"regions": 50, "stores": 500, "sales": "1M"})
dfs = generate(spark, plan)
```

---

**Related:** [JDBC Connector](./jdbc.md) | [CSV Connector](./csv.md) | [API Reference](../reference/api.md)
