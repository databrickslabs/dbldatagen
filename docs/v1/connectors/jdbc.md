---
sidebar_position: 2
title: "JDBC Database Extraction"
---

# JDBC Connector

> **TL;DR:** Connect to any SQLAlchemy-compatible database with `extract_from_jdbc()` to automatically extract schema (tables, columns, types, PKs, FKs, constraints) and produce a `DataGenPlan`. Optionally sample existing data for smarter strategy selection.

The JDBC connector uses [SQLAlchemy](https://www.sqlalchemy.org/) to inspect live database schemas and produce synthetic data plans. It's the best way to create a realistic test database from production schema.

## Installation

```bash
pip install "dbldatagen[v1-jdbc]"
```

This installs `sqlalchemy>=2.0` and `pyyaml>=6.0`. You may also need database-specific drivers:

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install pymysql

# SQL Server
pip install pyodbc

# Oracle
pip install cx_oracle

# SQLite (built-in with Python, no extra driver needed)
```

## Quick Start

```python
from dbldatagen.v1.connectors.jdbc import extract_from_jdbc
from dbldatagen.v1 import generate

# Extract schema from PostgreSQL
plan = extract_from_jdbc(
    "postgresql://user:pass@localhost:5432/mydb",
    tables=["customers", "orders", "order_items"],  # optional: subset
    default_rows=5000,
    sample=True  # sample existing data for smart defaults
)

# Generate synthetic clone
dfs = generate(spark, plan)

# Write to target database or files
for name, df in dfs.items():
    df.write.mode("overwrite").saveAsTable(f"synth_{name}")
```

## `extract_from_jdbc()` Function

The main convenience function:

```python
from dbldatagen.v1.connectors.jdbc import extract_from_jdbc

plan = extract_from_jdbc(
    connection_string="sqlite:///my.db",
    tables=None,  # None = all tables
    default_rows=1000,
    sample=True,
    schema=None  # database schema name
)
```

**Parameters:**
- `connection_string` (str): Any SQLAlchemy-compatible URL (see "Connection Strings" below)
- `tables` (list[str] | None): Restrict extraction to these table names. `None` = all tables in the schema.
- `default_rows` (int): Row count to assign each `TableSpec` (default: 1000)
- `sample` (bool): Whether to sample up to 100 rows per table for smarter strategy selection (default: `True`)
- `schema` (str | None): Database schema name (e.g., `"public"` in PostgreSQL, `"dbo"` in SQL Server). `None` = default schema.

**Returns:** `DataGenPlan`

## `JDBCConnector` Class

For more control, use the `JDBCConnector` class directly:

```python
from dbldatagen.v1.connectors.jdbc import JDBCConnector

connector = JDBCConnector(
    connection_string="postgresql://localhost/mydb",
    tables=["users", "posts", "comments"],
    default_rows=10_000,
    sample=True,
    schema="public"
)

# Extract as DataGenPlan
plan = connector.extract()

# Or export directly to YAML
connector.to_yaml("output_plan.yaml")
```

**Class Methods:**
- `extract() -> DataGenPlan`: Inspect the database and return a complete plan
- `to_yaml(output_path: str) -> None`: Extract and write the plan as a YAML file

## Connection Strings

SQLAlchemy connection string format:

```
dialect+driver://username:password@host:port/database
```

### PostgreSQL

```python
# Standard driver
"postgresql://user:pass@localhost:5432/mydb"

# psycopg2 (recommended)
"postgresql+psycopg2://user:pass@localhost:5432/mydb"

# Unix socket
"postgresql:///mydb?host=/var/run/postgresql"
```

### MySQL

```python
# pymysql
"mysql+pymysql://user:pass@localhost:3306/mydb"

# mysqlclient
"mysql+mysqldb://user:pass@localhost:3306/mydb"

# With SSL
"mysql+pymysql://user:pass@localhost:3306/mydb?ssl_ca=/path/to/ca.pem"
```

### SQLite

```python
# Relative path
"sqlite:///my_database.db"

# Absolute path (note 4 slashes on Unix)
"sqlite:////absolute/path/to/my.db"

# In-memory database
"sqlite:///:memory:"
```

### SQL Server

```python
# Windows Authentication
"mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server"

# SQL Server Authentication
"mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server"

# Azure SQL Database
"mssql+pyodbc://user@server:pass@server.database.windows.net/mydb?driver=ODBC+Driver+17+for+SQL+Server"
```

### Oracle

```python
# Oracle Instant Client
"oracle+cx_oracle://user:pass@host:1521/?service_name=myservice"

# TNS name
"oracle+cx_oracle://user:pass@tnsname"
```

### Databricks SQL Warehouse

```python
# Via databricks-sql-connector
"databricks://token:<your-token>@<workspace-url>:443/<catalog>/<schema>?http_path=<http-path>"
```

For full documentation, see [SQLAlchemy Database URLs](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).

## How Schema Extraction Works

The connector runs a three-stage inspection process:

### Stage 1: Discover Tables

Uses SQLAlchemy's `Inspector` to list all tables in the target schema:

```python
inspector = inspect(engine)
table_names = inspector.get_table_names(schema=schema)
```

If `tables` parameter is provided, only those tables are processed.

### Stage 2: Extract Metadata Per Table

For each table, extracts:

1. **Columns**: Name, SQL type, nullable flag
2. **Primary Key**: Columns in the PK constraint
3. **Unique Constraints**: Single-column unique constraints
4. **Foreign Keys**: Local column → (referred table, referred column)

```python
# Example extracted metadata for "orders" table:
# Columns: [
#   {"name": "id", "type": "INTEGER", "nullable": False},
#   {"name": "customer_id", "type": "INTEGER", "nullable": False},
#   {"name": "order_date", "type": "TIMESTAMP", "nullable": True},
#   {"name": "total", "type": "NUMERIC(10,2)", "nullable": False}
# ]
# PK: ["id"]
# FKs: {"customer_id": "customers.id"}
```

### Stage 3: Data Sampling (Optional)

If `sample=True`, runs a `SELECT * FROM {table} LIMIT 100` query to collect:

- **Sample values**: First 100 rows per column
- **Distinct count**: Number of unique values in sample

This enables smarter strategy selection (e.g., detecting enum-like columns with low cardinality).

### Stage 4: Strategy Selection

For each column, the connector calls `select_strategy()` to pick an appropriate generation method:

**Decision tree:**
1. **Primary Key** → `SequenceColumn()` (auto-increment)
2. **Foreign Key** → `ConstantColumn(None)` + `ForeignKeyRef` (with Zipf distribution)
3. **Low-cardinality sample** (&lt;10 distinct values in sample, cardinality &lt;1% of row count) → `WeightedChoice` with sampled values
4. **Boolean type** → `BooleanColumn(p=0.7)` (70% true rate)
5. **Date/timestamp type** → `DateColumn` or `TimestampColumn` with recent date range
6. **Numeric type** → `UniformColumn` with inferred min/max from samples
7. **String type** → Name-based inference (email → faker, phone → faker, etc.) or `StringColumn` with random strings

**Example:**
```python
# Column: "status" (VARCHAR), samples: ["pending", "shipped", "delivered"]
# Strategy: WeightedChoice with equal weights

# Column: "email" (VARCHAR), no samples
# Strategy: FakerColumn(provider="email")

# Column: "created_at" (TIMESTAMP), no samples
# Strategy: TimestampColumn(start="2020-01-01", end="now")
```

## Type Mapping

SQL types are mapped to dbldatagen.v1 `DataType`:

| SQL Type | DataType | Notes |
|----------|----------|-------|
| `INTEGER`, `SMALLINT`, `TINYINT` | `INT` | 32-bit |
| `BIGINT`, `INT64`, `LONG` | `LONG` | 64-bit |
| `FLOAT`, `REAL` | `FLOAT` | 32-bit |
| `DOUBLE`, `FLOAT64`, `DECIMAL`, `NUMERIC`, `MONEY` | `DOUBLE` | 64-bit |
| `BOOLEAN`, `BOOL`, `BIT` | `BOOLEAN` | True/False |
| `DATE` | `DATE` | Date only (no time) |
| `TIMESTAMP`, `DATETIME`, `TIMESTAMPTZ` | `TIMESTAMP` | Date + time |
| `VARCHAR`, `CHAR`, `TEXT`, `STRING`, `CLOB` | `STRING` | Variable-length text |

For unsupported types (e.g., JSON, ARRAY, GEOGRAPHY), defaults to `STRING`.

## Complete Example: PostgreSQL E-commerce DB

```python
from pyspark.sql import SparkSession
from dbldatagen.v1.connectors.jdbc import extract_from_jdbc
from dbldatagen.v1 import generate

spark = SparkSession.builder.getOrCreate()

# Connect to production PostgreSQL (read-only recommended!)
plan = extract_from_jdbc(
    connection_string="postgresql://readonly:pass@prod-db:5432/ecommerce",
    tables=[
        "customers",
        "addresses",
        "products",
        "orders",
        "order_items",
        "reviews"
    ],
    default_rows=10_000,
    sample=True,  # sample 100 rows per table for smart defaults
    schema="public"
)

print(f"Extracted {len(plan.tables)} tables:")
for table in plan.tables:
    pk_cols = table.primary_key.columns if table.primary_key else []
    fk_cols = [c.name for c in table.columns if c.foreign_key]
    print(f"  {table.name}: {len(table.columns)} columns, PK={pk_cols}, FKs={fk_cols}")

# Output:
# Extracted 6 tables:
#   customers: 8 columns, PK=['id'], FKs=[]
#   addresses: 10 columns, PK=['id'], FKs=['customer_id']
#   products: 6 columns, PK=['id'], FKs=[]
#   orders: 7 columns, PK=['id'], FKs=['customer_id', 'shipping_address_id']
#   order_items: 6 columns, PK=['id'], FKs=['order_id', 'product_id']
#   reviews: 7 columns, PK=['id'], FKs=['product_id', 'customer_id']

# Generate synthetic data
dfs = generate(spark, plan)

# Write to dev database
for name, df in dfs.items():
    df.write.jdbc(
        url="postgresql://dev-db:5432/ecommerce_dev",
        table=name,
        mode="overwrite",
        properties={"user": "dev", "password": "dev"}
    )

print("Synthetic dev database ready!")
```

## Customizing the Plan

The extracted plan is a starting point — you'll often want to edit it:

### Export to YAML for editing

```python
connector = JDBCConnector("postgresql://localhost/mydb")
connector.to_yaml("plan.yaml")

# Edit plan.yaml manually, then load:
from dbldatagen.v1 import load_plan, generate

plan = load_plan("plan.yaml")
dfs = generate(spark, plan)
```

### Override row counts

```python
plan = extract_from_jdbc(
    "postgresql://localhost/mydb",
    default_rows=1000  # default for all tables
)

# Adjust specific tables
for table in plan.tables:
    if table.name == "customers":
        table.rows = 50_000
    elif table.name == "orders":
        table.rows = 250_000  # 5:1 ratio
```

### Replace strategies

```python
from dbldatagen.v1.schema import FakerColumn

plan = extract_from_jdbc("sqlite:///my.db")

# Find the "email" column in "users" table
for table in plan.tables:
    if table.name == "users":
        for col in table.columns:
            if col.name == "email":
                col.gen = FakerColumn(provider="email")
                col.unique = True  # enforce uniqueness
```

## Handling Large Schemas

For databases with 100+ tables, extraction can be slow. Strategies:

### 1. Extract subset of tables

```python
# Only extract tables you need for testing
plan = extract_from_jdbc(
    "postgresql://localhost/mydb",
    tables=["orders", "customers", "products"],  # subset
    sample=False  # skip sampling for speed
)
```

### 2. Disable sampling

```python
# Skip the SELECT queries (faster, less accurate strategies)
plan = extract_from_jdbc(
    "postgresql://localhost/mydb",
    sample=False
)
```

### 3. Extract once, reuse YAML

```python
# One-time extraction
connector = JDBCConnector("postgresql://localhost/mydb")
connector.to_yaml("prod_schema.yaml")

# Reuse the YAML file (no DB connection needed)
from dbldatagen.v1 import load_plan, generate

plan = load_plan("prod_schema.yaml")
dfs = generate(spark, plan)
```

## Troubleshooting

### "No module named 'psycopg2'" (or other driver)

Install the database-specific driver:

```bash
pip install psycopg2-binary  # PostgreSQL
pip install pymysql           # MySQL
pip install pyodbc            # SQL Server
```

### "Table not found" or "Schema not found"

Specify the correct schema name:

```python
# PostgreSQL: schema is usually "public"
plan = extract_from_jdbc(
    "postgresql://localhost/mydb",
    schema="public"
)

# SQL Server: schema is often "dbo"
plan = extract_from_jdbc(
    "mssql+pyodbc://localhost/mydb",
    schema="dbo"
)
```

### Sampling fails with "Permission denied"

Disable sampling if the connection user lacks SELECT permissions:

```python
plan = extract_from_jdbc(
    "postgresql://limited_user:pass@localhost/mydb",
    sample=False  # no SELECT queries
)
```

### Foreign keys not detected

SQLAlchemy only detects FKs that are defined in the database schema. If your DB uses application-level FKs (not DB constraints), you'll need to add them manually in YAML:

```yaml
tables:
- name: orders
  columns:
  - name: customer_id
    gen: constant
    value: null
    foreign_key:
      ref: customers.id
      distribution:
        type: zipf
        exponent: 1.2
```

### Unsupported column type (JSON, ARRAY, etc.)

Unsupported types default to `STRING`. To customize:

```python
from dbldatagen.v1.schema import JsonColumn

plan = extract_from_jdbc("postgresql://localhost/mydb")

for table in plan.tables:
    if table.name == "events":
        for col in table.columns:
            if col.name == "metadata":
                # Replace STRING with JSON
                col.dtype = DataType.STRING  # Spark writes as STRING
                col.gen = JsonColumn(
                    schema={
                        "user_id": "int",
                        "action": "string",
                        "timestamp": "timestamp"
                    }
                )
```

## Comparison with Other Connectors

| Feature | JDBC | SQL | CSV |
|---------|------|-----|-----|
| **Input** | Live database | SQL query text | CSV files |
| **FK detection** | From DB constraints | From JOINs | None |
| **Type inference** | Exact (from DB schema) | Heuristic (from names + context) | pandas dtypes |
| **Data sampling** | Yes (optional) | No | Full file |
| **Setup complexity** | Medium (needs driver + credentials) | Low (just SQL text) | Low (just files) |
| **Use case** | Clone production schema | Bootstrap test data for queries | Expand sample datasets |

## See Also

- [SQL Connector](./sql.md) — Extract from SQL queries
- [CSV Connector](./csv.md) — Infer schema from CSV files
- [YAML Plans Guide](../data-generation/yaml-plans.md) — Customize generated plans
- [Foreign Keys Guide](../core-concepts/foreign-keys.md) — Deep dive on FK generation
- [Column Strategies Guide](../core-concepts/column-strategies.md) — All generation strategies
