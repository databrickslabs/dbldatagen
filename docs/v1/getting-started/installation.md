---
sidebar_position: 2
title: "Installation"
---

# Installation

> **TL;DR:** Install with `pip install "dbldatagen[v1]"`. Add `[v1-faker]` for realistic text, `[v1-jdbc]` for database connectors, or `[v1-dev]` for everything. Python 3.10+ required.

## Basic Installation

The core `dbldatagen.v1` package requires only `pydantic>=2.0`:

```bash
pip install "dbldatagen[v1]"
```

This installs the schema definitions and DSL. To actually generate data, you'll need PySpark (provided by Databricks runtimes or via the `[spark]` extra).

## Optional Extras

Install additional features with extras:

```bash
# Faker support (names, addresses, emails, etc.)
pip install "dbldatagen[v1-faker]"

# All optional dependencies
pip install "dbldatagen[v1-dev]"
```

### Available Extras

| Extra | Dependencies | Purpose |
|-------|--------------|---------|
| `[spark]` | `pyspark>=4.0` | Spark DataFrame API (not needed on Databricks, which provides PySpark) |
| `[v1-faker]` | `faker>=20.0` | Realistic text generation (names, addresses, emails, phone numbers) |
| `[v1-jdbc]` | `sqlalchemy>=2.0`, `pyyaml>=6.0` | JDBC connectors for reverse engineering database schemas |
| `[v1-csv]` | `pandas>=2.0`, `pyyaml>=6.0` | CSV output and schema inference |
| `[v1-sql]` | `sqlglot>=20.0`, `pyyaml>=6.0` | SQL DDL parsing and YAML plan generation |
| `[v1-dev]` | All of the above | Everything except `[dev]` |
| `[dev]` | `pytest`, `ruff`, etc. | Development tools (testing, linting, packaging) |

## Dependency Matrix

Core and optional dependencies:

| Package | Version | Required | Extra | Purpose |
|---------|---------|----------|-------|---------|
| `pydantic` | >= 2.0 | Yes | — | Schema definition and validation |
| `pyspark` | >= 4.0 | No | `[spark]` | Spark DataFrame API (provided by Databricks runtime) |
| `faker` | >= 20.0 | No | `[v1-faker]` | Realistic text (names, addresses, emails) |
| `sqlalchemy` | >= 2.0 | No | `[v1-jdbc]` | JDBC connection and metadata introspection |
| `pandas` | >= 2.0 | No | `[v1-csv]` | CSV I/O and schema inference |
| `sqlglot` | >= 20.0 | No | `[v1-sql]` | SQL DDL parsing and transpilation |
| `pyyaml` | >= 6.0 | No | `[v1-jdbc]`, `[v1-csv]`, `[v1-sql]` | YAML plan file loading |

## Python Version Requirement

Python 3.10 or higher is required:

```bash
python --version  # Must be 3.10, 3.11, 3.12, or 3.13
```

## Databricks Installation

On Databricks, install directly from a notebook cell:

### From PyPI (when published)

```python
%pip install dbldatagen[v1-faker]
```

### From Wheel File (local or Volumes)

```python
# Upload wheel to Databricks Volumes, then:
%pip install /Volumes/catalog/schema/volume/dbldatagen-0.3.21-py3-none-any.whl[v1-faker]
```

### From Git Repository

```python
%pip install git+https://github.com/databrickslabs/dbldatagen.git
```

**Note:** On Databricks, you do NOT need the `[spark]` extra because PySpark is provided by the runtime. Always restart the Python kernel after installing:

```python
dbutils.library.restartPython()
```

## Verifying Installation

Test that the installation works:

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey, pk_auto, text

# Define a minimal table
table = TableSpec(
    name="test",
    rows=100,
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        text("name", values=["Alice", "Bob", "Charlie"]),
    ],
)

# Generate (requires PySpark)
plan = DataGenPlan(tables=[table], seed=42)
dfs = generate(spark, plan)  # 'spark' must be a SparkSession
dfs["test"].show()
```

If this runs without errors, you're good to go.

## Next Steps

- [Quickstart](./quickstart.md) -- Learn the Python and YAML APIs
- [Running on Databricks](./databricks.md) -- Databricks-specific patterns
