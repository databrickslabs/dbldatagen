"""Schema extraction connectors for dbldatagen.v1.

Connect to external data sources, read their schema, and produce a
DataGenPlan (or YAML file) suitable for synthetic data generation.

Install extras for the connector you need::

    pip install 'dbldatagen[v1-jdbc]'   # SQLAlchemy-based (SQLite, Postgres, Oracle, …)
    pip install 'dbldatagen[v1-csv]'    # pandas-based CSV schema inference
"""

from __future__ import annotations

from dbldatagen.v1.connectors.base import InferredColumn, SchemaConnector
from dbldatagen.v1.connectors.strategy import select_strategy
from dbldatagen.v1.connectors.types import map_python_type, map_sql_type
from dbldatagen.v1.schema import DataGenPlan


try:
    from dbldatagen.v1.connectors.jdbc import JDBCConnector, extract_from_jdbc

    _HAS_JDBC = True
except ImportError:
    _HAS_JDBC = False

try:
    from dbldatagen.v1.connectors.csv import CSVConnector, extract_from_csv

    _HAS_CSV = True
except ImportError:
    _HAS_CSV = False

try:
    from dbldatagen.v1.connectors.sql import SQLParseError, extract_from_sql, sql_to_yaml

    _HAS_SQL = True
except ImportError:
    _HAS_SQL = False


def extract_from_database(
    connection_string: str,
    tables: list[str] | None = None,
    **kwargs,
) -> DataGenPlan:
    """Extract schema from a SQL database via SQLAlchemy.

    Args:
        connection_string: SQLAlchemy URL (e.g. ``"sqlite:///my.db"``)
        tables: table names to extract (``None`` = all)
        **kwargs: forwarded to :class:`JDBCConnector`

    Raises:
        ImportError: if the ``jdbc`` extra is not installed.
    """
    if not _HAS_JDBC:
        raise ImportError("JDBC connector requires SQLAlchemy. Install with: pip install 'dbldatagen[v1-jdbc]'")
    return extract_from_jdbc(connection_string, tables=tables, **kwargs)


def extract_from_sql_query(
    sql: str,
    *,
    dialect: str | None = None,
    row_counts: dict[str, int | str] | None = None,
    seed: int = 42,
) -> DataGenPlan:
    """Extract schema from a SQL query via sqlglot.

    Args:
        sql: SQL text (SELECT, CTE, multi-statement, etc.)
        dialect: sqlglot dialect name (e.g. ``"spark"``, ``"bigquery"``)
        row_counts: optional per-table row count overrides
        seed: global seed for deterministic generation

    Raises:
        ImportError: if the ``sql`` extra is not installed.
    """
    if not _HAS_SQL:
        raise ImportError("SQL connector requires sqlglot. Install with: pip install 'dbldatagen[v1-sql]'")
    return extract_from_sql(sql, dialect=dialect, row_counts=row_counts, seed=seed)


__all__ = [
    "InferredColumn",
    "SchemaConnector",
    "extract_from_database",
    "extract_from_sql_query",
    "map_python_type",
    "map_sql_type",
    "select_strategy",
]

if _HAS_JDBC:
    __all__ += ["JDBCConnector", "extract_from_jdbc"]
if _HAS_CSV:
    __all__ += ["CSVConnector", "extract_from_csv"]
if _HAS_SQL:
    __all__ += ["SQLParseError", "extract_from_sql", "sql_to_yaml"]
