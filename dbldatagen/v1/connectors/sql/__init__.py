"""SQL connector — parse SQL queries and produce dbldatagen.v1 plans.

Parse a SQL query, infer table schemas, and produce a dbldatagen.v1
``DataGenPlan`` for every referenced table so the original query
can run successfully.

Install extra::

    pip install 'dbldatagen[v1-sql]'
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import yaml

from dbldatagen.v1 import generate
from dbldatagen.v1.connectors.sql.inference import infer_schema
from dbldatagen.v1.connectors.sql.parser import SQLParseError, parse_sql
from dbldatagen.v1.connectors.sql.plan_builder import build_plan
from dbldatagen.v1.schema import DataGenPlan


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def extract_from_sql(
    sql: str,
    *,
    dialect: str | None = None,
    row_counts: dict[str, int | str] | None = None,
    seed: int = 42,
) -> DataGenPlan:
    """Parse a SQL query and build a dbldatagen.v1 ``DataGenPlan``.

    Parameters
    ----------
    sql : str
        One or more SQL statements (SELECT, CTE, multi-statement, etc.).
    dialect : str | None
        SQL dialect hint (``"spark"``, ``"bigquery"``, ``"snowflake"``,
        ``"tsql"``, ``"postgres"``, ``"mysql"``, etc.).
    row_counts : dict | None
        Override row counts per table, e.g. ``{"customers": 1000}``.
    seed : int
        Global seed for deterministic generation.

    Returns
    -------
    DataGenPlan
    """
    extracted = parse_sql(sql, dialect=dialect)
    inferred = infer_schema(extracted)
    return build_plan(inferred, row_counts=row_counts, seed=seed)


def sql_generate(
    spark: SparkSession,
    sql: str,
    *,
    dialect: str | None = None,
    row_counts: dict[str, int | str] | None = None,
    seed: int = 42,
    register_temp_views: bool = True,
) -> dict[str, DataFrame]:
    """One-shot: parse SQL, generate data, optionally register as temp views.

    Returns ``dict[str, DataFrame]`` keyed by table name.
    """
    plan = extract_from_sql(sql, dialect=dialect, row_counts=row_counts, seed=seed)
    dfs = generate(spark, plan)
    if register_temp_views:
        for name, df in dfs.items():
            df.createOrReplaceTempView(name)
    return dfs


def sql_to_yaml(
    sql: str,
    *,
    dialect: str | None = None,
    row_counts: dict[str, int | str] | None = None,
    seed: int = 42,
) -> str:
    """Parse SQL and return the equivalent YAML plan string."""
    plan = extract_from_sql(sql, dialect=dialect, row_counts=row_counts, seed=seed)
    result: str = yaml.dump(
        plan.model_dump(mode="json", exclude_defaults=False),
        default_flow_style=False,
        sort_keys=False,
    )
    return result


__all__ = ["SQLParseError", "extract_from_sql", "sql_generate", "sql_to_yaml"]
