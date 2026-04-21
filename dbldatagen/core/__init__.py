"""dbldatagen.core -- Deterministic distributed synthetic data generator for Spark."""

from __future__ import annotations


try:
    import pydantic  # noqa: F401
except ImportError:
    raise ImportError(
        "dbldatagen.core requires pydantic>=2.0. " "Install with: pip install 'dbldatagen[core]'"
    ) from None

from typing import TYPE_CHECKING

from dbldatagen.core.engine.generator import generate_table as _generate_table
from dbldatagen.core.engine.planner import resolve_plan as _resolve_plan
from dbldatagen.core.spec import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
    array,
    decimal,
    expression,
    faker,
    fk,
    integer,
    pattern,
    pk_auto,
    pk_pattern,
    pk_uuid,
    struct,
    text,
    timestamp,
)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def generate(spark: SparkSession, plan: DataGenPlan) -> dict[str, DataFrame]:
    """Generate all tables from a DataGenPlan.

    Returns dict[str, DataFrame] keyed by table name.
    Tables are generated in dependency order (parents first).
    """
    resolved = _resolve_plan(plan)
    table_map = {t.name: t for t in plan.tables}
    results = {}
    for table_name in resolved.generation_order:
        table_spec = table_map[table_name]
        df = _generate_table(spark, table_spec, resolved)
        results[table_name] = df
    return results


__all__ = [
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "ForeignKeyRef",
    "PrimaryKey",
    "TableSpec",
    "array",
    "decimal",
    "expression",
    "faker",
    "fk",
    "generate",
    "integer",
    "pattern",
    "pk_auto",
    "pk_pattern",
    "pk_uuid",
    "struct",
    "text",
    "timestamp",
]
