"""dbldatagen.v1 -- Deterministic distributed synthetic data generator for Spark."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbldatagen.v1.dsl import (
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
from dbldatagen.v1.engine.generator import generate_table as _generate_table
from dbldatagen.v1.engine.planner import resolve_plan as _resolve_plan
from dbldatagen.v1.engine.streaming import generate_stream as _generate_stream
from dbldatagen.v1.ingest import (
    detect_changes as _detect_changes,
)
from dbldatagen.v1.ingest import (
    generate_ingest as _generate_ingest,
)
from dbldatagen.v1.ingest import (
    generate_ingest_batch as _generate_ingest_batch,
)
from dbldatagen.v1.ingest import (
    write_ingest_to_delta as _write_ingest_to_delta,
)
from dbldatagen.v1.ingest_schema import (
    IngestMode,
    IngestPlan,
    IngestStrategy,
    IngestTableConfig,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from dbldatagen.v1.ingest import IngestStream


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


def generate_stream(spark: SparkSession, table_spec: TableSpec, **kwargs) -> DataFrame:
    """Generate a single streaming DataFrame from a TableSpec.

    Returns a streaming DataFrame using Spark's rate source.
    Supports all column types except FK columns and Feistel (random-unique) PKs.
    """
    return _generate_stream(spark, table_spec, **kwargs)


def generate_ingest(spark: SparkSession, plan_or_base: IngestPlan | DataGenPlan, **kwargs) -> IngestStream:
    """Generate a complete ingest stream (full-row ingestion simulation).

    Returns an IngestStream with initial snapshot and lazy per-batch access.
    """
    return _generate_ingest(spark, plan_or_base, **kwargs)


def generate_ingest_batch(
    spark: SparkSession, plan_or_base: IngestPlan | DataGenPlan, batch_id: int, **kwargs
) -> dict[str, DataFrame]:
    """Generate a single ingest batch independently."""
    return _generate_ingest_batch(spark, plan_or_base, batch_id, **kwargs)


def write_ingest_to_delta(spark: SparkSession, plan_or_base: IngestPlan | DataGenPlan, **kwargs) -> dict[str, str]:
    """Generate an ingest stream and write it to Delta tables."""
    return _write_ingest_to_delta(spark, plan_or_base, **kwargs)


def detect_changes(
    spark: SparkSession,
    before_df: DataFrame,
    after_df: DataFrame,
    key_columns: list[str],
    data_columns: list[str] | None = None,
) -> dict[str, DataFrame]:
    """Identify inserts, updates, deletes between two snapshots."""
    return _detect_changes(spark, before_df, after_df, key_columns, data_columns)


__all__ = [
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "ForeignKeyRef",
    "IngestMode",
    "IngestPlan",
    "IngestStrategy",
    "IngestTableConfig",
    "PrimaryKey",
    "TableSpec",
    "array",
    "decimal",
    "detect_changes",
    "expression",
    "faker",
    "fk",
    "generate",
    "generate_ingest",
    "generate_ingest_batch",
    "generate_stream",
    "integer",
    "pattern",
    "pk_auto",
    "pk_pattern",
    "pk_uuid",
    "struct",
    "text",
    "timestamp",
    "write_ingest_to_delta",
]
