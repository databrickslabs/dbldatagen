"""dbldatagen.core -- Deterministic distributed synthetic data generator for Spark."""

from __future__ import annotations


try:
    import pydantic  # noqa: F401
except ImportError:
    raise ImportError(
        "dbldatagen.core requires pydantic>=2.0. " "Install with: pip install 'dbldatagen[core]'"
    ) from None

from typing import TYPE_CHECKING

from dbldatagen.core.engine.cdc import (
    CDCStream,
    DeltaWriteResult,
    generate_cdc,
    generate_cdc_batch,
    generate_cdc_bulk,
    write_cdc_to_delta,
)

# ``generate_expected_state`` is a driver-side O(N) test oracle with a
# 100K-row hard cap — it's deliberately NOT on the top-level __all__
# to keep users from discovering it as a production-path helper.  Tests
# and advanced debugging can still import it via
# ``from dbldatagen.core.engine.cdc import generate_expected_state``.
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
    double,
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
from dbldatagen.core.spec.cdc_schema import (
    CDCFormat,
    CDCPlan,
    CDCTableConfig,
    MutationSpec,
    OperationWeights,
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
    "CDCFormat",
    "CDCPlan",
    "CDCStream",
    "CDCTableConfig",
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "DeltaWriteResult",
    "ForeignKeyRef",
    "MutationSpec",
    "OperationWeights",
    "PrimaryKey",
    "TableSpec",
    "array",
    "decimal",
    "double",
    "expression",
    "faker",
    "fk",
    "generate",
    "generate_cdc",
    "generate_cdc_batch",
    "generate_cdc_bulk",
    "integer",
    "pattern",
    "pk_auto",
    "pk_pattern",
    "pk_uuid",
    "struct",
    "text",
    "timestamp",
    "write_cdc_to_delta",
]
