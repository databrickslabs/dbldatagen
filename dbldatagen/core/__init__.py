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
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import ResolvedPlan, resolve_plan
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


def generate(
    spark: SparkSession,
    plan: DataGenPlan,
    resolved_plan: ResolvedPlan | None = None,
) -> dict[str, DataFrame]:
    """Generate all tables from a DataGenPlan.

    Returns dict[str, DataFrame] keyed by table name.
    Tables are generated in dependency order (parents first).

    ``resolved_plan`` is optional — pass a pre-computed ``ResolvedPlan``
    (from ``resolve_plan(plan)``) to avoid re-resolving when iterating
    over multiple seeds or when composing with lower-level helpers like
    ``generate_table``.  This matches the ``generate_cdc`` path, which
    also resolves once and threads the result through every batch.

    A ``resolved_plan`` must have been produced from *this* ``plan``
    object: the generator follows ``resolved.generation_order`` but
    fetches each ``TableSpec`` from ``plan.tables``, so a mismatch
    (e.g. ``generate(planA, resolved_plan=resolve_plan(planB))``)
    silently uses planA's table seeds and planB's FK topology,
    corrupting FK child-column output without any error.  The identity
    check below catches the mismatch at entry.
    """
    if resolved_plan is not None and resolved_plan.plan is not plan:
        raise ValueError(
            "resolved_plan was produced from a different DataGenPlan than the "
            "one passed to generate().  The caller must pass the same plan "
            "object used for ``resolve_plan(plan)``; otherwise FK resolution "
            "and table seeds refer to different plans.  Re-resolve with "
            "``resolve_plan(plan)`` or drop the ``resolved_plan`` argument."
        )
    resolved = resolved_plan if resolved_plan is not None else resolve_plan(plan)
    table_map = {t.name: t for t in plan.tables}
    results = {}
    for table_name in resolved.generation_order:
        table_spec = table_map[table_name]
        df = generate_table(spark, table_spec, resolved)
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
    "ResolvedPlan",
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
    "generate_table",
    "integer",
    "pattern",
    "pk_auto",
    "pk_pattern",
    "pk_uuid",
    "resolve_plan",
    "struct",
    "text",
    "timestamp",
    "write_cdc_to_delta",
]
