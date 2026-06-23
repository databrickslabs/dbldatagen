"""Top-level generation entry point for `dbldatagen.core`.

Defines `generate`, which materializes every table in a plan and returns the
resulting DataFrames keyed by table name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import ResolvedPlan, resolve_plan
from dbldatagen.core.spec.schema import DataGenPlan


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def generate(
    spark: SparkSession,
    plan: DataGenPlan,
    resolved_plan: ResolvedPlan | None = None,
) -> dict[str, DataFrame]:
    """Generates every table in a plan and returns them keyed by name.

    Tables are generated in dependency order, so a foreign-key child is built
    after the parent table it references.

    Args:
        spark: Active `SparkSession` used to build the DataFrames.
        plan: The plan to generate. Its `seed` must be set, directly or
            propagated from `DataGenPlan` to each `TableSpec`.
        resolved_plan: Optional pre-resolved plan from `resolve_plan(plan)`,
            reused to avoid re-resolving when generating the same plan more than
            once (default None). It must come from the same plan object.

    Returns:
        A dict mapping each table name to its generated `DataFrame`, ordered
        with parents before children.

    Raises:
        ValueError: If `resolved_plan` was produced from a different plan than
            the one passed in.
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
