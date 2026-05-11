"""Public top-level generation API for ``dbldatagen.core``.

``generate`` is re-exported from ``dbldatagen.core`` so the public
import path ``from dbldatagen.core import generate`` is unchanged.
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
    """Generate all tables from a DataGenPlan.

    Returns dict[str, DataFrame] keyed by table name.
    Tables are generated in dependency order (parents first).

    ``resolved_plan`` is optional — pass a pre-computed ``ResolvedPlan``
    (from ``resolve_plan(plan)``) to avoid re-resolving when iterating
    over multiple seeds or when composing with lower-level helpers like
    ``generate_table``.

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
