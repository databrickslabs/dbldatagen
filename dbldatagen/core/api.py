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
    """Generates all tables from a ``DataGenPlan``.

    Tables are generated in dependency order (parents first) so that
    foreign-key children can reference the parent rows that were just
    built.  When ``resolved_plan`` is omitted, this calls
    ``resolve_plan(plan)`` once on entry; pass a pre-computed
    ``ResolvedPlan`` to skip re-resolution when iterating over multiple
    seeds or composing with lower-level helpers like ``generate_table``.

    Args:
        spark: Active ``SparkSession`` used to construct the underlying
          ``DataFrame`` objects.
        plan: The ``DataGenPlan`` to materialise.  Must have ``seed`` set
          (either directly or via Pydantic-time propagation to each
          ``TableSpec``).
        resolved_plan: Optional pre-resolved plan from
          ``resolve_plan(plan)``.  Must have been produced from this
          exact ``plan`` object (identity check, not equality); a
          mismatch would silently combine one plan's table seeds with
          another's FK topology and corrupt FK child columns.

    Returns:
        A ``dict`` mapping each table name to its generated
        ``DataFrame``.  Keys come from ``TableSpec.name``; the iteration
        order follows ``resolved.generation_order`` (parents before
        children).

    Raises:
        ValueError: ``resolved_plan`` was produced from a different
          ``DataGenPlan`` than the one passed in.
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
