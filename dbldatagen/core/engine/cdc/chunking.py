"""Chunk-level CDC generation: multi-batch fusion for large-scale cluster runs."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from dbldatagen.core.engine.cdc._common import (
    _table_has_faker_columns,
    apply_fk_delete_guard,
    compute_periods_from_config,
)
from dbldatagen.core.engine.cdc.bulk import generate_bulk_inserts
from dbldatagen.core.engine.cdc.formats import apply_format
from dbldatagen.core.engine.cdc.fused import generate_fused_deletes, generate_fused_updates
from dbldatagen.core.engine.cdc.single_batch import generate_cdc_batch_for_table
from dbldatagen.core.engine.cdc.stateless import insert_range
from dbldatagen.core.engine.planner import resolve_plan
from dbldatagen.core.engine.utils import resolve_batch_size, union_all
from dbldatagen.core.spec.cdc_schema import CDCPlan


def _generate_chunk_for_table(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_ids: list[int],
    fmt_name: str,
) -> DataFrame | None:
    """Generate all CDC rows for one table across multiple batches.

    Uses THREE fused DataFrames (one spark.range each) instead of
    per-batch generation, so Spark sees 3 scans instead of 48:

    1. Bulk insert fusion (ONE spark.range for all inserts)
    2. Fused deletes (ONE spark.range filtered by death_tick IN batch_ids)
    3. Fused updates (ONE spark.range filtered by update_due for any batch)

    Falls back to per-batch generation for tables with Faker columns.
    """

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]

    # Faker columns require per-batch generation (Python UDF pools vary per batch)
    if _table_has_faker_columns(table_spec):
        return _generate_chunk_per_batch(spark, plan, table_name, batch_ids, fmt_name)

    config = plan.config_for(table_name)
    global_seed = table_spec.seed if table_spec.seed is not None else plan.base_plan.seed
    initial_rows = int(table_spec.rows)
    resolved = resolve_plan(plan.base_plan)

    # Apply FK parent delete guard
    effective_config = apply_fk_delete_guard(plan, table_name, config)

    batch_size = resolve_batch_size(effective_config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, effective_config)

    # --- Bulk insert fusion (driver-side is cheap: just start_k arithmetic) ---
    insert_infos = []
    for batch_id in batch_ids:
        start_k, end_k = insert_range(batch_id, initial_rows, periods.inserts_per_batch)
        insert_count = end_k - start_k
        if insert_count > 0:
            insert_infos.append((batch_id, start_k, insert_count))

    insert_df = None
    if insert_infos:
        insert_df = generate_bulk_inserts(
            spark,
            table_spec,
            resolved,
            insert_infos,
            global_seed,
            plan,
        )

    parts: list[DataFrame] = []
    if insert_df is not None:
        parts.append(apply_format(insert_df, fmt_name))

    # --- Fused deletes: ONE spark.range for all batch_ids ---
    del_df = generate_fused_deletes(
        spark,
        table_spec,
        resolved,
        periods,
        batch_ids,
        global_seed,
        initial_rows,
        effective_config.min_life,
        plan,
    )
    if del_df is not None:
        parts.append(apply_format(del_df, fmt_name))

    # --- Fused updates: ONE spark.range for before + after ---
    ub_df, ua_df = generate_fused_updates(
        spark,
        table_spec,
        resolved,
        periods,
        batch_ids,
        global_seed,
        initial_rows,
        effective_config.min_life,
        plan,
        update_window=effective_config.update_window,
    )
    if ub_df is not None:
        parts.append(apply_format(ub_df, fmt_name))
    if ua_df is not None:
        parts.append(apply_format(ua_df, fmt_name))

    if not parts:
        return None

    return union_all(parts)


def _generate_chunk_per_batch(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_ids: list[int],
    fmt_name: str,
) -> DataFrame | None:
    """Fallback: generate chunk using per-batch approach (for Faker tables)."""
    parts: list[DataFrame] = []
    for batch_id in batch_ids:
        batch_df = _generate_single_batch_for_table(
            spark,
            plan,
            table_name,
            batch_id,
            fmt_name,
        )
        if batch_df is not None:
            parts.append(batch_df)

    if not parts:
        return None

    return union_all(parts)


def _generate_single_batch_for_table(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_id: int,
    fmt_name: str,
) -> DataFrame | None:
    """Generate one batch for one table, formatted, as a single DataFrame."""
    result = generate_cdc_batch_for_table(spark, plan, table_name, batch_id)
    combined = result.to_dataframe()
    if combined is None:
        return None
    return apply_format(combined, fmt_name)
