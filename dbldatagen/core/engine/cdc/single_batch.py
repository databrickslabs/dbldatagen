"""Single-batch CDC generation path — stateless three-stream architecture.

Produces insert, update, and delete DataFrames for a single table
and batch using three disjoint ``spark.range()`` streams.  No
driver-side state is required; row lifecycle is computed via the
modular-recurrence functions in ``cdc_stateless``.

Three Streams per batch:
    Stream A (Inserts): spark.range(start_k, end_k)
    Stream B (Deletes): rows where death_tick(k) == batch_n
    Stream C (Updates): rows where update_due(k, batch_n) is True
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc_generator._common import (
    RawBatchResult,
    _add_cdc_metadata,
    _apply_columns_with_write_batch,
    _precompute_write_batches,
    _table_has_faker_columns,
    apply_fk_delete_guard,
    batch_timestamp,
    compute_periods_from_config,
)
from dbldatagen.core.engine.cdc_stateless import (
    CDCPeriods,
    death_tick_expr,
    delete_indices_at_batch_fast,
    insert_range,
    max_k_at_batch,
    pre_image_batch,
    pre_image_batch_expr,
    update_due_expr,
    update_indices_at_batch,
)
from dbldatagen.core.engine.generator import build_all_column_exprs, generate_table
from dbldatagen.core.engine.planner import ResolvedPlan, resolve_plan
from dbldatagen.core.engine.seed import compute_batch_seed
from dbldatagen.core.engine.utils import (
    apply_column_phases,
    create_range_df,
    get_pk_columns,
    resolve_batch_size,
    union_all,
)
from dbldatagen.core.spec.cdc_schema import CDCPlan
from dbldatagen.core.spec.schema import ColumnSpec, SequenceColumn, TableSpec


# ---------------------------------------------------------------------------
# Initial snapshot
# ---------------------------------------------------------------------------


def generate_initial_snapshot(
    spark: SparkSession,
    plan: CDCPlan,
) -> dict[str, DataFrame]:
    """Generate the initial (batch 0) snapshot for all CDC tables.

    This is simply the base plan generation with an ``_op`` = 'I' and
    ``_batch_id`` = 0 marker added.
    """
    resolved = resolve_plan(plan.base_plan)
    table_map = {t.name: t for t in plan.base_plan.tables}
    results = {}
    for table_name in resolved.generation_order:
        if table_name not in plan.cdc_tables:
            continue
        table_spec = table_map[table_name]
        df = generate_table(spark, table_spec, resolved)
        df = (
            df.withColumn("_op", F.lit("I"))
            .withColumn("_batch_id", F.lit(0))
            .withColumn("_ts", F.lit(plan.start_timestamp).cast("timestamp"))
        )
        results[table_name] = df
    return results


# ---------------------------------------------------------------------------
# Main entry point: three-stream batch generation
# ---------------------------------------------------------------------------


def generate_cdc_batch_for_table(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_id: int,
    resolved_plan: ResolvedPlan | None = None,
) -> RawBatchResult:
    """Generate one batch of CDC changes for a single table.

    Uses three disjoint streams (inserts, deletes, updates) computed
    from modular-recurrence functions.  Zero driver-side state.

    Parameters
    ----------
    resolved_plan :
        Pre-resolved plan.  If ``None``, ``resolve_plan`` is called
        internally.  Pass this when generating multiple batches to
        avoid redundant FK validation and topological sorts.

    Returns a ``RawBatchResult`` with separate DataFrames for inserts,
    update before-images, update after-images, and deletes.
    """
    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    config = plan.config_for(table_name)
    global_seed = table_spec.seed if table_spec.seed is not None else plan.base_plan.seed
    initial_rows = int(table_spec.rows)

    # Apply FK parent delete guard
    effective_config = apply_fk_delete_guard(plan, table_name, config)

    batch_size = resolve_batch_size(effective_config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, effective_config)
    resolved = resolved_plan if resolved_plan is not None else resolve_plan(plan.base_plan)
    batch_ts = batch_timestamp(plan, batch_id)
    use_native = not _table_has_faker_columns(table_spec)

    # --- Stream A: Inserts ---
    inserts = None
    if periods.inserts_per_batch > 0:
        inserts = _generate_insert_stream(
            spark,
            table_spec,
            resolved,
            periods,
            batch_id,
            global_seed,
        )
        inserts = _add_cdc_metadata(inserts, "I", batch_id, batch_ts)

    # --- Stream B: Deletes ---
    deletes = None
    if use_native:
        deletes = _generate_delete_stream_native(
            spark,
            table_spec,
            resolved,
            periods,
            batch_id,
            global_seed,
            initial_rows,
            effective_config.min_life,
        )
        deletes = _add_cdc_metadata(deletes, "D", batch_id, batch_ts)
    else:
        del_indices = delete_indices_at_batch_fast(
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            effective_config.min_life,
        )
        if del_indices:
            deletes = _generate_delete_stream(
                spark,
                table_spec,
                resolved,
                periods,
                del_indices,
                batch_id,
                global_seed,
                initial_rows,
            )
            deletes = _add_cdc_metadata(deletes, "D", batch_id, batch_ts)

    # --- Stream C: Updates (before + after images) ---
    updates_before = None
    updates_after = None
    if use_native:
        updates_before = _generate_update_before_stream_native(
            spark,
            table_spec,
            resolved,
            periods,
            batch_id,
            global_seed,
            initial_rows,
            effective_config.min_life,
            update_window=effective_config.update_window,
        )
        updates_before = _add_cdc_metadata(updates_before, "UB", batch_id, batch_ts)

        updates_after = _generate_update_after_stream_native(
            spark,
            table_spec,
            resolved,
            periods,
            batch_id,
            global_seed,
            initial_rows,
            effective_config.min_life,
            update_window=effective_config.update_window,
        )
        updates_after = _add_cdc_metadata(updates_after, "U", batch_id, batch_ts)
    else:
        upd_indices = update_indices_at_batch(
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
            effective_config.min_life,
            update_window=effective_config.update_window,
        )
        if upd_indices:
            updates_before = _generate_update_before_stream(
                spark,
                table_spec,
                resolved,
                periods,
                upd_indices,
                batch_id,
                global_seed,
                initial_rows,
            )
            updates_before = _add_cdc_metadata(updates_before, "UB", batch_id, batch_ts)

            updates_after = _generate_update_after_stream(
                spark,
                table_spec,
                resolved,
                periods,
                upd_indices,
                batch_id,
                global_seed,
                initial_rows,
            )
            updates_after = _add_cdc_metadata(updates_after, "U", batch_id, batch_ts)

    return RawBatchResult(
        table_name=table_name,
        batch_id=batch_id,
        inserts=inserts,
        updates_before=updates_before,
        updates_after=updates_after,
        deletes=deletes,
    )


# ---------------------------------------------------------------------------
# Stream A: Inserts
# ---------------------------------------------------------------------------


def _generate_insert_stream(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    batch_id: int,
    global_seed: int,
) -> DataFrame | None:
    """Generate insert rows using spark.range(start_k, end_k).

    New rows use the batch-specific seed so values differ from initial load.
    PK sequences continue from the correct offset.
    """
    initial_rows = int(table_spec.rows)
    start_k, end_k = insert_range(batch_id, initial_rows, periods.inserts_per_batch)
    insert_count = end_k - start_k
    if insert_count <= 0:
        return None

    batch_seed = compute_batch_seed(global_seed, batch_id)

    # Build columns -- for PKs we need to continue the sequence
    new_columns = []
    pk_cols = get_pk_columns(table_spec)

    for col in table_spec.columns:
        if col.name in pk_cols and isinstance(col.gen, SequenceColumn):
            new_col = ColumnSpec(
                name=col.name,
                dtype=col.dtype,
                gen=SequenceColumn(
                    start=col.gen.start + start_k * col.gen.step,
                    step=col.gen.step,
                ),
                nullable=col.nullable,
                null_fraction=col.null_fraction,
                foreign_key=col.foreign_key,
            )
            new_columns.append(new_col)
        else:
            new_columns.append(col)

    insert_spec = TableSpec(
        name=table_spec.name,
        columns=new_columns,
        rows=insert_count,
        primary_key=table_spec.primary_key,
        seed=batch_seed,
    )
    return generate_table(spark, insert_spec, resolved_plan)


# ---------------------------------------------------------------------------
# Stream B: Deletes
# ---------------------------------------------------------------------------


def _generate_pre_image_for_indices(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    row_indices: list[int],
    batch_id: int,
    global_seed: int,
    initial_rows: int,
) -> DataFrame:
    """Generate row images at their pre-image batch (last-written state).

    Groups rows by pre_image_batch, generates each group with the
    appropriate seed, and unions the results.  Used by both delete
    and update-before streams.
    """
    batch_groups: dict[int, list[int]] = {}
    for k in row_indices:
        pib = pre_image_batch(
            k,
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.update_period,
        )
        batch_groups.setdefault(pib, []).append(k)

    dfs = []
    for write_batch, indices in batch_groups.items():
        seed = compute_batch_seed(global_seed, write_batch)
        df = generate_for_indices(spark, table_spec, resolved_plan, indices, seed)
        dfs.append(df)

    return union_all(dfs)


def _generate_delete_stream(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    del_indices: list[int],
    batch_id: int,
    global_seed: int,
    initial_rows: int,
) -> DataFrame:
    """Generate delete rows (before-images of dying rows)."""
    return _generate_pre_image_for_indices(
        spark,
        table_spec,
        resolved_plan,
        periods,
        del_indices,
        batch_id,
        global_seed,
        initial_rows,
    )


# ---------------------------------------------------------------------------
# Stream C: Updates
# ---------------------------------------------------------------------------


def _generate_update_before_stream(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    upd_indices: list[int],
    batch_id: int,
    global_seed: int,
    initial_rows: int,
) -> DataFrame:
    """Generate update before-images."""
    return _generate_pre_image_for_indices(
        spark,
        table_spec,
        resolved_plan,
        periods,
        upd_indices,
        batch_id,
        global_seed,
        initial_rows,
    )


def _generate_update_after_stream(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    upd_indices: list[int],
    batch_id: int,
    global_seed: int,
    initial_rows: int,
) -> DataFrame:
    """Generate update after-images.

    The after-image uses the current batch's seed.
    """
    seed = compute_batch_seed(global_seed, batch_id)
    return generate_for_indices(spark, table_spec, resolved_plan, upd_indices, seed)


# ---------------------------------------------------------------------------
# Shared: generate columns for specific row indices at a given seed
# ---------------------------------------------------------------------------


def generate_for_indices(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    row_indices: list[int],
    seed: int,
) -> DataFrame:
    """Generate columns for *only* the given row indices.

    Creates a small DataFrame from just the requested indices and
    computes column expressions against those.  All column builders are
    pure functions of ``_synth_row_id``, so the output is identical to
    generating the full range and filtering.
    """
    # Create DataFrame from only the needed indices
    idx_data = [(int(i),) for i in row_indices]
    df = spark.createDataFrame(idx_data, ["_synth_row_id"])
    id_col = F.col("_synth_row_id")

    row_count = max(row_indices) + 1 if row_indices else 0
    fk_res = resolved_plan.fk_resolutions if resolved_plan is not None else None
    col_exprs, udf_columns, seeded_columns = build_all_column_exprs(
        table_spec,
        id_col,
        fk_res,
        seed=seed,
        row_count=row_count,
    )

    return apply_column_phases(df, id_col, col_exprs, udf_columns, seeded_columns)


# ---------------------------------------------------------------------------
# Spark-native stream generators (push filtering + pre-image to cluster)
# ---------------------------------------------------------------------------


def _generate_delete_stream_native(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    batch_id: int,
    global_seed: int,
    initial_rows: int,
    min_life: int,
) -> DataFrame | None:
    """Spark-native delete stream: filter + pre-image on cluster.

    Uses spark.range(max_k) filtered by death_tick_expr == batch_id,
    then applies CASE WHEN on pre_image_batch_expr for seed selection.
    """
    upper_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
    if upper_k <= 0:
        return None

    df, id_col = create_range_df(spark, upper_k)

    # Filter: death_tick(k) == batch_id
    dt_expr = death_tick_expr(
        id_col,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        min_life,
    )
    df = df.filter(dt_expr == F.lit(batch_id).cast("long"))

    # Compute pre-image batch for each row
    pib_expr = pre_image_batch_expr(
        id_col,
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.update_period,
    )
    df = df.withColumn("_write_batch", pib_expr)

    # Build columns with CASE WHEN on _write_batch for seed selection
    wbs = _precompute_write_batches(batch_id, periods.update_period)
    result = _apply_columns_with_write_batch(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        unique_wbs=wbs,
        row_count=upper_k,
    )
    return result


def _generate_update_before_stream_native(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    batch_id: int,
    global_seed: int,
    initial_rows: int,
    min_life: int,
    update_window: int | None = None,
) -> DataFrame | None:
    """Spark-native update before-image stream.

    Uses spark.range(max_k) filtered by update_due_expr, then applies
    pre_image_batch_expr for the before-image seed.
    """
    upper_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
    if upper_k <= 0:
        return None

    df, id_col = create_range_df(spark, upper_k)

    # Filter: update_due(k, batch_id)
    ud_expr = update_due_expr(
        id_col,
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        periods.update_period,
        min_life,
        update_window=update_window,
    )
    df = df.filter(ud_expr)

    # Pre-image batch for the before-image
    pib_expr = pre_image_batch_expr(
        id_col,
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.update_period,
    )
    df = df.withColumn("_write_batch", pib_expr)

    # Build columns with CASE WHEN on _write_batch
    wbs = _precompute_write_batches(batch_id, periods.update_period)
    result = _apply_columns_with_write_batch(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        unique_wbs=wbs,
        row_count=upper_k,
    )
    return result


def _generate_update_after_stream_native(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    batch_id: int,
    global_seed: int,
    initial_rows: int,
    min_life: int,
    update_window: int | None = None,
) -> DataFrame | None:
    """Spark-native update after-image stream.

    Uses spark.range(max_k) filtered by update_due_expr. The after-image
    always uses the current batch's seed.
    """
    upper_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
    if upper_k <= 0:
        return None

    df, id_col = create_range_df(spark, upper_k)

    # Filter: update_due(k, batch_id)
    ud_expr = update_due_expr(
        id_col,
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        periods.update_period,
        min_life,
        update_window=update_window,
    )
    df = df.filter(ud_expr)

    # After-image: always current batch seed — no CASE WHEN needed
    result = _apply_columns_with_write_batch(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        unique_wbs=[batch_id],
        row_count=upper_k,
    )
    return result
