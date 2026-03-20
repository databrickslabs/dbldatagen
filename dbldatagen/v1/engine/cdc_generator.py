"""Core CDC batch generation -- stateless three-stream architecture.

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

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights
from dbldatagen.v1.engine.cdc_state import resolve_batch_size
from dbldatagen.v1.engine.cdc_stateless import (
    CDCPeriods,
    batch_timestamp_str,
    birth_tick_expr,
    compute_periods,
    death_tick_expr,
    delete_indices_at_batch_fast,
    insert_range,
    max_k_at_batch,
    pre_image_batch,
    pre_image_batch_expr,
    update_due_expr,
    update_indices_at_batch,
)
from dbldatagen.v1.engine.fk import build_fk_column
from dbldatagen.v1.engine.generator import (
    build_all_column_exprs,
    build_all_column_exprs_case_when,
    build_column_expr,
    generate_table,
)
from dbldatagen.v1.engine.planner import FKResolution, ResolvedPlan, resolve_plan
from dbldatagen.v1.engine.seed import compute_batch_seed, derive_column_seed
from dbldatagen.v1.engine.utils import (
    apply_column_phases,
    apply_null_fraction,
    case_when_chain,
    create_range_df,
    get_pk_columns,
    union_all,
)
from dbldatagen.v1.schema import ColumnSpec, FakerColumn, SequenceColumn, TableSpec


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class RawBatchResult:
    """Raw CDC output for one table in one batch."""

    table_name: str
    batch_id: int
    inserts: DataFrame | None
    updates_before: DataFrame | None
    updates_after: DataFrame | None
    deletes: DataFrame | None

    def to_dataframe(self) -> DataFrame | None:
        """Combine all streams into a single DataFrame, or None if empty."""
        parts = [df for df in (self.inserts, self.updates_before, self.updates_after, self.deletes) if df is not None]
        if not parts:
            return None
        return union_all(parts)


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
    global_seed = plan.base_plan.seed
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


def _table_has_faker_columns(table_spec: TableSpec) -> bool:
    """Check if a table has any Faker columns (which require Python UDF pools)."""
    return any(isinstance(col.gen, FakerColumn) for col in table_spec.columns)


def _precompute_write_batches(batch_id: int, update_period: int | float) -> list[int]:
    """Compute the superset of possible pre_image_batch values on the driver.

    The pre_image_batch formula produces values in
    ``range(max(0, batch_id - up), batch_id)`` where ``up`` is the
    update_period.  This is at most ``up`` values and avoids a Spark
    ``.collect()`` round-trip.
    """
    import math

    if math.isinf(update_period):
        # No updates — pre-image is always birth_tick.
        # Possible birth_ticks: 0 .. batch_id-1
        return list(range(batch_id))
    up = int(update_period)
    if up <= 0:
        return list(range(batch_id))
    return list(range(max(0, batch_id - up), batch_id))


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


def _apply_columns_with_write_batch(
    df: DataFrame,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    global_seed: int,
    id_col: Column,
    *,
    unique_wbs: list[int] | None = None,
    row_count: int | None = None,
    extra_exprs: list[Column] | None = None,
) -> DataFrame | None:
    """Apply column expressions using CASE WHEN on _write_batch for seed selection.

    The DataFrame must have ``_synth_row_id`` and ``_write_batch`` columns.

    Parameters
    ----------
    unique_wbs : list[int] | None
        Pre-computed set of possible _write_batch values.  If ``None``,
        falls back to a ``.collect()`` (slow — triggers an extra Spark job).
    row_count : int | None
        Upper bound on row IDs.  If ``None``, falls back to a
        ``.collect()`` on ``max(_synth_row_id)``.
    extra_exprs : list[Column] | None
        Additional Column expressions (e.g. metadata columns) to include
        in the flat ``select`` alongside the data columns.
    """
    wb_col = F.col("_write_batch")

    if unique_wbs is None:
        # Fallback: collect from DataFrame (triggers a Spark job)
        unique_wbs = sorted(row["_write_batch"] for row in df.select("_write_batch").distinct().collect())

    if not unique_wbs:
        return None

    if row_count is None:
        # Fallback: collect from DataFrame (triggers a Spark job)
        max_row_stats = df.agg(F.max("_synth_row_id")).collect()
        row_count = int(max_row_stats[0][0]) + 1 if max_row_stats[0][0] is not None else 0

    fk_res = resolved_plan.fk_resolutions if resolved_plan is not None else None
    col_exprs, udf_columns, seeded_columns = build_all_column_exprs_case_when(
        table_spec,
        id_col,
        wb_col,
        unique_wbs,
        global_seed,
        fk_res,
        row_count=row_count,
    )

    if extra_exprs:
        col_exprs.extend(extra_exprs)

    select_list = [id_col]
    # Keep _write_batch only when UDF columns need it AND there are
    # multiple write-batch values (single-value skips CASE WHEN entirely,
    # so _write_batch isn't referenced and may not exist on the DataFrame).
    if (udf_columns or seeded_columns) and len(unique_wbs) > 1:
        select_list.append(wb_col)
    select_list.extend(col_exprs)
    df = df.select(*select_list)

    for col_name, col_expr in udf_columns:
        df = df.withColumn(col_name, col_expr)

    for col_name, col_expr in seeded_columns:
        df = df.withColumn(col_name, col_expr)

    df = df.drop("_synth_row_id", "_write_batch")
    return df


# ---------------------------------------------------------------------------
# Fused multi-batch generators (ONE spark.range per stream per chunk)
# ---------------------------------------------------------------------------


def generate_fused_deletes(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    batch_ids: list[int],
    global_seed: int,
    initial_rows: int,
    min_life: int,
    plan: CDCPlan,
) -> DataFrame | None:
    """Delete rows for ALL batches in batch_ids via ONE spark.range scan."""
    import math

    if _table_has_faker_columns(table_spec):
        return None

    max_batch = max(batch_ids)
    upper_k = max_k_at_batch(max_batch, initial_rows, periods.inserts_per_batch)
    if upper_k <= 0 or math.isinf(periods.death_period):
        return None

    df, id_col = create_range_df(spark, upper_k)

    dt = death_tick_expr(
        id_col,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        min_life,
    )
    df = df.filter(dt.isin(*[int(b) for b in batch_ids]))
    df = df.withColumn("_batch_id", dt.cast("int"))

    # _write_batch = pre_image_batch with batch_n = death_tick (varies per row)
    t_birth = birth_tick_expr(id_col, initial_rows, periods.inserts_per_batch)
    batch_n_col = F.col("_batch_id").cast("long")

    if math.isinf(periods.update_period) or int(periods.update_period) <= 0:
        df = df.withColumn("_write_batch", t_birth.cast("long"))
    else:
        up = int(periods.update_period)
        up_lit = F.lit(up).cast("long")
        remainder = (batch_n_col + id_col) % up_lit
        prev = F.when(
            remainder == F.lit(0).cast("long"),
            batch_n_col - up_lit,
        ).otherwise(batch_n_col - remainder)
        df = df.withColumn("_write_batch", F.greatest(prev, t_birth).cast("long"))

    all_wbs = set()
    for b in batch_ids:
        all_wbs.update(_precompute_write_batches(b, periods.update_period))

    return _build_fused_output(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        sorted(all_wbs),
        upper_k,
        batch_ids,
        plan,
        op="D",
    )


def generate_fused_updates(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    periods: CDCPeriods,
    batch_ids: list[int],
    global_seed: int,
    initial_rows: int,
    min_life: int,
    plan: CDCPlan,
    update_window: int | None = None,
) -> tuple[DataFrame | None, DataFrame | None]:
    """Update before+after rows for ALL batches via ONE spark.range scan.

    Returns ``(before_df, after_df)``.
    """
    import math

    if _table_has_faker_columns(table_spec):
        return None, None

    max_batch = max(batch_ids)
    min_batch = min(batch_ids)
    upper_k = max_k_at_batch(max_batch, initial_rows, periods.inserts_per_batch)

    if upper_k <= 0 or math.isinf(periods.update_period):
        return None, None

    up = int(periods.update_period)
    if up <= 0:
        return None, None

    df, id_col = create_range_df(spark, upper_k)

    # For row k, the update batch b satisfies (b + k) % up == 0.
    # In [min_batch, max_batch]: b = ceil((min_batch + k) / up) * up - k
    up_lit = F.lit(up).cast("long")
    min_b_lit = F.lit(min_batch).cast("long")
    max_b_lit = F.lit(max_batch).cast("long")

    ceil_div = F.ceil((min_b_lit + id_col).cast("double") / up_lit.cast("double")).cast("long")
    candidate_b = (ceil_div * up_lit - id_col).cast("long")

    # Filter: candidate in range, alive, eligible for update
    t_birth = birth_tick_expr(id_col, initial_rows, periods.inserts_per_batch)
    t_death = death_tick_expr(
        id_col,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        min_life,
    )

    base_filter = (
        (candidate_b >= min_b_lit)
        & (candidate_b <= max_b_lit)
        & (candidate_b > t_birth)  # born before this batch (age > 0)
        & (t_death > candidate_b)  # not yet dead (min_life already in death_tick)
    )

    # Apply update_window filter: only rows with age <= window are eligible
    if update_window is not None:
        age = candidate_b - t_birth
        base_filter = base_filter & (age <= F.lit(update_window).cast("long"))

    df = df.filter(base_filter)
    df = df.withColumn("_batch_id", candidate_b.cast("int"))

    # Before-image: _write_batch = pre_image_batch(k, candidate_b)
    batch_n_col = F.col("_batch_id").cast("long")
    remainder = (batch_n_col + id_col) % up_lit
    prev = F.when(
        remainder == F.lit(0).cast("long"),
        batch_n_col - up_lit,
    ).otherwise(batch_n_col - remainder)
    df = df.withColumn("_write_batch", F.greatest(prev, t_birth).cast("long"))

    all_wbs = set()
    for b in batch_ids:
        all_wbs.update(_precompute_write_batches(b, periods.update_period))

    before_df = _build_fused_output(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        sorted(all_wbs),
        upper_k,
        batch_ids,
        plan,
        op="UB",
    )

    # After-image: _write_batch = batch_id (current batch seed)
    after_base = df.drop("_write_batch").withColumn(
        "_write_batch",
        F.col("_batch_id").cast("long"),
    )
    after_df = _build_fused_output(
        after_base,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        sorted(int(b) for b in batch_ids),
        upper_k,
        batch_ids,
        plan,
        op="U",
    )

    return before_df, after_df


def _build_fused_output(
    df: DataFrame,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    global_seed: int,
    id_col: Column,
    unique_wbs: list[int],
    row_count: int,
    batch_ids: list[int],
    plan: CDCPlan,
    op: str,
) -> DataFrame | None:
    """Build column expressions + metadata for a fused multi-batch DataFrame.

    The input ``df`` must have ``_synth_row_id``, ``_batch_id``, and
    ``_write_batch`` columns.  Delegates to ``_apply_columns_with_write_batch``
    with metadata columns passed via ``extra_exprs``.
    """
    bid_col = F.col("_batch_id")

    # --- Metadata columns (included in the same flat select) ---
    meta: list[Column] = [
        F.lit(op).alias("_op"),
        bid_col,
    ]

    # PERFORMANCE NOTE — dynamic timestamp computation:
    # Instead of a CASE WHEN with one branch per batch_id (up to 365+),
    # compute the timestamp arithmetically: base_epoch + batch_id * interval.
    # This keeps the plan at O(1) nodes instead of O(N_batches).  The CASE
    # WHEN fallback is only used for single-batch or missing-interval cases.
    if len(batch_ids) > 1 and plan.batch_interval_seconds:
        from datetime import datetime

        base_ts = datetime.fromisoformat(
            plan.start_timestamp.replace("Z", "+00:00"),
        )
        base_epoch = int(base_ts.timestamp())
        interval = plan.batch_interval_seconds
        ts_expr = (F.lit(base_epoch).cast("long") + bid_col.cast("long") * F.lit(interval).cast("long")).cast(
            "timestamp"
        )
    else:
        # Fallback for single batch or missing interval
        ts_mappings = [(int(b), F.lit(batch_timestamp(plan, int(b)))) for b in batch_ids]
        ts_expr = case_when_chain(bid_col, ts_mappings).cast("timestamp")
    meta.append(ts_expr.alias("_ts"))

    return _apply_columns_with_write_batch(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        unique_wbs=unique_wbs,
        row_count=row_count,
        extra_exprs=meta,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def compute_periods_from_config(
    initial_rows: int,
    batch_size: int,
    config: CDCTableConfig,
) -> CDCPeriods:
    """Compute CDC periods from a CDCTableConfig."""
    return compute_periods(
        initial_rows=initial_rows,
        batch_size=batch_size,
        insert_weight=config.operations.insert,
        update_weight=config.operations.update,
        delete_weight=config.operations.delete,
        min_life=config.min_life,
    )


def _table_has_fk_dependents(plan: CDCPlan, table_name: str) -> bool:
    """Check if any other CDC table has an FK referencing *table_name*."""
    for t in plan.base_plan.tables:
        if t.name == table_name:
            continue
        if t.name not in plan.cdc_tables:
            continue
        for col in t.columns:
            if col.foreign_key is not None:
                parent = col.foreign_key.ref.split(".", 1)[0]
                if parent == table_name:
                    return True
    return False


def _add_cdc_metadata(
    df: DataFrame | None,
    op: str,
    batch_id: int,
    batch_ts: str,
) -> DataFrame | None:
    """Attach _op, _batch_id, _ts metadata columns to a CDC DataFrame."""
    if df is None:
        return None
    return (
        df.withColumn("_op", F.lit(op))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
    )


def apply_fk_delete_guard(plan: CDCPlan, table_name: str, config: CDCTableConfig) -> CDCTableConfig:
    """Disable deletes for tables that are FK parents.

    Returns the original config if no guard is needed, or a copy
    with delete weight set to 0.
    """
    if _table_has_fk_dependents(plan, table_name) and config.operations.delete > 0:
        return config.model_copy(
            update={
                "operations": OperationWeights(
                    insert=config.operations.insert,
                    update=config.operations.update,
                    delete=0,
                ),
            }
        )
    return config


def batch_timestamp(plan: CDCPlan, batch_id: int) -> str:
    """Compute the timestamp string for a given batch."""
    return batch_timestamp_str(plan.start_timestamp, plan.batch_interval_seconds, batch_id)


# ---------------------------------------------------------------------------
# Bulk insert fusion: ONE spark.range() for all inserts in a chunk
# ---------------------------------------------------------------------------


def generate_bulk_inserts(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    batch_infos: list[tuple[int, int, int]],
    global_seed: int,
    plan: CDCPlan,
) -> DataFrame | None:
    """Generate inserts for multiple batches in ONE spark.range() call.

    Each element of *batch_infos* is ``(batch_id, start_index, insert_count)``.

    Returns None if the table has Faker columns (caller should fall back to
    per-batch generation).
    """
    total_inserts = sum(info[2] for info in batch_infos)
    if total_inserts == 0:
        return None

    # Check for Faker columns -- can't vary UDF pool per batch
    for col_spec in table_spec.columns:
        if isinstance(col_spec.gen, FakerColumn):
            return None

    inserts_per_batch = batch_infos[0][2]
    first_start_index = batch_infos[0][1]

    df, raw_id = create_range_df(spark, total_inserts)

    local_id = (raw_id % F.lit(inserts_per_batch)).cast("long")
    batch_offset = F.floor(raw_id.cast("double") / F.lit(inserts_per_batch)).cast("int")

    pk_cols = get_pk_columns(table_spec)

    col_exprs: list = []
    udf_columns: list[tuple[str, F.Column]] = []
    table_name = table_spec.name

    for col_spec in table_spec.columns:
        if col_spec.name in pk_cols and isinstance(col_spec.gen, SequenceColumn):
            pk_start = col_spec.gen.start + first_start_index * col_spec.gen.step
            expr = (raw_id * F.lit(col_spec.gen.step) + F.lit(pk_start)).cast("long")
            col_exprs.append(expr.alias(col_spec.name))
            continue

        if col_spec.foreign_key is not None:
            fk_key = (table_name, col_spec.name)
            if resolved_plan is not None and fk_key in resolved_plan.fk_resolutions:
                combined = _build_case_when_fk(
                    local_id,
                    batch_offset,
                    batch_infos,
                    table_name,
                    col_spec,
                    global_seed,
                    resolved_plan.fk_resolutions[fk_key],
                )
                udf_columns.append((col_spec.name, combined))
            else:
                col_exprs.append(F.lit(None).alias(col_spec.name))
            continue

        combined = _build_case_when_column(
            local_id,
            batch_offset,
            batch_infos,
            table_spec,
            col_spec,
            global_seed,
        )
        col_exprs.append(combined.alias(col_spec.name))

    col_exprs.append(F.lit("I").alias("_op"))

    bid_expr = _build_case_when_lit(
        batch_offset,
        [(i, info[0]) for i, info in enumerate(batch_infos)],
    )
    col_exprs.append(bid_expr.alias("_batch_id"))

    ts_expr = _build_case_when_lit(
        batch_offset,
        [(i, batch_timestamp(plan, info[0])) for i, info in enumerate(batch_infos)],
    )
    col_exprs.append(ts_expr.cast("timestamp").alias("_ts"))

    df = df.select(raw_id, *col_exprs)

    for col_name, col_expr in udf_columns:
        df = df.withColumn(col_name, col_expr)

    df = df.drop("_synth_row_id")
    return df


def _build_case_when_column(
    local_id: Column,
    batch_offset: Column,
    batch_infos: list[tuple[int, int, int]],
    table_spec: TableSpec,
    col_spec: ColumnSpec,
    global_seed: int,
) -> F.Column:
    """Build CASE WHEN over batch_offset to select the correct column expression.

    Each batch uses a different seed, so the expression varies per batch.
    The result is: WHEN batch_offset == 0 THEN expr_0 WHEN ... ELSE expr_N.
    """
    batch_exprs = []
    for i, (batch_id, _, insert_count) in enumerate(batch_infos):
        batch_seed = compute_batch_seed(global_seed, batch_id)
        col_seed = derive_column_seed(batch_seed, table_spec.name, col_spec.name)
        expr = build_column_expr(col_spec, local_id, col_seed, insert_count, batch_seed)
        expr = apply_null_fraction(expr, col_seed, local_id, col_spec.null_fraction)
        batch_exprs.append((i, expr))

    return case_when_chain(batch_offset, batch_exprs)


def _build_case_when_fk(
    local_id: Column,
    batch_offset: Column,
    batch_infos: list[tuple[int, int, int]],
    table_name: str,
    col_spec: ColumnSpec,
    global_seed: int,
    fk_resolution: FKResolution,
) -> F.Column:
    """Build CASE WHEN over batch_offset for FK columns.

    Each batch derives a different column seed, producing different FK
    distribution samples. Returns a UDF column (applied via withColumn).
    """
    batch_exprs = []
    for i, (batch_id, _, _) in enumerate(batch_infos):
        batch_seed = compute_batch_seed(global_seed, batch_id)
        col_seed = derive_column_seed(batch_seed, table_name, col_spec.name)
        expr = build_fk_column(local_id, col_seed, fk_resolution)
        batch_exprs.append((i, expr))

    return case_when_chain(batch_offset, batch_exprs)


def _build_case_when_lit(batch_offset: Column, mappings: list[tuple[int, object]]) -> F.Column:
    """Build CASE WHEN for simple literal values (batch_id, timestamp)."""
    branches = [(k, F.lit(v)) for k, v in mappings]
    return case_when_chain(batch_offset, branches)


# ---------------------------------------------------------------------------
# Bulk update/delete fusion: ONE DataFrame per operation type
# ---------------------------------------------------------------------------


def generate_bulk_changes(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None,
    change_infos: list[dict],
    global_seed: int,
    plan: CDCPlan,
) -> DataFrame | None:
    """Generate update-before, update-after, and delete rows in bulk.

    Uses the stateless three-stream architecture to compute row indices
    per batch, then fuses them into a single DataFrame with CASE WHEN
    expressions for seed selection.

    Parameters
    ----------
    change_infos :
        List of dicts, one per batch, each with keys:
        ``batch_id``, ``update_indices``, ``delete_indices``,
        ``update_write_batches``, ``delete_write_batches``.

    Returns None if there are no update/delete rows, or if the table has
    Faker columns (caller should fall back to per-batch generation).
    """
    import numpy as np
    import pandas as pd

    # Check for Faker columns -- can't bulk-fuse
    for col_spec in table_spec.columns:
        if isinstance(col_spec.gen, FakerColumn):
            return None

    # Collect all rows into numpy arrays
    total_upd = sum(len(ci["update_indices"]) for ci in change_infos)
    total_del = sum(len(ci["delete_indices"]) for ci in change_infos)
    total_rows = total_upd * 2 + total_del

    if total_rows == 0:
        return None

    synth_ids = np.empty(total_rows, dtype=np.int64)
    batch_ids = np.empty(total_rows, dtype=np.int32)
    write_batches = np.empty(total_rows, dtype=np.int32)
    op_codes = np.empty(total_rows, dtype=np.int8)

    cursor = 0
    for ci in change_infos:
        bid = ci["batch_id"]
        n_upd = len(ci["update_indices"])
        n_del = len(ci["delete_indices"])

        if n_upd > 0:
            idx_arr = ci["update_indices"]
            wb_arr = ci["update_write_batches"]

            end = cursor + n_upd
            synth_ids[cursor:end] = idx_arr
            batch_ids[cursor:end] = bid
            write_batches[cursor:end] = wb_arr
            op_codes[cursor:end] = 0  # UB
            cursor = end

            end = cursor + n_upd
            synth_ids[cursor:end] = idx_arr
            batch_ids[cursor:end] = bid
            write_batches[cursor:end] = bid
            op_codes[cursor:end] = 1  # UA
            cursor = end

        if n_del > 0:
            idx_arr = ci["delete_indices"]
            wb_arr = ci["delete_write_batches"]

            end = cursor + n_del
            synth_ids[cursor:end] = idx_arr
            batch_ids[cursor:end] = bid
            write_batches[cursor:end] = wb_arr
            op_codes[cursor:end] = 2  # D
            cursor = end

    pdf = pd.DataFrame(
        {
            "_synth_row_id": synth_ids[:cursor],
            "_batch_id": batch_ids[:cursor],
            "_write_batch": write_batches[:cursor],
            "_op_code": op_codes[:cursor],
        }
    )
    df = spark.createDataFrame(pdf)

    id_col = F.col("_synth_row_id")
    wb_col = F.col("_write_batch")
    bid_col = F.col("_batch_id")
    op_col = F.col("_op_code")

    unique_wbs = sorted(set(write_batches[:cursor].tolist()))
    row_count = int(synth_ids[:cursor].max()) + 1 if cursor > 0 else 0

    # FK columns get NULL (no per-wb FK resolution in bulk changes)
    col_exprs, _udf_columns, seeded_columns = build_all_column_exprs_case_when(
        table_spec,
        id_col,
        wb_col,
        unique_wbs,
        global_seed,
        fk_resolutions=None,
        row_count=row_count,
    )

    op_expr = F.when(op_col == F.lit(0), F.lit("UB")).when(op_col == F.lit(1), F.lit("U")).otherwise(F.lit("D"))
    col_exprs.append(op_expr.alias("_op"))
    col_exprs.append(bid_col.alias("_batch_id"))

    unique_bids = sorted(set(batch_ids[:cursor].tolist()))
    ts_mappings = [(bid, batch_timestamp(plan, bid)) for bid in unique_bids]
    ts_expr = F.lit(ts_mappings[-1][1])
    for i in range(len(ts_mappings) - 2, -1, -1):
        ts_expr = F.when(
            bid_col == F.lit(ts_mappings[i][0]),
            F.lit(ts_mappings[i][1]),
        ).otherwise(ts_expr)
    col_exprs.append(ts_expr.cast("timestamp").alias("_ts"))

    df = df.select(id_col, *col_exprs)

    for col_name, col_expr in seeded_columns:
        df = df.withColumn(col_name, col_expr)

    df = df.drop("_synth_row_id")
    return df
