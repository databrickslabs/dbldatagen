"""Core ingestion batch generation.

Simulates upstream systems dumping full rows into staging tables.
Three strategies:
    - SYNTHETIC: Adapter pattern — converts IngestPlan to CDCPlan and
      reuses all CDC fused generators.
    - DELTA: Reads from Delta tables for update selection, uses native
      Spark operations for deterministic row selection.
    - STATELESS: Three-range targeted scan — O(batch_size) work per batch,
      zero shuffle, no Delta reads, fully batch-independent.

Two output modes:
    - INCREMENTAL: Each batch = new + changed rows only.
    - SNAPSHOT: Each batch = all live rows at that point in time.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.v1.cdc_schema import (
    CDCFormat,
    CDCPlan,
    CDCTableConfig,
    OperationWeights,
)
from dbldatagen.v1.engine.cdc_state import resolve_batch_size
from dbldatagen.v1.engine.generator import generate_table
from dbldatagen.v1.engine.planner import ResolvedPlan, resolve_plan
from dbldatagen.v1.engine.seed import compute_batch_seed
from dbldatagen.v1.engine.utils import apply_null_fraction, get_pk_columns, union_all
from dbldatagen.v1.ingest_schema import (
    IngestPlan,
    IngestTableConfig,
)
from dbldatagen.v1.schema import TableSpec


# ---------------------------------------------------------------------------
# Synthetic strategy: IngestPlan → CDCPlan adapter
# ---------------------------------------------------------------------------


def _convert_to_cdc_plan(ingest_plan: IngestPlan) -> CDCPlan:
    """Internal adapter: IngestPlan → CDCPlan.

    Maps ingest fractions to CDC operation weights and preserves
    all configuration including update_window.
    """
    table_configs = {}
    for name, cfg in ingest_plan.table_configs.items():
        table_configs[name] = CDCTableConfig(
            operations=OperationWeights(
                insert=cfg.insert_fraction,
                update=cfg.update_fraction,
                delete=cfg.delete_fraction,
            ),
            batch_size=cfg.batch_size,
            mutations=cfg.mutations,
            min_life=cfg.min_life,
            update_window=cfg.update_window,
        )

    default_cfg = ingest_plan.default_config
    default_cdc = CDCTableConfig(
        operations=OperationWeights(
            insert=default_cfg.insert_fraction,
            update=default_cfg.update_fraction,
            delete=default_cfg.delete_fraction,
        ),
        batch_size=default_cfg.batch_size,
        mutations=default_cfg.mutations,
        min_life=default_cfg.min_life,
        update_window=default_cfg.update_window,
    )

    return CDCPlan(
        base_plan=ingest_plan.base_plan,
        num_batches=ingest_plan.num_batches,
        table_configs=table_configs,
        default_config=default_cdc,
        format=CDCFormat.RAW,
        batch_interval_seconds=ingest_plan.batch_interval_seconds,
        start_timestamp=ingest_plan.start_timestamp,
        cdc_tables=ingest_plan.ingest_tables,
    )


# ---------------------------------------------------------------------------
# Batch timestamp helper
# ---------------------------------------------------------------------------


def _ingest_batch_timestamp(plan: IngestPlan, batch_id: int) -> str:
    """Compute the timestamp string for a given ingest batch."""
    from dbldatagen.v1.engine.cdc_stateless import batch_timestamp_str

    return batch_timestamp_str(plan.start_timestamp, plan.batch_interval_seconds, batch_id)


# ---------------------------------------------------------------------------
# Metadata columns
# ---------------------------------------------------------------------------


def _add_ingest_metadata(
    df: DataFrame,
    plan: IngestPlan,
    batch_id: int,
) -> DataFrame:
    """Add _batch_id and _load_ts metadata columns if configured."""
    if plan.include_batch_id:
        df = df.withColumn("_batch_id", F.lit(batch_id))
    if plan.include_load_timestamp:
        ts = _ingest_batch_timestamp(plan, batch_id)
        df = df.withColumn("_load_ts", F.lit(ts).cast("timestamp"))
    return df


def _drop_cdc_columns(df: DataFrame) -> DataFrame:
    """Drop CDC-specific columns (_op, _ts, _batch_id) from a DataFrame.

    The _batch_id column comes from the CDC engine; the ingest layer
    adds its own _batch_id via _add_ingest_metadata() if configured.
    """
    cols_to_drop = [c for c in df.columns if c in ("_op", "_ts", "_batch_id")]
    return df.drop(*cols_to_drop) if cols_to_drop else df


# ---------------------------------------------------------------------------
# Synthetic incremental batch
# ---------------------------------------------------------------------------


def generate_synthetic_incremental_batch(
    spark: SparkSession,
    plan: IngestPlan,
    batch_id: int,
) -> dict[str, DataFrame]:
    """Generate one incremental batch using the synthetic strategy.

    Reuses CDC generation, then strips CDC columns and keeps only
    inserts + update after-images (the "new data" arriving).
    """
    from dbldatagen.v1.cdc import _generate_batch

    cdc_plan = _convert_to_cdc_plan(plan)
    raw_batch = _generate_batch(spark, cdc_plan, batch_id, "raw")

    result = {}
    for table_name, df in raw_batch.items():
        # Keep inserts + update after-images (new values arriving)
        # Also keep deletes if delete_fraction > 0 (so downstream can detect absence)
        cfg = plan.config_for(table_name)
        if cfg.delete_fraction > 0:
            # Include deletes so downstream can detect removals
            filtered = df.filter(F.col("_op").isin("I", "U", "D"))
        else:
            filtered = df.filter(F.col("_op").isin("I", "U"))

        filtered = _drop_cdc_columns(filtered)
        filtered = _add_ingest_metadata(filtered, plan, batch_id)
        result[table_name] = filtered

    return result


# ---------------------------------------------------------------------------
# Synthetic snapshot batch
# ---------------------------------------------------------------------------


def generate_synthetic_snapshot_batch(
    spark: SparkSession,
    plan: IngestPlan,
    batch_id: int,
) -> dict[str, DataFrame]:
    """Generate one snapshot batch using the synthetic strategy.

    Uses generate_expected_state() from the CDC engine to get all
    live rows at the given batch.
    """
    from dbldatagen.v1.cdc import generate_expected_state

    cdc_plan = _convert_to_cdc_plan(plan)
    result = {}

    for table_name in plan.ingest_tables:
        state_df = generate_expected_state(spark, cdc_plan, table_name, batch_id)
        state_df = _add_ingest_metadata(state_df, plan, batch_id)
        result[table_name] = state_df

    return result


# ---------------------------------------------------------------------------
# Initial snapshot
# ---------------------------------------------------------------------------


def generate_initial_snapshot(
    spark: SparkSession,
    plan: IngestPlan,
) -> dict[str, DataFrame]:
    """Generate the initial (batch 0) snapshot for all ingest tables."""
    resolved = resolve_plan(plan.base_plan)
    table_map = {t.name: t for t in plan.base_plan.tables}
    results = {}

    for table_name in plan.ingest_tables:
        table_spec = table_map[table_name]
        df = generate_table(spark, table_spec, resolved)
        df = df.withColumn("_action", F.lit("I"))
        df = _add_ingest_metadata(df, plan, 0)
        results[table_name] = df

    return results


# ---------------------------------------------------------------------------
# Delta strategy: read-driven generation
# ---------------------------------------------------------------------------


def _compute_operation_counts(
    cfg: IngestTableConfig,
    abs_batch_size: int,
) -> tuple[int, int, int]:
    """Compute insert/update/delete counts with correct remainder assignment."""
    from dbldatagen.v1.engine.utils import split_with_remainder

    return split_with_remainder(
        abs_batch_size,
        (cfg.insert_fraction, cfg.update_fraction, cfg.delete_fraction),
    )


def _generate_inserts(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved: ResolvedPlan,
    batch_id: int,
    inserts_per_batch: int,
    initial_rows: int,
    global_seed: int,
) -> DataFrame | None:
    """Generate insert rows directly via insert_range + generate_table.

    Bypasses the CDC plan conversion — computes the PK offset range
    inline and builds a TableSpec with adjusted SequenceColumn start.
    """
    from dbldatagen.v1.engine.cdc_stateless import insert_range
    from dbldatagen.v1.schema import ColumnSpec, SequenceColumn, TableSpec

    start_k, end_k = insert_range(batch_id, initial_rows, inserts_per_batch)
    count = end_k - start_k
    if count <= 0:
        return None

    batch_seed = compute_batch_seed(global_seed, batch_id)

    pk_cols = get_pk_columns(table_spec)
    new_columns = [
        (
            ColumnSpec(
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
            if col.name in pk_cols and isinstance(col.gen, SequenceColumn)
            else col
        )
        for col in table_spec.columns
    ]

    insert_spec = TableSpec(
        name=table_spec.name,
        columns=new_columns,
        rows=count,
        primary_key=table_spec.primary_key,
        seed=batch_seed,
    )
    return generate_table(spark, insert_spec, resolved)


def generate_delta_incremental_batch(
    spark: SparkSession,
    plan: IngestPlan,
    table_name: str,
    batch_id: int,
    current_df: DataFrame,
    *,
    _resolved: ResolvedPlan | None = None,
) -> dict[str, DataFrame]:
    """Generate one incremental batch using the delta strategy.

    Reads from the current state DataFrame, selects rows to update/delete
    using deterministic methods, and generates mutations.

    Parameters
    ----------
    _resolved : optional
        Pre-computed ``resolve_plan()`` result. Pass this when calling
        in a loop to avoid re-resolving every batch.
    """
    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    cfg = plan.config_for(table_name)
    resolved = _resolved if _resolved is not None else resolve_plan(plan.base_plan)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)

    abs_batch_size = resolve_batch_size(cfg.batch_size, initial_rows)
    insert_count, update_count, delete_count = _compute_operation_counts(
        cfg,
        abs_batch_size,
    )
    batch_seed = compute_batch_seed(global_seed, batch_id)

    pk_cols = table_spec.primary_key.columns if table_spec.primary_key else []
    parts: list[DataFrame] = []

    # --- Inserts: direct spark.range with offset sequence ---
    if insert_count > 0:
        insert_df = _generate_inserts(
            spark,
            table_spec,
            resolved,
            batch_id,
            insert_count,
            initial_rows,
            global_seed,
        )
        if insert_df is not None:
            parts.append(insert_df)

    # --- Updates: select rows deterministically, then mutate ---
    if update_count > 0 and pk_cols:
        candidates = current_df
        if cfg.update_window is not None and "_batch_id" in current_df.columns:
            candidates = candidates.filter(F.col("_batch_id") >= F.lit(batch_id - cfg.update_window))

        update_pks = _select_rows_deterministic(
            candidates,
            pk_cols,
            update_count,
            batch_seed,
            "update",
        )
        if update_pks is not None:
            mutated = _mutate_selected_rows(
                current_df,
                update_pks.select(*pk_cols),
                pk_cols,
                table_spec,
                batch_seed,
                cfg,
            )
            if mutated is not None:
                parts.append(mutated)

    # --- Deletes ---
    if delete_count > 0 and pk_cols:
        delete_df = _select_rows_deterministic(
            current_df,
            pk_cols,
            delete_count,
            batch_seed,
            "delete",
        )
        if delete_df is not None:
            parts.append(delete_df)

    if not parts:
        schema_df = generate_table(spark, table_spec, resolved)
        return {table_name: schema_df.limit(0)}

    result_df = union_all(parts, allow_missing_columns=True)
    result_df = _add_ingest_metadata(result_df, plan, batch_id)
    return {table_name: result_df}


def _select_rows_deterministic(
    df: DataFrame,
    pk_cols: list[str],
    count: int,
    batch_seed: int,
    purpose: str,
) -> DataFrame | None:
    """Select rows deterministically using xxhash64 ranking.

    This avoids .sample() which is not deterministic across partition layouts.
    Uses orderBy + limit instead of row_number() to avoid a windowless
    Window (which forces all data to a single partition).
    """
    if count <= 0:
        return None

    # Hash-based deterministic ordering
    seed_lit = F.lit(batch_seed).cast("long")
    purpose_lit = F.lit(purpose)
    hash_col = F.xxhash64(*[F.col(c) for c in pk_cols], seed_lit, purpose_lit)

    return df.orderBy(hash_col).limit(count)


def _mutate_selected_rows(
    current_df: DataFrame,
    update_pks: DataFrame,
    pk_cols: list[str],
    table_spec: TableSpec,
    batch_seed: int,
    cfg: IngestTableConfig,
) -> DataFrame | None:
    """Generate mutated versions of selected rows.

    Uses a flat ``select([all_column_exprs])`` instead of chained
    ``withColumn`` to avoid O(n^2) Catalyst plan growth.
    """
    from dbldatagen.v1.engine.seed import derive_column_seed

    selected = current_df.join(update_pks, pk_cols, "inner")

    pk_set = set(pk_cols)
    meta_cols = {"_batch_id", "_load_ts"}
    data_cols = [c for c in selected.columns if c not in pk_set and c not in meta_cols]
    mutable = set(cfg.mutations.columns) if cfg.mutations.columns else set(data_cols)

    # Build column-spec lookup for dtype info
    col_specs_map = {cs.name: cs for cs in table_spec.columns}
    pk_exprs = [F.col(c) for c in pk_cols]

    # Build all column expressions in one pass — flat select
    select_exprs: list = []
    for col_name in selected.columns:
        if col_name in meta_cols:
            continue  # drop metadata; ingest layer re-adds its own
        if col_name not in mutable or col_name not in col_specs_map:
            select_exprs.append(F.col(col_name))
            continue

        col_spec = col_specs_map[col_name]
        col_seed = derive_column_seed(batch_seed, table_spec.name, col_spec.name)
        pk_hash = F.xxhash64(*pk_exprs, F.lit(col_seed).cast("long"))

        dtype_val = col_spec.dtype.value if col_spec.dtype else None
        if dtype_val in ("int", "long"):
            select_exprs.append(pk_hash.cast("long").alias(col_name))
        elif dtype_val in ("float", "double"):
            select_exprs.append((pk_hash.cast("double") / F.lit(9223372036854775807).cast("double")).alias(col_name))
        elif dtype_val == "string":
            select_exprs.append(pk_hash.cast("string").alias(col_name))
        else:
            select_exprs.append(F.col(col_name))

    return selected.select(*select_exprs)


# ---------------------------------------------------------------------------
# Snapshot assembly for delta strategy
# ---------------------------------------------------------------------------


def generate_delta_snapshot_batch(
    spark: SparkSession,
    plan: IngestPlan,
    table_name: str,
    batch_id: int,
    current_df: DataFrame,
) -> dict[str, DataFrame]:
    """Generate one snapshot batch using the delta strategy.

    Starts from current state, applies changes, returns full snapshot.
    """
    inc_result = generate_delta_incremental_batch(
        spark,
        plan,
        table_name,
        batch_id,
        current_df,
    )
    inc_df = inc_result[table_name]

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    pk_cols = []
    if table_spec.primary_key:
        pk_cols = table_spec.primary_key.columns

    if not pk_cols:
        # No PK — just return current + inserts
        snapshot = current_df.unionByName(inc_df, allowMissingColumns=True)
        snapshot = _add_ingest_metadata(snapshot, plan, batch_id)
        return {table_name: snapshot}

    # Separate inserts, updates, deletes from incremental batch
    # For delta strategy, the incremental batch contains mixed rows
    # Start from current, remove updated/deleted PKs, add new versions
    inc_pks = inc_df.select(*pk_cols)

    # Remove rows that appear in the incremental batch (they were updated or deleted)
    remaining = current_df.join(inc_pks, pk_cols, "left_anti")
    # Add back the incremental rows (new inserts + updated rows)
    snapshot = remaining.unionByName(inc_df, allowMissingColumns=True)
    snapshot = _add_ingest_metadata(snapshot, plan, batch_id)
    return {table_name: snapshot}


# ---------------------------------------------------------------------------
# Stateless strategy: three-range targeted scan
# ---------------------------------------------------------------------------


def generate_stateless_incremental_batch(
    spark: SparkSession,
    plan: IngestPlan,
    table_name: str,
    batch_id: int,
) -> dict[str, DataFrame]:
    """Generate one incremental batch statelessly via targeted scans.

    Uses three independent ``spark.range()`` calls — one for inserts,
    one for updates (stride scan), one for deletes (exact indices) —
    so total work is O(batch_size), not O(max_k).

    No ``current_df`` needed; any batch can be generated independently.
    """
    from dbldatagen.v1.engine.cdc_stateless import (
        birth_tick_expr,
        compute_periods,
        death_tick_expr,
        delete_indices_at_batch_fast,
        insert_range,
        max_k_at_batch,
    )

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    cfg = plan.config_for(table_name)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)

    abs_batch_size = resolve_batch_size(cfg.batch_size, initial_rows)
    periods = compute_periods(
        initial_rows,
        abs_batch_size,
        cfg.insert_fraction,
        cfg.update_fraction,
        cfg.delete_fraction,
        min_life=cfg.min_life,
    )

    batch_seed = compute_batch_seed(global_seed, batch_id)
    pk_set = get_pk_columns(table_spec)
    max_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)

    parts: list[DataFrame] = []

    # --- 1. INSERTS: exact range, 100% selectivity ---
    if periods.inserts_per_batch > 0 and batch_id >= 1:
        start_k, end_k = insert_range(batch_id, initial_rows, periods.inserts_per_batch)
        if end_k > start_k:
            insert_df = spark.range(start_k, end_k).withColumnRenamed("id", "_synth_row_id")
            insert_exprs = _build_stateless_column_exprs(
                table_spec,
                F.col("_synth_row_id"),
                batch_seed,
                global_seed,
                max_k,
                cfg,
                pk_set,
            )
            insert_df = insert_df.select(F.col("_synth_row_id"), *insert_exprs, F.lit("I").alias("_action"))
            parts.append(insert_df)

    # --- 2. UPDATES: stride scan, ~50-60% selectivity ---
    if periods.updates_per_batch > 0 and batch_id >= 1:
        import math

        up = int(periods.update_period) if not math.isinf(periods.update_period) else 0
        if up > 0:
            target_mod = (up - (batch_id % up)) % up
            # Tight upper bound: scan only the first `updates_per_batch` stride
            # elements (approximately the initial universe).  This avoids the
            # over-generation that occurs as inserted rows accumulate in k-space,
            # AND eliminates the need for `.limit()` which forces a CollectLimit
            # plan exchange and breaks distributed pipelining at scale.
            #
            # Why this works: update_period = initial_rows // updates_per_batch,
            # so target_mod + updates_per_batch * up ≈ initial_rows.  The scan
            # covers [target_mod, ~initial_rows) — all initial rows at the stride
            # offset.  Initial rows have t_birth=0 < batch_id so they all pass the
            # not_birth filter.  Inserted rows (k >= initial_rows) are excluded.
            scan_end = min(max_k, target_mod + periods.updates_per_batch * up)
            update_candidates = spark.range(target_mod, scan_end, up).withColumnRenamed("id", "_synth_row_id")
            id_col = F.col("_synth_row_id")

            # Filter: alive, not birth batch, not dying, within update window
            t_birth = birth_tick_expr(id_col, initial_rows, periods.inserts_per_batch)
            t_death = death_tick_expr(
                id_col,
                initial_rows,
                periods.inserts_per_batch,
                periods.death_period,
                cfg.min_life,
            )
            batch_lit = F.lit(batch_id).cast("long")
            alive = (t_birth <= batch_lit) & (batch_lit < t_death)
            not_birth = t_birth < batch_lit
            conditions = alive & not_birth

            if cfg.update_window is not None:
                age = batch_lit - t_birth
                conditions = conditions & (age <= F.lit(cfg.update_window).cast("long"))

            update_df = update_candidates.filter(conditions)

            update_exprs = _build_stateless_column_exprs(
                table_spec,
                F.col("_synth_row_id"),
                batch_seed,
                global_seed,
                max_k,
                cfg,
                pk_set,
                is_update=True,
            )
            update_df = update_df.select(F.col("_synth_row_id"), *update_exprs, F.lit("U").alias("_action"))
            parts.append(update_df)

    # --- 3. DELETES: exact indices from driver ---
    if periods.deletes_per_batch > 0 and batch_id >= 1:
        delete_ks = delete_indices_at_batch_fast(
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            cfg.min_life,
        )
        if delete_ks:
            from pyspark.sql.types import LongType, StructField, StructType

            schema = StructType([StructField("_synth_row_id", LongType(), False)])
            delete_df = spark.createDataFrame([(k,) for k in delete_ks], schema=schema)
            # Generate PK columns for delete rows (identity only)
            delete_exprs = _build_stateless_pk_exprs(
                table_spec,
                F.col("_synth_row_id"),
                global_seed,
                max_k,
                pk_set,
            )
            # Fill non-PK columns with NULL so unionByName works
            for col_spec in table_spec.columns:
                if col_spec.name not in pk_set:
                    delete_exprs.append(F.lit(None).alias(col_spec.name))

            delete_df = delete_df.select(F.col("_synth_row_id"), *delete_exprs, F.lit("D").alias("_action"))
            parts.append(delete_df)

    # --- Union ---
    if not parts:
        # Empty batch: return empty DataFrame with correct schema
        resolved = resolve_plan(plan.base_plan)
        schema_df = generate_table(spark, table_spec, resolved)
        empty = schema_df.limit(0).withColumn("_action", F.lit("I"))
        empty = _add_ingest_metadata(empty, plan, batch_id)
        return {table_name: empty}

    result_df = union_all(parts, allow_missing_columns=True)
    result_df = result_df.drop("_synth_row_id")
    result_df = _add_ingest_metadata(result_df, plan, batch_id)
    return {table_name: result_df}


def generate_stateless_snapshot_batch(
    spark: SparkSession,
    plan: IngestPlan,
    table_name: str,
    batch_id: int,
) -> dict[str, DataFrame]:
    """Generate a full snapshot of all alive rows at *batch_id*.

    Scans ``spark.range(0, max_k)`` and filters to alive rows.
    PK columns use identity seeding (global_seed); state columns use
    a per-row seed derived from the row's last-write batch for variation.
    """
    import math

    from dbldatagen.v1.engine.cdc_stateless import (
        birth_tick_expr,
        compute_periods,
        is_alive_expr,
        max_k_at_batch,
    )
    from dbldatagen.v1.engine.generator import build_column_expr
    from dbldatagen.v1.engine.seed import derive_column_seed

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    cfg = plan.config_for(table_name)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)

    abs_batch_size = resolve_batch_size(cfg.batch_size, initial_rows)
    periods = compute_periods(
        initial_rows,
        abs_batch_size,
        cfg.insert_fraction,
        cfg.update_fraction,
        cfg.delete_fraction,
        min_life=cfg.min_life,
    )

    max_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
    pk_set = get_pk_columns(table_spec)

    # Scan all rows ever created, filter to alive
    base_df = spark.range(0, max_k).withColumnRenamed("id", "_synth_row_id")
    id_col = F.col("_synth_row_id")

    alive = is_alive_expr(
        id_col,
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        cfg.min_life,
    )
    alive_df = base_df.filter(alive)

    # Compute last-write batch per row for state column seeding
    up = int(periods.update_period) if not math.isinf(periods.update_period) else 0
    if up > 0:
        t_birth = birth_tick_expr(id_col, initial_rows, periods.inserts_per_batch)
        batch_lit = F.lit(batch_id).cast("long")
        up_lit = F.lit(up).cast("long")
        remainder = (batch_lit + id_col) % up_lit
        most_recent_update = F.when(remainder == F.lit(0).cast("long"), batch_lit).otherwise(batch_lit - remainder)
        last_write = F.greatest(most_recent_update, t_birth).cast("long")
    else:
        # No updates: last write is always birth
        last_write = birth_tick_expr(id_col, initial_rows, periods.inserts_per_batch)

    alive_df = alive_df.withColumn("_last_write", last_write)

    # Build column expressions
    select_exprs: list = [id_col]
    for col_spec in table_spec.columns:
        if col_spec.name in pk_set:
            # Identity: always from global_seed
            col_seed = derive_column_seed(global_seed, table_spec.name, col_spec.name)
            expr = build_column_expr(col_spec, id_col, col_seed, max_k, global_seed)
        else:
            # State: mix in last_write for per-row variation
            col_seed = derive_column_seed(global_seed, table_spec.name, col_spec.name)
            state_cell_seed = F.xxhash64(F.lit(col_seed).cast("long"), id_col, F.col("_last_write"))
            expr = build_column_expr(
                col_spec,
                id_col,
                col_seed,
                max_k,
                global_seed,
                cell_seed_override=state_cell_seed,
            )

        if col_spec.null_fraction > 0 and col_spec.name not in pk_set:
            col_seed = derive_column_seed(global_seed, table_spec.name, col_spec.name)
            expr = apply_null_fraction(expr, col_seed, id_col, col_spec.null_fraction)

        select_exprs.append(expr.alias(col_spec.name))

    result_df = alive_df.select(*select_exprs)
    result_df = result_df.drop("_synth_row_id")
    result_df = _add_ingest_metadata(result_df, plan, batch_id)
    return {table_name: result_df}


def _build_stateless_column_exprs(
    table_spec: TableSpec,
    id_col: Column,
    batch_seed: int,
    global_seed: int,
    max_k: int,
    cfg: IngestTableConfig,
    pk_set: set[str],
    *,
    is_update: bool = False,
) -> list:
    """Build column expressions for stateless incremental batches.

    PK columns use identity seeding (global_seed) — same value for same k.

    For inserts: all state columns use batch_seed for variation.
    For updates: only columns listed in ``cfg.mutations.columns`` use
    batch_seed; immutable columns keep identity seeding so their values
    match the original row.  When ``mutations.columns`` is None (all
    eligible), every non-PK column is mutable.

    Returns a list of aliased Column expressions.
    """
    from dbldatagen.v1.engine.generator import build_column_expr
    from dbldatagen.v1.engine.seed import derive_column_seed
    from dbldatagen.v1.schema import FakerColumn as FakerColumnType

    mutable_cols = set(cfg.mutations.columns) if cfg.mutations.columns else None
    select_exprs = []

    for col_spec in table_spec.columns:
        is_pk = col_spec.name in pk_set
        # Determine whether this column should mutate in this row.
        # - PKs never mutate.
        # - On updates, only explicitly mutable columns mutate.
        # - On inserts (or when mutable_cols is None), all non-PK columns
        #   get fresh values.
        use_identity = is_pk or (is_update and mutable_cols is not None and col_spec.name not in mutable_cols)

        if use_identity:
            col_seed = derive_column_seed(global_seed, table_spec.name, col_spec.name)
        else:
            col_seed = derive_column_seed(batch_seed, table_spec.name, col_spec.name)

        seed_for_expr = global_seed if use_identity else batch_seed

        # FakerColumn needs special handling — build_column_expr doesn't
        # support it (it's normally handled via pandas_udf in generate_table).
        if isinstance(col_spec.gen, FakerColumnType):
            from dbldatagen.v1.engine.columns.faker_pool import build_faker_column

            expr = build_faker_column(
                id_col,
                col_seed,
                provider=col_spec.gen.provider,
                kwargs=col_spec.gen.kwargs or None,
                locale=col_spec.gen.locale,
            )
        else:
            expr = build_column_expr(col_spec, id_col, col_seed, max_k, seed_for_expr)

        if not is_pk:
            # Null seed always from global_seed for consistency across batches
            null_seed = derive_column_seed(global_seed, table_spec.name, col_spec.name)
            expr = apply_null_fraction(expr, null_seed, id_col, col_spec.null_fraction)

        select_exprs.append(expr.alias(col_spec.name))

    return select_exprs


def _build_stateless_pk_exprs(
    table_spec: TableSpec,
    id_col: Column,
    global_seed: int,
    max_k: int,
    pk_set: set[str],
) -> list:
    """Build only PK column expressions for delete rows."""
    from dbldatagen.v1.engine.generator import build_column_expr
    from dbldatagen.v1.engine.seed import derive_column_seed

    exprs = []
    for col_spec in table_spec.columns:
        if col_spec.name in pk_set:
            col_seed = derive_column_seed(global_seed, table_spec.name, col_spec.name)
            expr = build_column_expr(col_spec, id_col, col_seed, max_k, global_seed)
            exprs.append(expr.alias(col_spec.name))
    return exprs


# ---------------------------------------------------------------------------
# Change detection utility
# ---------------------------------------------------------------------------


def detect_changes(
    spark: SparkSession,
    before_df: DataFrame,
    after_df: DataFrame,
    key_columns: list[str],
    data_columns: list[str] | None = None,
) -> dict[str, DataFrame]:
    """Identify inserts, updates, deletes between two snapshots.

    Parameters
    ----------
    spark : SparkSession
    before_df : DataFrame
        Snapshot at time T.
    after_df : DataFrame
        Snapshot at time T+1.
    key_columns : list[str]
        Primary key column names.
    data_columns : list[str] | None
        Columns to compare for change detection. If None, uses all
        non-key, non-metadata columns.

    Returns
    -------
    dict with keys: 'inserts', 'updates', 'deletes', 'unchanged'
    """
    keys = key_columns
    meta_cols = {"_batch_id", "_load_ts"}

    if data_columns is None:
        data_cols = [c for c in after_df.columns if c not in keys and c not in meta_cols]
    else:
        data_cols = data_columns

    # Inserts: in after but not in before
    inserts = after_df.join(before_df.select(*keys), keys, "left_anti")

    # Deletes: in before but not in after
    deletes = before_df.join(after_df.select(*keys), keys, "left_anti")

    # Updates + Unchanged: inner join on keys
    joined = after_df.alias("new").join(before_df.alias("old"), keys, "inner")

    if data_cols:
        diff_cond = reduce(
            lambda a, b: a | b,
            [
                F.col(f"new.{c}").isNull()
                != F.col(f"old.{c}").isNull()
                | F.when(
                    F.col(f"new.{c}").isNotNull() & F.col(f"old.{c}").isNotNull(),
                    F.col(f"new.{c}") != F.col(f"old.{c}"),
                ).otherwise(F.lit(False))
                for c in data_cols
            ],
        )

        updates = joined.filter(diff_cond).select(*[F.col(f"new.{c}").alias(c) for c in after_df.columns])
        unchanged = joined.filter(~diff_cond).select(*[F.col(f"new.{c}").alias(c) for c in after_df.columns])
    else:
        updates = joined.limit(0).select(*[F.col(f"new.{c}").alias(c) for c in after_df.columns])
        unchanged = joined.select(*[F.col(f"new.{c}").alias(c) for c in after_df.columns])

    return {
        "inserts": inserts,
        "updates": updates,
        "deletes": deletes,
        "unchanged": unchanged,
    }
