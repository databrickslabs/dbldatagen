"""Fused multi-batch CDC generation — ONE spark.range per stream per chunk.

The single-batch path produces one DataFrame per (table, batch).  The
fused path produces deletes/updates for a *range* of batches in a single
scan by filtering on ``death_tick`` / ``update_due`` expressions that
take ``batch_id`` as a Spark column rather than a Python scalar.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc._common import (
    _apply_columns_with_write_batch,
    _precompute_write_batches,
    _table_has_faker_columns,
    batch_timestamp,
)
from dbldatagen.core.engine.cdc.stateless import (
    CDCPeriods,
    birth_tick_expr,
    death_tick_expr,
    max_k_at_batch,
)
from dbldatagen.core.engine.planner import ResolvedPlan
from dbldatagen.core.engine.utils import case_when_chain, create_range_df
from dbldatagen.core.spec.cdc_schema import CDCPlan
from dbldatagen.core.spec.schema import TableSpec


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
    # ``_batch_id`` lives as long everywhere else in the engine (CDC
    # output schema, seed derivation, window filters); this column is
    # the one place the fused path used ``int`` without a cap on
    # ``num_batches``.  Past 2**31 batches the int cast would overflow
    # and Spark ANSI mode would raise.  Keep long end-to-end.
    df = df.withColumn("_batch_id", dt.cast("long"))

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

    all_wbs: set[int] = set()
    all_wbs_finite = True
    for b in batch_ids:
        wbs = _precompute_write_batches(b, periods.update_period)
        if wbs is None:
            all_wbs_finite = False
            break
        all_wbs.update(wbs)

    return _build_fused_output(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        sorted(all_wbs) if all_wbs_finite else None,
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

    # Integer ceil-div ``(a + b - 1) div b``.  Materialize the numerator
    # as a named column so we can apply SQL ``div`` (true integer
    # division) — Spark's ``/`` always returns double and silently
    # loses precision above 2**53 (~9e15), which is reachable at the
    # 500M-3B row scale this path targets.  ``F.floor(long/long)``
    # likewise routes through double.
    df = df.withColumn("_ceil_num", min_b_lit + id_col + up_lit - F.lit(1).cast("long"))
    ceil_div = F.expr(f"_ceil_num div {up}")
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
    # See death_batch_df comment: ``_batch_id`` is long everywhere else;
    # avoid an int overflow at high ``num_batches``.
    df = df.withColumn("_batch_id", candidate_b.cast("long")).drop("_ceil_num")

    # Before-image: _write_batch = pre_image_batch(k, candidate_b)
    batch_n_col = F.col("_batch_id").cast("long")
    remainder = (batch_n_col + id_col) % up_lit
    prev = F.when(
        remainder == F.lit(0).cast("long"),
        batch_n_col - up_lit,
    ).otherwise(batch_n_col - remainder)
    df = df.withColumn("_write_batch", F.greatest(prev, t_birth).cast("long"))

    all_wbs_before: set[int] = set()
    all_wbs_before_finite = True
    for b in batch_ids:
        wbs = _precompute_write_batches(b, periods.update_period)
        if wbs is None:
            all_wbs_before_finite = False
            break
        all_wbs_before.update(wbs)

    before_df = _build_fused_output(
        df,
        table_spec,
        resolved_plan,
        global_seed,
        id_col,
        sorted(all_wbs_before) if all_wbs_before_finite else None,
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
    unique_wbs: list[int] | None,
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
