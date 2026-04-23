"""Shared CDC helpers used across the ``cdc`` package.

Collected from the former ``cdc/_common.py`` (CDCStream, plan
normalisation, chunk sizing) and the former
``cdc_generator/_common.py`` (RawBatchResult, metadata columns,
FK-aware config adjustment, period derivation, write-batch column
application).  Keeping a single private ``_common`` per package keeps
the per-path modules (``single_batch``, ``fused``, ``bulk``) focused
on their own algorithm without pulling in a sibling package.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc.stateless import (
    CDCPeriods,
    batch_timestamp_epoch,
    compute_periods,
)
from dbldatagen.core.engine.generator import build_all_column_exprs_case_when
from dbldatagen.core.engine.planner import ResolvedPlan
from dbldatagen.core.engine.utils import _LazyList, resolve_batch_size, union_all
from dbldatagen.core.spec.cdc_schema import CDCFormat, CDCPlan, CDCTableConfig, OperationWeights
from dbldatagen.core.spec.schema import DataGenPlan, FakerColumn, TableSpec


# ---------------------------------------------------------------------------
# Stream + plan normalisation + chunk sizing
# ---------------------------------------------------------------------------


@dataclass
class CDCStream:
    """Complete CDC stream: initial snapshot + batches of changes.

    Attributes
    ----------
    initial : dict[str, DataFrame]
        Full table snapshots at batch 0 (all ops = insert).
    batches :
        Lazy list of batch dicts ``{table_name: DataFrame}``.
        Supports indexing, iteration, and ``len()``.
        Batches are generated on first access and cached.
    plan : CDCPlan
        The plan used to generate this stream.
    """

    initial: dict[str, DataFrame] = field(default_factory=dict)
    batches: _LazyList[dict[str, DataFrame]] | list[dict[str, DataFrame]] = field(default_factory=list)
    plan: CDCPlan | None = None


def _normalize_plan(
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None,
    format: str | CDCFormat | None,
) -> CDCPlan:
    """Normalise input to a CDCPlan, applying overrides."""
    if isinstance(plan_or_base, DataGenPlan):
        kwargs: dict = {"base_plan": plan_or_base}
        if num_batches is not None:
            kwargs["num_batches"] = num_batches
        if format is not None:
            kwargs["format"] = CDCFormat(format) if isinstance(format, str) else format
        return CDCPlan(**kwargs)
    else:
        plan = plan_or_base
        if num_batches is not None:
            plan = plan.model_copy(update={"num_batches": num_batches})
        if format is not None:
            fmt = CDCFormat(format) if isinstance(format, str) else format
            plan = plan.model_copy(update={"format": fmt})
        return plan


# Target rows per chunk for auto-sizing: large enough for cluster
# utilization, small enough to avoid OOM on wide schemas.
_AUTO_CHUNK_TARGET_ROWS = 20_000_000


def _auto_chunk_size(plan: CDCPlan) -> int:
    """Pick chunk_size so each chunk has ~_AUTO_CHUNK_TARGET_ROWS rows."""
    target_rows = _AUTO_CHUNK_TARGET_ROWS
    max_rows_per_batch = 1
    for table_name in plan.cdc_tables:
        config = plan.config_for(table_name)
        table_spec = next(t for t in plan.base_plan.tables if t.name == table_name)
        batch_size = resolve_batch_size(config.batch_size, int(table_spec.rows))
        _ins_frac, upd_frac, _del_frac = config.operations.fractions
        # Each batch produces: batch_size + update_before_images
        rows = batch_size + int(batch_size * upd_frac)
        max_rows_per_batch = max(max_rows_per_batch, rows)
    chunk = max(1, target_rows // max(max_rows_per_batch, 1))
    return min(chunk, plan.num_batches)


# ---------------------------------------------------------------------------
# Raw batch result
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
# Table-level predicates
# ---------------------------------------------------------------------------


def _table_has_faker_columns(table_spec: TableSpec) -> bool:
    """Check if a table has any Faker columns (which require Python UDF pools)."""
    return any(isinstance(col.gen, FakerColumn) for col in table_spec.columns)


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


# ---------------------------------------------------------------------------
# Metadata column attachment
# ---------------------------------------------------------------------------


def _add_cdc_metadata(
    df: DataFrame | None,
    op: str,
    batch_id: int,
    batch_ts_epoch: int,
) -> DataFrame | None:
    """Attach _op, _batch_id, _ts metadata columns to a CDC DataFrame.

    ``batch_ts_epoch`` is UTC seconds — casting a long to timestamp is
    session-timezone-independent, unlike parsing a local-naive
    formatted string.  See ``batch_timestamp_epoch`` for rationale.
    """
    if df is None:
        return None
    return (
        df.withColumn("_op", F.lit(op))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ts", F.lit(batch_ts_epoch).cast("long").cast("timestamp"))
    )


# ---------------------------------------------------------------------------
# FK-aware config adjustment
# ---------------------------------------------------------------------------


def apply_fk_delete_guard(plan: CDCPlan, table_name: str, config: CDCTableConfig) -> CDCTableConfig:
    """Disable deletes for tables that are FK parents.

    Returns the original config if no guard is needed, or a copy
    with delete weight set to 0.

    **Scope:** this guard is only reachable for plans that passed
    ``CDCPlan._reject_cross_cdc_foreign_keys`` — FK references between
    two CDC tables are rejected at plan-construction time today,
    because the FK reconstruction in ``fk.build_fk_column`` uses the
    plan-time parent row count and pk_seed rather than tracking
    per-batch parent lifecycle.  The remaining cases this guard
    handles (static parent → CDC child, CDC parent → non-CDC child)
    are safe.  If we ever lift the cross-CDC restriction, this guard
    must grow to also handle PK-changing updates and transitive
    delete cascades.
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


# ---------------------------------------------------------------------------
# Period + timestamp derivation
# ---------------------------------------------------------------------------


def batch_timestamp(plan: CDCPlan, batch_id: int) -> int:
    """Compute the UTC epoch seconds for a given batch's timestamp.

    Returning epoch (not a formatted string) keeps downstream
    ``F.lit(epoch).cast("timestamp")`` calls session-TZ-independent
    and aligned with the fused multi-batch path.  See
    ``batch_timestamp_epoch`` for the TZ-divergence rationale.
    """
    return batch_timestamp_epoch(plan.start_timestamp, plan.batch_interval_seconds, batch_id)


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


# ---------------------------------------------------------------------------
# Write-batch helpers (shared by single-batch native and fused paths)
# ---------------------------------------------------------------------------


def _precompute_write_batches(batch_id: int, update_period: int | float) -> list[int] | None:
    """Compute the superset of possible pre_image_batch values on the driver.

    The pre_image_batch formula produces values in
    ``range(max(0, batch_id - up), batch_id)`` where ``up`` is the
    update_period.  This is at most ``up`` values and avoids a Spark
    ``.collect()`` round-trip.

    When ``update_period`` is infinite (no updates), the pre-image is
    always the birth_tick.  Rather than enumerating all possible birth
    ticks (which grows with ``batch_id``), returns ``None`` to signal
    callers to collect distinct values from the DataFrame instead.
    """
    import math

    if math.isinf(update_period):
        # No updates — birth_tick range is unbounded.  Let caller collect.
        return None
    up = int(update_period)
    if up <= 0:
        return None
    return list(range(max(0, batch_id - up), batch_id))


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
