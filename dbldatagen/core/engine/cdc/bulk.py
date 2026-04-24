"""Bulk-insert fusion — ONE spark.range() for all inserts in a chunk.

When a chunk spans many batches that each produce inserts, the
single-batch path would issue N separate generators.  The bulk-insert
path unions them into one spark.range and uses CASE WHEN on the batch
offset to select the correct seed for each row.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc._common import batch_timestamp
from dbldatagen.core.engine.fk import build_fk_column
from dbldatagen.core.engine.generator import build_column_expr
from dbldatagen.core.engine.planner import FKResolution, ResolvedPlan
from dbldatagen.core.engine.seed import compute_batch_seed, derive_column_seed
from dbldatagen.core.engine.utils import (
    apply_null_fraction,
    case_when_chain,
    create_range_df,
    get_pk_columns,
)
from dbldatagen.core.spec.cdc_schema import CDCPlan
from dbldatagen.core.spec.schema import ColumnSpec, FakerColumn, SequenceColumn, TableSpec


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

    # batch_infos is a list of (batch_id, start_k, insert_count) tuples.
    # The local_id / batch_offset arithmetic below assumes every batch in
    # the chunk has the SAME insert_count so ``raw_id % inserts_per_batch``
    # and ``raw_id // inserts_per_batch`` partition the range cleanly.
    # Today the CDC engine produces uniform inserts_per_batch for any
    # given table (derived from batch_size x operation weights, constant
    # per plan), and ``_generate_chunk_for_table`` filters out batches
    # with insert_count == 0.  If a future refactor breaks that invariant
    # — e.g. per-batch insert budgets, warm-up ramps, deletion-aware
    # pacing — the arithmetic silently misaligns PKs and FKs.  Raise
    # (not assert) so the break survives ``python -O``: the docstring
    # warns that misalignment silently produces wrong PKs/FKs, which is
    # exactly what a stripped assert would let through.
    inserts_per_batch = batch_infos[0][2]
    if not all(info[2] == inserts_per_batch for info in batch_infos):
        raise RuntimeError(
            f"generate_bulk_inserts requires uniform inserts_per_batch across "
            f"the chunk; got {[info[2] for info in batch_infos]}.  If per-batch "
            f"insert counts vary, the chunk needs CASE WHEN offsets instead of "
            f"the uniform-division arithmetic used here."
        )
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
            if resolved_plan is None or fk_key not in resolved_plan.fk_resolutions:
                # Same contract as generator._build_fk_column_expr — never
                # silently emit ``F.lit(None)`` on a missing FKResolution,
                # which reopens the silent-all-NULL class of bug the
                # ForeignKeyColumn strategy was introduced (commit a78597b)
                # to close.  Raise loudly so the caller sees what they
                # forgot to thread.
                raise RuntimeError(
                    f"FK column '{table_name}.{col_spec.name}' has no "
                    f"FKResolution in the bulk-inserts path — caller must "
                    f"resolve the plan (via ``resolve_plan`` / ``generate_cdc_bulk``) "
                    f"before reaching ``generate_bulk_inserts``.  Calling the "
                    f"chunking path directly requires a ResolvedPlan carrying "
                    f"this column's FK."
                )
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

    SCALING NOTE: ``build_column_expr`` is called once per batch per
    column.  For struct / array columns this materialises a full nested
    expression tree (one per struct field, one per array slot) inside
    every CASE WHEN branch, so the plan-side work is
    ``O(N_batches x N_leaf_exprs)``.  The fused path (``cdc/fused.py``)
    solved the same scaling by pre-computing a ``column_seed_map`` and
    having each leaf expression look up its seed by row.  Back-porting
    that structure to the bulk path requires every batch to reuse one
    expression tree parameterised by a runtime seed Column -- a
    non-trivial refactor.  At ~100 batches/chunk on wide nested tables
    Catalyst compile slows perceptibly; below that the CASE WHEN cost
    is negligible next to the per-row execution savings.  If chunks
    grow beyond that, revisit here.
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
