"""Bulk-insert fusion — ONE spark.range() for all inserts in a chunk.

When a chunk spans many batches that each produce inserts, the
single-batch path would issue N separate generators.  The bulk-insert
path unions them into one spark.range and uses CASE WHEN on the batch
offset to select the correct seed for each row.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc_generator._common import batch_timestamp
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
