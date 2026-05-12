"""Shared engine utilities."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def create_range_df(
    spark: SparkSession,
    row_count: int,
) -> tuple[DataFrame, Column]:
    """Creates a range ``DataFrame`` with a ``_synth_row_id`` column.

    Renames ``spark.range()``'s default ``id`` column to avoid
    collisions with user columns named ``id``.  The renamed column
    is reserved internally and dropped before the final ``DataFrame``
    is returned to the user.

    Args:
        spark: Active ``SparkSession``.
        row_count: Number of rows to generate.  Must be non-negative.

    Returns:
        A two-tuple ``(df, id_col)`` where ``df`` has a single
        ``_synth_row_id`` column and ``id_col`` is the corresponding
        ``Column`` reference for use in downstream expressions.
    """
    df = spark.range(row_count).withColumnRenamed("id", "_synth_row_id")
    return df, F.col("_synth_row_id")


def apply_null_fraction(
    expr: Column,
    column_seed: int,
    id_col: Column,
    null_fraction: float,
) -> Column:
    """Wraps *expr* with a null mask when *null_fraction* > 0.

    Args:
        expr: The Spark ``Column`` expression to wrap.
        column_seed: Per-column seed (planning-time constant).
        id_col: Row-id ``Column`` (typically
          ``F.col("_synth_row_id")``).
        null_fraction: Target fraction of rows to mark NULL, in
          ``[0.0, 1.0]``.

    Returns:
        ``expr`` unchanged when ``null_fraction <= 0``; otherwise
        ``F.when(null_mask, NULL).otherwise(expr)``.
    """
    if null_fraction <= 0:
        return expr
    from dbldatagen.core.engine.seed import null_mask_expr

    is_null = null_mask_expr(column_seed, id_col, null_fraction)
    return F.when(is_null, F.lit(None)).otherwise(expr)


def apply_column_phases(
    df: DataFrame,
    id_col: Column,
    col_exprs: list[Column],
    udf_columns: list[tuple[str, Column]],
    seeded_columns: list[tuple[str, Column]],
) -> DataFrame:
    """Applies the three-phase column application pattern.

    Phase 1 -- flat ``select`` for Spark SQL expressions (cheapest).
    Phase 2 -- ``withColumn`` for UDF-based columns (FK, Faker) so
        each one anchors against the already-projected scalar
        columns.
    Phase 3 -- ``withColumn`` for ``seed_from`` columns so they
        reference the already-materialised parent column.
    Finally, drops the internal ``_synth_row_id`` column.

    Args:
        df: Source ``DataFrame`` carrying the ``_synth_row_id``
          column produced by ``create_range_df``.
        id_col: ``Column`` reference to ``_synth_row_id``.
        col_exprs: Phase-1 Spark SQL expression columns to project.
        udf_columns: Phase-2 list of ``(name, expr)`` pairs for
          UDF-based columns added via ``withColumn``.
        seeded_columns: Phase-3 list of ``(name, expr)`` pairs for
          ``seed_from``-derived columns added after their parents.

    Returns:
        A ``DataFrame`` with the user-facing columns in declaration
        order; ``_synth_row_id`` is no longer present.
    """
    df = df.select(id_col, *col_exprs)

    for col_name, col_expr in udf_columns:
        df = df.withColumn(col_name, col_expr)

    for col_name, col_expr in seeded_columns:
        df = df.withColumn(col_name, col_expr)

    return df.drop("_synth_row_id")
