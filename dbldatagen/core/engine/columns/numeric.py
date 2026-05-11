"""Numeric column generation via Spark SQL expressions (Tier 1).

Supports int, long, float, double, and decimal types within a [min, max] range,
with optional distribution control.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.core.engine.distributions import (
    _CONTINUOUS_PRECISION,
    apply_distribution,
    normal_sample_expr,
)
from dbldatagen.core.engine.seed import cell_seed_expr
from dbldatagen.core.spec.schema import DataType, Distribution, Normal


# Default DECIMAL shape when a DECIMAL column has no explicit precision/scale.
# Kept as (18, 2) for backward compatibility with plans authored before
# precision/scale were user-configurable.
_DEFAULT_DECIMAL_PRECISION = 18
_DEFAULT_DECIMAL_SCALE = 2


def build_range_column(
    id_col: Column | str,
    column_seed: int | Column,
    min_val: float | int,
    max_val: float | int,
    distribution: Distribution | None = None,
    dtype: DataType | None = None,
    cell_seed_override: Column | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> Column:
    """Generates numeric values in ``[min_val, max_val]`` via Spark SQL.

    For integer ``dtype`` (or when both bounds are ``int`` and
    ``dtype`` is ``None``), the range is discrete and sampled via
    ``apply_distribution``.  For float / double / decimal types, the
    values are continuous; ``Normal`` is sampled directly (centred,
    clipped to the range) while every other distribution maps a
    uniform fraction onto ``[min_val, max_val]``.

    Args:
        id_col: Row-id ``Column`` reference or column name.
        column_seed: Per-column seed.  Scalar ``int`` for single-batch
          generation; ``Column`` for the multi-write-batch path.
        min_val: Inclusive lower bound.
        max_val: Inclusive upper bound.  Must be ``>= min_val``.
        distribution: Sampling distribution.  ``None`` defaults to
          ``Uniform``.
        dtype: Target ``DataType``.  When ``None``, the function
          infers ``LONG`` for integer bounds and ``DOUBLE`` otherwise.
        cell_seed_override: When provided, used as the per-cell seed
          ``Column`` instead of ``cell_seed_expr(column_seed, id_col)``.
        precision: Total digit count for ``DataType.DECIMAL``;
          defaults to ``18`` when unset.
        scale: Fractional digit count for ``DataType.DECIMAL``;
          defaults to ``2`` when unset.

    Returns:
        A Spark ``Column`` of the resolved Spark type containing the
        sampled values.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    seed_col = cell_seed_override if cell_seed_override is not None else cell_seed_expr(column_seed, id_col)

    # Determine whether we need integer or floating-point output
    is_integer = (
        dtype in (DataType.INT, DataType.LONG, DataType.INTEGER, None)
        and isinstance(min_val, int)
        and isinstance(max_val, int)
    )

    if is_integer:
        return _build_integer_range(seed_col, int(min_val), int(max_val), distribution, dtype)
    else:
        return _build_float_range(seed_col, float(min_val), float(max_val), distribution, dtype, precision, scale)


def _build_integer_range(
    seed_col: Column,
    min_val: int,
    max_val: int,
    distribution: Distribution | None,
    dtype: DataType | None,
) -> Column:
    """Generate integers in [min_val, max_val] inclusive."""
    range_size = max_val - min_val + 1
    if range_size <= 0:
        return F.lit(min_val)

    idx = apply_distribution(seed_col, range_size, distribution)
    result = idx + F.lit(min_val)

    spark_type = _resolve_spark_type(dtype, integer=True)
    return result.cast(spark_type)


def _build_float_range(
    seed_col: Column,
    min_val: float,
    max_val: float,
    distribution: Distribution | None,
    dtype: DataType | None,
    precision: int | None = None,
    scale: int | None = None,
) -> Column:
    """Generate floating-point values in [min_val, max_val]."""
    span = max_val - min_val
    if span <= 0:
        return F.lit(min_val)

    if isinstance(distribution, Normal):
        # Use the continuous normal directly, scaled to [min, max] (centred)
        mid = (min_val + max_val) / 2.0
        sd = span / 6.0  # ~99.7% within range
        raw = normal_sample_expr(seed_col, mean=mid, stddev=sd)
        result = F.greatest(F.lit(min_val), F.least(raw, F.lit(max_val)))
    else:
        # Map seed to [0, 1) then scale
        frac = F.pmod(seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
        result = frac * F.lit(span) + F.lit(min_val)

    spark_type = _resolve_spark_type(dtype, integer=False, precision=precision, scale=scale)
    if dtype == DataType.DECIMAL:
        effective_scale = scale if scale is not None else _DEFAULT_DECIMAL_SCALE
        return F.round(result, effective_scale).cast(spark_type)
    return result.cast(spark_type)


def _resolve_spark_type(
    dtype: DataType | None,
    integer: bool = True,
    precision: int | None = None,
    scale: int | None = None,
) -> T.DataType:
    """Map a DataType enum to a PySpark DataType."""
    if dtype is None:
        return T.LongType() if integer else T.DoubleType()
    if dtype == DataType.DECIMAL:
        return T.DecimalType(
            precision if precision is not None else _DEFAULT_DECIMAL_PRECISION,
            scale if scale is not None else _DEFAULT_DECIMAL_SCALE,
        )
    mapping = {
        DataType.INT: T.IntegerType(),
        DataType.INTEGER: T.IntegerType(),
        DataType.LONG: T.LongType(),
        DataType.FLOAT: T.FloatType(),
        DataType.DOUBLE: T.DoubleType(),
    }
    return mapping.get(dtype, T.LongType() if integer else T.DoubleType())
