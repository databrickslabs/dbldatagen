"""Numeric column generation via Spark SQL expressions (Tier 1).

Supports int, long, float, double, and decimal types within a [min, max] range,
with optional distribution control.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.v1.engine.distributions import (
    _CONTINUOUS_PRECISION,
    apply_distribution,
    normal_sample_expr,
)
from dbldatagen.v1.engine.seed import cell_seed_expr
from dbldatagen.v1.schema import DataType, Distribution, Normal


def build_range_column(
    id_col: Column | str,
    column_seed: int | Column,
    min_val: float | int,
    max_val: float | int,
    distribution: Distribution | None = None,
    dtype: DataType | None = None,
    cell_seed_override: Column | None = None,
) -> Column:
    """Generate numeric values in [min_val, max_val] using Spark SQL expressions.

    For integer types the range is discrete; for float/double/decimal the values
    are continuous.
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
        return _build_float_range(seed_col, float(min_val), float(max_val), distribution, dtype)


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
        frac = (F.abs(seed_col) % F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
        result = frac * F.lit(span) + F.lit(min_val)

    spark_type = _resolve_spark_type(dtype, integer=False)
    if dtype == DataType.DECIMAL:
        return F.round(result, 2).cast(spark_type)
    return result.cast(spark_type)


def _resolve_spark_type(dtype: DataType | None, integer: bool = True) -> T.DataType:
    """Map a DataType enum to a PySpark DataType."""
    if dtype is None:
        return T.LongType() if integer else T.DoubleType()
    mapping = {
        DataType.INT: T.IntegerType(),
        DataType.INTEGER: T.IntegerType(),
        DataType.LONG: T.LongType(),
        DataType.FLOAT: T.FloatType(),
        DataType.DOUBLE: T.DoubleType(),
        DataType.DECIMAL: T.DecimalType(18, 2),
    }
    return mapping.get(dtype, T.LongType() if integer else T.DoubleType())
