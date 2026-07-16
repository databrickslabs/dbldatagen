"""Numeric column generation.

Generates int, long, float, double, and decimal values within a range, with
optional distribution control and step-based (lattice) snapping.
"""

from __future__ import annotations

import math
from typing import cast

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.core.engine.distributions import (
    _CONTINUOUS_PRECISION,
    apply_distribution,
    normal_value_expr,
)
from dbldatagen.core.engine.seed import cell_seed_expr
from dbldatagen.core.spec._constants import (
    DEFAULT_DECIMAL_PRECISION,
    DEFAULT_DECIMAL_SCALE,
)
from dbldatagen.core.spec.schema import DataType, Distribution, Normal


def build_range_column(
    id_column: Column | str,
    column_seed: int,
    min_val: float | int,
    max_val: float | int,
    distribution: Distribution | None = None,
    dtype: DataType | None = None,
    precision: int | None = None,
    scale: int | None = None,
    step: float | int | None = None,
) -> Column:
    """Builds a numeric column with values sampled from a range.

    The bounds are inclusive. Integer types produce a discrete range; float,
    double, and decimal types produce continuous values. When `step` is set,
    output is snapped to the lattice `min_val, min_val + step, ...` within the
    range. A `Normal` distribution with explicit `mean`/`stddev` is sampled in
    value units; other distributions sample uniformly over the range.

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        column_seed: Per-column seed.
        min_val: Inclusive lower bound.
        max_val: Inclusive upper bound; must be >= min_val.
        distribution: Optional sampling distribution (default None, meaning
            uniform).
        dtype: Optional target type; when None, inferred as LONG for integer
            bounds and DOUBLE otherwise (default None).
        precision: Optional total number of digits for DECIMAL (default None,
            meaning 10).
        scale: Optional number of fractional digits for DECIMAL (default None,
            meaning 0).
        step: Optional step size; when None, output is continuous
            (default None).

    Returns:
        A Spark `Column` of the resolved type holding the sampled values.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)

    seed_col = cell_seed_expr(column_seed, id_column)

    # A float step on integer bounds forces float output so the lattice points
    # can be represented (e.g. min=0, max=10, step=0.5 -> {0.0, 0.5, ..., 10.0}).
    is_integer = (
        dtype in (DataType.INT, DataType.LONG, DataType.INTEGER, None)
        and isinstance(min_val, int)
        and isinstance(max_val, int)
        and (step is None or isinstance(step, int))
    )

    if is_integer:
        # is_integer guarantees step is int or None; cast narrows the type for mypy.
        return _build_integer_range(seed_col, int(min_val), int(max_val), distribution, dtype, cast("int | None", step))
    else:
        return _build_float_range(seed_col, float(min_val), float(max_val), distribution, dtype, precision, scale, step)


def _build_integer_range(
    seed_col: Column,
    min_val: int,
    max_val: int,
    distribution: Distribution | None,
    dtype: DataType | None,
    step: int | None = None,
) -> Column:
    """Builds an integer column sampled from the range, snapped to the step size when set.

    Args:
        seed_col: Per-cell seed `Column` (long).
        min_val: Inclusive lower bound.
        max_val: Inclusive upper bound.
        distribution: Optional sampling distribution (None means uniform).
        dtype: Optional target data type, e.g. integer or long (default None,
            meaning LONG).
        step: Optional step size (default None, meaning 1).

    Returns:
        A Spark `Column` of the resolved integer type.

    Raises:
        ValueError: If the range produces no values (min_val > max_val).
    """
    step_size = int(step) if step is not None else 1
    lattice_count = (max_val - min_val) // step_size + 1
    if lattice_count <= 0:
        # RangeColumn.validate_range rejects min > max at plan time; reaching here
        # means the validator was bypassed. Raise so the bypass surfaces rather
        # than silently collapsing to a constant.
        raise ValueError(
            f"_build_integer_range: lattice_count={lattice_count} (min_val="
            f"{min_val}, max_val={max_val}, step={step_size}) is non-positive; "
            f"schema validator should have rejected this."
        )

    if isinstance(distribution, Normal) and (distribution.mean is not None or distribution.stddev is not None):
        # Explicit value-space Normal: sample in value units, snap to the nearest
        # lattice index, and clamp. Every other distribution routes through
        # apply_distribution instead.
        value = normal_value_expr(seed_col, float(min_val), float(max_val), distribution.mean, distribution.stddev)
        index = F.round((value - F.lit(float(min_val))) / F.lit(float(step_size))).cast("long")
        index = F.greatest(F.lit(0), F.least(index, F.lit(lattice_count - 1)))
    else:
        index = apply_distribution(seed_col, lattice_count, distribution)
    result = index * F.lit(step_size) + F.lit(min_val)

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
    step: float | int | None = None,
) -> Column:
    """Builds a float, double, or decimal column sampled from the range.

    Snaps values to the step size when set. Decimal output is rounded to `scale`
    digits.

    Args:
        seed_col: Per-cell seed `Column` (long).
        min_val: Inclusive lower bound.
        max_val: Inclusive upper bound.
        distribution: Optional sampling distribution (None means uniform).
        dtype: Optional target data type, e.g. double or decimal (default None,
            meaning DOUBLE).
        precision: Optional total number of digits for DECIMAL (None means 10).
        scale: Optional number of fractional digits for DECIMAL (None means 0).
        step: Optional step size (None means continuous).

    Returns:
        A Spark `Column` of the resolved type.

    Raises:
        ValueError: If max_val < min_val.
    """
    span = max_val - min_val
    if span < 0:
        # min > max is rejected by the schema validator; reaching here means it
        # was bypassed. Raise rather than silently collapsing to a constant.
        raise ValueError(
            f"_build_float_range: span={span} (min_val={min_val}, "
            f"max_val={max_val}) is negative; schema validator should "
            f"have rejected this."
        )
    if min_val == max_val:
        return F.lit(min_val)

    if step is not None and not isinstance(distribution, Normal):
        # Pick a uniform index in [0, lattice_count) and scale, giving exact
        # uniform coverage of the lattice points without the half-bin endpoint
        # asymmetry of "draw continuous, then snap". The 1e-9 epsilon absorbs
        # float-precision noise in span / step so min=0, max=1, step=0.1 yields
        # 11 lattice points (0.0 ... 1.0), not 10.
        lattice_count = math.floor(span / float(step) + 1e-9) + 1
        index = F.pmod(seed_col, F.lit(lattice_count))
        result = index.cast("double") * F.lit(float(step)) + F.lit(min_val)
    elif isinstance(distribution, Normal):
        # Value-space Normal honoring explicit mean/stddev; None on either field
        # auto-centers (midpoint, span/6).
        clamped = normal_value_expr(seed_col, min_val, max_val, distribution.mean, distribution.stddev)
        if step is not None:
            # Snap to the lattice; round-to-nearest preserves the bell-curve shape
            # and the final clamp catches a near-max value snapping past max_val.
            offset_snap = F.round((clamped - F.lit(min_val)) / F.lit(float(step))) * F.lit(float(step))
            clamped = F.greatest(F.lit(min_val), F.least(F.lit(min_val) + offset_snap, F.lit(max_val)))
        result = clamped
    else:
        # Continuous uniform: map seed to [0, 1) then scale.
        fraction = F.pmod(seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
        result = fraction * F.lit(span) + F.lit(min_val)

    spark_type = _resolve_spark_type(dtype, integer=False, precision=precision, scale=scale)
    if dtype == DataType.DECIMAL:
        effective_scale = scale if scale is not None else DEFAULT_DECIMAL_SCALE
        return F.round(result, effective_scale).cast(spark_type)
    return result.cast(spark_type)


def _resolve_spark_type(
    dtype: DataType | None,
    integer: bool = True,
    precision: int | None = None,
    scale: int | None = None,
) -> T.DataType:
    """Maps a DataType to the corresponding PySpark type.

    Args:
        dtype: Target data type, or None to use the default.
        integer: Whether the default (when dtype is None) is LongType rather
            than DoubleType (default True).
        precision: Optional total number of digits for DECIMAL (default None,
            meaning 10).
        scale: Optional number of fractional digits for DECIMAL (default None,
            meaning 0).

    Returns:
        The corresponding PySpark `DataType`.
    """
    if dtype is None:
        return T.LongType() if integer else T.DoubleType()
    if dtype == DataType.DECIMAL:
        return T.DecimalType(
            precision if precision is not None else DEFAULT_DECIMAL_PRECISION,
            scale if scale is not None else DEFAULT_DECIMAL_SCALE,
        )
    mapping = {
        DataType.INT: T.IntegerType(),
        DataType.INTEGER: T.IntegerType(),
        DataType.LONG: T.LongType(),
        DataType.FLOAT: T.FloatType(),
        DataType.DOUBLE: T.DoubleType(),
    }
    return mapping.get(dtype, T.LongType() if integer else T.DoubleType())
