"""Numeric column generation via Spark SQL expressions (Tier 1).

Supports int, long, float, double, and decimal types within a [min, max] range,
with optional distribution control.
"""

from __future__ import annotations

import math

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


# Fallback DECIMAL shape when a column has dtype=DECIMAL but no explicit
# precision/scale.  Matches Spark's ``DecimalType()`` default of ``(10, 0)``
# (see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.DecimalType.html)
# so plans without precision/scale produce the same shape Spark would
# produce on its own.
_DEFAULT_DECIMAL_PRECISION = 10
_DEFAULT_DECIMAL_SCALE = 0


def build_range_column(
    id_col: Column | str,
    column_seed: int,
    min_val: float | int,
    max_val: float | int,
    distribution: Distribution | None = None,
    dtype: DataType | None = None,
    precision: int | None = None,
    scale: int | None = None,
    step: float | int | None = None,
) -> Column:
    """Generates numeric values in ``[min_val, max_val]`` via Spark SQL.

    For integer ``dtype`` (or when ``min_val``, ``max_val``, and
    ``step`` are all integer and ``dtype`` is ``None``), the range is
    discrete and sampled via ``apply_distribution``.  For float /
    double / decimal types, the values are continuous; ``Normal`` is
    sampled directly (centred, clipped to the range) while every
    other distribution maps a uniform fraction onto
    ``[min_val, max_val]``.

    When ``step`` is set, output is snapped to the lattice
    ``{min_val, min_val + step, min_val + 2*step, ...}`` intersected
    with ``[min_val, max_val]``.  Integer-path and float-uniform paths
    use the lattice-index approach (exact uniform across lattice
    points).  Float ``Normal`` snaps the continuous draw to the
    nearest lattice point, preserving the bell-curve shape.

    Args:
        id_col: Row-id ``Column`` reference or column name.
        column_seed: Per-column seed (planning-time constant).
        min_val: Inclusive lower bound.
        max_val: Inclusive upper bound.  Must be ``>= min_val``.
        distribution: Sampling distribution.  ``None`` defaults to
          ``Uniform``.
        dtype: Target ``DataType``.  When ``None``, the function
          infers ``LONG`` for integer bounds and ``DOUBLE`` otherwise.
        precision: Total digit count for ``DataType.DECIMAL``;
          defaults to ``10`` (Spark's ``DecimalType()`` default) when unset.
        scale: Fractional digit count for ``DataType.DECIMAL``;
          defaults to ``0`` (Spark's ``DecimalType()`` default) when unset.
        step: Optional lattice spacing.  ``None`` (default) leaves the
          output continuous within the range.  Schema validator
          (``RangeColumn.validate_range``) rejects ``step <= 0`` at
          plan time.

    Returns:
        A Spark ``Column`` of the resolved Spark type containing the
        sampled values.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    seed_col = cell_seed_expr(column_seed, id_col)

    # Determine whether we need integer or floating-point output.
    # ``step`` participates: a float ``step`` on integer bounds forces
    # float output so the lattice points can be represented (e.g.,
    # ``min=0, max=10, step=0.5`` → {0.0, 0.5, ..., 10.0}).
    is_integer = (
        dtype in (DataType.INT, DataType.LONG, DataType.INTEGER, None)
        and isinstance(min_val, int)
        and isinstance(max_val, int)
        and (step is None or isinstance(step, int))
    )

    if is_integer:
        return _build_integer_range(seed_col, int(min_val), int(max_val), distribution, dtype, step)
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
    """Generate integers in [min_val, max_val] inclusive, snapped to the step lattice when step is set."""
    s = int(step) if step is not None else 1
    n_lattice = (max_val - min_val) // s + 1
    if n_lattice <= 0:
        # Schema validator (``RangeColumn.validate_range``) rejects
        # ``min > max`` at plan time; reaching here means the validator
        # was bypassed (e.g. under ``python -O`` with custom-built
        # RangeColumn).  Raise instead of silently collapsing to a
        # constant so the bypass surfaces.
        raise ValueError(
            f"_build_integer_range: n_lattice={n_lattice} (min_val="
            f"{min_val}, max_val={max_val}, step={s}) is non-positive; "
            f"schema validator should have rejected this."
        )

    idx = apply_distribution(seed_col, n_lattice, distribution)
    result = idx * F.lit(s) + F.lit(min_val)

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
    """Generate floating-point values in [min_val, max_val], snapped to the step lattice when step is set."""
    span = max_val - min_val
    if span < 0:
        # ``min > max`` is rejected by the schema validator; reaching
        # here means the validator was bypassed.  Raise instead of
        # silently collapsing to a constant.
        raise ValueError(
            f"_build_float_range: span={span} (min_val={min_val}, "
            f"max_val={max_val}) is negative; schema validator should "
            f"have rejected this."
        )
    if min_val == max_val:
        # Legitimate degenerate range -- emit the single literal.
        return F.lit(min_val)

    if step is not None and not isinstance(distribution, Normal):
        # Lattice-index for non-Normal distributions (including
        # Uniform / Zipf / Exponential / LogNormal, which the float
        # path currently folds into a uniform fraction anyway).  Pick
        # a uniform index in ``[0, n_lattice)`` and scale -- gives
        # exact uniform across the lattice points without the half-bin
        # endpoint asymmetry that "draw continuous, then snap" would
        # introduce.  ``1e-9`` epsilon absorbs float-precision noise
        # in ``span / step`` so ``min=0, max=1, step=0.1`` yields 11
        # lattice points (0.0 ... 1.0), not 10.
        n_lattice = math.floor(span / float(step) + 1e-9) + 1
        idx = F.pmod(seed_col, F.lit(n_lattice))
        result = idx.cast("double") * F.lit(float(step)) + F.lit(min_val)
    elif isinstance(distribution, Normal):
        # Use the continuous normal directly, scaled to [min, max] (centred)
        mid = (min_val + max_val) / 2.0
        sd = span / 6.0  # ~99.7% within range
        raw = normal_sample_expr(seed_col, mean=mid, stddev=sd)
        clamped = F.greatest(F.lit(min_val), F.least(raw, F.lit(max_val)))
        if step is not None:
            # Snap continuous Normal to the lattice.  Round-to-nearest
            # preserves the bell-curve shape across the lattice points;
            # the final clamp catches the edge case where snap pushes
            # a near-max raw value to ``min_val + n_lattice*step`` past
            # ``max_val``.
            offset_snap = F.round((clamped - F.lit(min_val)) / F.lit(float(step))) * F.lit(float(step))
            clamped = F.greatest(F.lit(min_val), F.least(F.lit(min_val) + offset_snap, F.lit(max_val)))
        result = clamped
    else:
        # Map seed to [0, 1) then scale -- continuous uniform path.
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
