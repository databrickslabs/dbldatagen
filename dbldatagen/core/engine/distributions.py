"""Distribution sampling via Spark SQL expressions.

Each function maps a per-cell seed column (int64) to a sampled index or value
using only Spark's built-in expressions (no Python UDFs).
"""

from __future__ import annotations

import functools
import math

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.spec.schema import (
    Distribution,
    Exponential,
    LogNormal,
    Normal,
    Uniform,
    WeightedValues,
    Zipf,
)


# 0.01% granularity is enough for weighted-values selection.
_UNIFORM_PRECISION = 10_000

# 2**53 = full IEEE 754 mantissa; round-trips exactly through cast("double").
_CONTINUOUS_PRECISION = 2**53


def _uniform_fraction(seed_col: Column) -> Column:
    """Maps a long-typed seed column to a uniform double in `[0.0, 1.0)`.

    Args:
        seed_col: A long-typed seed or hash column.

    Returns:
        A Spark double `Column` uniformly distributed in `[0.0, 1.0)`.
    """
    return F.pmod(seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))


def uniform_sample(cell_seed_col: Column, n: int) -> Column:
    """Maps a seed column to a uniform index in `[0, n)`.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        n: Exclusive upper bound on the index; must be positive.

    Returns:
        A Spark long `Column` of uniform indices in `[0, n)`.

    Raises:
        ValueError: If `n <= 0`.
    """
    if n <= 0:
        raise ValueError("n must be positive")
    if n == 1:
        return F.lit(0)
    return F.pmod(cell_seed_col, F.lit(n))


def weighted_sample_expr(
    cell_seed_col: Column,
    values: list,
    weights: dict[str, float],
) -> Column:
    """Selects from `values` using cumulative weights.

    Falls back to uniform selection when all weights are zero.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        values: The list of allowed values.
        weights: Mapping of value (rendered as `str`) to relative weight. Values
            absent from the mapping get weight 0 and are never selected.

    Returns:
        A Spark `Column` whose element type matches `values`.
    """
    total_weight = sum(weights.values())
    if total_weight <= 0:
        return _array_index(cell_seed_col, values)

    cumulative = 0.0
    thresholds: list[tuple[float, object]] = []
    for value in values:
        normalized_weight = weights.get(str(value), 0.0) / total_weight
        cumulative += normalized_weight
        thresholds.append((cumulative, value))

    fraction = F.pmod(cell_seed_col, F.lit(_UNIFORM_PRECISION)).cast("double") / F.lit(float(_UNIFORM_PRECISION))

    return functools.reduce(
        lambda accumulated, threshold: F.when(fraction < F.lit(threshold[0]), F.lit(threshold[1])).otherwise(
            accumulated
        ),
        reversed(thresholds[:-1]),
        F.lit(thresholds[-1][1]),
    )


def normal_sample_expr(
    cell_seed_col: Column,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Samples `N(mean, stddev)` using a Box-Muller transform.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        mean: Optional distribution mean (default 0.0).
        stddev: Optional standard deviation (default 1.0).

    Returns:
        A Spark double `Column` of `N(mean, stddev)` samples.

    Note:
        Output is unbounded. Callers that need a bounded result must clamp it.
    """
    hash1 = F.xxhash64(cell_seed_col, F.lit(0).cast("long"))
    hash2 = F.xxhash64(cell_seed_col, F.lit(1).cast("long"))
    epsilon = 1.0 / _CONTINUOUS_PRECISION
    uniform1 = F.greatest(_uniform_fraction(hash1), F.lit(epsilon))
    uniform2 = _uniform_fraction(hash2)
    standard_normal = F.sqrt(F.lit(-2.0) * F.log(uniform1)) * F.cos(F.lit(2.0 * math.pi) * uniform2)
    return standard_normal * F.lit(stddev) + F.lit(mean)


def normal_value_expr(
    cell_seed_col: Column,
    min_val: float,
    max_val: float,
    mean: float | None = None,
    stddev: float | None = None,
) -> Column:
    """Samples a normal distribution clamped to a specified value range.

    `mean` and `stddev` are in the range's value units. When unset, the bell is
    auto-centered: `mean` becomes the midpoint `(min_val + max_val) / 2` and
    `stddev` becomes `(max_val - min_val) / 6`.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        min_val: Inclusive lower bound, in value units.
        max_val: Inclusive upper bound, in value units.
        mean: Optional peak in value units (default None, meaning the midpoint).
        stddev: Optional spread in value units (default None, meaning
            (max_val - min_val) / 6).

    Returns:
        A Spark double `Column` of values in `[min_val, max_val]`.
    """
    effective_mean = mean if mean is not None else (min_val + max_val) / 2.0
    effective_stddev = stddev if stddev is not None else (max_val - min_val) / 6.0
    raw = normal_sample_expr(cell_seed_col, mean=effective_mean, stddev=effective_stddev)
    return F.greatest(F.lit(min_val), F.least(raw, F.lit(max_val)))


def zipf_sample_expr(cell_seed_col: Column, n: int, exponent: float = 1.5) -> Column:
    """Samples a Zipf-like distribution over `[0, n)` using an inverse power-law CDF.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        n: Exclusive upper bound on the index; `n <= 1` returns 0.
        exponent: Optional power-law exponent; must be > 1.0 (default 1.5).

    Returns:
        A Spark long `Column` of indices in `[0, n)`.

    Raises:
        ValueError: If `exponent <= 1.0`.
    """
    if n <= 1:
        return F.lit(0)
    if exponent <= 1.0:
        # Defensive: Zipf.validate_params enforces exponent > 1, but raise
        # (not assert) so a validator bypass also fails under python -O.
        raise ValueError(f"zipf_sample_expr requires exponent > 1, got {exponent}")
    uniform = F.greatest(_uniform_fraction(cell_seed_col), F.lit(1e-9))
    inverse_exponent = 1.0 / (exponent - 1.0)
    scaled_index = F.lit(float(n)) * F.pow(F.lit(1.0) - uniform, F.lit(inverse_exponent))
    index = F.floor(scaled_index).cast("long")
    return F.greatest(F.lit(0), F.least(index, F.lit(n - 1)))


def exponential_sample_expr(
    cell_seed_col: Column,
    n: int,
    rate: float = 1.0,
) -> Column:
    """Samples an exponential distribution over `[0, n)` using an inverse CDF.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        n: Exclusive upper bound on the index; `n <= 1` returns 0.
        rate: Optional rate parameter (lambda); must be positive (default 1.0).

    Returns:
        A Spark long `Column` of indices in `[0, n)`.
    """
    if n <= 1:
        return F.lit(0)
    uniform = F.least(_uniform_fraction(cell_seed_col), F.lit(0.999999))  # avoid log(0)
    sample = F.lit(-1.0) * F.log(F.lit(1.0) - uniform) / F.lit(rate)
    index = F.floor(sample * F.lit(float(n) / 5.0)).cast("long")
    return F.greatest(F.lit(0), F.least(index, F.lit(n - 1)))


def lognormal_sample_expr(
    cell_seed_col: Column,
    n: int,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Samples a log-normal distribution over `[0, n)`.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        n: Exclusive upper bound on the index; `n <= 1` returns 0.
        mean: Optional mean of the underlying normal (default 0.0).
        stddev: Optional standard deviation of the underlying normal
            (default 1.0).

    Returns:
        A Spark long `Column` of indices in `[0, n)`.
    """
    if n <= 1:
        return F.lit(0)
    standard_normal = normal_sample_expr(cell_seed_col, mean=mean, stddev=stddev)
    sample = F.exp(standard_normal)
    median = math.exp(mean)
    scale = float(n) / (median * 10.0)
    # Clamp as double before the long cast: extreme mean+stddev pushes
    # sample*scale past 2**63 and trips ARITHMETIC_OVERFLOW under Spark ANSI.
    scaled_index = F.floor(sample * F.lit(scale))
    clamped = F.least(F.lit(float(n - 1)), F.greatest(F.lit(0.0), scaled_index))
    return clamped.cast("long")


def apply_distribution(
    cell_seed_col: Column,
    n: int,
    distribution: Distribution | None,
) -> Column:
    """Routes a `Distribution` to its sampling function.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        n: Exclusive upper bound on the output index.
        distribution: The distribution to sample under (None means uniform).

    Returns:
        A Spark long `Column` of indices in `[0, n)`.

    Raises:
        ValueError: If `distribution` is `WeightedValues` (handled out-of-band
            by `weighted_sample_expr`).
        TypeError: If `distribution` is an unhandled `Distribution` subclass.
    """
    if distribution is None or isinstance(distribution, Uniform):
        return uniform_sample(cell_seed_col, n)
    if isinstance(distribution, Zipf):
        return zipf_sample_expr(cell_seed_col, n, distribution.exponent)
    if isinstance(distribution, Exponential):
        return exponential_sample_expr(cell_seed_col, n, distribution.rate)
    if isinstance(distribution, Normal):
        # Index-space auto-center only (midpoint n/2, spread n/6). A Normal
        # carrying explicit value-space mean/stddev never reaches here: RangeColumn
        # routes it to normal_value_expr, and other hosts reject it at plan time.
        raw_sample = normal_sample_expr(cell_seed_col, mean=n / 2.0, stddev=n / 6.0)
        index = F.floor(raw_sample).cast("long")
        return F.greatest(F.lit(0), F.least(index, F.lit(n - 1)))
    if isinstance(distribution, LogNormal):
        return lognormal_sample_expr(cell_seed_col, n, distribution.mean, distribution.stddev)
    if isinstance(distribution, WeightedValues):
        # WeightedValues needs the discrete value list and is dispatched at the
        # column level via weighted_sample_expr; this raise is the engine-level
        # backstop for a plan that bypassed schema validation.
        raise ValueError(
            "WeightedValues distribution reached apply_distribution; only "
            "ValuesColumn dispatches WeightedValues correctly (via "
            "weighted_sample_expr with the discrete value list).  Schema "
            "validation should have caught this."
        )
    raise TypeError(
        f"apply_distribution: unhandled Distribution subclass "
        f"{type(distribution).__name__}.  Add an isinstance branch above."
    )


def _array_index(cell_seed_col: Column, values: list) -> Column:
    """Selects from a list using the seed as a uniform index.

    Args:
        cell_seed_col: Per-cell seed `Column` (long).
        values: The list of allowed values.

    Returns:
        A Spark `Column` whose element type matches `values`.
    """
    value_array = F.array(*[F.lit(value) for value in values])
    index = F.pmod(cell_seed_col, F.lit(len(values)))
    return value_array[index.cast("int")]
