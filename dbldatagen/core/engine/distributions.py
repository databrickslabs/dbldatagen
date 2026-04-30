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


def uniform_sample(cell_seed_col: Column, n: int) -> Column:
    """Map a seed column to a uniform index in [0, n).

    ``pmod`` (not ``abs % n``) avoids ``abs(Long.MIN_VALUE)`` overflow
    under Spark ANSI mode.
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
    """Build a CASE/WHEN chain that selects from *values* using cumulative weights.

    ``weights`` maps value (as string key) to a relative weight.  Values not
    present in *weights* receive zero weight and will never be selected.
    """
    # Normalise weights so they sum to 1.0
    total = sum(weights.values())
    if total <= 0:
        # Fall back to uniform
        return _array_index(cell_seed_col, values)

    cum = 0.0
    thresholds: list[tuple[float, object]] = []
    for v in values:
        w = weights.get(str(v), 0.0) / total
        cum += w
        thresholds.append((cum, v))

    frac = F.pmod(cell_seed_col, F.lit(_UNIFORM_PRECISION)).cast("double") / F.lit(float(_UNIFORM_PRECISION))

    return functools.reduce(
        lambda acc, threshold: F.when(frac < F.lit(threshold[0]), F.lit(threshold[1])).otherwise(acc),
        reversed(thresholds[:-1]),
        F.lit(thresholds[-1][1]),
    )


def normal_sample_expr(
    cell_seed_col: Column,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Sample N(mean, stddev) via the Box-Muller transform.

    Z = sqrt(-2 ln U1) * cos(2 pi U2), where U1, U2 are two independent
    uniform draws in (0, 1].  U1 is clamped away from 0 so log stays
    finite.  Callers that need a bounded output (RangeColumn, LogNormal)
    must clamp after the call -- Box-Muller can return past 6 sigma.
    """
    h1 = F.xxhash64(cell_seed_col, F.lit(0).cast("long"))
    h2 = F.xxhash64(cell_seed_col, F.lit(1).cast("long"))
    eps = 1.0 / _CONTINUOUS_PRECISION
    u1 = F.greatest(
        F.pmod(h1, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION)),
        F.lit(eps),
    )
    u2 = F.pmod(h2, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
    z = F.sqrt(F.lit(-2.0) * F.log(u1)) * F.cos(F.lit(2.0 * math.pi) * u2)
    return z * F.lit(stddev) + F.lit(mean)


def zipf_sample_expr(cell_seed_col: Column, n: int, exponent: float = 1.5) -> Column:
    """Approximate Zipfian sampling via inverse power-law CDF.

    index = floor(N * (1-u)^(1/(exponent-1))), clamped to [0, N-1].
    """
    if n <= 1:
        return F.lit(0)
    if exponent <= 1.0:
        # Defensive: Zipf.validate_params enforces exponent > 1, but raise
        # (not assert) so a validator bypass also fails under python -O.
        raise ValueError(f"zipf_sample_expr requires exponent > 1, got {exponent}")
    u = F.pmod(cell_seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
    u = F.greatest(u, F.lit(1e-9))
    power = 1.0 / (exponent - 1.0)
    inv = F.lit(float(n)) * F.pow(F.lit(1.0) - u, F.lit(power))
    idx = F.floor(inv).cast("long")
    return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))


def exponential_sample_expr(
    cell_seed_col: Column,
    n: int,
    rate: float = 1.0,
) -> Column:
    """Map a seed to [0, n) with exponential distribution.

    Inverse CDF x = -ln(1-u)/rate, scaled to [0, n).  Clips the right
    tail (modulo would wrap tail draws back into early bins and break
    the monotonically-decreasing shape).
    """
    if n <= 1:
        return F.lit(0)
    u = F.pmod(cell_seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
    u = F.least(u, F.lit(0.999999))  # avoid log(0)
    x = -F.log(F.lit(1.0) - u) / F.lit(rate)
    idx = F.floor(x * F.lit(float(n) / 5.0)).cast("long")
    return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))


def lognormal_sample_expr(
    cell_seed_col: Column,
    n: int,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Map a seed to [0, n) with log-normal distribution.

    Sample Z ~ N(mean, stddev), then floor(exp(Z) * scale) clamped to
    [0, n-1].  scale places the median (exp(mean)) at ~10% of n so the
    heavy right tail fits with minimal clipping.
    """
    if n <= 1:
        return F.lit(0)
    z = normal_sample_expr(cell_seed_col, mean=mean, stddev=stddev)
    x = F.exp(z)
    median = math.exp(mean)
    scale = float(n) / (median * 10.0)
    # Clamp as double before the long cast: extreme mean+stddev pushes
    # x*scale past 2**63 and trips ARITHMETIC_OVERFLOW under Spark ANSI.
    x_scaled = F.floor(x * F.lit(scale))
    clamped = F.least(F.lit(float(n - 1)), F.greatest(F.lit(0.0), x_scaled))
    return clamped.cast("long")


def apply_distribution(
    cell_seed_col: Column,
    n: int,
    distribution: Distribution | None,
) -> Column:
    """Route to the appropriate sampling function. Returns longs in [0, n)."""
    if distribution is None or isinstance(distribution, Uniform):
        return uniform_sample(cell_seed_col, n)
    if isinstance(distribution, Zipf):
        return zipf_sample_expr(cell_seed_col, n, distribution.exponent)
    if isinstance(distribution, Exponential):
        return exponential_sample_expr(cell_seed_col, n, distribution.rate)
    if isinstance(distribution, Normal):
        raw = normal_sample_expr(cell_seed_col, mean=n / 2.0, stddev=n / 6.0)
        idx = F.floor(raw).cast("long")
        return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))
    if isinstance(distribution, LogNormal):
        return lognormal_sample_expr(cell_seed_col, n, distribution.mean, distribution.stddev)
    if isinstance(distribution, WeightedValues):
        # WeightedValues needs the discrete value list and is dispatched
        # at the column level via weighted_sample_expr.  Schema validators
        # on RangeColumn/TimestampColumn/ForeignKeyRef reject this at plan
        # time; this raise is the engine-level backstop.
        raise ValueError(
            "WeightedValues distribution reached apply_distribution; only "
            "ValuesColumn dispatches WeightedValues correctly (via "
            "weighted_sample_expr with the discrete value list).  Schema "
            "validation should have caught this."
        )
    return uniform_sample(cell_seed_col, n)  # type: ignore[unreachable]


def _array_index(cell_seed_col: Column, values: list) -> Column:
    """Pick from a Python list using cell seed as index."""
    arr = F.array(*[F.lit(v) for v in values])
    idx = F.pmod(cell_seed_col, F.lit(len(values)))
    return arr[idx.cast("int")]
