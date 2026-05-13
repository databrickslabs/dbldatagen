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
    """Map a long-typed seed column to a uniform double in ``[0.0, 1.0)``.

    The repeated ``pmod(seed, P) / P`` algebra appeared at four call
    sites in this module (Box-Muller's two uniform draws, Zipf's
    inverse CDF, exponential's inverse CDF); centralising it removes
    drift risk if ``_CONTINUOUS_PRECISION`` ever changes.
    """
    return F.pmod(seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))


def uniform_sample(cell_seed_col: Column, n: int) -> Column:
    """Maps a seed column to a uniform index in ``[0, n)``.

    Uses ``F.pmod`` (not ``F.abs(x) % n``) to avoid the
    ``abs(Long.MIN_VALUE)`` overflow trap under Spark ANSI mode.

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        n: Exclusive upper bound on the output index.  Must be
          strictly positive; ``n == 1`` short-circuits to a constant
          ``0``.

    Returns:
        A Spark ``Column`` (long) holding uniform indices in
        ``[0, n)``.

    Raises:
        ValueError: ``n <= 0``.
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
    """Builds a CASE/WHEN chain that selects from *values* using cumulative weights.

    Falls back to ``_array_index`` (uniform) if all weights are zero,
    rather than emitting an all-NULL column.

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        values: The list of allowed values (``ValuesColumn.values``).
        weights: Mapping of value (rendered as ``str``) to relative
          weight.  Values absent from this mapping receive weight
          ``0`` and are never selected.

    Returns:
        A Spark ``Column`` whose runtime type matches ``values``'
        element type.
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
    """Samples ``N(mean, stddev)`` via the Box-Muller transform.

    ``Z = sqrt(-2 ln U1) * cos(2 pi U2)`` where ``U1, U2`` are two
    independent uniform draws in ``(0, 1]``.  ``U1`` is clamped away
    from ``0`` so ``log`` stays finite.  Callers that need a bounded
    output (``RangeColumn``, ``LogNormal``) must clamp after the call
    — Box-Muller can return past 6 sigma.

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        mean: Distribution mean.  Defaults to ``0.0``.
        stddev: Standard deviation.  Defaults to ``1.0``.

    Returns:
        A Spark ``Column`` (double) holding ``N(mean, stddev)``
        samples.
    """
    h1 = F.xxhash64(cell_seed_col, F.lit(0).cast("long"))
    h2 = F.xxhash64(cell_seed_col, F.lit(1).cast("long"))
    eps = 1.0 / _CONTINUOUS_PRECISION
    u1 = F.greatest(_uniform_fraction(h1), F.lit(eps))
    u2 = _uniform_fraction(h2)
    z = F.sqrt(F.lit(-2.0) * F.log(u1)) * F.cos(F.lit(2.0 * math.pi) * u2)
    return z * F.lit(stddev) + F.lit(mean)


def zipf_sample_expr(cell_seed_col: Column, n: int, exponent: float = 1.5) -> Column:
    """Approximates Zipfian sampling via inverse power-law CDF.

    ``index = floor(N * (1-u)^(1/(exponent-1)))``, clamped to
    ``[0, N-1]``.  The schema-level ``Zipf.validate_params`` enforces
    ``exponent > 1`` at plan time; this function also raises as a
    defensive backstop against validator bypass.

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        n: Exclusive upper bound on the output index.  ``n <= 1``
          short-circuits to a constant ``0``.
        exponent: Power-law exponent.  Must be strictly greater than
          ``1.0``.  Defaults to ``1.5``.

    Returns:
        A Spark ``Column`` (long) holding Zipf-distributed indices in
        ``[0, n)``.

    Raises:
        ValueError: ``exponent <= 1.0``.
    """
    if n <= 1:
        return F.lit(0)
    if exponent <= 1.0:
        # Defensive: Zipf.validate_params enforces exponent > 1, but raise
        # (not assert) so a validator bypass also fails under python -O.
        raise ValueError(f"zipf_sample_expr requires exponent > 1, got {exponent}")
    u = F.greatest(_uniform_fraction(cell_seed_col), F.lit(1e-9))
    power = 1.0 / (exponent - 1.0)
    inv = F.lit(float(n)) * F.pow(F.lit(1.0) - u, F.lit(power))
    idx = F.floor(inv).cast("long")
    return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))


def exponential_sample_expr(
    cell_seed_col: Column,
    n: int,
    rate: float = 1.0,
) -> Column:
    """Maps a seed to ``[0, n)`` with exponential distribution.

    Inverse CDF ``x = -ln(1 - u) / rate``, scaled to ``[0, n)``.
    Clips the right tail (modulo would wrap tail draws back into
    early bins and break the monotonically-decreasing shape).

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        n: Exclusive upper bound on the output index.  ``n <= 1``
          short-circuits to a constant ``0``.
        rate: Rate parameter (``lambda``); must be positive.
          Defaults to ``1.0``.

    Returns:
        A Spark ``Column`` (long) holding exponentially-distributed
        indices in ``[0, n)``.
    """
    if n <= 1:
        return F.lit(0)
    u = F.least(_uniform_fraction(cell_seed_col), F.lit(0.999999))  # avoid log(0)
    x = -F.log(F.lit(1.0) - u) / F.lit(rate)
    idx = F.floor(x * F.lit(float(n) / 5.0)).cast("long")
    return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))


def lognormal_sample_expr(
    cell_seed_col: Column,
    n: int,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Maps a seed to ``[0, n)`` with log-normal distribution.

    Sample ``Z ~ N(mean, stddev)``, then
    ``floor(exp(Z) * scale)`` clamped to ``[0, n-1]``.  ``scale``
    places the median ``exp(mean)`` at ~10% of ``n`` so the heavy
    right tail fits with minimal clipping.

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        n: Exclusive upper bound on the output index.  ``n <= 1``
          short-circuits to a constant ``0``.
        mean: Mean of the underlying normal distribution.  Defaults
          to ``0.0``.  ``LogNormal.validate_params`` enforces
          ``|mean| <= 100`` at plan time.
        stddev: Standard deviation of the underlying normal
          distribution.  Defaults to ``1.0``.

    Returns:
        A Spark ``Column`` (long) holding log-normal-distributed
        indices in ``[0, n)``.
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
    """Routes to the appropriate sampling function for a ``Distribution``.

    The single dispatch point for every ``RangeColumn`` / FK / range
    sampling site.  ``WeightedValues`` is rejected here because it
    needs the discrete value list (dispatched directly via
    ``weighted_sample_expr``); the schema validators on the strategies
    that don't support ``WeightedValues`` catch this at plan time, so
    this raise is the engine-level backstop.

    Args:
        cell_seed_col: Per-cell seed ``Column`` (long).
        n: Exclusive upper bound on the output index.
        distribution: The ``Distribution`` instance to sample under.
          ``None`` falls back to ``Uniform``.

    Returns:
        A Spark ``Column`` (long) holding sampled indices in
        ``[0, n)``.

    Raises:
        ValueError: ``distribution`` is ``WeightedValues`` (which is
          handled out-of-band by ``weighted_sample_expr``).
        TypeError: ``distribution`` is a ``Distribution`` subclass
          that this dispatcher does not handle (forgot an
          ``isinstance`` branch when adding a new subclass).
    """
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
    raise TypeError(
        f"apply_distribution: unhandled Distribution subclass "
        f"{type(distribution).__name__}.  Add an isinstance branch above."
    )


def _array_index(cell_seed_col: Column, values: list) -> Column:
    """Pick from a Python list using cell seed as index."""
    arr = F.array(*[F.lit(v) for v in values])
    idx = F.pmod(cell_seed_col, F.lit(len(values)))
    return arr[idx.cast("int")]
