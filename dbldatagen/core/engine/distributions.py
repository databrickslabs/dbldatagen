"""Distribution sampling via Spark SQL expressions.

Each function maps a per-cell seed column (int64) to a sampled index or value,
staying entirely within Spark's built-in expression API (Tier 1) so there is no
Python UDF overhead.
"""

from __future__ import annotations

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


# Precision for mapping a seed hash to [0, 1): pmod(seed, P) / P.
# 10000 gives 0.01% granularity (used for weighted-values selection,
# where the user-facing weight resolution is already coarse).
_UNIFORM_PRECISION = 10_000

# Precision for continuous distributions (normal, exponential,
# log-normal) and float range fractional mapping.  Set to 2**53 so the
# resulting double spans the full IEEE 754 mantissa -- earlier 1e6 gave
# only ~20 bits of entropy per draw, which collided visibly at
# n_rows > ~1M and compressed float range outputs onto a millionth-step
# grid.  2**53 fits in a signed int64 (2**53 < 2**63 - 1) and round-trips
# exactly through ``cast("double")``, so pmod/divide stay lossless.
_CONTINUOUS_PRECISION = 2**53


# ---------------------------------------------------------------------------
# Core sampling helpers
# ---------------------------------------------------------------------------


def uniform_sample(cell_seed_col: Column, n: int) -> Column:
    """Map a seed column to a uniform index in [0, n).

    Uses ``pmod(seed, n)`` (not ``abs(seed) % n``) because
    ``abs(Long.MIN_VALUE)`` overflows under Spark ANSI mode and Spark's
    ``%`` on a negative dividend returns a negative remainder.  Output is
    uniform on ``[0, n)`` up to modulo-bias of order ``n / 2**64``, which
    is negligible for any ``n`` in practice.
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

    # Map seed to [0, 1) range.  pmod avoids abs(Long.MIN_VALUE) overflow.
    frac = F.pmod(cell_seed_col, F.lit(_UNIFORM_PRECISION)).cast("double") / F.lit(float(_UNIFORM_PRECISION))

    expr = None
    for cum_w, val in reversed(thresholds):
        if expr is None:
            expr = F.lit(val)
        else:
            expr = F.when(frac < F.lit(cum_w), F.lit(val)).otherwise(expr)  # type: ignore[unreachable]
    return expr  # type: ignore[return-value]


# mypy incorrectly narrows expr to non-None after the first loop iteration
# and marks this else branch as unreachable — it runs on every iteration after the first


def normal_sample_expr(
    cell_seed_col: Column,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Sample N(mean, stddev) via the Box-Muller transform.

    Two independent uniform draws U1, U2 in (0, 1] yield one standard
    normal Z via:

        Z = sqrt(-2 ln U1) * cos(2 pi U2)

    Box-Muller produces an exact standard normal (vs the
    Central-Limit-Theorem approximation that summed 12 uniforms),
    using only two ``xxhash64`` calls per row instead of twelve.  At
    the 500M-3B row scale this engine targets, the Spark plan is
    much smaller (2 hashes + a sqrt/log/cos vs 12 hashes + an
    11-deep accumulator tree) and Catalyst compile time drops
    accordingly.

    U1 is clamped away from 0 so ``log(U1)`` stays finite.  Box-Muller
    can return values past 6 sigma (rare, ~1 in 5e8) -- callers that
    need a bounded range (e.g. ``RangeColumn`` with Normal) clamp
    after the call; ``LogNormal`` already clamps before its long
    cast.
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
    """Approximate Zipfian sampling via inverse power-law CDF in Spark SQL.

    Maps seed to [0, 1) then applies the inverse CDF:
        index = floor(N * (1-u)^(1/(exponent-1)))
    Clamps result to [0, N-1].

    ``exponent > 1`` is enforced by ``Zipf.validate_params``; the
    power-law CDF does not converge for ``exponent <= 1`` and any code
    path reaching here with a sub-1 exponent would be a validator
    bypass.  Raise (not assert) so the bypass fails loudly under
    ``python -O`` too, where a stripped assert would silently emit
    wrong-shaped data.
    """
    if n <= 1:
        return F.lit(0)
    if exponent <= 1.0:
        raise ValueError(f"zipf_sample_expr requires exponent > 1, got {exponent}")
    u = F.pmod(cell_seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
    # Shift u away from 0 to avoid log(0)
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

    Uses the inverse CDF: x = -ln(1-u)/rate, then scales to [0, n) and
    clips the right tail. Clipping (not modulo) preserves the
    monotonically-decreasing exponential shape — modulo would wrap
    tail draws back into early bins.
    """
    if n <= 1:
        return F.lit(0)
    u = F.pmod(cell_seed_col, F.lit(_CONTINUOUS_PRECISION)).cast("double") / F.lit(float(_CONTINUOUS_PRECISION))
    u = F.least(u, F.lit(0.999999))  # avoid log(0)
    x = -F.log(F.lit(1.0) - u) / F.lit(rate)
    # Scale so that ~99% of draws (for rate=1) land within [0, n) before clipping.
    idx = F.floor(x * F.lit(float(n) / 5.0)).cast("long")  # /5 to spread nicely
    return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))


def lognormal_sample_expr(
    cell_seed_col: Column,
    n: int,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Map a seed to [0, n) with log-normal distribution.

    Samples Z ~ N(mean, stddev), then returns ``floor(exp(Z) * scale)``
    clipped to [0, n-1]. ``scale`` places the distribution's median
    (``exp(mean)``) at roughly 10% of n so the heavy right tail fits in
    range with minimal clipping. Note: the scale is chosen from ``mean``
    only — very large ``stddev`` values (e.g. >= 3) will clip aggressively.
    """
    if n <= 1:
        return F.lit(0)
    z = normal_sample_expr(cell_seed_col, mean=mean, stddev=stddev)
    x = F.exp(z)
    median = math.exp(mean)
    scale = float(n) / (median * 10.0)
    # Clamp as double BEFORE the long cast.  For extreme ``mean`` (near
    # the validator's +/-100 bound) ``scale`` can be ~1e51; tail samples
    # of ``z`` push ``x * scale`` past ``2**63`` and the long-cast
    # raises ``ARITHMETIC_OVERFLOW`` under Spark ANSI mode, ~before~
    # the ``F.least(..., n-1)`` clamp could have saved us.  Order
    # matters: clamp on double first, THEN cast to long -- that keeps
    # the output in ``[0, n-1]`` regardless of how large ``x * scale``
    # gets in double space.
    x_scaled = F.floor(x * F.lit(scale))
    clamped = F.least(F.lit(float(n - 1)), F.greatest(F.lit(0.0), x_scaled))
    return clamped.cast("long")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def apply_distribution(
    cell_seed_col: Column,
    n: int,
    distribution: Distribution | None,
) -> Column:
    """Route to the appropriate sampling function based on distribution type.

    Returns a Column of longs in [0, n).
    """
    if distribution is None or isinstance(distribution, Uniform):
        return uniform_sample(cell_seed_col, n)
    if isinstance(distribution, Zipf):
        return zipf_sample_expr(cell_seed_col, n, distribution.exponent)
    if isinstance(distribution, Exponential):
        return exponential_sample_expr(cell_seed_col, n, distribution.rate)
    if isinstance(distribution, Normal):
        # Map normal to [0, n): centre at n/2, stddev = n/6 (covers ~99.7%)
        raw = normal_sample_expr(cell_seed_col, mean=n / 2.0, stddev=n / 6.0)
        idx = F.floor(raw).cast("long")
        return F.greatest(F.lit(0), F.least(idx, F.lit(n - 1)))
    if isinstance(distribution, LogNormal):
        return lognormal_sample_expr(cell_seed_col, n, distribution.mean, distribution.stddev)
    if isinstance(distribution, WeightedValues):
        # ``WeightedValues`` is dispatched at the column level via
        # ``weighted_sample_expr`` (see ``columns/string.py:build_values_column``)
        # because it needs the discrete value list.  Reaching this branch
        # means a ``WeightedValues`` distribution was attached to a
        # numeric / temporal / FK column where there is no value list to
        # weight -- silently substituting ``uniform_sample`` here would
        # drop the user's intent.  The schema validators on
        # ``RangeColumn`` / ``TimestampColumn`` / ``ForeignKeyRef`` reject
        # the misuse at plan time; this raise is the engine-level
        # backstop for any path that bypasses validation.
        raise ValueError(
            "WeightedValues distribution reached apply_distribution; only "
            "ValuesColumn dispatches WeightedValues correctly (via "
            "weighted_sample_expr with the discrete value list).  Schema "
            "validation should have caught this."
        )
    # Fallback for any future Distribution subclass we forgot to dispatch.
    return uniform_sample(cell_seed_col, n)  # type: ignore[unreachable]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _array_index(cell_seed_col: Column, values: list) -> Column:
    """Pick from a Python list using cell seed as index."""
    arr = F.array(*[F.lit(v) for v in values])
    idx = F.pmod(cell_seed_col, F.lit(len(values)))
    return arr[idx.cast("int")]
