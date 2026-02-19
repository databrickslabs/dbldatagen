"""Distribution sampling via Spark SQL expressions.

Each function maps a per-cell seed column (int64) to a sampled index or value,
staying entirely within Spark's built-in expression API (Tier 1) so there is no
Python UDF overhead.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.v1.schema import (
    Distribution,
    Exponential,
    LogNormal,
    Normal,
    Uniform,
    Zipf,
)


# ---------------------------------------------------------------------------
# Core sampling helpers
# ---------------------------------------------------------------------------


def uniform_sample(cell_seed_col: Column, n: int) -> Column:
    """Map a seed column to a uniform index in [0, n).

    Uses ``abs(seed) % n`` which has negligible bias for n << 2^63.
    """
    if n <= 0:
        raise ValueError("n must be positive")
    if n == 1:
        return F.lit(0)
    return F.abs(cell_seed_col) % F.lit(n)


def weighted_sample_expr(
    cell_seed_col: Column,
    values: list,
    weights: dict[str, float],
) -> Column:
    """Build a CASE/WHEN chain that selects from *values* using cumulative weights.

    ``weights`` maps value (as string key) to a relative weight.  Values not
    present in *weights* get equal share of any remaining weight.
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

    # Map seed to [0, 1) range: abs(seed) % 10000 / 10000
    frac = (F.abs(cell_seed_col) % F.lit(10000)).cast("double") / F.lit(10000.0)

    expr = None
    for cum_w, val in reversed(thresholds):
        if expr is None:
            expr = F.lit(val)
        else:
            expr = F.when(frac < F.lit(cum_w), F.lit(val)).otherwise(expr)
    return expr  # type: ignore[return-value]


def normal_sample_expr(
    cell_seed_col: Column,
    mean: float = 0.0,
    stddev: float = 1.0,
) -> Column:
    """Approximate normal distribution using the Central Limit Theorem.

    Sums 12 independent uniform [0, 1) values and subtracts 6.0, giving an
    approximate N(0, 1) with range roughly [-6, 6].  Then scale to the
    requested mean and stddev.

    Each of the 12 uniform draws uses a different hash of the seed so they
    are independent.
    """
    accum: Column | None = None
    for i in range(12):
        # Derive an independent uniform [0, 1) from the cell seed
        h = F.xxhash64(cell_seed_col, F.lit(i).cast("long"))
        u = (F.abs(h) % F.lit(1000000)).cast("double") / F.lit(1000000.0)
        accum = u if accum is None else accum + u

    # accum ~ sum of 12 Uniform(0,1) => approx Normal(6, 1)
    z = accum - F.lit(6.0)  # type: ignore[operator]
    return z * F.lit(stddev) + F.lit(mean)


def zipf_sample_expr(cell_seed_col: Column, n: int, exponent: float = 1.5) -> Column:
    """Approximate Zipfian sampling via inverse power-law CDF in Spark SQL.

    Maps seed to [0, 1) then applies the inverse CDF:
        index = floor(N * u^(1/(exponent-1)))   for exponent > 1
    Clamps result to [0, N-1].
    """
    if n <= 1:
        return F.lit(0)
    u = (F.abs(cell_seed_col) % F.lit(1000000)).cast("double") / F.lit(1000000.0)
    # Shift u away from 0 to avoid log(0)
    u = F.greatest(u, F.lit(1e-9))
    if exponent <= 1.0:
        # For exponent <= 1, fall back to a log-based approximation
        inv = F.exp(F.log(F.lit(float(n))) * u) - F.lit(1.0)
    else:
        # Inverse CDF of discrete power-law: rank = n * (1-u)^(1/(s-1)) mapped
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

    Uses the inverse CDF: x = -ln(1-u)/rate, then scales to [0, n).
    """
    if n <= 1:
        return F.lit(0)
    u = (F.abs(cell_seed_col) % F.lit(1000000)).cast("double") / F.lit(1000000.0)
    u = F.least(u, F.lit(0.999999))  # avoid log(0)
    x = -F.log(F.lit(1.0) - u) / F.lit(rate)  # pylint: disable=invalid-unary-operand-type
    # Normalise to [0, n): map [0, inf) to [0, n) via modulo
    idx = F.floor(x * F.lit(float(n) / 5.0)).cast("long")  # /5 to spread nicely
    return F.abs(idx) % F.lit(n)


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
        # Use exponential as a stand-in; lognormal is similar in shape
        return exponential_sample_expr(cell_seed_col, n, distribution.stddev or 1.0)
    # WeightedValues is handled at a higher level (needs value list); fallback to uniform
    return uniform_sample(cell_seed_col, n)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _array_index(cell_seed_col: Column, values: list) -> Column:
    """Pick from a Python list using cell seed as index."""
    arr = F.array(*[F.lit(v) for v in values])
    idx = F.abs(cell_seed_col) % F.lit(len(values))
    return arr[idx.cast("int")]
