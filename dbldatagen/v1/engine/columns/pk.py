"""Primary key generation: sequential, formatted, and random-unique (Feistel).

Sequential and formatted PKs are Tier 1 (pure Spark SQL).  Random-unique PKs
use a NumPy-vectorized Feistel cipher inside a ``pandas_udf`` (Tier 2).
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.v1.engine.seed import mix64


# ---------------------------------------------------------------------------
# Tier 1: Sequential PK
# ---------------------------------------------------------------------------


def build_sequential_pk(id_col: Column | str, start: int = 1, step: int = 1) -> Column:
    """Sequential PK: ``start + id * step``.

    Zero overhead -- a simple arithmetic expression on the row id.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    return (id_col * F.lit(step) + F.lit(start)).cast("long")


# ---------------------------------------------------------------------------
# Tier 1: Formatted string PK
# ---------------------------------------------------------------------------


def build_formatted_pk(id_col: Column | str, template: str) -> Column:
    """Formatted string PK like ``'CUST-{digit:8}'`` -> ``'CUST-00000001'``.

    The ``{digit:N}`` placeholder is replaced with the zero-padded row id.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    import re

    m = re.search(r"\{digit:(\d+)\}", template)
    if m:
        width = int(m.group(1))
        prefix = template[: m.start()]
        suffix = template[m.end() :]
        padded_id = F.lpad((id_col + F.lit(1)).cast("string"), width, "0")
        parts = []
        if prefix:
            parts.append(F.lit(prefix))
        parts.append(padded_id)
        if suffix:
            parts.append(F.lit(suffix))
        return F.concat(*parts) if len(parts) > 1 else parts[0]

    # No placeholder -- just append the id
    return F.concat(F.lit(template), (id_col + F.lit(1)).cast("string"))


# ---------------------------------------------------------------------------
# Tier 2: Feistel cipher for random-unique PKs
# ---------------------------------------------------------------------------

_MIX_C1 = np.int64(-0x40A7B4924E0B5A67)  # 0xBF58476D1CE4E5B9 as signed
_MIX_C2 = np.int64(-0x6B2FB644ECCEEE15)  # 0x94D049BB133111EB as signed


def _mix64_vec(values: np.ndarray, key: np.int64) -> np.ndarray:
    """Vectorised splitmix64 mixing (NumPy, no Python row loops).

    Uses numpy int64 overflow (wrapping) which is the desired behaviour for
    hash mixing.
    """
    x = values.astype(np.int64) ^ key
    x = (x ^ (x >> np.int64(30))) * _MIX_C1
    x = (x ^ (x >> np.int64(27))) * _MIX_C2
    x = x ^ (x >> np.int64(31))
    return x


def feistel_permute_batch(
    indices: np.ndarray,
    N: int,
    seed: int,
    rounds: int = 6,
) -> np.ndarray:
    """NumPy-vectorised Feistel cipher permutation on [0, N).

    Returns a permutation: every input in [0, N) maps to a unique output in
    [0, N).  Uses cycle-walking to handle non-power-of-2 domain sizes.

    All operations are NumPy-vectorised -- no Python loops over rows.
    """
    if N <= 1:
        return np.zeros_like(indices, dtype=np.int64)

    # Find smallest even-bit-width power of 2 >= N
    num_bits = (N - 1).bit_length()
    if num_bits % 2 == 1:
        num_bits += 1
    half = num_bits // 2
    mask = np.int64((1 << half) - 1)

    # Precompute round keys
    keys = [np.int64(mix64(seed, r)) for r in range(rounds)]

    result = _feistel_round(indices.astype(np.int64), half, mask, keys, rounds)

    # Cycle-walk: for non-power-of-2 domains, some outputs land outside [0, N).
    # Re-apply the permutation only to those elements until all are in range.
    max_iterations = 64  # safety bound (expected < 2 iterations on average)
    for _ in range(max_iterations):
        out_of_range = result >= N
        if not out_of_range.any():
            break
        result[out_of_range] = _feistel_round(result[out_of_range], half, mask, keys, rounds)

    return result


def _feistel_round(
    values: np.ndarray,
    half: int,
    mask: np.int64,
    keys: list[np.int64],
    rounds: int,
) -> np.ndarray:
    """Apply one full Feistel network pass to an array of values."""
    left = (values >> np.int64(half)) & mask
    right = values & mask
    for r in range(rounds):
        new_left = right
        new_right = left ^ (_mix64_vec(right, keys[r]) & mask)
        left, right = new_left, new_right  # type: ignore[assignment,unused-ignore]
    return (left << np.int64(half)) | right


def build_random_unique_pk_udf(N: int, seed: int) -> Callable[..., Column]:
    """Return a ``pandas_udf`` column expression that applies Feistel permutation.

    Usage::

        pk_expr = build_random_unique_pk_udf(table_rows, pk_seed)
        df = df.select(pk_expr(F.col("id")).alias("pk"))
    """
    import pandas as pd

    @F.pandas_udf(T.LongType())  # type: ignore[call-overload]
    def _feistel_pk(id_series: pd.Series) -> pd.Series:
        ids = id_series.values.astype(np.int64)
        permuted = feistel_permute_batch(ids, N, seed)
        return pd.Series(permuted, dtype="int64")

    return _feistel_pk  # type: ignore[no-any-return]
