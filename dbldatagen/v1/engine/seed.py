"""Deterministic seed derivation for partition-independent generation.

All randomness in dbldatagen.v1 derives from a global seed, a table name, a column
name, and a row index.  The functions in this module handle the two-phase
derivation described in the architecture doc:

    Phase 1 (planning):  derive_column_seed(global_seed, table, column) -> int
    Phase 2 (execution):  cell_seed_expr(column_seed, id_col) -> Spark Column
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def derive_column_seed(global_seed: int, table_name: str, column_name: str) -> int:
    """Derive a per-column seed from global seed + table + column identity.

    Pure Python, called once per column during planning.  The result is a signed
    64-bit integer (Java long compatible) that is deterministic for a given
    (global_seed, table, column) triple.
    """
    h = global_seed & 0xFFFFFFFFFFFFFFFF
    for c in table_name:
        h = ((h * 31) + ord(c)) & 0xFFFFFFFFFFFFFFFF
    for c in column_name:
        h = ((h * 37) + ord(c)) & 0xFFFFFFFFFFFFFFFF
    return _to_signed64(h)


def cell_seed_expr(column_seed: int, id_col: Column | str = "id") -> Column:
    """Return a Spark expression that computes a per-cell seed.

    ``cell_seed = xxhash64(column_seed, id)``

    This is partition-independent because *id* comes from ``spark.range(n)``.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    return F.xxhash64(F.lit(column_seed).cast("long"), id_col)


def null_mask_expr(
    column_seed: int,
    id_col: Column | str,
    null_fraction: float,
) -> Column:
    """Return a boolean Column that is ``True`` for rows that should be NULL.

    Uses a separate seed (XOR with constant) to avoid correlation with the
    value seed for the same column.
    """
    if null_fraction <= 0.0:
        return F.lit(False)
    if null_fraction >= 1.0:
        return F.lit(True)
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    null_seed = column_seed ^ 0xDEADBEEF
    null_hash = F.xxhash64(F.lit(null_seed).cast("long"), id_col)
    threshold = int(null_fraction * 10000)
    return (F.abs(null_hash) % F.lit(10000)) < F.lit(threshold)


def mix64(key: int, index: int) -> int:
    """Fast 64-bit mixing function (splitmix64-style).

    For Python-side use only (e.g. generating Feistel round keys).
    Bijective for a fixed *key*.  Returns a signed 64-bit integer.
    """
    x = (key ^ index) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    x = (x ^ (x >> 31)) & 0xFFFFFFFFFFFFFFFF
    return _to_signed64(x)


def compute_batch_seed(global_seed: int, batch_id: int) -> int:
    """Compute the seed for a given batch.

    Batch 0 uses global_seed directly.
    Batch n uses global_seed + n * 10000 (wrapped to signed 64-bit).
    """
    if batch_id == 0:
        return global_seed
    raw = global_seed + batch_id * 10000
    return _to_signed64(raw)


def _to_signed64(n: int) -> int:
    """Convert an unsigned 64-bit int to a signed 64-bit int (Java long range)."""
    n = n & 0xFFFFFFFFFFFFFFFF
    if n >= 0x8000000000000000:
        return n - 0x10000000000000000
    return n
