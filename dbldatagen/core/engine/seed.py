"""Deterministic seed derivation for partition-independent generation.

All randomness derives from a global seed, the table name, the column name, and
the row index. Seeds are derived in two phases: `derive_column_seed` computes a
per-column seed at planning time, and `cell_seed_expr` turns it into a per-row
seed at execution time.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


# XOR constant to decorrelate the null mask seed from the value seed.
_NULL_SEED_XOR = 0xDEADBEEF

# Golden ratio hash constant (2^32 / phi) for mixing element/segment seeds.
GOLDEN_RATIO_HASH = 0x9E3779B9

# 2**53 is the largest integer exactly representable as a float64, so a hash
# reduced into this range converts to a double without precision loss.
_UNIFORM_PRECISION = 2**53


def derive_column_seed(global_seed: int, table_name: str, column_name: str) -> int:
    """Derives a per-column seed from the global seed, table name, and column name.

    The same inputs always produce the same seed.

    Args:
        global_seed: The plan-level seed.
        table_name: Name of the table the column belongs to. Pass an empty
            string when chaining seeds for nested struct fields.
        column_name: Name of the column or struct field.

    Returns:
        A signed 64-bit integer seed.
    """
    seed_hash = global_seed & 0xFFFFFFFFFFFFFFFF
    for char in table_name:
        seed_hash = ((seed_hash * 31) + ord(char)) & 0xFFFFFFFFFFFFFFFF
    for char in column_name:
        seed_hash = ((seed_hash * 37) + ord(char)) & 0xFFFFFFFFFFFFFFFF
    return to_signed64(seed_hash)


def cell_seed_expr(column_seed: int, id_column: Column | str = "id") -> Column:
    """Builds a Spark expression for a per-row seed.

    Combines the column seed with the row ID. The result is independent of how
    the data is partitioned.

    Args:
        column_seed: Per-column seed.
        id_column: Optional row-id column, given as a `Column` or column name
            (default "id").

    Returns:
        A Spark `Column` holding the per-row seed.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)
    return F.xxhash64(F.lit(column_seed).cast("long"), id_column)


def uniform_fraction(seed_column: Column) -> Column:
    """Maps a hash column to a uniform double in [0.0, 1.0).

    Args:
        seed_column: A long-typed seed or hash column.

    Returns:
        A Spark double `Column` uniformly distributed in [0.0, 1.0).
    """
    return F.pmod(seed_column, F.lit(_UNIFORM_PRECISION)).cast("double") / F.lit(float(_UNIFORM_PRECISION))


def null_mask_expr(
    column_seed: int,
    id_column: Column | str,
    null_fraction: float,
) -> Column:
    """Builds a boolean column that is True for rows that should be NULL.

    Uses a seed decorrelated from the column's value seed, so null placement is
    independent of the generated values. A row is marked NULL when its uniform
    draw in [0.0, 1.0) falls below `null_fraction`.

    Args:
        column_seed: Per-column seed.
        id_column: Row-id column, given as a `Column` or column name.
        null_fraction: Fraction of rows to mark NULL, in [0.0, 1.0]. 0.0 yields
            all False; values >= 1.0 yield all True.

    Returns:
        A Spark boolean `Column`, True for rows to emit as NULL.
    """
    if null_fraction <= 0.0:
        return F.lit(False)
    if null_fraction >= 1.0:
        return F.lit(True)
    if isinstance(id_column, str):
        id_column = F.col(id_column)
    null_seed = seed_xor(column_seed, _NULL_SEED_XOR)
    null_hash = cell_seed_expr(null_seed, id_column)
    return uniform_fraction(null_hash) < F.lit(null_fraction)


def to_signed64(n: int) -> int:
    """Converts a Python int to the signed 64-bit range used by Spark.

    Out-of-range values wrap using two's-complement, matching the Java long
    semantics Spark relies on.

    Args:
        n: Any Python integer.

    Returns:
        `n` mapped into [-2**63, 2**63 - 1].
    """
    return ((n + 0x8000000000000000) % 0x10000000000000000) - 0x8000000000000000


def seed_xor(column_seed: int, constant: int) -> int:
    """Derives a decorrelated sub-seed by XORing a column seed with a constant.

    Used to draw several independent random values from the same base seed.
    Clamps the value to Spark's signed 64-bit range.

    Args:
        column_seed: Per-column seed.
        constant: A distinct constant per sub-draw.

    Returns:
        The XOR of the inputs, mapped into the signed 64-bit range.
    """
    return to_signed64(column_seed ^ constant)
