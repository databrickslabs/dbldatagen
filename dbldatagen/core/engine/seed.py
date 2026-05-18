"""Deterministic seed derivation for partition-independent generation.

All randomness in dbldatagen.core derives from a global seed, a table name, a column
name, and a row index.  The functions in this module handle the two-phase
derivation described in the architecture doc:

    Phase 1 (planning):  derive_column_seed(global_seed, table, column) -> int
    Phase 2 (execution):  cell_seed_expr(column_seed, id_col) -> Spark Column
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


# Precision for null fraction mapping: null_fraction is quantized to
# 1/_NULL_PRECISION granularity (0.01% with default 10000).
_NULL_PRECISION = 10000

# XOR constant to decorrelate the null mask seed from the value seed.
_NULL_SEED_XOR = 0xDEADBEEF

# Golden ratio hash constant (2^32 / phi) for mixing element/segment seeds.
GOLDEN_RATIO_HASH = 0x9E3779B9


def derive_column_seed(global_seed: int, table_name: str, column_name: str) -> int:
    """Derives a per-column seed from global seed + table + column identity.

    Pure Python, called once per column during planning.  Same triple
    always returns the same seed, so re-running ``resolve_plan`` on the
    same ``DataGenPlan`` produces byte-identical downstream data.

    Args:
        global_seed: The plan-level seed (typically
          ``DataGenPlan.seed`` after propagation).
        table_name: Name of the table the column belongs to.
        column_name: Name of the column.  When chaining seeds for
          nested struct fields, callers pass the field name and an
          empty ``table_name`` for the second-and-onwards hop.

    Returns:
        A signed 64-bit int (Java long compatible) deterministic for
        the given ``(global_seed, table_name, column_name)`` triple.
    """
    h = global_seed & 0xFFFFFFFFFFFFFFFF
    for c in table_name:
        h = ((h * 31) + ord(c)) & 0xFFFFFFFFFFFFFFFF
    for c in column_name:
        h = ((h * 37) + ord(c)) & 0xFFFFFFFFFFFFFFFF
    return to_signed64(h)


def cell_seed_expr(column_seed: int, id_col: Column | str = "id") -> Column:
    """Returns a Spark expression that computes a per-cell seed.

    ``cell_seed = xxhash64(column_seed, id)``

    Partition-independent because ``id`` comes from ``spark.range(n)``.

    Args:
        column_seed: Per-column seed (planning-time constant).
        id_col: Row-id column.  Either a Spark ``Column`` reference
          or the name of the column (defaults to ``"id"``).

    Returns:
        A Spark ``Column`` (long) holding the per-cell seed for every
        row in the input ``DataFrame``.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    return F.xxhash64(F.lit(column_seed).cast("long"), id_col)


def null_mask_expr(
    column_seed: int,
    id_col: Column | str,
    null_fraction: float,
) -> Column:
    """Returns a boolean Column that is ``True`` for rows that should be NULL.

    Uses a separate seed (XOR with constant) to avoid correlation with the
    value seed for the same column.

    Args:
        column_seed: Per-column seed.
        id_col: Row-id column.  Either a Spark ``Column`` reference
          or the name of the column.
        null_fraction: Target fraction of rows to mark NULL, in
          ``[0.0, 1.0]``.  ``0.0`` returns a ``False`` literal,
          ``>= 1.0`` returns a ``True`` literal; in-between values
          are quantised to ``1 / _NULL_PRECISION`` granularity.

    Returns:
        A Spark boolean ``Column``: ``True`` for rows the caller
        should emit as NULL, ``False`` otherwise.  Cheap to combine
        with ``F.when(mask, F.lit(None)).otherwise(value_expr)``.

    Raises:
        ValueError: ``null_fraction`` is strictly between ``0.0`` and
          ``1.0`` but smaller than ``1 / _NULL_PRECISION``.  Raises
          rather than silently emitting zero NULLs.
    """
    if null_fraction <= 0.0:
        return F.lit(False)
    if null_fraction >= 1.0:
        return F.lit(True)
    threshold = int(null_fraction * _NULL_PRECISION)
    if threshold == 0:
        # Below the engine's 1/_NULL_PRECISION granularity: ``int(f * N)``
        # would round to 0 and the mask would silently emit zero NULLs
        # for any non-zero fraction.  Raise so a user asking for a 1e-5
        # null rate sees why it didn't materialise instead of debugging
        # "why are there no nulls" at query time.
        raise ValueError(
            f"null_fraction={null_fraction} is below the engine's "
            f"1/{_NULL_PRECISION} = {1.0 / _NULL_PRECISION} granularity.  "
            f"Pick a larger fraction or raise _NULL_PRECISION in seed.py."
        )
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    null_seed = column_seed ^ _NULL_SEED_XOR
    null_hash = cell_seed_expr(null_seed, id_col)
    return F.pmod(null_hash, F.lit(_NULL_PRECISION)) < F.lit(threshold)


def to_signed64(n: int) -> int:
    """Converts an arbitrary Python int to a signed 64-bit int (Java long range).

    Bit-truncates to 64 bits and re-interprets as two's-complement
    signed, so values outside the int64 range wrap consistently with
    the JVM long semantics that Spark uses.

    Args:
        n: Any Python int (may be negative, may exceed 64 bits).

    Returns:
        ``n`` mapped into ``[-2**63, 2**63 - 1]``.
    """
    # Offset-mod-offset two's-complement reinterpretation: shift up by
    # 2**63 so signed range [-2**63, 2**63-1] becomes unsigned [0, 2**64-1],
    # apply % 2**64 to fold any out-of-range input back, then shift down.
    return ((n + 0x8000000000000000) % 0x10000000000000000) - 0x8000000000000000
