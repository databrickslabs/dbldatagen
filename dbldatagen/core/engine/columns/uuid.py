"""Deterministic UUID generation from double xxhash64 (Tier 1 Spark SQL).

Each row gets a unique, deterministic UUID-formatted string derived from two
independent 64-bit hashes of the cell seed and row id.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.seed import to_signed64


def build_uuid_column(id_col: Column | str, column_seed: int) -> Column:
    """Generates a deterministic UUID-formatted string.

    Two independent ``xxhash64`` values are combined to produce 128
    bits of pseudorandom data, formatted as a standard UUID:
    ``xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx``.  Output is reproducible
    for any given ``(column_seed, id)`` pair.

    Args:
        id_col: Row-id ``Column`` reference or column name.
        column_seed: Per-column seed (planning-time constant).

    Returns:
        A Spark ``Column`` (string) holding the UUID-formatted
        values.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    # Clamp seed and seed+1 into signed-64 range; at Long.MAX_VALUE the
    # naive "+ 1" overflows (F.lit rejects Python int outside int64).
    seed_col = F.lit(to_signed64(column_seed)).cast("long")
    seed_col_plus1 = F.lit(to_signed64(column_seed + 1)).cast("long")

    # Two independent 64-bit hashes give the 128 bits of UUID payload.
    hi = F.xxhash64(seed_col, id_col)
    lo = F.xxhash64(seed_col_plus1, id_col)

    # Slice the 8-4-4-4-12 UUID layout out of (hi, lo) via bit shifts.
    # Note: PySpark ``Column & int`` is *logical* AND, not bitwise --
    # we must use ``.bitwiseAND(...)``.  ``F.shiftright`` is arithmetic
    # (sign-extending) on BIGINT, so the masks are also load-bearing:
    # they zero the sign-extended bits, making each chunk non-negative
    # so ``%0Nx`` produces exactly N hex chars regardless of input sign.
    return F.format_string(
        "%08x-%04x-%04x-%04x-%012x",
        F.shiftright(hi, 32).bitwiseAND(0xFFFFFFFF),  # bits [63:32] of hi -> 8 chars
        F.shiftright(hi, 16).bitwiseAND(0xFFFF),      # bits [31:16] of hi -> 4 chars
        hi.bitwiseAND(0xFFFF),                        # bits [15:0]  of hi -> 4 chars
        F.shiftright(lo, 48).bitwiseAND(0xFFFF),      # bits [63:48] of lo -> 4 chars
        lo.bitwiseAND(0xFFFFFFFFFFFF),                # bits [47:0]  of lo -> 12 chars
    )
