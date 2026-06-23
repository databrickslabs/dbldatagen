"""Deterministic UUID generation.

Each row gets a UUID-formatted string derived from two independent 64-bit hashes
of the column seed and row id. Output is reproducible for a given seed and row.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.seed import to_signed64


def build_uuid_column(id_column: Column | str, column_seed: int) -> Column:
    """Builds a deterministic UUID-formatted string column.

    Combines two independent 64-bit hashes into 128 bits and formats them as
    `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. The same seed and row id always
    produce the same value.

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        column_seed: Per-column seed.

    Returns:
        A Spark string `Column` of UUID-formatted values.

    Note:
        The output is UUID-shaped but not an RFC 4122 UUID. It carries no
        version or variant bits. Parsers that validate for those bits will
        reject it.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)

    # Clamp seed and seed+1 into signed-64 range; at Long.MAX_VALUE the
    # naive "+ 1" overflows (F.lit rejects Python int outside int64).
    seed_col = F.lit(to_signed64(column_seed)).cast("long")
    seed_col_plus1 = F.lit(to_signed64(column_seed + 1)).cast("long")

    # Two independent 64-bit hashes give the 128 bits of UUID payload.
    high_bits = F.xxhash64(seed_col, id_column)
    low_bits = F.xxhash64(seed_col_plus1, id_column)

    # Slice the 8-4-4-4-12 UUID layout out of (high_bits, low_bits) via bit shifts.
    # PySpark ``Column & int`` is *logical* AND, so we must use ``.bitwiseAND(...)``.
    # ``F.shiftright`` is arithmetic (sign-extending) on BIGINT, so the masks are
    # load-bearing: they zero the sign-extended bits, making each chunk non-negative
    # so ``%0Nx`` produces exactly N hex chars regardless of input sign.
    return F.format_string(
        "%08x-%04x-%04x-%04x-%012x",
        F.shiftright(high_bits, 32).bitwiseAND(0xFFFFFFFF),  # bits [63:32] -> 8 chars
        F.shiftright(high_bits, 16).bitwiseAND(0xFFFF),  # bits [31:16] -> 4 chars
        high_bits.bitwiseAND(0xFFFF),  # bits [15:0] -> 4 chars
        F.shiftright(low_bits, 48).bitwiseAND(0xFFFF),  # bits [63:48] -> 4 chars
        low_bits.bitwiseAND(0xFFFFFFFFFFFF),  # bits [47:0] -> 12 chars
    )
