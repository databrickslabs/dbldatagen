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

    hi = F.xxhash64(seed_col, id_col)
    lo = F.xxhash64(seed_col_plus1, id_col)

    hi_hex = F.lower(F.lpad(F.hex(hi), 16, "0"))
    lo_hex = F.lower(F.lpad(F.hex(lo), 16, "0"))

    # Format as UUID: 8-4-4-4-12
    return F.concat(
        F.substring(hi_hex, 1, 8),
        F.lit("-"),
        F.substring(hi_hex, 9, 4),
        F.lit("-"),
        F.substring(hi_hex, 13, 4),
        F.lit("-"),
        F.substring(lo_hex, 1, 4),
        F.lit("-"),
        F.substring(lo_hex, 5, 12),
    )
