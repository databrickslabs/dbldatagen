"""Deterministic UUID generation from double xxhash64 (Tier 1 Spark SQL).

Each row gets a unique, deterministic UUID-formatted string derived from two
independent 64-bit hashes of the cell seed and row id.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def build_uuid_column(id_col: Column | str, column_seed: int) -> Column:
    """Generate a deterministic UUID-formatted string.

    Two independent xxhash64 values are combined to produce 128 bits of
    pseudorandom data, formatted as a standard UUID:
    ``xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx``
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    hi = F.xxhash64(F.lit(column_seed).cast("long"), id_col)
    lo = F.xxhash64(F.lit(column_seed + 1).cast("long"), id_col)

    # Convert each 64-bit hash to a 16-char lowercase hex string
    hi_hex = F.lower(F.lpad(F.hex(F.abs(hi)), 16, "0"))
    lo_hex = F.lower(F.lpad(F.hex(F.abs(lo)), 16, "0"))

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
