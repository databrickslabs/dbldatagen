"""Deterministic UUID generation from double xxhash64 (Tier 1 Spark SQL).

Each row gets a unique, deterministic UUID-formatted string derived from two
independent 64-bit hashes of the cell seed and row id.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def build_uuid_column(id_col: Column | str, column_seed: int | Column) -> Column:
    """Generate a deterministic UUID-formatted string.

    Two independent xxhash64 values are combined to produce 128 bits of
    pseudorandom data, formatted as a standard UUID:
    ``xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx``

    PERFORMANCE NOTE: The ``int | Column`` branching for ``column_seed`` is
    required for fused multi-batch CDC where the seed varies per row via
    map-based lookup (see ``column_seed_map`` in seed.py).  Do not simplify
    to int-only.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    if isinstance(column_seed, Column):
        seed_col = column_seed
        seed_col_plus1 = column_seed + F.lit(1).cast("long")
    else:
        seed_col = F.lit(column_seed).cast("long")
        seed_col_plus1 = F.lit(column_seed + 1).cast("long")

    hi = F.xxhash64(seed_col, id_col)
    lo = F.xxhash64(seed_col_plus1, id_col)

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
