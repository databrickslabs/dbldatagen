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


def cell_seed_expr(column_seed: int | Column, id_col: Column | str = "id") -> Column:
    """Return a Spark expression that computes a per-cell seed.

    ``cell_seed = xxhash64(column_seed, id)``

    This is partition-independent because *id* comes from ``spark.range(n)``.

    PERFORMANCE NOTE — ``int | Column`` signature:
        *column_seed* may be a scalar ``int`` (single-batch generation) or a
        ``Column`` (fused multi-batch CDC via map-based seed lookup).  The
        Column path eliminates O(N_batches x N_columns) CASE WHEN branches in
        the Spark plan, which otherwise causes Catalyst compilation to stall
        for minutes while the cluster sits idle.  Verified at 500M-3B row
        scale (365 batches x 10+ columns).  Do not refactor this back to
        int-only -- it is intentional.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    seed_col = column_seed if isinstance(column_seed, Column) else F.lit(column_seed).cast("long")
    return F.xxhash64(seed_col, id_col)


def null_mask_expr(
    column_seed: int | Column,
    id_col: Column | str,
    null_fraction: float,
) -> Column:
    """Return a boolean Column that is ``True`` for rows that should be NULL.

    Uses a separate seed (XOR with constant) to avoid correlation with the
    value seed for the same column.

    PERFORMANCE NOTE: Accepts ``int | Column`` for the same reason as
    ``cell_seed_expr`` — see that function's docstring.  The ``bitwiseXOR``
    path for Column seeds is required for fused multi-batch CDC.
    """
    if null_fraction <= 0.0:
        return F.lit(False)
    if null_fraction >= 1.0:
        return F.lit(True)
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    null_seed: int | Column
    if isinstance(column_seed, Column):
        null_seed = column_seed.bitwiseXOR(F.lit(0xDEADBEEF).cast("long"))
    else:
        null_seed = column_seed ^ 0xDEADBEEF
    null_hash = cell_seed_expr(null_seed, id_col)
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


# ---------------------------------------------------------------------------
# Map-based seed lookup (for fused multi-batch CDC)
# ---------------------------------------------------------------------------


def column_seed_map(
    global_seed: int,
    unique_wbs: list[int],
    table_name: str,
    column_name: str,
) -> Column:
    """Build a Spark map literal that maps write_batch → column_seed.

    Precomputes ``derive_column_seed(compute_batch_seed(global_seed, wb), ...)``
    for each write-batch value, then encodes the mapping as a single Spark
    ``MapType`` literal.  At execution time, ``element_at(map, _write_batch)``
    gives O(1) seed lookup per row — one expression node instead of N CASE
    WHEN branches.

    PERFORMANCE NOTE -- do not replace with CASE WHEN or dynamic arithmetic:
        This map-literal approach is the result of two failed alternatives:
        1. CASE WHEN per column: O(N_batches x N_columns) plan nodes.  With
           365 batches x 10 columns = 3,650+ branches, Catalyst spends
           minutes compiling while the cluster idles at ~10% CPU.
        2. Dynamic Spark SQL arithmetic (multiply-accumulate hash):
           ``ARITHMETIC_OVERFLOW`` in Spark ANSI mode (PySpark 4.x default)
           because Long multiplication wraps are disallowed.
        The map literal precomputes seeds on the driver (microseconds) and
        gives O(1) lookup at execution time.  Verified at 500M-3B row scale
        with full cluster utilization.
    """
    entries: list[Column] = []
    for wb in unique_wbs:
        s = compute_batch_seed(global_seed, wb)
        col_seed = derive_column_seed(s, table_name, column_name)
        entries.append(F.lit(wb).cast("long"))
        entries.append(F.lit(col_seed).cast("long"))
    return F.create_map(*entries)


def column_seed_lookup(
    seed_map: Column,
    wb_col: Column,
) -> Column:
    """Look up the column seed for a given write_batch from a map literal."""
    return F.element_at(seed_map, wb_col.cast("long"))
