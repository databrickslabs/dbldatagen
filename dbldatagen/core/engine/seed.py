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


# Stride between consecutive batch seeds; large enough to avoid overlap
# between per-column seeds within a single batch.
_BATCH_SEED_STRIDE = 10000

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


def cell_seed_expr(column_seed: int | Column, id_col: Column | str = "id") -> Column:
    """Returns a Spark expression that computes a per-cell seed.

    ``cell_seed = xxhash64(column_seed, id)``

    Partition-independent because ``id`` comes from ``spark.range(n)``.

    PERFORMANCE NOTE — ``int | Column`` signature:
        *column_seed* may be a scalar ``int`` (single-batch generation) or a
        ``Column`` (multi-write-batch generation via map-based seed lookup).
        The Column path eliminates O(N_batches x N_columns) CASE WHEN branches
        in the Spark plan, which otherwise causes Catalyst compilation to stall
        for minutes while the cluster sits idle.  Verified at 500M-3B row
        scale (365 batches x 10+ columns).  Do not refactor this back to
        int-only -- it is intentional.

    Args:
        column_seed: Per-column seed (scalar ``int`` or Spark
          ``Column`` — see PERFORMANCE NOTE).
        id_col: Row-id column.  Either a Spark ``Column`` reference
          or the name of the column (defaults to ``"id"``).

    Returns:
        A Spark ``Column`` (long) holding the per-cell seed for every
        row in the input ``DataFrame``.
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
    """Returns a boolean Column that is ``True`` for rows that should be NULL.

    Uses a separate seed (XOR with constant) to avoid correlation with the
    value seed for the same column.

    PERFORMANCE NOTE: Accepts ``int | Column`` for the same reason as
    ``cell_seed_expr`` — see that function's docstring.

    Args:
        column_seed: Per-column seed (scalar ``int`` or Spark
          ``Column``).
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
    null_seed: int | Column
    if isinstance(column_seed, Column):
        null_seed = column_seed.bitwiseXOR(F.lit(_NULL_SEED_XOR).cast("long"))
    else:
        null_seed = column_seed ^ _NULL_SEED_XOR
    null_hash = cell_seed_expr(null_seed, id_col)
    return F.pmod(null_hash, F.lit(_NULL_PRECISION)) < F.lit(threshold)


def compute_batch_seed(global_seed: int, batch_id: int) -> int:
    """Computes the seed for a given batch.

    Batch ``0`` uses ``global_seed`` directly so single-batch
    generation matches the pre-batched output byte-for-byte.  Batches
    ``n >= 1`` use ``global_seed + n * _BATCH_SEED_STRIDE``
    (wrapped to signed 64-bit).

    Args:
        global_seed: The plan-level seed.
        batch_id: Zero-based batch index.

    Returns:
        Signed 64-bit batch seed.
    """
    if batch_id == 0:
        return global_seed
    raw = global_seed + batch_id * _BATCH_SEED_STRIDE
    return to_signed64(raw)


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
    n = n & 0xFFFFFFFFFFFFFFFF
    if n >= 0x8000000000000000:
        return n - 0x10000000000000000
    return n


# ---------------------------------------------------------------------------
# Map-based seed lookup (for multi-write-batch generation paths)
# ---------------------------------------------------------------------------


def column_seed_map(
    global_seed: int,
    unique_wbs: list[int],
    table_name: str,
    column_name: str,
) -> Column:
    """Builds a Spark map literal that maps ``write_batch`` → column seed.

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

    Args:
        global_seed: The plan-level seed.
        unique_wbs: The distinct ``_write_batch`` values present in
          the DataFrame (typically ``list(range(num_batches))``).
        table_name: Owning table name; threaded into
          ``derive_column_seed`` so different tables get
          uncorrelated maps.
        column_name: Owning column name; threaded into
          ``derive_column_seed``.

    Returns:
        A Spark ``MapType`` ``Column`` (long → long) suitable for
        ``element_at(map, F.col("_write_batch"))``.
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
    """Looks up the column seed for a given ``_write_batch`` from a map literal.

    Args:
        seed_map: Map literal produced by ``column_seed_map`` or
          ``struct_field_seed_map``.
        wb_col: Spark ``Column`` carrying the per-row
          ``_write_batch`` value.

    Returns:
        A Spark ``Column`` (long) holding the per-row column seed.
    """
    return F.element_at(seed_map, wb_col.cast("long"))


def struct_field_seed_map(
    global_seed: int,
    unique_wbs: list[int],
    table_name: str,
    field_path: list[str],
) -> Column:
    """Like ``column_seed_map`` but for a ``StructColumn`` descendant field.

    ``field_path`` is the chain from the ``TableSpec``'s top-level column to
    the leaf field inside any depth of nesting:

        ["addr", "city"]          -- one-level struct
        ["addr", "geo", "lat"]    -- struct-of-struct
        ["owner", "address", "zip"]  -- any nesting depth

    For each ``wb`` the seed is built by chaining ``derive_column_seed``:
    the first hop uses ``(table_name, field_path[0])`` exactly like a
    top-level column, then each further name in ``field_path`` is
    chained with an empty-string table argument (matching the scalar
    path's ``derive_column_seed(parent_seed, "", name)`` per-field
    hash).  Used by ``_build_struct_column`` under the multi-write-batch
    path so nested-struct children match the scalar path byte-for-byte.
    Before this helper took a path (only ``parent_col_name`` +
    ``field_name``), the Column branch raised at plan time whenever a
    struct's child was itself a struct — nested structs were silently
    broken on the Column-typed-seed path.

    Args:
        global_seed: The plan-level seed.
        unique_wbs: Distinct ``_write_batch`` values present in the
          DataFrame.
        table_name: Owning table name.
        field_path: Non-empty chain of struct field names from the
          top-level column down to the leaf field whose seed is
          being computed.

    Returns:
        A Spark ``MapType`` ``Column`` (long → long) suitable for
        ``element_at(map, F.col("_write_batch"))``.

    Raises:
        ValueError: ``field_path`` is empty.
    """
    if not field_path:
        raise ValueError("struct_field_seed_map requires a non-empty field_path")
    entries: list[Column] = []
    for wb in unique_wbs:
        s = compute_batch_seed(global_seed, wb)
        seed = derive_column_seed(s, table_name, field_path[0])
        for name in field_path[1:]:
            seed = derive_column_seed(seed, "", name)
        entries.append(F.lit(wb).cast("long"))
        entries.append(F.lit(seed).cast("long"))
    return F.create_map(*entries)
