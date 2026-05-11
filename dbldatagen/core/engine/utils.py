"""Shared engine utilities."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Generic, TypeVar, overload

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.spec.schema import TableSpec


T = TypeVar("T")


class _LazyList(Generic[T]):
    """Cached lazy list: generates items on first access, caches for reuse.

    Supports indexing (including negative and slices), iteration,
    and ``len()``.

    .. note:: Memory

        The cache grows unboundedly — every accessed item is retained.
        For long sequences (1000+ items), iterating the whole list keeps
        every generated value in memory.  Access individual items and
        discard references when memory is a concern.

    Parameters
    ----------
    length :
        Total number of items.
    generator :
        ``(index) -> T`` — called on cache miss.  *index* is the
        zero-based position in the list.
    """

    def __init__(self, length: int, generator: Callable[[int], T]) -> None:
        self._length = length
        self._generator = generator
        self._cache: dict[int, T] = {}

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index: int | slice) -> T | list[T]:
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(self._length))]
        if index < 0:
            index += self._length
        if index < 0 or index >= self._length:
            raise IndexError(f"index {index} out of range")
        if index not in self._cache:
            self._cache[index] = self._generator(index)
        return self._cache[index]

    def __iter__(self) -> Iterator[T]:
        for i in range(self._length):
            yield self[i]

    def __len__(self) -> int:
        return self._length


def resolve_batch_size(batch_size_spec: int | float | str, initial_rows: int) -> int:
    """Converts a batch-size spec into an absolute row count.

    Args:
        batch_size_spec: Either an ``int`` row count, a ``float`` in
          ``(0.0, 1.0]`` interpreted as a fraction of ``initial_rows``,
          or a human-readable string like ``"50K"`` /  ``"1M"`` /
          ``"100"`` (parsed via ``parse_human_count``).
        initial_rows: The baseline row count used when
          ``batch_size_spec`` is a fractional ``float``.

    Returns:
        An absolute batch size (``int``).  At least ``1`` when the
        fractional interpretation rounds below one.
    """
    if isinstance(batch_size_spec, str):
        from dbldatagen.core.spec.schema import parse_human_count

        batch_size_spec = parse_human_count(batch_size_spec)
    if isinstance(batch_size_spec, float) and batch_size_spec <= 1.0:
        return max(1, int(initial_rows * batch_size_spec))
    return int(batch_size_spec)


def split_with_remainder(
    total: int,
    fractions: tuple[float, float, float],
) -> tuple[int, int, int]:
    """Splits *total* into three integer counts proportional to *fractions*.

    Remainder (from integer truncation) is assigned to the category
    with the largest non-zero fraction.  If all fractions are zero,
    the remainder goes to the first category.

    Args:
        total: Total count to distribute.  Must be non-negative.
        fractions: Three-element tuple of non-negative fractions
          ``(a, b, c)``.  Normalised internally; need not sum to
          ``1.0``.

    Returns:
        A three-element tuple ``(a, b, c)`` of ``int`` counts that
        sum to ``total``.
    """
    a_frac, b_frac, c_frac = fractions
    total_w = a_frac + b_frac + c_frac
    if total_w > 0:
        a_frac /= total_w
        b_frac /= total_w
        c_frac /= total_w

    a = int(total * a_frac)
    b = int(total * b_frac)
    c = int(total * c_frac)

    remainder = total - a - b - c
    if remainder > 0:
        candidates = [(a_frac, 0), (b_frac, 1), (c_frac, 2)]
        candidates = [(f, idx) for f, idx in candidates if f > 0]
        if candidates:
            best_idx = max(candidates, key=lambda x: x[0])[1]
            if best_idx == 0:
                a += remainder
            elif best_idx == 1:
                b += remainder
            else:
                c += remainder
        else:
            a += remainder

    return a, b, c


def union_all(
    dfs: list[DataFrame],
    *,
    allow_missing_columns: bool = False,
) -> DataFrame:
    """Unions a non-empty list of ``DataFrame`` objects by column name.

    Args:
        dfs: Non-empty list of ``DataFrame`` objects to union.
          Schema alignment is by column name, not position.
        allow_missing_columns: When ``True``, columns missing from a
          ``DataFrame`` are filled with ``NULL`` in the union (Spark
          3.1+ ``unionByName`` ``allowMissingColumns`` semantics).
          Defaults to ``False``.

    Returns:
        A single ``DataFrame`` carrying the union of all inputs by
        name.
    """
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=allow_missing_columns)
    return result


def create_range_df(
    spark: SparkSession,
    row_count: int,
) -> tuple[DataFrame, Column]:
    """Creates a range ``DataFrame`` with a ``_synth_row_id`` column.

    Renames ``spark.range()``'s default ``id`` column to avoid
    collisions with user columns named ``id``.  The renamed column
    is reserved internally and dropped before the final ``DataFrame``
    is returned to the user.

    Args:
        spark: Active ``SparkSession``.
        row_count: Number of rows to generate.  Must be non-negative.

    Returns:
        A two-tuple ``(df, id_col)`` where ``df`` has a single
        ``_synth_row_id`` column and ``id_col`` is the corresponding
        ``Column`` reference for use in downstream expressions.
    """
    df = spark.range(row_count).withColumnRenamed("id", "_synth_row_id")
    return df, F.col("_synth_row_id")


def apply_null_fraction(
    expr: Column,
    column_seed: int | Column,
    id_col: Column,
    null_fraction: float,
) -> Column:
    """Wraps *expr* with a null mask when *null_fraction* > 0.

    Args:
        expr: The Spark ``Column`` expression to wrap.
        column_seed: Per-column seed.  Scalar ``int`` for the
          single-batch path; ``Column`` for the multi-write-batch
          path.
        id_col: Row-id ``Column`` (typically
          ``F.col("_synth_row_id")``).
        null_fraction: Target fraction of rows to mark NULL, in
          ``[0.0, 1.0]``.

    Returns:
        ``expr`` unchanged when ``null_fraction <= 0``; otherwise
        ``F.when(null_mask, NULL).otherwise(expr)``.
    """
    if null_fraction <= 0:
        return expr
    from dbldatagen.core.engine.seed import null_mask_expr

    is_null = null_mask_expr(column_seed, id_col, null_fraction)
    return F.when(is_null, F.lit(None)).otherwise(expr)


def apply_column_phases(
    df: DataFrame,
    id_col: Column,
    col_exprs: list[Column],
    udf_columns: list[tuple[str, Column]],
    seeded_columns: list[tuple[str, Column]],
) -> DataFrame:
    """Applies the three-phase column application pattern.

    Phase 1 -- flat ``select`` for Spark SQL expressions (cheapest).
    Phase 2 -- ``withColumn`` for UDF-based columns (FK, Faker) so
        each one anchors against the already-projected scalar
        columns.
    Phase 3 -- ``withColumn`` for ``seed_from`` columns so they
        reference the already-materialised parent column.
    Finally, drops the internal ``_synth_row_id`` column.

    Args:
        df: Source ``DataFrame`` carrying the ``_synth_row_id``
          column produced by ``create_range_df``.
        id_col: ``Column`` reference to ``_synth_row_id``.
        col_exprs: Phase-1 Spark SQL expression columns to project.
        udf_columns: Phase-2 list of ``(name, expr)`` pairs for
          UDF-based columns added via ``withColumn``.
        seeded_columns: Phase-3 list of ``(name, expr)`` pairs for
          ``seed_from``-derived columns added after their parents.

    Returns:
        A ``DataFrame`` with the user-facing columns in declaration
        order; ``_synth_row_id`` is no longer present.
    """
    df = df.select(id_col, *col_exprs)

    for col_name, col_expr in udf_columns:
        df = df.withColumn(col_name, col_expr)

    for col_name, col_expr in seeded_columns:
        df = df.withColumn(col_name, col_expr)

    return df.drop("_synth_row_id")


def case_when_chain(
    discriminator: Column,
    branches: list[tuple[int, Column]],
) -> Column:
    """Builds a CASE WHEN chain over *discriminator* for a list of branches.

    Emits ``WHEN disc == k0 THEN v0 WHEN disc == k1 THEN v1 ... ELSE v_last``.
    When there is exactly one branch, returns the expression directly
    (no CASE WHEN needed) so single-batch generation produces a
    minimal Spark plan.

    Args:
        discriminator: Spark ``Column`` whose value selects a branch.
        branches: Non-empty list of ``(key, expr)`` pairs.  The last
          pair acts as the ``ELSE`` arm; the remainder become
          ``WHEN disc == key THEN expr`` clauses in order.

    Returns:
        A Spark ``Column`` representing the CASE WHEN expression
        (or the single branch's expression unchanged when
        ``len(branches) == 1``).
    """
    if len(branches) == 1:
        return branches[0][1]

    result = branches[-1][1]
    for idx in range(len(branches) - 2, -1, -1):
        result = F.when(
            discriminator == F.lit(branches[idx][0]),
            branches[idx][1],
        ).otherwise(result)
    return result


def get_pk_columns(table_spec: TableSpec) -> set[str]:
    """Extracts primary key column names as a set.

    Args:
        table_spec: The table whose PK column names to return.

    Returns:
        ``set(table_spec.primary_key.columns)`` when the table has a
        primary key, otherwise an empty set.
    """
    if table_spec.primary_key:
        return set(table_spec.primary_key.columns)
    return set()
