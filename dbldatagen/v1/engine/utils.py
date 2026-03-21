"""Shared engine utilities."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Generic, TypeVar, overload

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.v1.schema import TableSpec


T = TypeVar("T")


class _LazyList(Generic[T]):
    """Cached lazy list: generates items on first access, caches for reuse.

    Supports indexing (including negative and slices), iteration,
    and ``len()``.

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


def split_with_remainder(
    total: int,
    fractions: tuple[float, float, float],
) -> tuple[int, int, int]:
    """Split *total* into three integer counts proportional to *fractions*.

    Remainder (from integer truncation) is assigned to the category
    with the largest non-zero fraction.  If all fractions are zero,
    the remainder goes to the first category.
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
    """Union a non-empty list of DataFrames by name.

    Parameters
    ----------
    dfs : list[DataFrame]
        Must contain at least one DataFrame.
    allow_missing_columns : bool
        If True, missing columns are filled with NULL.
    """
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=allow_missing_columns)
    return result


def create_range_df(
    spark: SparkSession,
    row_count: int,
) -> tuple[DataFrame, Column]:
    """Create a range DataFrame with ``_synth_row_id`` column.

    Renames ``spark.range()``'s default ``id`` column to avoid collisions
    with user columns named ``id``.

    Returns ``(df, id_col)`` where ``id_col`` is ``F.col("_synth_row_id")``.
    """
    df = spark.range(row_count).withColumnRenamed("id", "_synth_row_id")
    return df, F.col("_synth_row_id")


def apply_null_fraction(
    expr: Column,
    column_seed: int | Column,
    id_col: Column,
    null_fraction: float,
) -> Column:
    """Wrap *expr* with a null mask when *null_fraction* > 0.

    Returns *expr* unchanged when ``null_fraction <= 0``.
    """
    if null_fraction <= 0:
        return expr
    from dbldatagen.v1.engine.seed import null_mask_expr

    is_null = null_mask_expr(column_seed, id_col, null_fraction)
    return F.when(is_null, F.lit(None)).otherwise(expr)


def apply_column_phases(
    df: DataFrame,
    id_col: Column,
    col_exprs: list[Column],
    udf_columns: list[tuple[str, Column]],
    seeded_columns: list[tuple[str, Column]],
) -> DataFrame:
    """Apply the three-phase column application pattern and drop ``_synth_row_id``.

    Phase 1: flat ``select`` for Spark SQL expressions.
    Phase 2: ``withColumn`` for UDF-based columns (FK, Faker).
    Phase 3: ``withColumn`` for seed_from columns.
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
    """Build a CASE WHEN chain over *discriminator* for a list of ``(key, expr)`` pairs.

    Returns ``WHEN disc == k0 THEN v0 WHEN disc == k1 THEN v1 ... ELSE v_last``.
    When there is exactly one branch, returns the expression directly
    (no CASE WHEN needed).
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
    """Extract primary key column names as a set."""
    if table_spec.primary_key:
        return set(table_spec.primary_key.columns)
    return set()
