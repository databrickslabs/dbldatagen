"""Shared engine utilities."""

from __future__ import annotations

from pyspark.sql import DataFrame


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
