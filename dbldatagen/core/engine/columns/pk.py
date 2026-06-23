"""Sequential primary key generation.

Builds an integer key as `start + id * step` directly from each row's index.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def build_sequential_pk(id_column: Column | str, start: int = 1, step: int = 1) -> Column:
    """Builds a sequential primary key column.

    The value at row `i` is `start + i * step`, cast to long.

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        start: Optional value at row 0 (default 1).
        step: Optional increment per row; may be negative (default 1).

    Returns:
        A Spark long `Column` holding `start + id * step`.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)
    return (id_column * F.lit(step) + F.lit(start)).cast("long")
