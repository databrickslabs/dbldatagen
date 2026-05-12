"""Sequential primary key generation (pure Spark SQL)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def build_sequential_pk(id_col: Column | str, start: int = 1, step: int = 1) -> Column:
    """Builds a sequential PK column: ``start + id * step``.

    Zero overhead -- a simple arithmetic expression on the row id,
    cast to ``long``.  Used by ``SequenceColumn``; ``TableSpec``
    validates that ``start + (rows - 1) * step`` does not overflow
    int64 at plan time.

    Args:
        id_col: Row-id ``Column`` reference or the name of the
          column.
        start: Sequence value at ``id == 0``.  Defaults to ``1``.
        step: Increment per row.  May be negative.  Defaults to
          ``1``.

    Returns:
        A Spark ``Column`` (long) holding ``start + id * step``.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    return (id_col * F.lit(step) + F.lit(start)).cast("long")
