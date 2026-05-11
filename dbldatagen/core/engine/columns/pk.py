"""Primary key generation: sequential and formatted PKs (pure Spark SQL)."""

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


def build_formatted_pk(id_col: Column | str, template: str) -> Column:
    """Builds a formatted string PK column.

    Templates like ``"CUST-{digit:8}"`` produce ``"CUST-00000001"``,
    ``"CUST-00000002"``, ... -- the ``{digit:N}`` placeholder is
    replaced with the zero-padded row id ``(id + 1)``.  Templates
    without a placeholder fall back to ``template + str(id + 1)``.

    Args:
        id_col: Row-id ``Column`` reference or the name of the
          column.
        template: Pattern string.  May contain a single
          ``{digit:N}`` placeholder where ``N`` is the zero-padded
          width.

    Returns:
        A Spark ``Column`` (string) holding the formatted PK.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    import re

    m = re.search(r"\{digit:(\d+)\}", template)
    if m:
        width = int(m.group(1))
        prefix = template[: m.start()]
        suffix = template[m.end() :]
        padded_id = F.lpad((id_col + F.lit(1)).cast("string"), width, "0")
        parts = []
        if prefix:
            parts.append(F.lit(prefix))
        parts.append(padded_id)
        if suffix:
            parts.append(F.lit(suffix))
        return F.concat(*parts) if len(parts) > 1 else parts[0]

    # No placeholder -- just append the id
    return F.concat(F.lit(template), (id_col + F.lit(1)).cast("string"))
