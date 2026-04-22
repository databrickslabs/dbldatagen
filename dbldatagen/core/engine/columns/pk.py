"""Primary key generation: sequential and formatted PKs (pure Spark SQL)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def build_sequential_pk(id_col: Column | str, start: int = 1, step: int = 1) -> Column:
    """Sequential PK: ``start + id * step``.

    Zero overhead -- a simple arithmetic expression on the row id.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    return (id_col * F.lit(step) + F.lit(start)).cast("long")


def build_formatted_pk(id_col: Column | str, template: str) -> Column:
    """Formatted string PK like ``'CUST-{digit:8}'`` -> ``'CUST-00000001'``.

    The ``{digit:N}`` placeholder is replaced with the zero-padded row id.
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
