"""String column generation: values, patterns, constants, expressions (Tier 1).

All generation uses Spark SQL expressions -- no Python UDFs required for these
column types.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.v1.engine.distributions import (
    apply_distribution,
    weighted_sample_expr,
)
from dbldatagen.v1.engine.seed import cell_seed_expr
from dbldatagen.v1.schema import Distribution, WeightedValues


# ---------------------------------------------------------------------------
# Values column (categorical)
# ---------------------------------------------------------------------------


def build_values_column(
    id_col: Column | str,
    column_seed: int,
    values_list: list,
    distribution: Distribution | None = None,
    cell_seed_override: Column | None = None,
) -> Column:
    """Pick from a list of values.

    For ``WeightedValues`` distributions, a CASE/WHEN chain is used; otherwise
    the cell seed indexes uniformly (or via the given distribution) into a
    Spark array literal.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    if not values_list:
        return F.lit(None)
    if len(values_list) == 1:
        return F.lit(values_list[0])

    seed_col = cell_seed_override if cell_seed_override is not None else cell_seed_expr(column_seed, id_col)

    if isinstance(distribution, WeightedValues):
        return weighted_sample_expr(seed_col, values_list, distribution.weights)

    arr = F.array(*[F.lit(v) for v in values_list])
    idx = apply_distribution(seed_col, len(values_list), distribution)
    return arr[idx.cast("int")]


# ---------------------------------------------------------------------------
# Pattern column  (template-based string generation)
# ---------------------------------------------------------------------------

_PLACEHOLDER_RE = re.compile(r"\{(seq|uuid|digit|alpha|hex):?(\d+)?[a-z]?\}")


def build_pattern_column(
    id_col: Column | str,
    column_seed: int | Column,
    template: str,
) -> Column:
    """Generate strings from a template like ``'ORD-{digit:4}-{alpha:3}'``.

    Supported placeholders:
        {seq}       -- row sequence number (from id)
        {digit:N}   -- N random digits
        {alpha:N}   -- N random uppercase alpha characters
        {hex:N}     -- N random hex characters
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)

    parts: list[Column] = []
    pos = 0

    for placeholder_idx, m in enumerate(_PLACEHOLDER_RE.finditer(template)):
        # Literal text before this placeholder
        if m.start() > pos:
            parts.append(F.lit(template[pos : m.start()]))

        kind = m.group(1)
        width = int(m.group(2)) if m.group(2) else 1

        if kind == "seq":
            parts.append(F.lpad((id_col + F.lit(1)).cast("string"), width, "0"))
        elif kind == "digit":
            parts.append(_random_digits(id_col, column_seed, placeholder_idx, width))
        elif kind == "alpha":
            parts.append(_random_alpha(id_col, column_seed, placeholder_idx, width))
        elif kind == "hex":
            parts.append(_random_hex(id_col, column_seed, placeholder_idx, width))

        pos = m.end()

    # Trailing literal
    if pos < len(template):
        parts.append(F.lit(template[pos:]))

    if not parts:
        return F.lit(template)
    if len(parts) == 1:
        return parts[0]
    return F.concat(*parts)


def _seed_xor(column_seed: int | Column, constant: int) -> int | Column:
    """XOR a column seed (int or Column) with a constant.

    PERFORMANCE NOTE: The ``int | Column`` path is required for fused
    multi-batch CDC where column_seed is a Column from map-based lookup
    (see ``column_seed_map`` in seed.py).  Do not simplify to int-only.
    """
    if isinstance(column_seed, Column):
        return column_seed.bitwiseXOR(F.lit(constant).cast("long"))
    return column_seed ^ constant


def _random_digits(
    id_col: Column,
    column_seed: int | Column,
    idx: int,
    width: int,
) -> Column:
    """Generate *width* random digits from the cell seed."""
    seed = cell_seed_expr(_seed_xor(column_seed, (idx + 1) * 0x9E3779B9), id_col)
    # Take abs, convert to string, pad/truncate to width
    raw = F.abs(seed).cast("string")
    return F.rpad(F.substring(raw, 1, width), width, "0")


def _random_alpha(
    id_col: Column,
    column_seed: int | Column,
    idx: int,
    width: int,
) -> Column:
    """Generate *width* random uppercase letters.

    Each character is derived from a separate hash to ensure independence.
    """
    mixed_seed = _seed_xor(column_seed, (idx + 1) * 0x9E3779B9)
    seed_col = mixed_seed if isinstance(mixed_seed, Column) else F.lit(mixed_seed).cast("long")
    chars: list[Column] = []
    for i in range(width):
        h = F.xxhash64(seed_col, id_col, F.lit(i).cast("long"))
        # Map to A-Z (26 chars)
        char_idx = (F.abs(h) % F.lit(26)).cast("int")
        chars.append(F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), char_idx + F.lit(1), 1))
    if len(chars) == 1:
        return chars[0]
    return F.concat(*chars)


def _random_hex(
    id_col: Column,
    column_seed: int | Column,
    idx: int,
    width: int,
) -> Column:
    """Generate *width* random hexadecimal characters."""
    seed = cell_seed_expr(_seed_xor(column_seed, (idx + 1) * 0x9E3779B9), id_col)
    raw = F.hex(F.abs(seed))
    return F.lower(F.rpad(F.substring(raw, 1, width), width, "0"))


# ---------------------------------------------------------------------------
# Constant and expression columns
# ---------------------------------------------------------------------------


def build_constant_column(value: object) -> Column:
    """Return a literal Spark column with a fixed value for every row."""
    return F.lit(value)


def build_expression_column(expr_str: str) -> Column:
    """Return a Spark column from a SQL expression string.

    The expression can reference other columns in the same table, e.g.
    ``"quantity * unit_price"`` or ``"concat(first_name, ' ', last_name)"``.
    """
    return F.expr(expr_str)
