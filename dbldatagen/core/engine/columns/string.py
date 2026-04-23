"""String column generation: values, patterns, constants, expressions (Tier 1).

All generation uses Spark SQL expressions -- no Python UDFs required for these
column types.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.distributions import (
    apply_distribution,
    weighted_sample_expr,
)
from dbldatagen.core.engine.seed import GOLDEN_RATIO_HASH, cell_seed_expr, to_signed64
from dbldatagen.core.spec.schema import Distribution, WeightedValues


# ---------------------------------------------------------------------------
# Values column (categorical)
# ---------------------------------------------------------------------------


def build_values_column(
    id_col: Column | str,
    column_seed: int | Column,
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

# Max widths for {digit:N} and {hex:N}: both use pmod(seed, base**width),
# and F.lit(base**width) must fit in int64. 10**18 and 16**15 do; 10**19
# and 16**16 don't. These ceilings are comfortably above any realistic
# PK-pattern use; the guard gives a clear error instead of a py4j long
# conversion failure.
_MAX_DIGIT_WIDTH = 18
_MAX_HEX_WIDTH = 15


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

    Int branch clamps through ``to_signed64`` — XOR with a Python-side
    ``constant`` that exceeds signed-64 (e.g. a large ``idx * hash``
    multiplier) would otherwise push the result outside F.lit's
    accepted range and raise at query-build time, even though the
    XOR's low 64 bits are well-defined.
    """
    if isinstance(column_seed, Column):
        return column_seed.bitwiseXOR(F.lit(constant).cast("long"))
    return to_signed64(column_seed ^ constant)


def _random_digits(
    id_col: Column,
    column_seed: int | Column,
    idx: int,
    width: int,
) -> Column:
    """Generate *width* uniform decimal digits via ``pmod(seed, 10**width)``.

    Leading decimal digits of an int64 are Benford-biased (digit 0 never
    appears as a leading digit of a positive int; 1 and 2 dominate);
    trailing digits via ``pmod`` are uniform. ``pmod`` is non-negative
    for every int64 including ``Long.MIN_VALUE``, so ``'-'`` can't leak
    into the output. Matters on the ``pk_pattern`` FK critical path.
    """
    if width > _MAX_DIGIT_WIDTH:
        raise ValueError(f"{{digit:N}} width must be <= {_MAX_DIGIT_WIDTH} (got {width})")
    seed = cell_seed_expr(_seed_xor(column_seed, (idx + 1) * GOLDEN_RATIO_HASH), id_col)
    digits = F.pmod(seed, F.lit(10**width))
    return F.lpad(digits.cast("string"), width, "0")


def _random_alpha(
    id_col: Column,
    column_seed: int | Column,
    idx: int,
    width: int,
) -> Column:
    """Generate *width* random uppercase letters.

    Each character is derived from a separate hash to ensure independence.
    """
    mixed_seed = _seed_xor(column_seed, (idx + 1) * GOLDEN_RATIO_HASH)
    seed_col = mixed_seed if isinstance(mixed_seed, Column) else F.lit(mixed_seed).cast("long")
    chars: list[Column] = []
    for i in range(width):
        h = F.xxhash64(seed_col, id_col, F.lit(i).cast("long"))
        # pmod is non-negative for every int64 including Long.MIN_VALUE;
        # the older (abs(h) % 26) pattern returns a negative index for
        # Long.MIN_VALUE and raises ARITHMETIC_OVERFLOW under ANSI.
        char_idx = F.pmod(h, F.lit(26)).cast("int")
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
    """Generate *width* uniform hexadecimal characters via ``pmod(seed, 16**width)``.

    Same bug class as ``_random_digits``: a positive int64 has its top
    bit clear, so its hex representation's leading nibble is always in
    ``0-7`` — digits ``8-f`` never appear as the leading hex char of
    ``abs(seed)``. Uses ``pmod`` so Long.MIN_VALUE can't leak and all
    16 hex characters are equally likely at every position.
    """
    if width > _MAX_HEX_WIDTH:
        raise ValueError(f"{{hex:N}} width must be <= {_MAX_HEX_WIDTH} (got {width})")
    seed = cell_seed_expr(_seed_xor(column_seed, (idx + 1) * GOLDEN_RATIO_HASH), id_col)
    value = F.pmod(seed, F.lit(16**width))
    return F.lower(F.lpad(F.hex(value), width, "0"))


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
