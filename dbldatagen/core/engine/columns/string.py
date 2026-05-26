"""String column generation: values, patterns, constants, expressions (Tier 1).

All generation uses Spark SQL expressions -- no Python UDFs required for these
column types.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.uuid import build_uuid_column
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
    column_seed: int,
    values_list: list,
    distribution: Distribution | None = None,
) -> Column:
    """Picks from a list of values.

    For ``WeightedValues`` distributions, dispatches to a CASE/WHEN
    chain built by ``weighted_sample_expr``.  Otherwise the cell seed
    indexes uniformly (or via the given distribution) into a Spark
    array literal.

    Args:
        id_col: Row-id ``Column`` reference or column name.
        column_seed: Per-column seed.
        values_list: The allowed-values list.  Empty produces an
          all-NULL column; single-element short-circuits to a literal.
        distribution: Sampling distribution.  ``Uniform`` (default)
          indexes uniformly; ``WeightedValues`` uses cumulative
          weights; other distributions skew the index.

    Returns:
        A Spark ``Column`` whose runtime element type matches
        ``values_list``'s element type.
    """
    if isinstance(id_col, str):
        id_col = F.col(id_col)
    if not values_list:
        return F.lit(None)
    if len(values_list) == 1:
        return F.lit(values_list[0])

    seed_col = cell_seed_expr(column_seed, id_col)

    if isinstance(distribution, WeightedValues):
        return weighted_sample_expr(seed_col, values_list, distribution.weights)

    arr = F.array(*[F.lit(v) for v in values_list])
    idx = apply_distribution(seed_col, len(values_list), distribution)
    return arr[idx.cast("int")]


# ---------------------------------------------------------------------------
# Pattern column  (template-based string generation)
# ---------------------------------------------------------------------------

_PLACEHOLDER_RE = re.compile(r"\{(seq|uuid|digit|alpha|hex):?(\d+)?[a-z]?\}")

# Per-placeholder width caps. The rationale differs by kind:
#   * {digit:N} / {hex:N}: both use pmod(seed, base**width), and
#     F.lit(base**width) must fit in int64. 10**18 and 16**15 do;
#     10**19 and 16**16 don't. The guard gives a clear error instead
#     of a py4j long-conversion failure.
#   * {alpha:N}: each character materialises a separate xxhash64 +
#     substring expression that gets concat'd, so width directly
#     controls the per-row Catalyst plan size. 64 is comfortably
#     above any realistic token use.
#   * {seq:N}: F.lpad(value, width, "0") doesn't bomb Catalyst (one
#     expression) but emits width-character strings per row. 24
#     covers sequence values up to 10**24 with zero-padding.
_MAX_DIGIT_WIDTH = 18
_MAX_HEX_WIDTH = 15
_MAX_ALPHA_WIDTH = 64
_MAX_SEQ_WIDTH = 24


def build_pattern_column(
    id_col: Column | str,
    column_seed: int,
    template: str,
) -> Column:
    """Generates strings from a template like ``"ORD-{digit:4}-{alpha:3}"``.

    Supported placeholders:
        {seq}       -- row sequence number (from id)
        {uuid}      -- deterministic UUID (36 chars, no width modifier)
        {digit:N}   -- N random digits
        {alpha:N}   -- N random uppercase alpha characters
        {hex:N}     -- N random hex characters

    Args:
        id_col: Row-id ``Column`` reference or column name.
        column_seed: Per-column seed.
        template: Template string containing literal text and any of
          the placeholders above.

    Returns:
        A Spark ``Column`` (string) holding the rendered values.

    Raises:
        ValueError: ``{uuid}`` placeholder carries a width modifier,
          or any ``{kind:N}`` width exceeds its per-kind ceiling
          (``{digit:N}`` 18, ``{hex:N}`` 15, ``{alpha:N}`` 64,
          ``{seq:N}`` 24).
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
            if width > _MAX_SEQ_WIDTH:
                raise ValueError(f"{{seq:N}} width must be <= {_MAX_SEQ_WIDTH} (got {width})")
            parts.append(F.lpad((id_col + F.lit(1)).cast("string"), width, "0"))
        elif kind == "uuid":
            # {uuid} has no width modifier -- a 36-char UUID with the
            # user asking for N chars is almost always a bug (they
            # probably want {hex:N} or {alpha:N}).  Reject rather than
            # silently ignore the width.
            if m.group(2) is not None:
                raise ValueError(
                    f"{{uuid}} does not accept a width modifier (got '{m.group(0)}'). "
                    f"UUIDs are always 36 characters; use {{hex:N}} or {{alpha:N}} for a "
                    f"short random token."
                )
            parts.append(_random_uuid(id_col, column_seed, placeholder_idx))
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


def _seed_xor(column_seed: int, constant: int) -> int:
    """XOR a column seed with a constant, clamped to signed-64.

    ``to_signed64`` keeps XOR results inside int64 so ``F.lit``
    accepts them at query-build time.
    """
    return to_signed64(column_seed ^ constant)


def _random_uuid(
    id_col: Column,
    column_seed: int,
    idx: int,
) -> Column:
    """Generate a UUID embedded inside a pattern template.

    Each ``{uuid}`` in a template gets an independent seed derivation so
    two placeholders in the same row don't emit identical UUIDs. Uses
    the same XOR-with-(idx+1)*GOLDEN_RATIO_HASH pattern as the other
    ``_random_*`` helpers for consistency.
    """
    return build_uuid_column(id_col, _seed_xor(column_seed, (idx + 1) * GOLDEN_RATIO_HASH))


def _random_digits(
    id_col: Column,
    column_seed: int,
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
    column_seed: int,
    idx: int,
    width: int,
) -> Column:
    """Generate *width* random uppercase letters.

    Each character is derived from a separate hash to ensure independence.
    """
    if width > _MAX_ALPHA_WIDTH:
        raise ValueError(f"{{alpha:N}} width must be <= {_MAX_ALPHA_WIDTH} (got {width})")
    mixed_seed = _seed_xor(column_seed, (idx + 1) * GOLDEN_RATIO_HASH)
    seed_col = F.lit(mixed_seed).cast("long")
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
    column_seed: int,
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
    """Returns a literal Spark column with a fixed value for every row.

    Args:
        value: The literal value emitted on every row.  Any
          JSON-serialisable type Spark's ``F.lit`` accepts.

    Returns:
        A Spark ``Column`` (literal) whose runtime type matches the
        Python type of ``value``.
    """
    return F.lit(value)


def build_expression_column(expr_str: str) -> Column:
    """Returns a Spark column from a SQL expression string.

    The expression can reference other columns in the same table,
    e.g. ``"quantity * unit_price"`` or
    ``"concat(first_name, ' ', last_name)"``.

    Security note: ``expr_str`` is passed directly to ``F.expr()`` and
    can execute arbitrary Spark SQL.  Do not use ``ExpressionColumn``
    with untrusted plan YAML in multi-tenant environments.

    Args:
        expr_str: A Spark SQL expression string.

    Returns:
        A Spark ``Column`` carrying the parsed expression; its
        runtime type is whatever the expression evaluates to.
    """
    return F.expr(expr_str)
