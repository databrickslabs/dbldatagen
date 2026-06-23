"""String column generation: values, patterns, constants, and expressions."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.uuid import build_uuid_column
from dbldatagen.core.engine.distributions import (
    apply_distribution,
    weighted_sample_expr,
)
from dbldatagen.core.engine.seed import GOLDEN_RATIO_HASH, cell_seed_expr, seed_xor
from dbldatagen.core.spec._constants import (
    MAX_ALPHA_WIDTH,
    MAX_DIGIT_WIDTH,
    MAX_HEX_WIDTH,
    MAX_SEQ_WIDTH,
    PLACEHOLDER_RE,
)
from dbldatagen.core.spec.schema import Distribution, WeightedValues


def build_values_column(
    id_column: Column | str,
    column_seed: int,
    values_list: list,
    distribution: Distribution | None = None,
) -> Column:
    """Builds a column that selects from a list of values.

    `WeightedValues` selects using cumulative weights; other distributions index
    into the value list, uniformly by default.

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        column_seed: Per-column seed.
        values_list: The allowed values. An empty list yields an all-NULL
            column; a single-element list yields that value on every row.
        distribution: Optional sampling distribution (default None, meaning
            uniform).

    Returns:
        A Spark `Column` whose element type matches the values.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)
    if not values_list:
        return F.lit(None)
    if len(values_list) == 1:
        return F.lit(values_list[0])

    seed_col = cell_seed_expr(column_seed, id_column)

    if isinstance(distribution, WeightedValues):
        return weighted_sample_expr(seed_col, values_list, distribution.weights)

    value_array = F.array(*[F.lit(value) for value in values_list])
    index = apply_distribution(seed_col, len(values_list), distribution)
    return value_array[index.cast("int")]


# ``PLACEHOLDER_RE`` and the ``MAX_*_WIDTH`` caps imported above are
# shared with the schema validator (``PatternColumn.validate_template``
# in ``core/spec/schema.py``) via ``core/spec/_constants.py`` so the
# accept-set the schema promises is exactly what the engine accepts.
# See the docstring in _constants.py for the per-kind cap rationale.


def build_pattern_column(
    id_column: Column | str,
    column_seed: int,
    template: str,
) -> Column:
    """Builds a string column from a template such as "ORD-{digit:4}-{alpha:3}".

    Supported placeholders:
        {seq}     - row sequence number
        {uuid}    - deterministic UUID (36 chars, no width modifier)
        {digit:N} - N random digits
        {alpha:N} - N random uppercase letters
        {hex:N}   - N random hex characters

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        column_seed: Per-column seed.
        template: Template string with literal text and placeholders.

    Returns:
        A Spark string `Column` with values matching the template.

    Raises:
        ValueError: If `{uuid}` carries a width modifier, or a `{kind:N}` width
            exceeds its limit ({digit} 18, {hex} 15, {alpha} 64, {seq} 24).
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)

    parts: list[Column] = []
    position = 0

    for placeholder_index, m in enumerate(PLACEHOLDER_RE.finditer(template)):
        if m.start() > position:
            parts.append(F.lit(template[position : m.start()]))

        kind = m.group("kind")
        width_str = m.group("width")
        width = int(width_str) if width_str else 1

        match kind:
            case "seq":
                if width > MAX_SEQ_WIDTH:
                    raise ValueError(f"{{seq:N}} width must be <= {MAX_SEQ_WIDTH} (got {width})")
                parts.append(F.lpad((id_column + F.lit(1)).cast("string"), width, "0"))
            case "uuid":
                # {uuid} takes no width modifier: a 36-char UUID alongside a width
                # request is almost always a bug (the user wants {hex:N} or
                # {alpha:N}), so reject rather than silently ignore it.
                if width_str is not None:
                    raise ValueError(
                        f"{{uuid}} does not accept a width modifier (got '{m.group(0)}'). "
                        f"UUIDs are always 36 characters; use {{hex:N}} or {{alpha:N}} for a "
                        f"short random token."
                    )
                parts.append(
                    build_uuid_column(id_column, seed_xor(column_seed, (placeholder_index + 1) * GOLDEN_RATIO_HASH))
                )
            case "digit":
                parts.append(_random_digits(id_column, column_seed, placeholder_index, width))
            case "alpha":
                parts.append(_random_alpha(id_column, column_seed, placeholder_index, width))
            case "hex":
                parts.append(_random_hex(id_column, column_seed, placeholder_index, width))
            case _:
                # Drift guard: PLACEHOLDER_RE's (seq|uuid|digit|alpha|hex) alternation
                # is the only producer of kind, so reaching this arm means the regex
                # in core/spec/_constants.py grew a kind without a dispatch arm here.
                raise RuntimeError(
                    f"PLACEHOLDER_RE matched kind {kind!r} but build_pattern_column "
                    f"has no dispatch arm for it -- the regex alternation in "
                    f"core/spec/_constants.py and the ``match`` block here must "
                    f"stay in sync.  Adding a new placeholder kind requires "
                    f"updating both."
                )

        position = m.end()

    if position < len(template):
        parts.append(F.lit(template[position:]))

    if not parts:
        return F.lit(template)
    if len(parts) == 1:
        return parts[0]
    return F.concat(*parts)


def _random_digits(
    id_column: Column,
    column_seed: int,
    placeholder_index: int,
    width: int,
) -> Column:
    """Builds a column of uniform numeric digits.

    Args:
        id_column: Row-id column.
        column_seed: Per-column seed.
        placeholder_index: Index of this placeholder within the template.
        width: Number of digits to generate.

    Returns:
        A Spark string `Column` of zero-padded digits.

    Raises:
        ValueError: If `width` exceeds the digit limit (18).
    """
    if width > MAX_DIGIT_WIDTH:
        raise ValueError(f"{{digit:N}} width must be <= {MAX_DIGIT_WIDTH} (got {width})")
    seed = cell_seed_expr(seed_xor(column_seed, (placeholder_index + 1) * GOLDEN_RATIO_HASH), id_column)
    digits = F.pmod(seed, F.lit(10**width))
    return F.lpad(digits.cast("string"), width, "0")


def _random_alpha(
    id_column: Column,
    column_seed: int,
    placeholder_index: int,
    width: int,
) -> Column:
    """Builds a column of random uppercase letters.

    Args:
        id_column: Row-id column.
        column_seed: Per-column seed.
        placeholder_index: Index of this placeholder within the template.
        width: Number of letters to generate.

    Returns:
        A Spark string `Column` of uppercase letters.

    Raises:
        ValueError: If `width` exceeds the alpha limit (64).
    """
    if width > MAX_ALPHA_WIDTH:
        raise ValueError(f"{{alpha:N}} width must be <= {MAX_ALPHA_WIDTH} (got {width})")
    mixed_seed = seed_xor(column_seed, (placeholder_index + 1) * GOLDEN_RATIO_HASH)
    seed_col = F.lit(mixed_seed).cast("long")
    chars: list[Column] = []
    for char_position in range(width):
        char_hash = F.xxhash64(seed_col, id_column, F.lit(char_position).cast("long"))
        # pmod is non-negative for every int64 including Long.MIN_VALUE; the older
        # (abs(h) % 26) pattern returns a negative index for Long.MIN_VALUE and
        # raises ARITHMETIC_OVERFLOW under ANSI.
        char_index = F.pmod(char_hash, F.lit(26)).cast("int")
        chars.append(F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), char_index + F.lit(1), 1))
    if len(chars) == 1:
        return chars[0]
    return F.concat(*chars)


def _random_hex(
    id_column: Column,
    column_seed: int,
    placeholder_index: int,
    width: int,
) -> Column:
    """Builds a column of uniform hexadecimal characters.

    Args:
        id_column: Row-id column.
        column_seed: Per-column seed.
        placeholder_index: Index of this placeholder within the template.
        width: Number of hex characters to generate.

    Returns:
        A Spark string `Column` of lowercase hex characters.

    Raises:
        ValueError: If `width` exceeds the hex limit (15).
    """
    if width > MAX_HEX_WIDTH:
        raise ValueError(f"{{hex:N}} width must be <= {MAX_HEX_WIDTH} (got {width})")
    seed = cell_seed_expr(seed_xor(column_seed, (placeholder_index + 1) * GOLDEN_RATIO_HASH), id_column)
    value = F.pmod(seed, F.lit(16**width))
    return F.lower(F.lpad(F.hex(value), width, "0"))
