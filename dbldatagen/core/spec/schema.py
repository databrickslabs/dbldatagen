"""Pydantic v2 models for the ``dbldatagen.core`` plan schema.

Defines the ``Distribution`` discriminated union (``Uniform``,
``Normal``, ``LogNormal``, ``Zipf``, ``Exponential``,
``WeightedValues``), the ``ColumnStrategy`` discriminated union
(``RangeColumn``, ``ValuesColumn``, ``FakerColumn``, ``PatternColumn``,
``SequenceColumn``, ``UUIDColumn``, ``ExpressionColumn``,
``TimestampColumn``, ``ConstantColumn``, ``ForeignKeyColumn``,
``StructColumn``, ``ArrayColumn``), and the three composition models
``ColumnSpec`` / ``TableSpec`` / ``DataGenPlan`` -- the complete
vocabulary for describing a synthetic data generation plan.

Every model inherits ``extra="forbid"`` so unknown YAML/JSON keys
fail validation instead of being silently dropped.  Validators on
each strategy reject illegal parameter combinations at plan time so
the failure surfaces next to the offending declaration rather than
deep inside the engine.
"""

from __future__ import annotations

import math
import re
import warnings
from collections import Counter
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dbldatagen.core.engine.seed import NULL_PRECISION
from dbldatagen.core.spec._constants import (
    MAX_ALPHA_WIDTH,
    MAX_DIGIT_WIDTH,
    MAX_HEX_WIDTH,
    MAX_SEQ_WIDTH,
    PLACEHOLDER_RE,
)


class _StrictModel(BaseModel):
    """Base for every public plan model.

    ``extra="forbid"`` rejects unknown fields at validation time instead
    of silently ignoring them -- a typo like ``niull_fraction`` in a
    YAML plan would otherwise round-trip as a legal model that silently
    dropped the user's intent.  The cost is that loaders of pre-2026
    plans with unknown keys now get a ValueError instead of silent
    ignore; that's the right direction for a correctness-first engine.
    """

    model_config = ConfigDict(extra="forbid")


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _require_finite(field_name: str, value: float, owner: str) -> None:
    """Reject NaN / +/-Inf for a float field.

    ``value < N``, ``value > N``, ``value <= N`` all return False
    when ``value`` is NaN, so a downstream validator like
    ``stddev < 0`` silently passes NaN through and the engine ships
    ``F.lit(nan)`` into the Spark plan -- every row materialises as
    NaN with no signal at plan time.  Catch at the float boundary.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(
            f"{owner}.{field_name}={value} is not finite (NaN/Inf).  "
            f"Use a concrete numeric value; NaN-vs-* comparisons "
            f"silently return False and would let every other check pass."
        )


# Engine-internal column names that user columns must not collide with.
# Currently only ``_synth_row_id`` (added by ``create_range_df`` and
# dropped by ``apply_column_phases``).  Add new internal names here if
# the engine introduces more.  The validator rejects exact matches
# only -- legitimate customer names like ``_modified_at`` or
# ``_partition_key`` are accepted, even though they begin with an
# underscore, because they don't shadow anything in this set.
_RESERVED_INTERNAL_COLUMN_NAMES: frozenset[str] = frozenset({"_synth_row_id"})


def parse_fk_ref(ref: str) -> tuple[str, str]:
    """Parse and validate a ``ForeignKeyRef.ref`` string.

    Single source of truth for the ``"table.column"`` format.  Called
    by ``ForeignKeyRef.validate_ref_format`` at construction time and
    by the planner when it needs the parent table / column names, so
    the same parse rule applies everywhere without redundant
    ``split(".")`` calls scattered across modules.

    Args:
        ref: An FK reference string in ``"table.column"`` form.  Each
          half must match ``_IDENTIFIER_RE`` -- the same rule
          ``TableSpec.name`` and ``ColumnSpec.name`` enforce.

    Returns:
        A ``(table_name, column_name)`` tuple.

    Raises:
        ValueError: ``ref`` is not exactly two identifier-shaped
          components separated by a single ``.``.  Catches ``"."``,
          ``" . "``, ``"orders."``, ``".id"``, ``"a.b.c"``,
          ``"1orders.id"``, ``"orders abc.id"``, and similar
          malformed forms.
    """
    parts = ref.split(".")
    if len(parts) != 2 or not all(_IDENTIFIER_RE.fullmatch(p) for p in parts):
        raise ValueError(
            f"ForeignKeyRef.ref='{ref}' must use 'table.column' "
            f"format with each half matching {_IDENTIFIER_RE.pattern!r} "
            f"(letters / digits / underscore, must not start with a digit)."
        )
    return parts[0], parts[1]


# Upper bound on ArrayColumn.max_length.  Each slot materialises its own
# Spark expression tree at plan time, so Catalyst work scales linearly
# with this value (and with nested arrays/structs, multiplicatively).
# 1000 is already far beyond realistic array columns; past that, the
# user wants rows or MapType.
_MAX_ARRAY_LENGTH = 1000

# Engine granularity for null-mask generation, derived from
# ``dbldatagen/core/engine/seed.py::NULL_PRECISION`` (the import above
# is the single source of truth).  ``null_fraction`` validators reject
# below-granularity values at plan time so a user who asks for
# ``null_fraction=1e-5`` with precision=10000 (which would give
# ``int(1e-5 * 10000) == 0`` and silently emit zero NULLs) gets the
# error next to the ``ColumnSpec`` declaration instead of deep inside
# the engine.
_MIN_NULL_FRACTION = 1.0 / NULL_PRECISION


class Uniform(_StrictModel):
    """Uniform distribution (default).

    Each value in the target range is equally likely.  This is the
    default for every strategy that accepts a ``distribution`` field.

    Attributes:
        type: Discriminator literal; always ``"uniform"``.  Set
          automatically; consumers should not pass it explicitly.
    """

    type: Literal["uniform"] = "uniform"


class Normal(_StrictModel):
    """Normal/Gaussian distribution.

    Samples are drawn from N(``mean``, ``stddev``) and then mapped onto
    the strategy's target range.  ``stddev >= 0`` is enforced at
    validation time.

    Attributes:
        type: Discriminator literal; always ``"normal"``.
        mean: Distribution mean.  Defaults to ``0.0``.
        stddev: Standard deviation.  Must be non-negative; ``0.0`` is
          accepted and produces a degenerate (constant) distribution.
          Defaults to ``1.0``.
    """

    type: Literal["normal"] = "normal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Normal:
        _require_finite("mean", self.mean, "Normal")
        _require_finite("stddev", self.stddev, "Normal")
        if self.stddev < 0:
            raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        return self


class LogNormal(_StrictModel):
    """Log-normal distribution.

    The natural-log of the samples is normally distributed.  Useful
    for heavy-tailed positive quantities (incomes, file sizes, page
    response times).  ``|mean|`` is capped at ``100`` because the
    engine computes ``math.exp(mean)`` on the driver to position the
    distribution's median; values past the cap would overflow or
    underflow double precision and leak ``inf`` / ``0`` into the
    Spark plan.

    Attributes:
        type: Discriminator literal; always ``"lognormal"``.
        mean: Mean of the underlying normal distribution.  Must be in
          ``[-100.0, 100.0]``.  Defaults to ``0.0``.
        stddev: Standard deviation of the underlying normal
          distribution.  Must be non-negative.  Defaults to ``1.0``.
    """

    type: Literal["lognormal"] = "lognormal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> LogNormal:
        _require_finite("mean", self.mean, "LogNormal")
        _require_finite("stddev", self.stddev, "LogNormal")
        if self.stddev < 0:
            raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        # ``lognormal_sample_expr`` computes ``math.exp(mean)`` on the
        # driver to position the distribution's median inside ``[0, n)``.
        # Python's ``math.exp`` overflows at mean >= 710 (raising
        # OverflowError at plan time) and underflows to 0 at
        # mean <= -745 (which divides the scale factor by zero and
        # leaks ``inf`` into the Spark plan as ``F.lit(inf)``).  Cap
        # well below both boundaries -- |mean| <= 100 covers every
        # realistic log-normal use (median factor of ~2.7e43 is already
        # absurd) while leaving a wide margin against float drift.
        if not -100.0 <= self.mean <= 100.0:
            raise ValueError(
                f"LogNormal.mean must be in [-100, 100], got {self.mean}.  "
                f"math.exp(mean) is computed on the driver and overflows "
                f"around mean=710 / underflows near mean=-745; this cap keeps "
                f"the scale factor finite and within double range."
            )
        return self


class Zipf(_StrictModel):
    """Zipfian / power-law distribution.

    Common for realistic cardinality skew: a small fraction of values
    (popular customers, hot keys, viral content) account for most of
    the mass.  The engine samples via inverse power-law CDF, which
    only converges for ``exponent > 1`` -- values at or below ``1``
    are rejected at validation rather than silently falling back to
    a different shape.

    Attributes:
        type: Discriminator literal; always ``"zipf"``.
        exponent: Power-law exponent.  Must be strictly greater than
          ``1.0``.  Smaller values (e.g. ``1.05``) give heavier tails
          (more skew toward the top values); larger values give
          milder skew approaching ``Uniform`` from above.  Defaults
          to ``1.5``.
    """

    type: Literal["zipf"] = "zipf"
    exponent: float = 1.5

    @model_validator(mode="after")
    def validate_params(self) -> Zipf:
        _require_finite("exponent", self.exponent, "Zipf")
        # Zipfian sampling via inverse power-law CDF converges only for
        # ``exponent > 1``.  The engine had a ``<= 1`` fallback that used
        # ``exp(log(n) * u) - 1`` -- not Zipf, not uniform, silently
        # wrong-shaped data for any caller who trusted the dtype.  Reject
        # at validation so the user picks an actual Zipf exponent or a
        # different distribution.
        if self.exponent <= 1.0:
            raise ValueError(
                f"Zipf.exponent must be > 1 (power-law CDF only converges "
                f"in that range), got {self.exponent}.  For milder skew use "
                f"``Normal`` or ``LogNormal``; for uniform, use ``Uniform``."
            )
        return self


class Exponential(_StrictModel):
    """Exponential distribution.

    Models inter-arrival times of memoryless events (e.g. customer
    sessions, web requests).  Samples are drawn with rate parameter
    ``rate`` and then mapped onto the strategy's target range.

    Attributes:
        type: Discriminator literal; always ``"exponential"``.
        rate: Rate parameter (``lambda``); the reciprocal is the
          distribution's mean.  Must be strictly positive.  Defaults
          to ``1.0``.
    """

    type: Literal["exponential"] = "exponential"
    rate: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Exponential:
        _require_finite("rate", self.rate, "Exponential")
        if self.rate <= 0:
            raise ValueError(f"rate must be > 0, got {self.rate}")
        return self


class WeightedValues(_StrictModel):
    """Explicit weighted selection from a list of values.

    Pair with ``ValuesColumn`` to give each discrete value its own
    probability mass.  Weights do not have to sum to ``1.0``; the
    engine normalises them.  ``WeightedValues`` is **not** accepted
    by strategies that sample from a continuous range
    (``RangeColumn``, ``TimestampColumn``) or by ``ForeignKeyRef``,
    because there is no value list to weight in those settings.

    Attributes:
        type: Discriminator literal; always ``"weighted"``.
        weights: Mapping of value (rendered as ``str`` for plan
          serialisation) to non-negative weight.  Must be non-empty
          and every weight must be ``>= 0``.  The keys are coerced
          back to the ``ValuesColumn.values`` element type at
          materialisation.
    """

    type: Literal["weighted"] = "weighted"
    weights: dict[str, float]

    @model_validator(mode="after")
    def validate_weights(self) -> WeightedValues:
        if not self.weights:
            raise ValueError("weights must not be empty")
        # NaN / Inf weights make ``sum(weights)`` and per-bucket
        # thresholds NaN; the engine's ``frac < threshold`` then
        # returns False for every row and every sample collapses to
        # the last value in the cumulative distribution.  Catch at
        # validation so the user sees the offending key.
        for key, weight in self.weights.items():
            _require_finite(f"weights[{key!r}]", weight, "WeightedValues")
        if any(w < 0 for w in self.weights.values()):
            raise ValueError("weights must be non-negative")
        return self


Distribution = Annotated[
    Uniform | Normal | LogNormal | Zipf | Exponential | WeightedValues,
    Field(discriminator="type"),
]


class RangeColumn(_StrictModel):
    """Generate values from a numeric range.

    Inclusive of both ``min`` and ``max``.  When ``step`` is set the
    output snaps to the lattice ``{min, min + step, min + 2*step, ...}``
    intersected with ``[min, max]``; otherwise samples are drawn
    continuously.  The integer-range path requires
    ``(max - min + 1) < 2**63`` so the size literal fits in signed
    int64.  ``WeightedValues`` is rejected on this strategy -- skewed
    numeric ranges should use ``Normal`` / ``LogNormal`` / ``Zipf`` /
    ``Exponential``.

    Attributes:
        strategy: Discriminator literal; always ``"range"``.
        min: Inclusive lower bound.  Mixed ``int``/``float`` allowed.
          Defaults to ``0``.
        max: Inclusive upper bound; must be ``>= min``.  Defaults to
          ``100``.
        step: Optional step granularity.  When set, must be strictly
          positive and produces lattice-snapped output.  When
          ``None`` (default), output is continuous within the range.
        distribution: Sampling distribution over the range.  Defaults
          to ``Uniform``.  ``WeightedValues`` is not accepted here.
    """

    strategy: Literal["range"] = "range"
    min: float | int = 0
    max: float | int = 100
    step: float | int | None = None
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_range(self) -> RangeColumn:
        if isinstance(self.distribution, WeightedValues):
            raise ValueError(
                "RangeColumn does not support WeightedValues.  WeightedValues "
                "weights a discrete value list; RangeColumn produces values "
                "from a continuous numeric range with no list to weight.  "
                "Use ValuesColumn for weighted categorical selection, or "
                "Zipf / Normal / LogNormal / Exponential to skew a numeric "
                "range."
            )
        # NaN comparisons short-circuit ({NaN} > {NaN}, {NaN} <= 0,
        # etc., all return False), so a RangeColumn(min=float('nan'),
        # max=float('nan')) would slip past every other check below
        # and ship to the engine, which then emits all-NaN output
        # under ANSI mode and ARITHMETIC_OVERFLOW errors otherwise.
        # Infinity is similar: the range-size and step math below
        # produces inf or nan downstream.
        for field_name in ("min", "max", "step"):
            value = getattr(self, field_name)
            if value is not None:
                _require_finite(field_name, value, "RangeColumn")
        if self.min > self.max:
            raise ValueError(f"min ({self.min}) must be <= max ({self.max})")
        if self.step is not None and self.step <= 0:
            raise ValueError(f"step must be > 0, got {self.step}")
        # The integer-range engine path computes ``range_size = max - min
        # + 1`` and passes it to ``F.lit``, which only accepts signed
        # int64 values.  A plan spanning the full int64 domain (e.g.
        # ``min=-2**63, max=2**63-1``) overflows ``range_size`` to
        # ``2**64`` and Py4J rejects the literal at query-build time.
        # Cap at ``2**63 - 1`` so any range that made it past the type
        # system also fits in the engine.
        if isinstance(self.min, int) and isinstance(self.max, int):
            range_size = self.max - self.min + 1
            if range_size >= 2**63:
                raise ValueError(
                    f"integer range size ({range_size}) >= 2**63 overflows "
                    f"F.lit's int64 bound.  Narrow [min, max] so "
                    f"(max - min + 1) < 2**63."
                )
        return self


class ValuesColumn(_StrictModel):
    """Pick from an explicit list of allowed values.

    The element type is whatever the caller puts into ``values``
    (strings, ints, booleans, ...); Pydantic preserves the literal
    types at validation.  When ``distribution`` is
    ``WeightedValues``, the engine cross-validates that every weight
    key matches a value in this list (and rejects unweighted-but-
    listed or weighted-but-not-listed values at plan construction).

    Attributes:
        strategy: Discriminator literal; always ``"values"``.
        values: Non-empty list of allowed values.  Element type is
          preserved through plan dump / reload.
        distribution: Sampling distribution.  Defaults to ``Uniform``
          (each value equally likely).  Use ``WeightedValues`` for
          explicit per-value probabilities; the weight keys must
          match the values in this list.
    """

    strategy: Literal["values"] = "values"
    values: list[Any]
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_values(self) -> ValuesColumn:
        if not self.values:
            raise ValueError("values list must not be empty")
        # Duplicates silently inflate the probability mass on the
        # repeated entry under Uniform (and create ambiguous keying
        # under WeightedValues since ``str(v)`` collides).  The user
        # almost certainly meant either ``WeightedValues`` (with
        # explicit weights) or distinct values; reject duplicates so
        # the intent is unambiguous at plan time.  Skip hashing
        # entirely when the happy path holds.
        try:
            seen_count = len(set(self.values))
        except TypeError:
            # Values may include unhashable types (lists, dicts).
            # Walk Counter on stringified form -- approximate but
            # surfaces the common case of duplicate dict literals
            # in YAML plans.
            seen_count = len({repr(v) for v in self.values})
        if seen_count != len(self.values):
            counter = Counter(repr(v) for v in self.values)
            dupes = sorted(name for name, count in counter.items() if count > 1)
            raise ValueError(
                f"ValuesColumn has duplicate entries: {dupes}.  Duplicates "
                f"silently double the probability mass under Uniform.  Use "
                f"distinct values, or switch to WeightedValues with explicit "
                f"weights."
            )
        # Cross-validate: WeightedValues.weights has str keys; the engine
        # does ``weights.get(str(v), 0.0)`` for each value, so any value
        # whose str() isn't a weight key silently contributes zero.  If
        # EVERY value is zero, the engine falls back to uniform without
        # a warning — the user's weighted distribution is silently
        # ignored.  Catch at plan time.
        if isinstance(self.distribution, WeightedValues):
            value_keys = {str(v) for v in self.values}
            missing = value_keys - set(self.distribution.weights)
            if missing:
                raise ValueError(
                    f"WeightedValues.weights is missing entries for values "
                    f"{sorted(missing)}.  Every value must have a corresponding "
                    f"weight (keyed by ``str(value)``), or the engine will "
                    f"silently assign zero weight and fall back to uniform."
                )
            if sum(self.distribution.weights.get(str(v), 0.0) for v in self.values) <= 0:
                raise ValueError(
                    "WeightedValues.weights sum to zero across the values "
                    "list — cumulative distribution is degenerate.  Assign "
                    "at least one positive weight."
                )
        return self


class FakerColumn(_StrictModel):
    """Generate data using a Faker provider method.

    Calls ``Faker(...).<provider>(**kwargs)`` per row via a pooled
    ``pandas_udf``.  ``faker`` is an optional dependency; install via
    ``pip install 'dbldatagen[core-faker]'``.  At materialisation the
    engine raises ``ImportError`` if the library is missing and
    ``ValueError`` if ``provider`` is not a known Faker method.

    Attributes:
        strategy: Discriminator literal; always ``"faker"``.
        provider: Faker provider method name (e.g. ``"first_name"``,
          ``"company"``, ``"ipv4"``).  Must be a real method on
          ``Faker``; checked at materialisation.
        kwargs: Keyword arguments forwarded to the provider.  Defaults
          to an empty mapping.
        locale: Optional Faker locale (e.g. ``"en_US"``,
          ``"de_DE"``).  When ``None`` (default), falls back to
          ``DataGenPlan.default_locale``.
    """

    strategy: Literal["faker"] = "faker"
    provider: str = Field(min_length=1)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    locale: str | None = None

    @field_validator("provider")
    @classmethod
    def _strip_provider(cls, value: str) -> str:
        # ``Field(min_length=1)`` admits ``"   "`` (3 chars, all
        # whitespace) -- the engine then calls ``getattr(Faker(),
        # "   ")`` which raises AttributeError deep in the pandas_udf.
        # Catch at plan time with a clear message.
        if not value.strip():
            raise ValueError(
                f"FakerColumn.provider={value!r} is whitespace-only.  "
                f"Set provider to a real Faker method name such as "
                f"'first_name', 'company', or 'ipv4'."
            )
        return value


class PatternColumn(_StrictModel):
    """Generate strings from a pattern template.

    Placeholders:
        {seq}     -- row sequence number (monotonic)
        {uuid}    -- deterministic UUID
        {alpha:N} -- N random alpha chars
        {digit:N} -- N random digits
        {hex:N}   -- N random hex chars
    Example: ``"ORD-{digit:4}-{alpha:3}"`` -> ``"ORD-3847-KMX"``

    The ``{uuid}`` placeholder is a fixed-width 36-character literal
    (no ``:N`` modifier).  Every ``{kind:N}`` placeholder has a
    per-kind width ceiling defined in
    ``dbldatagen/core/spec/_constants.py`` (``MAX_DIGIT_WIDTH``,
    ``MAX_HEX_WIDTH``, ``MAX_ALPHA_WIDTH``, ``MAX_SEQ_WIDTH``); the
    schema validator rejects templates exceeding any cap at plan
    time, and the engine imports the same constants so the
    accept-sets can't drift.  The digit/hex caps come from int64-fit
    requirements (``pmod(seed, base**width)``); the alpha/seq caps
    bound per-row Catalyst plan size and emitted string length.

    Attributes:
        strategy: Discriminator literal; always ``"pattern"``.
        template: Pattern string containing literal text and any of
          the placeholders above.
    """

    strategy: Literal["pattern"] = "pattern"
    template: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_template(self) -> PatternColumn:
        # ``Field(min_length=1)`` admits ``"   "``; reject
        # whitespace-only too -- a pure-whitespace template would
        # generate identical whitespace for every row, which is
        # almost certainly a user typo.
        if not self.template.strip():
            raise ValueError(
                f"PatternColumn.template={self.template!r} is whitespace-only.  "
                f"Templates need literal text and/or placeholders such as "
                f"'{{digit:4}}' or '{{alpha:3}}'."
            )
        # A template with no placeholders is effectively a
        # ConstantColumn -- every row emits the same literal string.
        # That's almost always a misuse of PatternColumn; redirect
        # the user.
        matches = list(PLACEHOLDER_RE.finditer(self.template))
        if not matches:
            raise ValueError(
                f"PatternColumn.template={self.template!r} contains no "
                f"placeholders.  PatternColumn requires at least one of "
                f"'{{seq}}', '{{uuid}}', '{{digit:N}}', '{{hex:N}}', "
                f"'{{alpha:N}}', or '{{seq:N}}'.  For a literal value on "
                f"every row, use ConstantColumn."
            )
        # Per-kind width caps from _constants.py.  These must match
        # what build_pattern_column rejects at materialisation
        # (engine/columns/string.py); the shared constants prevent
        # drift.  Catching here gives the user a single error message
        # naming all violations rather than one-at-a-time from the
        # engine.
        cap_by_kind = {
            "digit": MAX_DIGIT_WIDTH,
            "hex": MAX_HEX_WIDTH,
            "alpha": MAX_ALPHA_WIDTH,
            "seq": MAX_SEQ_WIDTH,
        }
        violations = []
        for match in matches:
            kind = match.group("kind")
            width_str = match.group("width")
            if width_str is None:
                continue
            # ``{uuid}`` is a fixed 36-character literal -- the engine
            # rejects any width modifier with a hard error.  Match the
            # engine here so the schema and engine accept-sets agree.
            if kind == "uuid":
                violations.append(
                    f"{{uuid:{width_str}}} -- uuid takes no width modifier "
                    f"(UUIDs are always 36 characters; use {{hex:N}} / "
                    f"{{alpha:N}} for a short random token)"
                )
                continue
            if kind not in cap_by_kind:
                continue
            width = int(width_str)
            cap = cap_by_kind[kind]
            # Width 0 silently emits a zero-character placeholder
            # (``F.lpad("0", 0, "0")`` -> "", ``pmod(seed, base**0)``
            # -> 0).  Almost certainly a user typo; reject so the
            # intent is unambiguous at plan time.
            if width < 1:
                violations.append(f"{{{kind}:{width}}} width must be >= 1")
            elif width > cap:
                violations.append(f"{{{kind}:{width}}} exceeds cap {cap}")
        if violations:
            raise ValueError(
                f"PatternColumn.template={self.template!r} has width-cap "
                f"violations: {'; '.join(violations)}.  Caps live in "
                f"dbldatagen/core/spec/_constants.py and are shared with "
                f"the engine."
            )
        return self


class SequenceColumn(_StrictModel):
    """Monotonic integer sequence.

    The sequence value at row ``i`` is ``start + i * step``.  ``step``
    may be negative for a descending sequence; only ``step == 0`` is
    rejected (it would produce a constant column; use
    ``ConstantColumn`` for that).  The owning ``TableSpec`` validates
    that ``start + (rows - 1) * step`` does not overflow int64.

    Attributes:
        strategy: Discriminator literal; always ``"sequence"``.
        start: Value at row ``0``.  Defaults to ``1``.
        step: Increment per row.  May be positive or negative; must
          not be ``0``.  Defaults to ``1``.
    """

    strategy: Literal["sequence"] = "sequence"
    start: int = 1
    step: int = 1

    @model_validator(mode="after")
    def validate_step(self) -> SequenceColumn:
        if self.step == 0:
            raise ValueError("step must not be 0")
        return self


class UUIDColumn(_StrictModel):
    """Deterministic UUID generation (v5 from seed + row index).

    Output is reproducible: the same ``(table seed, row index)``
    always yields the same UUID string.  Strategy has no parameters
    beyond the discriminator.

    Attributes:
        strategy: Discriminator literal; always ``"uuid"``.
    """

    strategy: Literal["uuid"] = "uuid"


class ExpressionColumn(_StrictModel):
    """Spark SQL expression referencing other columns in the same table.

    Example: ``"quantity * unit_price"`` or
    ``"concat(first_name, ' ', last_name)"``.  Expressions may
    reference any same-table column that is generated before this one
    in declaration order.

    Security note: Expressions are passed directly to ``F.expr()`` and
    can execute arbitrary Spark SQL.  Do not use ``ExpressionColumn``
    with untrusted plan YAML in multi-tenant environments.

    Attributes:
        strategy: Discriminator literal; always ``"expression"``.
        expr: A Spark SQL expression string.  Referenced column names
          must exist on the same ``TableSpec``; the planner validates
          this at ``resolve_plan`` time.
    """

    strategy: Literal["expression"] = "expression"
    expr: str = Field(min_length=1)

    @field_validator("expr")
    @classmethod
    def _reject_whitespace_expr(cls, value: str) -> str:
        # ``Field(min_length=1)`` admits ``"   "`` -- a 3-char,
        # all-whitespace expr that Spark's ``F.expr`` then rejects
        # with a parse error far from the column declaration.  Catch
        # at plan time with a clear message.
        if not value.strip():
            raise ValueError(
                f"ExpressionColumn.expr={value!r} is whitespace-only.  "
                f"Provide a real Spark SQL expression (e.g., "
                f"'quantity * unit_price' or "
                f"'concat(first_name, \" \", last_name)')."
            )
        return value


class TimestampColumn(_StrictModel):
    """Generate timestamps within a range.

    Output is session-timezone-independent: the engine routes
    ``start``/``end`` through UTC epoch before sampling so the same
    plan run on different cluster timezones produces byte-identical
    timestamps.  ``WeightedValues`` is rejected here; skewed time
    ranges should use ``Normal`` / ``LogNormal`` / ``Zipf`` /
    ``Exponential``.

    Attributes:
        strategy: Discriminator literal; always ``"timestamp"``.
        start: Inclusive lower bound, as an ISO-8601 string
          (``"YYYY-MM-DD"`` or ``"YYYY-MM-DD HH:MM:SS"``).  Required
          -- no universal default makes sense for a time range, so
          callers must specify their own bounds.
        end: Inclusive upper bound, same format as ``start``; must be
          ``>= start``.  Required.
        distribution: Sampling distribution over the time range.
          Defaults to ``Uniform``.  ``WeightedValues`` is not
          accepted here.
    """

    strategy: Literal["timestamp"] = "timestamp"
    start: str
    end: str
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_timestamps(self) -> TimestampColumn:
        parsed = {}
        for field_name in ("start", "end"):
            val = getattr(self, field_name)
            try:
                parsed[field_name] = datetime.fromisoformat(val)
            except ValueError:
                raise ValueError(
                    f"TimestampColumn.{field_name}='{val}' is not a valid ISO timestamp. "
                    f"Expected format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'."
                ) from None
        # Comparing a tz-naive datetime to a tz-aware one raises
        # ``TypeError`` from Python rather than the ``ValueError`` the
        # validator contract promises.  Force both bounds to the same
        # awareness; the user must pick one regime (naive=local time, or
        # aware with an explicit offset) instead of mixing.
        start_aware = parsed["start"].tzinfo is not None
        end_aware = parsed["end"].tzinfo is not None
        if start_aware != end_aware:
            raise ValueError(
                f"TimestampColumn bounds differ in tz-awareness: "
                f"start='{self.start}' ({'aware' if start_aware else 'naive'}), "
                f"end='{self.end}' ({'aware' if end_aware else 'naive'}).  "
                f"Use the same regime on both -- either both naive (e.g., "
                f"'2024-01-01T00:00:00') or both aware with an explicit "
                f"offset (e.g., '2024-01-01T00:00:00+00:00')."
            )
        if parsed["start"] > parsed["end"]:
            raise ValueError(f"start ({self.start}) must be <= end ({self.end})")
        if isinstance(self.distribution, WeightedValues):
            raise ValueError(
                "TimestampColumn does not support WeightedValues.  WeightedValues "
                "weights a discrete value list; TimestampColumn samples from a "
                "continuous time range.  Use Uniform / Normal / Zipf / Exponential / "
                "LogNormal to skew the timestamp distribution."
            )
        return self


class ConstantColumn(_StrictModel):
    """Every row gets the same value.

    Useful for environment markers (``"prod"``), version stamps, or
    null literals.  ``value`` is preserved literally through plan
    dump / reload.

    Attributes:
        strategy: Discriminator literal; always ``"constant"``.
        value: The literal value emitted on every row.  Any
          JSON-serialisable type.
    """

    strategy: Literal["constant"] = "constant"
    value: Any


class ForeignKeyColumn(_StrictModel):
    """Marker strategy for columns whose values are resolved from ``foreign_key``.

    The actual generation — dtype inference, distribution, null fraction,
    and the lookup into the parent table's PK — is driven by
    ``ColumnSpec.foreign_key`` at plan resolution time.  This strategy
    exists so FK columns have a real type in the ``ColumnStrategy`` union
    instead of a ``ConstantColumn(value=None)`` sentinel that would
    silently produce all-NULL output if FK resolution were ever skipped.

    Attributes:
        strategy: Discriminator literal; always ``"foreign_key"``.
    """

    strategy: Literal["foreign_key"] = "foreign_key"


class StructColumn(_StrictModel):
    """Group child columns into a Spark struct (nested object in JSON).

    Each field is a full ``ColumnSpec`` — supports range, values,
    faker, timestamps, and even nested structs.  Field names must be
    unique within the struct (validated at construction); the engine
    drives schema inference from the field types.

    Example JSON output::

        {"address": {"street": "123 Main St", "city": "Austin", "zip": "78701"}}

    Attributes:
        strategy: Discriminator literal; always ``"struct"``.
        fields: Ordered list of child ``ColumnSpec`` objects.  Field
          names must be unique within this struct.  Fields may be
          ``StructColumn`` themselves (nested structs) or
          ``ArrayColumn`` (array of records).
    """

    strategy: Literal["struct"] = "struct"
    # ``min_length=1`` -- an empty fields list builds ``F.struct()``
    # with zero children, which Spark types as ``struct<>`` (an
    # ambiguous nothing-struct that downstream schema inference
    # cannot resolve).  Reject at plan time.  Forward ref resolved
    # by ``model_rebuild()`` below.
    fields: list[ColumnSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_field_names(self) -> StructColumn:
        # ``TableSpec.validate_unique_column_names`` only covers top-level
        # columns; a struct with two children named ``x`` would
        # otherwise build an invalid ``StructType`` and every
        # ``.select("addr.x")`` against the result becomes ambiguous.
        field_names = [f.name for f in self.fields]
        # Happy path: ``set`` length differs only when duplicates exist.
        # Skips building a dupes list when validation passes.
        if len(set(field_names)) != len(field_names):
            dupes = sorted(name for name, count in Counter(field_names).items() if count > 1)
            raise ValueError(
                f"StructColumn has duplicate field names: {dupes}.  "
                f"Each field must have a unique name within the struct."
            )
        return self

    @model_validator(mode="after")
    def validate_field_strategies(self) -> StructColumn:
        # Parallel to ``ArrayColumn.validate_lengths``'s element-strategy
        # rejection: a struct child whose ``gen`` is ``FakerColumn`` or
        # ``ForeignKeyColumn`` cannot be materialised by
        # ``_build_struct_column`` -- Faker requires a column-level
        # ``pandas_udf`` (not per-cell) and FK resolution keys on the
        # top-level ``(table, column)`` pair with no meaning inside a
        # nested struct.  The engine would raise a RuntimeError far
        # from the declaration; catch at plan time instead.
        for field_spec in self.fields:
            if isinstance(field_spec.gen, (FakerColumn, ForeignKeyColumn)):
                raise ValueError(
                    f"StructColumn.fields[{field_spec.name!r}].gen="
                    f"{type(field_spec.gen).__name__} is not supported.  "
                    f"Faker requires a column-level pandas_udf and "
                    f"ForeignKeyColumn keys on the top-level (table, "
                    f"column) pair -- neither works inside a struct.  "
                    f"Use a scalar strategy (RangeColumn, ValuesColumn, "
                    f"PatternColumn, ConstantColumn, ExpressionColumn) "
                    f"for struct field generation."
                )
        return self


class ArrayColumn(_StrictModel):
    """Generate a variable-length array of values.

    Each element is produced from ``element`` with a different seed
    offset.  The array length per row is random in
    ``[min_length, max_length]``.  ``max_length`` is capped at
    ``1000`` because the engine materialises one Spark expression per
    array slot; larger fan-outs stall Catalyst.  ``FakerColumn`` and
    ``ForeignKeyColumn`` are rejected as direct element types (wrap
    them in a ``StructColumn`` if you need an array of records).

    Example JSON output::

        {"tags": ["electronics", "sale", "new"]}

    Attributes:
        strategy: Discriminator literal; always ``"array"``.
        element: Element-generation strategy.  Any
          ``ColumnStrategy`` *except* ``FakerColumn`` and
          ``ForeignKeyColumn``.  May itself be a ``StructColumn`` or
          ``ArrayColumn`` (nesting is supported).
        min_length: Inclusive minimum array length per row.  Must be
          ``>= 0``.  ``0`` is allowed for sometimes-empty arrays.
          Defaults to ``1``.
        max_length: Inclusive maximum array length per row.  Must be
          ``>= 1`` and ``>= min_length``, and ``<= 1000``.  Defaults
          to ``5``.
    """

    strategy: Literal["array"] = "array"
    element: ColumnStrategy  # forward ref resolved by model_rebuild() below
    min_length: int = 1
    max_length: int = 5

    @model_validator(mode="after")
    def validate_lengths(self) -> ArrayColumn:
        if self.min_length < 0:
            raise ValueError(f"min_length must be >= 0, got {self.min_length}")
        if self.min_length > self.max_length:
            raise ValueError(f"min_length ({self.min_length}) must be <= max_length ({self.max_length})")
        # ``max_length == 0`` always produces empty arrays, which hit
        # ``F.array()`` with no element exprs -- Spark types that as
        # ``array<nothing>`` / ``array<null>`` depending on version, and
        # downstream schema inference gets confused.  A user who wanted
        # "sometimes empty" sets ``min_length=0, max_length>=1``; a
        # user who wanted "never present" should omit the column.
        if self.max_length == 0:
            raise ValueError(
                "max_length must be >= 1 (max_length == 0 always produces "
                "empty arrays with an ambiguous element type).  Omit the "
                "column if you want no array at all, or set max_length >= 1 "
                "and let min_length=0 produce sometimes-empty arrays."
            )
        # ``_build_array_column`` materialises ``max_length`` element
        # expressions at plan time — one per array slot, each with its
        # own ``build_column_expr`` dispatch tree.  At ``max_length =
        # 5000`` Catalyst compiles for minutes on a plan the user thinks
        # is tiny.  1000 is already absurdly large for realistic arrays;
        # anything higher wants a different representation (MapType,
        # exploded rows) anyway.
        if self.max_length > _MAX_ARRAY_LENGTH:
            raise ValueError(
                f"max_length ({self.max_length}) exceeds {_MAX_ARRAY_LENGTH}.  "
                f"Each array element expands to its own Spark expression at "
                f"plan time; large max_lengths stall Catalyst.  For wider "
                f"fan-out, model the data as rows or a MapType."
            )
        # ``build_column_expr``'s dispatch table only covers the strategies
        # that can be materialised as a single Spark expression per row;
        # Faker requires a pandas_udf applied at the column level (not
        # per-element), and ForeignKeyColumn requires FK resolution keyed
        # on ``(table_name, top_level_col)`` which has no meaning for an
        # element inside an array.  Both would reach the ``raise
        # ValueError("Unsupported column strategy")`` in the dispatcher
        # at generation time -- reject at plan construction so the user
        # sees the limitation next to their ArrayColumn declaration.
        if isinstance(self.element, (FakerColumn, ForeignKeyColumn)):
            raise ValueError(
                f"ArrayColumn.element={type(self.element).__name__} is not "
                f"supported.  Faker requires a column-level pandas_udf (not "
                f"per-element), and ForeignKeyColumn keys on the top-level "
                f"(table, column) pair which has no meaning for an array "
                f"element.  Use ValuesColumn, RangeColumn, PatternColumn, "
                f"or another scalar strategy for array elements; nest a "
                f"StructColumn containing the FakerColumn / ForeignKeyColumn "
                f"if you need an array of records."
            )
        return self


ColumnStrategy = Annotated[
    RangeColumn
    | ValuesColumn
    | FakerColumn
    | PatternColumn
    | SequenceColumn
    | UUIDColumn
    | ExpressionColumn
    | TimestampColumn
    | ConstantColumn
    | ForeignKeyColumn
    | StructColumn
    | ArrayColumn,
    Field(discriminator="strategy"),
]


class DataType(str, Enum):
    """Output data types for ``ColumnSpec``, mapped to Spark SQL types.

    Members ``INT``, ``LONG``, ``FLOAT``, ``DOUBLE``, ``STRING``,
    ``BOOLEAN``, ``DATE``, ``TIMESTAMP``, and ``DECIMAL`` correspond to
    the equivalent Spark SQL types.  ``INTEGER`` is an alias for
    ``INT``; ``_missing_`` also accepts ``"integer"`` -> ``INT``,
    ``"bool"`` -> ``BOOLEAN``, and ``"str"`` -> ``STRING`` so YAML /
    JSON plans written with any of those spellings deserialise cleanly.
    """

    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"

    INTEGER = "int"

    @classmethod
    def _missing_(cls, value: object) -> DataType | None:
        """Accept common alternate spellings (``"integer"`` → INT, etc.)."""
        if not isinstance(value, str):
            return None
        aliases = {
            "integer": cls.INT,
            "bool": cls.BOOLEAN,
            "str": cls.STRING,
        }
        return aliases.get(value.lower())


class PrimaryKey(_StrictModel):
    """Marks a column (or set of columns) as the primary key.

    For single-column PKs, just set ``columns`` to a single-element list.
    Composite PKs are accepted but cannot currently be referenced by
    ``ForeignKeyRef`` (the planner rejects FK refs into a composite-PK
    table at plan time): ``ForeignKeyRef.ref`` is single-column, and a
    single sub-column of a composite PK doesn't uniquely identify a
    parent row.

    Attributes:
        columns: Names of the columns that form the primary key, in
          declaration order.  Each name must match a ``ColumnSpec.name``
          on the owning ``TableSpec``.  Must be non-empty and contain
          no duplicates.
    """

    columns: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_columns(self) -> PrimaryKey:
        # ``Field(min_length=1)`` rejects empty lists but not duplicates.
        # A composite PK with a repeated column name has no well-defined
        # tuple identity, and the downstream FK planner keys resolutions
        # on ``(table, column)`` — a typo'd ``PrimaryKey(columns=["a", "a"])``
        # silently produces a single FK resolution and no signal back
        # to the user.  Reject at plan time, same shape as
        # ``StructColumn.validate_unique_field_names``.
        if len(set(self.columns)) != len(self.columns):
            dupes = sorted(name for name, count in Counter(self.columns).items() if count > 1)
            raise ValueError(
                f"PrimaryKey has duplicate column names: {dupes}.  "
                f"Each column may appear at most once in a composite key."
            )
        return self


class ForeignKeyRef(_StrictModel):
    """Defines a foreign key relationship to another table's primary key.

    ``ref`` uses the familiar "table.column" syntax.  Use ``distribution``
    (e.g. ``Zipf``) to skew child-to-parent mapping toward a small set of
    parents; use ``null_fraction`` for optional FKs.

    Attributes:
        ref: Reference to the parent PK in ``"table.column"`` form.  The
          parent table must exist in the same ``DataGenPlan``, the
          named column must be part of that table's ``primary_key``,
          and that ``primary_key`` must be single-column -- composite
          parent PKs are rejected at plan time (see ``PrimaryKey``).
        distribution: Sampling distribution over the parent row index
          range.  Defaults to ``Uniform`` (every parent row equally
          likely).  ``WeightedValues`` is rejected here -- it weights a
          discrete value list, not an integer index range.
        nullable: Whether the FK column is declared nullable in the
          resulting Spark schema.  Independent of ``null_fraction``,
          which drives runtime NULL injection.
        null_fraction: Probability in ``[0.0, 1.0]`` that a given child
          row leaves the FK unresolved (emits NULL instead of a parent
          reference).  Use ``0.0`` for mandatory FKs.  Values below the
          engine's null-mask granularity raise at validation.
    """

    ref: str
    distribution: Distribution = Uniform()
    nullable: bool = False
    null_fraction: float = 0.0

    @model_validator(mode="after")
    def validate_ref_format(self) -> ForeignKeyRef:
        # ``parse_fk_ref`` is the single source of truth for the
        # ``table.column`` format; the planner uses the same helper to
        # avoid drift between schema validation and downstream parsing.
        parse_fk_ref(self.ref)
        if not 0.0 <= self.null_fraction <= 1.0:
            raise ValueError(f"null_fraction must be in [0.0, 1.0], got {self.null_fraction}")
        if 0.0 < self.null_fraction < _MIN_NULL_FRACTION:
            raise ValueError(
                f"ForeignKeyRef.null_fraction={self.null_fraction} is below the "
                f"engine's {_MIN_NULL_FRACTION} granularity; ``int(f * N)`` would "
                f"round to zero and silently emit zero NULLs.  Pick a larger "
                f"fraction (>= {_MIN_NULL_FRACTION}) or raise NULL_PRECISION in "
                f"engine/seed.py."
            )
        if isinstance(self.distribution, WeightedValues):
            raise ValueError(
                "ForeignKeyRef does not support WeightedValues.  WeightedValues "
                "weights a discrete value list; FK skewing operates over the "
                "implicit parent-row index range, not a list.  Use Zipf "
                "(default for FK) or other continuous distributions to skew "
                "child references toward a small set of parents."
            )
        return self


class ColumnSpec(_StrictModel):
    """A single column in a table.

    At minimum, specify ``name`` and ``gen`` (the value-generation
    strategy).  ``dtype`` is inferred from ``gen`` when not set
    explicitly; the DSL helpers in ``dbldatagen.core.spec.dsl`` are the
    recommended way to build a ``ColumnSpec`` without having to spell
    out ``DataType`` directly.

    Attributes:
        name: Column name.  Must be a valid identifier (``[A-Za-z_]
          [A-Za-z0-9_]*``) and must not exactly match any name in
          ``_RESERVED_INTERNAL_COLUMN_NAMES`` (currently
          ``{"_synth_row_id"}``).  Other leading-underscore names
          (``_modified_at``, ``_partition_key``, ``_id``, ...) are
          accepted -- many real-world systems emit columns in that
          shape.
        dtype: Optional Spark ``DataType`` for the column.  When
          ``None``, the engine infers it from ``gen``.  Set explicitly
          when the strategy's natural output dtype is not what you
          want (e.g. ``DataType.DATE`` on a timestamp-shaped
          strategy).
        gen: The value-generation strategy for this column.  Use a
          DSL helper (``integer``, ``text``, ``faker``, ...) or
          instantiate a strategy directly (``RangeColumn``,
          ``ValuesColumn``, ...).
        nullable: Whether the column is declared nullable in the
          resulting Spark schema.  Independent of ``null_fraction``,
          which drives runtime NULL injection.
        null_fraction: Probability in ``[0.0, 1.0]`` that a given row
          emits ``NULL`` for this column.  Values below the engine's
          null-mask granularity raise at validation rather than
          silently rounding to zero.
        foreign_key: If set, this column is generated by resolving an
          FK reference to another table's primary key.  The strategy
          on ``gen`` is overridden by FK resolution at materialisation.
        seed_from: If set, the per-cell seed for this column is
          derived from another column on the same table rather than
          from the column's own name.  Useful for correlating
          generated values across columns (e.g. ``city`` ↔ ``zip``).
        precision: Total digit count for ``DataType.DECIMAL`` columns.
          Set together with ``scale``; falls back to
          ``(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)`` from
          ``dbldatagen.core.spec._constants`` when both are unset
          (matches Spark's ``DecimalType()`` default).  Rejected on
          non-DECIMAL dtypes.
        scale: Fractional digit count for ``DataType.DECIMAL`` columns.
          Set together with ``precision``.
    """

    # Inherits extra="forbid" from _StrictModel; extend with
    # populate_by_name so YAML plans can use either camelCase or
    # snake_case field keys.
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    dtype: DataType | None = None
    gen: ColumnStrategy
    nullable: bool = False
    null_fraction: float = 0.0
    foreign_key: ForeignKeyRef | None = None
    seed_from: str | None = None
    precision: int | None = None
    scale: int | None = None

    @model_validator(mode="after")
    def validate_column_name(self) -> ColumnSpec:
        """Reject column names that would collide with engine-internal metadata.

        The name must be a valid SQL / Python-like identifier
        (``[A-Za-z_][A-Za-z0-9_]*``) so it round-trips through Spark
        without backtick quoting, AND must not exactly match any
        name in ``_RESERVED_INTERNAL_COLUMN_NAMES``.

        Earlier versions rejected every leading-underscore name to
        avoid accidental shadowing; that rule turned out to be too
        strict -- many real customer / upstream-system columns
        legitimately begin with ``_`` (``_modified_at``, ``_id``,
        ``_partition_key``).  The narrower exact-match check
        preserves the anti-shadowing guarantee for the names the
        engine actually adds while accepting the customer pattern.
        """
        # fullmatch, not match: ``match`` + ``$`` allows a trailing ``\n``
        # because ``$`` anchors before a newline in default mode.
        if not _IDENTIFIER_RE.fullmatch(self.name):
            raise ValueError(
                f"Column name '{self.name}' is not a valid identifier " f"(must match [A-Za-z_][A-Za-z0-9_]*)."
            )
        if self.name in _RESERVED_INTERNAL_COLUMN_NAMES:
            raise ValueError(
                f"Column name '{self.name}' collides with an engine-internal "
                f"metadata column.  Reserved names: "
                f"{sorted(_RESERVED_INTERNAL_COLUMN_NAMES)}.  Rename to avoid "
                f"silent shadowing."
            )
        return self

    @model_validator(mode="after")
    def validate_decimal_precision_scale(self) -> ColumnSpec:
        """Precision/scale are only meaningful for DECIMAL and must go together.

        When both are unset on a DECIMAL column, the engine falls back to
        Spark's ``DecimalType()`` default of ``(10, 0)``.
        """
        has_precision = self.precision is not None
        has_scale = self.scale is not None
        if not (has_precision or has_scale):
            return self
        if self.dtype != DataType.DECIMAL:
            raise ValueError(
                f"Column '{self.name}': precision/scale are only valid when " f"dtype=DECIMAL, got dtype={self.dtype}"
            )
        if has_precision and not has_scale:
            raise ValueError(
                f"Column '{self.name}': precision={self.precision} was set but "
                f"scale is None.  DECIMAL requires both; set scale explicitly "
                f"(must be in [0, precision])."
            )
        if has_scale and not has_precision:
            raise ValueError(
                f"Column '{self.name}': scale={self.scale} was set but precision "
                f"is None.  DECIMAL requires both; set precision explicitly "
                f"(must be in [scale, 38])."
            )
        # Narrow ``int | None`` for mypy: the two checks above together
        # guarantee both are set when we get here, but mypy doesn't
        # track local ``has_*`` flags back to the attributes.  Raise
        # (not assert) so a future refactor that drops either check
        # above doesn't let ``None`` flow through silently under
        # ``python -O``.
        precision = self.precision
        scale = self.scale
        if precision is None or scale is None:
            raise RuntimeError(
                f"Column '{self.name}': precision/scale narrowing invariant "
                f"violated (precision={precision}, scale={scale}).  The "
                f"precision-and-scale checks above should have raised; "
                f"reaching this line means the preceding guards were removed."
            )
        # Spark DecimalType: 1 <= precision <= 38, 0 <= scale <= precision.
        if not 1 <= precision <= 38:
            raise ValueError(f"Column '{self.name}': precision must be in [1, 38], got {precision}")
        if not 0 <= scale <= precision:
            raise ValueError(f"Column '{self.name}': scale must be in [0, precision] (0..{precision}), got {scale}")
        # Range fit: catch at plan time rather than deferring to Spark's
        # ARITHMETIC_OVERFLOW / NUMERIC_VALUE_OUT_OF_RANGE at materialization.
        # Max magnitude representable in DecimalType(p, s) is just under
        # 10**(p-s); anything >= that overflows after cast.  Only checked
        # when precision/scale are explicit — the None/None default path
        # defers to Spark's DecimalType(10, 0) and inherits its rounding.
        # Walk every ColumnStrategy member that can carry a numeric
        # value -- not just RangeColumn / ValuesColumn.  Skipping a
        # member silently passes overflowing values through to
        # NUMERIC_VALUE_OUT_OF_RANGE deep in a Spark job.  Members
        # exempted by design (FakerColumn -> StringType, Pattern /
        # UUID / Struct / Array -> non-numeric, Timestamp -> timestamp,
        # ForeignKey -> inherits parent type, Expression -> raw SQL
        # that can't be statically evaluated) fall through to the
        # ``return self`` at the end with no check.
        limit = 10 ** (precision - scale)
        max_repr = limit - 10**-scale
        if isinstance(self.gen, RangeColumn):
            max_abs = max(abs(self.gen.min), abs(self.gen.max))
            if max_abs >= limit:
                raise ValueError(
                    f"Column '{self.name}': range [{self.gen.min}, {self.gen.max}] "
                    f"does not fit in decimal({precision}, {scale}) "
                    f"(max representable magnitude is {max_repr})"
                )
        elif isinstance(self.gen, ValuesColumn):
            # Walk numeric entries once; non-numeric values are skipped
            # (a string-typed ValuesColumn paired with dtype=DECIMAL is
            # a different misconfiguration handled elsewhere).
            numeric_values = [v for v in self.gen.values if isinstance(v, (int, float, Decimal))]
            if numeric_values:
                # Two-pass: max() over absolute values keeps mypy happy
                # about the comparator type, then re-find the original
                # (signed) value for the error message.  ``float`` cast
                # gives a uniform comparable type across int / float /
                # Decimal entries -- precision loss is irrelevant for
                # the magnitude-fit check.
                max_abs_value = max(abs(float(v)) for v in numeric_values)
                if max_abs_value >= limit:
                    offending = next(v for v in numeric_values if abs(float(v)) == max_abs_value)
                    raise ValueError(
                        f"Column '{self.name}': value {offending} in ValuesColumn "
                        f"does not fit in decimal({precision}, {scale}) "
                        f"(max representable magnitude is {max_repr})"
                    )
        elif isinstance(self.gen, SequenceColumn):
            # Sequence span requires ``rows`` from the owning TableSpec,
            # which isn't visible here (ColumnSpec doesn't see its
            # parent).  ``TableSpec.validate_sequence_column_overflow`` (below) does
            # the row-aware fit check; document the cross-cutting
            # responsibility so this method's reader knows it's covered.
            pass
        elif isinstance(self.gen, ConstantColumn):
            value = self.gen.value
            if isinstance(value, (int, float, Decimal)):
                # ``abs(float('nan')) >= limit`` is False, so NaN/Inf
                # values bypass the magnitude check below and ship to
                # the engine as ``F.lit(nan)`` -- catch them here so the
                # error names the constant.
                _require_finite("value", float(value), "ConstantColumn")
                if abs(float(value)) >= limit:
                    raise ValueError(
                        f"Column '{self.name}': constant value {value} "
                        f"does not fit in decimal({precision}, {scale}) "
                        f"(max representable magnitude is {max_repr})"
                    )
        elif isinstance(self.gen, ForeignKeyColumn):
            # FK columns inherit their dtype + precision/scale from the
            # parent's PK at plan resolution time.  Fit-check is the
            # parent's responsibility -- the child carries no value
            # space of its own to range-check.  Explicit branch (rather
            # than falling through silently) so the next reader knows
            # this exemption is intentional.
            pass
        # ExpressionColumn: raw SQL evaluated by Spark at run time --
        # we have no static expression value to fit-check.  Any
        # overflow surfaces as NUMERIC_VALUE_OUT_OF_RANGE at
        # materialisation; the user wrote the SQL and accepts that.
        # Other strategies (Faker/Pattern/UUID/Struct/Array/Timestamp)
        # don't produce numeric values that need a fit check here.
        return self

    @model_validator(mode="after")
    def validate_date_dtype_strategy(self) -> ColumnSpec:
        # ``DataType.DATE`` is only dispatched by the engine when paired
        # with ``TimestampColumn`` (``generator.py:549`` routes to
        # ``build_date_column``).  Strategies whose output type is
        # fixed and non-date (RangeColumn integers, PatternColumn /
        # SequenceColumn / UUIDColumn strings/ints) silently get their
        # native type cast and the ``DATE`` hint is dropped without
        # warning -- produces integers or strings where the user asked
        # for a date.
        #
        # ConstantColumn, ValuesColumn, ExpressionColumn, and
        # ForeignKeyColumn can all legitimately carry dates (a
        # user-typed date literal, a list of date values, a CAST in
        # SQL, or inherited from a date-typed parent PK), so they're
        # not flagged here.
        #
        # FakerColumn is NOT allowed with ``dtype=DATE``: the Faker
        # pool pandas_udf hardcodes ``StringType`` and ``str(val)``
        # stringifies every pool entry before the UDF ships, so even a
        # ``date_of_birth`` provider returns a string column regardless
        # of the declared dtype.  Leaving Faker+DATE through was a
        # silent type-declaration lie.
        # StructColumn and ArrayColumn produce composite Spark types
        # (struct<...>, array<element>) that cannot be coerced to DATE
        # at any cast; pairing them with ``dtype=DATE`` is a silent
        # type-declaration lie -- the resulting column ships as a
        # struct/array regardless of the requested DATE, and the user's
        # declared dtype never matches the materialised schema.
        _non_date_strategies = (
            RangeColumn,
            PatternColumn,
            SequenceColumn,
            UUIDColumn,
            FakerColumn,
            StructColumn,
            ArrayColumn,
        )
        if self.dtype == DataType.DATE and isinstance(self.gen, _non_date_strategies):
            if isinstance(self.gen, FakerColumn):
                type_word = "strings (the Faker pool stringifies every value)"
            elif isinstance(self.gen, (RangeColumn, SequenceColumn)):
                type_word = "integers"
            elif isinstance(self.gen, StructColumn):
                type_word = "a struct<...> composite (no DATE cast exists)"
            elif isinstance(self.gen, ArrayColumn):
                type_word = "an array<element> composite (no DATE cast exists)"
            else:
                type_word = "strings"
            raise ValueError(
                f"Column '{self.name}': dtype=DATE is not compatible with "
                f"{type(self.gen).__name__} (which produces {type_word}).  "
                f"Use TimestampColumn for a deterministic random date, or drop "
                f"``dtype=DATE`` to keep the strategy's native type."
            )
        return self

    @model_validator(mode="after")
    def validate_faker_dtype_compatibility(self) -> ColumnSpec:
        """Reject ``FakerColumn`` paired with a dtype the pool can't honour.

        Faker's pool ``pandas_udf`` outputs ``StringType``, and every
        pool entry is stringified via ``str(val)`` before the UDF
        ships.  Any declared dtype other than ``STRING`` (or ``None`` --
        engine-default STRING) would be a lie about the resulting
        column.  ``DATE`` is rejected here too: even a
        ``date_of_birth`` provider returns a string after the pool's
        ``str(val)`` step.  Catch the mismatch at plan time rather
        than let the column ship with a declared type the engine
        can't deliver.
        """
        if not isinstance(self.gen, FakerColumn):
            return self
        if self.dtype is None or self.dtype == DataType.STRING:
            return self
        raise ValueError(
            f"Column '{self.name}': FakerColumn always produces StringType "
            f"(the pool stringifies each value via ``str(val)``); declared "
            f"dtype={self.dtype.value} is incompatible.  Drop ``dtype`` "
            f"(or set ``dtype=DataType.STRING``) and cast downstream if you "
            f"need a different type."
        )

    @model_validator(mode="after")
    def validate_null_fraction(self) -> ColumnSpec:
        # Validators must not mutate ``self`` -- the engine drives null
        # injection off ``null_fraction`` directly, and a side-effect
        # that flips ``nullable`` makes round-tripping
        # ``dump -> validate`` non-idempotent (the dumped model has
        # ``nullable=True`` that the user never typed).  The two fields
        # are orthogonal: ``null_fraction`` controls how many cells get
        # nulled at generation time, ``nullable`` is downstream schema
        # metadata that the user sets if they want it.
        if not 0.0 <= self.null_fraction <= 1.0:
            raise ValueError(f"null_fraction must be in [0.0, 1.0], got {self.null_fraction}")
        if 0.0 < self.null_fraction < _MIN_NULL_FRACTION:
            raise ValueError(
                f"Column '{self.name}': null_fraction={self.null_fraction} is below "
                f"the engine's {_MIN_NULL_FRACTION} granularity; ``int(f * N)`` "
                f"would round to zero and silently emit zero NULLs.  Pick a larger "
                f"fraction (>= {_MIN_NULL_FRACTION}) or raise NULL_PRECISION in "
                f"engine/seed.py."
            )
        return self

    @model_validator(mode="after")
    def validate_foreign_key_strategy(self) -> ColumnSpec:
        # Enforce both directions of the ForeignKeyColumn <-> foreign_key
        # invariant.  Without this, a plan with one side set and the other
        # missing would either silently emit all-NULL (gen=FK, no ref) or
        # ignore the FK and use the other strategy (ref set, gen=anything
        # else); both are easy to miss at review time.
        if isinstance(self.gen, ForeignKeyColumn) and self.foreign_key is None:
            raise ValueError(
                f"Column '{self.name}' uses ForeignKeyColumn strategy but has no "
                f"foreign_key set. Use the fk() helper, or set foreign_key=ForeignKeyRef(...)."
            )
        if self.foreign_key is not None and not isinstance(self.gen, ForeignKeyColumn):
            raise ValueError(
                f"Column '{self.name}' has foreign_key set but gen is "
                f"{type(self.gen).__name__}. FK columns must use ForeignKeyColumn "
                f"(or the fk() DSL helper)."
            )
        # null_fraction can be set on ColumnSpec (like every other column
        # kind) or on the ForeignKeyRef.  Allow either source but reject
        # disagreeing non-zero values so the user intent is unambiguous;
        # planner.py resolves to ``max(...)`` when both agree.
        if (
            self.foreign_key is not None
            and self.null_fraction > 0
            and self.foreign_key.null_fraction > 0
            and self.null_fraction != self.foreign_key.null_fraction
        ):
            raise ValueError(
                f"Column '{self.name}': null_fraction is set on both "
                f"ColumnSpec ({self.null_fraction}) and ForeignKeyRef "
                f"({self.foreign_key.null_fraction}) with different values. "
                f"Set it in exactly one place, or use the same value in both."
            )
        return self


class TableSpec(_StrictModel):
    """Defines one table to generate.

    ``rows`` can be an int or a string like ``"10M"``, ``"1B"`` for
    readability; both forms are normalised to ``int`` at validation
    time.

    Attributes:
        name: Table name.  Must be a valid identifier
          (``[A-Za-z_][A-Za-z0-9_]*``).  Used as the key in the
          ``dict`` returned by ``generate()`` and as the parent name
          in ``ForeignKeyRef.ref`` ("``<name>.<column>``").
        columns: Ordered list of ``ColumnSpec`` describing every
          column to generate.  Column names must be unique within the
          table.
        rows: Number of rows to generate.  Accepts an ``int`` or a
          human-readable string (``"10K"``, ``"5M"``, ``"1B"``); the
          string form is parsed to ``int`` during validation.  Must be
          positive.
        primary_key: Optional ``PrimaryKey`` marking which columns
          form the table's PK.  Required when another table FKs into
          this one; optional otherwise.
        seed: Per-table seed.  Usually left as ``None`` and propagated
          from ``DataGenPlan.seed`` during plan validation.  Set
          explicitly only when generating a single ``TableSpec`` via
          ``generate_table`` outside a plan.
    """

    name: str
    # ``min_length=1`` -- a TableSpec with no columns has nothing to
    # generate; downstream ``build_all_column_exprs`` ``select(*[])``
    # produces an empty-schema DataFrame with no signal that the user's
    # plan was incomplete.  Reject up front.
    columns: list[ColumnSpec] = Field(min_length=1)
    rows: int | str
    primary_key: PrimaryKey | None = None
    seed: int | None = None

    @model_validator(mode="after")
    def resolve_row_count(self) -> TableSpec:
        """Normalise the user-facing ``int | str`` ``rows`` to ``int``.

        ``rows`` is declared ``int | str`` so users can write
        ``rows="10K"`` for readability.  This validator runs after
        Pydantic's type check, parses the string form via
        ``parse_human_count`` (which also returns ``int`` unchanged
        for ``int`` input), and stores the result back on ``self.rows``
        so all downstream validators / consumers see an ``int``.

        Engine call sites read ``self.rows`` via ``typing.cast(int, ...)``
        rather than runtime ``int(...)`` coercion: the validator is the
        single normalisation point, and a ``cast`` documents "trust
        the validator" without re-running the conversion at every read.
        """
        self.rows = TableSpec.parse_human_count(self.rows)
        if self.rows <= 0:
            raise ValueError(f"rows must be > 0, got {self.rows}")
        return self

    @model_validator(mode="after")
    def validate_name(self) -> TableSpec:
        # fullmatch, not match: ``re.match`` + ``$`` allows a trailing
        # newline because ``$`` anchors before a newline in default mode.
        # ``fullmatch`` requires the whole string to match the pattern.
        if not _IDENTIFIER_RE.fullmatch(self.name):
            raise ValueError(
                f"Table name '{self.name}' is not a valid identifier.  " f"Must match [a-zA-Z_][a-zA-Z0-9_]*."
            )
        return self

    @model_validator(mode="after")
    def validate_unique_column_names(self) -> TableSpec:
        """Reject duplicate column names within a table.

        Two ColumnSpecs with the same ``name`` would produce a Spark
        DataFrame with duplicate column names — later ``.select(...)``
        / ``.withColumn(...)`` operations silently shadow one, and
        SQL round-trip through Delta / Unity Catalog breaks.  Catch
        at plan construction so the failure names the offender.
        """
        names = [c.name for c in self.columns]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(
                f"Table '{self.name}' has duplicate column names: "
                f"{sorted(duplicates)}.  Each ColumnSpec must have a "
                f"unique ``name`` within its table."
            )
        return self

    @model_validator(mode="after")
    def validate_primary_key_columns_exist(self) -> TableSpec:
        """Reject ``PrimaryKey.columns`` that name columns missing from
        ``columns``.

        Without this check, ``PrimaryKey(columns=["does_not_exist"])``
        constructs successfully and only ``planner._validate_primary_keys``
        catches the typo at plan-resolve time.  Direct ``generate_table``
        callers (outside a ``DataGenPlan``) get no model-layer signal --
        the engine then fails with a confusing missing-column reference
        far from the offending declaration.  Catch at TableSpec
        construction so the error names the typo'd column and the
        available columns.
        """
        if self.primary_key is None:
            return self
        column_names = {c.name for c in self.columns}
        missing = [pk_col for pk_col in self.primary_key.columns if pk_col not in column_names]
        if missing:
            raise ValueError(
                f"Table '{self.name}' primary_key references columns "
                f"that don't exist: {missing}.  Available columns: "
                f"{sorted(column_names)}."
            )
        return self

    @staticmethod
    def parse_human_count(value: int | str) -> int:
        """Parses a row-count value into an ``int``.

        Accepts an already-typed ``int`` (returned unchanged) or a
        human-readable string with a ``K`` / ``M`` / ``B`` suffix
        (case-insensitive).  Used internally by ``resolve_row_count``
        to normalise the user-facing ``rows`` field before downstream
        validators run.

        Args:
            value: Either an ``int`` row count, or a string like
              ``"10K"``, ``"5M"``, ``"1.5B"``.  Fractional multipliers
              are allowed; the result is truncated to ``int``.

        Returns:
            The row count as an ``int``.  For ``int`` input, the input
            unchanged; for string input, the parsed magnitude.

        Raises:
            ValueError: ``value`` is a string that does not parse to
              an ``int`` after suffix stripping.
        """
        if isinstance(value, int):
            return value
        suffixes = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
        s = str(value).strip().upper()
        # Single try/except covers both branches so a malformed suffix
        # mantissa ('K' / '1.5.0K') gets the same friendly wrap as a
        # malformed plain-int input ('abc'); previously only the plain-int
        # path was wrapped and the float() call below leaked the raw
        # "could not convert string to float" message.
        try:
            for suffix, mult in suffixes.items():
                if s.endswith(suffix):
                    return int(float(s[: -len(suffix)]) * mult)
            return int(s)
        except ValueError:
            raise ValueError(
                f"Invalid row count '{value}'. Expected an integer or a "
                f"human-readable string like '10K', '1M', '1B'."
            ) from None

    @model_validator(mode="after")
    def validate_sequence_column_overflow(self) -> TableSpec:
        """Reject ``SequenceColumn`` configurations that would overflow
        int64 at the last row.

        The engine computes PK values as ``id * step + start`` in
        ``pk.py``, cast to long.  Under Spark ANSI mode (default on
        Databricks), ``(rows-1) * step + start`` exceeding
        ``2**63 - 1`` raises ``ARITHMETIC_OVERFLOW`` deep inside the
        Spark job with no pointer back to the offending column.  At
        realistic scales (500M-3B rows with unit step) there's no
        overflow path; the trap only fires on adversarial
        ``(step, start, rows)`` combinations.  Validate here so the
        error names the column and the magnitude, instead of
        surfacing as an opaque runtime Spark error.
        """
        long_max = 2**63 - 1
        long_min = -(2**63)
        # ``rows`` is declared ``int | str`` on the user-facing model;
        # ``resolve_row_count`` normalised it to ``int`` above (validator
        # ordering: resolve_row_count is declared before this).  Cast
        # once for mypy; no runtime work.
        rows = cast(int, self.rows)
        for col_spec in self.columns:
            if not isinstance(col_spec.gen, SequenceColumn):
                continue
            start = col_spec.gen.start
            step = col_spec.gen.step
            # The last emitted value is ``start + (rows - 1) * step``.
            # Check both bounds: with a positive step the max is at
            # row_count - 1; with a negative step the min is.
            last_val = start + (rows - 1) * step
            if last_val > long_max or last_val < long_min:
                raise ValueError(
                    f"Table '{self.name}' column '{col_spec.name}': "
                    f"SequenceColumn(start={start}, step={step}) over "
                    f"{self.rows} rows would emit {last_val} at the last "
                    f"row, which overflows int64 ([{long_min}, {long_max}]).  "
                    f"Pick a smaller ``step`` or ``start`` (or reduce "
                    f"``rows``)."
                )
            # DECIMAL bounds fit: lands the failure at plan time
            # instead of as a NUMERIC_VALUE_OUT_OF_RANGE at cast time
            # in Spark.  Only when precision/scale are explicit -- the
            # None/None default defers to Spark's DecimalType(10, 0)
            # and inherits its rounding, matching the RangeColumn
            # behavior in ``ColumnSpec.validate_decimal_precision_scale``.
            if col_spec.dtype == DataType.DECIMAL and col_spec.precision is not None and col_spec.scale is not None:
                decimal_limit = 10 ** (col_spec.precision - col_spec.scale)
                # start is the magnitude-max when step is negative;
                # last_val is the magnitude-max when step is positive.
                max_abs = max(abs(start), abs(last_val))
                if max_abs >= decimal_limit:
                    max_repr = decimal_limit - 10**-col_spec.scale
                    raise ValueError(
                        f"Table '{self.name}' column '{col_spec.name}': "
                        f"SequenceColumn(start={start}, step={step}) over "
                        f"{self.rows} rows would emit {last_val} at the last "
                        f"row, which does not fit in "
                        f"decimal({col_spec.precision}, {col_spec.scale}) "
                        f"(max representable magnitude is {max_repr})."
                    )
        return self


class DataGenPlan(_StrictModel):
    """Top-level plan describing all tables to generate.

    Tables are generated in dependency order (FK references resolved
    automatically).

    ``seed`` defaults to 42 for tutorial / demo convenience.  This is a
    known reproducibility trap: two independent callers who both omit
    ``seed`` will produce byte-identical data without realizing the
    reproduction is coincidental.  Constructing a ``DataGenPlan``
    without an explicit ``seed`` emits a ``UserWarning`` via the
    ``_warn_if_seed_missing`` validator so CI / log scrape catches the
    omission.  Production callers should always pass ``seed=<int>``.

    Attributes:
        tables: The ``TableSpec`` objects to generate.  FK references
          between tables are resolved at ``resolve_plan`` time; cycles
          are rejected.
        seed: Global seed for the plan.  Propagated to each
          ``TableSpec`` whose own ``seed`` is unset (table ``i`` gets
          ``seed + i``).  Defaults to ``42`` with a ``UserWarning``;
          set explicitly in production callers.
        default_locale: Default ``Faker`` locale (e.g. ``"en_US"``)
          used by ``FakerColumn`` strategies that do not override it
          locally.
    """

    # ``min_length=1`` -- a plan with no tables has nothing to
    # generate and is almost certainly a user error (e.g., YAML
    # ``tables: []`` instead of forgetting the section entirely).
    # Reject at plan time so the empty-plan failure surfaces at
    # construction, not at the engine's empty-iteration call site.
    tables: list[TableSpec] = Field(min_length=1)
    seed: int = 42
    default_locale: str = "en_US"

    @model_validator(mode="before")
    @classmethod
    def _warn_if_seed_missing(cls, data: Any) -> Any:  # noqa: ANN401 — validator signature
        """Warn when the caller didn't pass a ``seed``.

        The ``seed: int = 42`` default is convenient for quick demos but
        means every unseeded plan runs under the literal demo seed --
        two independent users get identical data, and a production
        plan that forgot to set ``seed`` is silently reproducible
        between them without any signal that the reproduction is
        coincidental.  Emit a ``UserWarning`` pointing at the omission
        so CI / log scrape can catch it; the default still fills in so
        tutorial code keeps working.
        """
        if isinstance(data, dict) and "seed" not in data:
            warnings.warn(
                "DataGenPlan constructed without an explicit ``seed`` -- "
                "defaulting to 42.  This is fine for demos but means every "
                "unseeded plan produces identical output across independent "
                "runs / callers.  Pass ``seed=<int>`` to silence this warning.",
                UserWarning,
                stacklevel=2,
            )
        return data

    @model_validator(mode="after")
    def validate_unique_table_names(self) -> DataGenPlan:
        """Reject plans containing two TableSpecs with the same name.

        Downstream ``resolve_plan`` builds ``{t.name: t for t in
        plan.tables}`` which silently drops all but the last
        colliding table.  FK references that targeted the dropped
        table then resolve to the survivor's row space, producing
        wrong join cardinality with no signal that the user's
        original plan had a typo.  Catch at plan time with the
        offending name.
        """
        names = [t.name for t in self.tables]
        if len(set(names)) != len(names):
            dupes = sorted(name for name, count in Counter(names).items() if count > 1)
            raise ValueError(
                f"DataGenPlan has duplicate TableSpec names: {dupes}.  "
                f"resolve_plan dedupes on name (silently dropping all but "
                f"the last); rename so each table is unique."
            )
        return self

    @model_validator(mode="after")
    def propagate_seeds(self) -> DataGenPlan:
        """Assign deterministic per-table seeds from global seed when not set.

        This is a *derivation* step, not a mutation of user intent: the
        resulting ``table.seed`` is what downstream code reads, and
        round-tripping ``model_dump -> model_validate`` preserves the
        values because the second pass's ``if table.seed is None``
        guard short-circuits.  Documented as intentional so a future
        reader doesn't conflate it with the ``ColumnSpec.nullable``
        mutation that was a real side-effect bug.
        """
        for i, table in enumerate(self.tables):
            if table.seed is None:
                table.seed = self.seed + i
        return self


# Resolve forward references for recursive types (StructColumn, ArrayColumn)
StructColumn.model_rebuild()
ArrayColumn.model_rebuild()
ColumnSpec.model_rebuild()
