"""Pydantic models that describe a data-generation plan.

Defines the `Distribution` and `ColumnStrategy` discriminated unions and the
three composition models (`ColumnSpec`, `TableSpec`, `DataGenPlan`). Each model
forbids unknown fields and validates its parameters at construction.
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

from dbldatagen.core.spec._constants import (
    MAX_ALPHA_WIDTH,
    MAX_DIGIT_WIDTH,
    MAX_HEX_WIDTH,
    MAX_SEQ_WIDTH,
    PLACEHOLDER_RE,
)


class _StrictModel(BaseModel):
    """Base model for every plan type; forbids unknown fields."""

    model_config = ConfigDict(extra="forbid")


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _require_finite(field_name: str, value: float, model: str) -> None:
    """Rejects a NaN or infinite float value.

    Args:
        field_name: Name of the field being checked.
        value: The value to check.
        model: Name of the owning model, used in the error message.

    Raises:
        ValueError: If `value` is NaN or infinite.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(
            f"{model}.{field_name}={value} is not finite (NaN/Inf).  "
            f"Use a concrete numeric value; NaN-vs-* comparisons "
            f"silently return False and would let every other check pass."
        )


# Engine-internal column names that user columns must not collide with. The
# validator rejects exact matches only, so customer names like ``_modified_at``
# are accepted even though they begin with an underscore.
_RESERVED_INTERNAL_COLUMN_NAMES: frozenset[str] = frozenset({"_synth_row_id"})


def parse_fk_ref(ref: str) -> tuple[str, str]:
    """Parses and validates a `"table.column"` reference string.

    Args:
        ref: A reference in `"table.column"` form; each part must be a valid
            identifier.

    Returns:
        A `(table_name, column_name)` tuple.

    Raises:
        ValueError: If `ref` is not two identifier-shaped parts separated by a
            single `.`.
    """
    parts = ref.split(".")
    if len(parts) != 2 or not all(_IDENTIFIER_RE.fullmatch(p) for p in parts):
        raise ValueError(
            f"ForeignKeyRef.ref='{ref}' must use 'table.column' "
            f"format with each half matching {_IDENTIFIER_RE.pattern!r} "
            f"(letters / digits / underscore, must not start with a digit)."
        )
    return parts[0], parts[1]


# Upper bound on ArrayColumn.max_length. Each slot materialises its own Spark
# expression tree at plan time, so Catalyst work scales linearly with this value
# (multiplicatively with nested arrays/structs). 1000 is already far beyond
# realistic array columns; past that, the user wants rows or MapType.
_MAX_ARRAY_LENGTH = 1000


class Uniform(_StrictModel):
    """Uniform distribution: every value in the range is equally likely.

    This is the default for every strategy that accepts a `distribution`.

    Attributes:
        type: Discriminator literal; always "uniform".
    """

    type: Literal["uniform"] = "uniform"


class Normal(_StrictModel):
    """Normal (Gaussian) distribution over the strategy's value range.

    `mean` and `stddev` are in the range's value units; samples are drawn from
    `N(mean, stddev)` and clamped to the bounds. When unset, the bell
    auto-centers on the range midpoint with a spread of `span / 6`.

    Attributes:
        type: Discriminator literal; always "normal".
        mean: Optional peak in value units (default None, meaning the midpoint).
        stddev: Optional spread in value units; must be non-negative
            (default None, meaning span / 6).

    Note:
        Explicit `mean`/`stddev` are honored only on numeric `RangeColumn`; on
        timestamp, foreign-key, and value-list columns, use a bare `Normal()`.
    """

    type: Literal["normal"] = "normal"
    mean: float | None = None
    stddev: float | None = None

    @model_validator(mode="after")
    def validate_params(self) -> Normal:
        """Rejects non-finite parameters and a negative `stddev`.

        Raises:
            ValueError: If `mean` or `stddev` is NaN/infinite, or `stddev` < 0.
        """
        if self.mean is not None:
            _require_finite("mean", self.mean, "Normal")
        if self.stddev is not None:
            _require_finite("stddev", self.stddev, "Normal")
            if self.stddev < 0:
                raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        return self


class LogNormal(_StrictModel):
    """Log-normal distribution: the log of the samples is normally distributed.

    Useful for heavy-tailed positive quantities (e.g. incomes, file sizes,
    response times).

    Attributes:
        type: Discriminator literal; always "lognormal".
        mean: Optional mean of the underlying normal distribution; must be in
            [-100.0, 100.0] (default 0.0).
        stddev: Optional standard deviation of the underlying normal
            distribution; must be non-negative (default 1.0).
    """

    type: Literal["lognormal"] = "lognormal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> LogNormal:
        """Rejects non-finite parameters, negative `stddev`, or out-of-range `mean`.

        Raises:
            ValueError: If `mean`/`stddev` is NaN/infinite, `stddev` < 0, or
                `mean` is outside [-100, 100].
        """
        _require_finite("mean", self.mean, "LogNormal")
        _require_finite("stddev", self.stddev, "LogNormal")
        if self.stddev < 0:
            raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        if not -100.0 <= self.mean <= 100.0:
            raise ValueError(
                f"LogNormal.mean must be in [-100, 100], got {self.mean}.  "
                f"math.exp(mean) is computed on the driver and overflows "
                f"around mean=710 / underflows near mean=-745; this cap keeps "
                f"the scale factor finite and within double range."
            )
        return self


class Zipf(_StrictModel):
    """Zipfian / power-law distribution for realistic cardinality skew.

    A small fraction of values account for most of the mass. Sampled via inverse
    power-law CDF, which converges only for `exponent > 1`.

    Attributes:
        type: Discriminator literal; always "zipf".
        exponent: Optional power-law exponent; must be > 1.0. Smaller values give
            heavier tails; larger values approach uniform (default 1.5).
    """

    type: Literal["zipf"] = "zipf"
    exponent: float = 1.5

    @model_validator(mode="after")
    def validate_params(self) -> Zipf:
        """Rejects a non-finite or non-converging `exponent`.

        Raises:
            ValueError: If `exponent` is NaN/infinite or <= 1.0.
        """
        _require_finite("exponent", self.exponent, "Zipf")
        if self.exponent <= 1.0:
            raise ValueError(
                f"Zipf.exponent must be > 1 (power-law CDF only converges "
                f"in that range), got {self.exponent}.  For milder skew use "
                f"``Normal`` or ``LogNormal``; for uniform, use ``Uniform``."
            )
        return self


class Exponential(_StrictModel):
    """Exponential distribution (e.g. for memoryless inter-arrival times).

    Attributes:
        type: Discriminator literal; always "exponential".
        rate: Optional rate parameter (lambda); the reciprocal is the mean; must
            be positive (default 1.0).
    """

    type: Literal["exponential"] = "exponential"
    rate: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Exponential:
        """Rejects a non-finite or non-positive `rate`.

        Raises:
            ValueError: If `rate` is NaN/infinite or <= 0.
        """
        _require_finite("rate", self.rate, "Exponential")
        if self.rate <= 0:
            raise ValueError(f"rate must be > 0, got {self.rate}")
        return self


class WeightedValues(_StrictModel):
    """Explicit weighted selection from a list of values.

    Pair with `ValuesColumn` to give each value its own probability mass. Weights
    are normalized and do not need to sum to 1.0. Not accepted by range,
    timestamp, or foreign-key strategies, which have no value list to weight.

    Attributes:
        type: Discriminator literal; always "weighted".
        weights: Mapping of values (as `str`) to non-negative weights. Must be
            non-empty.
    """

    type: Literal["weighted"] = "weighted"
    weights: dict[str, float]

    @model_validator(mode="after")
    def validate_weights(self) -> WeightedValues:
        """Rejects empty, non-finite, or negative weights.

        Raises:
            ValueError: If `weights` is empty, or any weight is NaN/infinite or
                negative.
        """
        if not self.weights:
            raise ValueError("weights must not be empty")
        for key, weight in self.weights.items():
            _require_finite(f"weights[{key!r}]", weight, "WeightedValues")
            if weight < 0:
                raise ValueError("weights must be non-negative")
        return self


Distribution = Annotated[
    Uniform | Normal | LogNormal | Zipf | Exponential | WeightedValues,
    Field(discriminator="type"),
]


def _reject_parametrized_normal(distribution: Distribution, model: str) -> None:
    """Rejects a `Normal` with an explicit `mean` or `stddev` on a non-numeric column.

    A `Normal`'s `mean` and `stddev` are only meaningful on a numeric range. On
    other column types, use a bare `Normal()`, which auto-centers.

    Args:
        distribution: The distribution to check.
        model: Name of the column type (e.g. "TimestampColumn"), used in the
            error message.

    Raises:
        ValueError: If `distribution` is a `Normal` with an explicit `mean` or
            `stddev`.
    """
    if isinstance(distribution, Normal) and (distribution.mean is not None or distribution.stddev is not None):
        raise ValueError(
            f"{model} does not support Normal with explicit mean/stddev.  "
            f"Normal's mean/stddev are value-space parameters honored only on "
            f"numeric RangeColumn; on {model} Normal auto-centers on the range.  "
            f"Use a bare Normal() here, or move the parametrized Normal to a "
            f"numeric RangeColumn."
        )


class RangeColumn(_StrictModel):
    """Generates values from a numeric range.

    Both `min` and `max` are inclusive. When `step` is set, output snaps to
    `min, min + step, ...` within the range; otherwise it is continuous.

    Attributes:
        strategy: Discriminator literal; always "range".
        min: Optional inclusive lower bound (default 0).
        max: Optional inclusive upper bound; must be >= min (default 100).
        step: Optional step size; must be positive when set (default None,
            meaning continuous).
        distribution: Optional sampling distribution; `WeightedValues` is not
            accepted (default Uniform).
    """

    strategy: Literal["range"] = "range"
    min: float | int = 0
    max: float | int = 100
    step: float | int | None = None
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_range(self) -> RangeColumn:
        """Validates the range bounds, step, and distribution.

        Raises:
            ValueError: If the distribution is `WeightedValues`, a bound or step
                is NaN/infinite, `min` > `max`, `step` <= 0, or the integer range
                size exceeds the int64 bound.
        """
        if isinstance(self.distribution, WeightedValues):
            raise ValueError(
                "RangeColumn does not support WeightedValues.  WeightedValues "
                "weights a discrete value list; RangeColumn produces values "
                "from a continuous numeric range with no list to weight.  "
                "Use ValuesColumn for weighted categorical selection, or "
                "Zipf / Normal / LogNormal / Exponential to skew a numeric "
                "range."
            )
        for field_name in ("min", "max", "step"):
            value = getattr(self, field_name)
            if value is not None:
                _require_finite(field_name, value, "RangeColumn")
        if self.min > self.max:
            raise ValueError(f"min ({self.min}) must be <= max ({self.max})")
        if self.step is not None and self.step <= 0:
            raise ValueError(f"step must be > 0, got {self.step}")
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
    """Selects from an explicit list of allowed values.

    The element type is whatever the caller puts in `values` (strings, ints,
    booleans, etc.).

    Attributes:
        strategy: Discriminator literal; always "values".
        values: Non-empty list of allowed values.
        distribution: Optional sampling distribution. Use `WeightedValues` for
            per-value probabilities (default Uniform).
    """

    strategy: Literal["values"] = "values"
    values: list[Any]
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_values(self) -> ValuesColumn:
        """Validates the value list and any weighted distribution.

        Raises:
            ValueError: If `values` is empty or has duplicates, the distribution
                is a parametrized `Normal`, or `WeightedValues` is missing a
                weight for any value or sums to zero.
        """
        if not self.values:
            raise ValueError("values list must not be empty")
        _reject_parametrized_normal(self.distribution, "ValuesColumn")
        try:
            seen_count = len(set(self.values))
        except TypeError:
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
    """Generates data using a Faker provider.

    Requires the `faker` extra (`pip install 'dbldatagen[core-faker]'`). Always
    produces StringType output.

    Attributes:
        strategy: Discriminator literal; always "faker".
        provider: Faker provider method name (e.g. "first_name", "company").
        kwargs: Optional keyword arguments forwarded to the provider (default
            empty).
        locale: Optional Faker locale, e.g. "de_DE" (default None, meaning the
            plan's default locale).
    """

    strategy: Literal["faker"] = "faker"
    provider: str = Field(min_length=1)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    locale: str | None = None

    @field_validator("provider")
    @classmethod
    def _strip_provider(cls, value: str) -> str:
        """Rejects a whitespace-only provider name.

        Raises:
            ValueError: If `provider` is empty or only whitespace.
        """
        if not value.strip():
            raise ValueError(
                f"FakerColumn.provider={value!r} is whitespace-only.  "
                f"Set provider to a real Faker method name such as "
                f"'first_name', 'company', or 'ipv4'."
            )
        return value


class PatternColumn(_StrictModel):
    """Generates strings from a pattern template.

    Placeholders:
        {seq}     - row sequence number
        {uuid}    - deterministic UUID
        {alpha:N} - N random letters
        {digit:N} - N random digits
        {hex:N}   - N random hex characters

    Example: `"ORD-{digit:4}-{alpha:3}"` -> `"ORD-3847-KMX"`.

    Attributes:
        strategy: Discriminator literal; always "pattern".
        template: Pattern string containing literal text and placeholders.
            `{kind:N}` widths are capped per kind (see `_constants.py`); `{uuid}`
            takes no width.
    """

    strategy: Literal["pattern"] = "pattern"
    template: Annotated[str, Field(min_length=1)]

    @model_validator(mode="after")
    def validate_template(self) -> PatternColumn:
        """Validates the template's content and placeholder widths.

        Raises:
            ValueError: If the template is whitespace-only, has no placeholders,
                gives `{uuid}` a width, or a `{kind:N}` width is < 1 or exceeds
                its cap.
        """
        if not self.template.strip():
            raise ValueError(
                f"PatternColumn.template={self.template!r} is whitespace-only.  "
                f"Templates need literal text and/or placeholders such as "
                f"'{{digit:4}}' or '{{alpha:3}}'."
            )
        matches = list(PLACEHOLDER_RE.finditer(self.template))
        if not matches:
            raise ValueError(
                f"PatternColumn.template={self.template!r} contains no "
                f"placeholders.  PatternColumn requires at least one of "
                f"'{{seq}}', '{{uuid}}', '{{digit:N}}', '{{hex:N}}', "
                f"'{{alpha:N}}', or '{{seq:N}}'.  For a literal value on "
                f"every row, use ConstantColumn."
            )
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
    """Monotonic integer sequence. The value at row `i` is `start + i * step`.

    Attributes:
        strategy: Discriminator literal; always "sequence".
        start: Optional starting value (default 1).
        step: Optional increment value per row; may be negative but not 0
            (default 1).
    """

    strategy: Literal["sequence"] = "sequence"
    start: int = 1
    step: int = 1

    @model_validator(mode="after")
    def validate_step(self) -> SequenceColumn:
        """Ensures a valid step value is provided.

        Raises:
            ValueError: If `step` is 0.
        """
        if self.step == 0:
            raise ValueError("step must not be 0")
        return self


class UUIDColumn(_StrictModel):
    """Deterministic UUID generation. The same seed and row always yield the same UUID.

    Attributes:
        strategy: Discriminator literal; always "uuid".
    """

    strategy: Literal["uuid"] = "uuid"


class ExpressionColumn(_StrictModel):
    """A Spark SQL expression over other columns in the same table.

    The expression may reference any column in the same table as long as the
    referenced column is declared before this one, e.g. `"quantity * unit_price"`.

    Attributes:
        strategy: Discriminator literal; always "expression".
        expr: A Spark SQL expression string. Referenced columns must exist in the
            same table. References are checked at resolution time.

    Note:
        `expr` is passed directly to `F.expr()` and can execute arbitrary Spark
        SQL. Do not use it with untrusted plan input in multi-tenant environments.
    """

    strategy: Literal["expression"] = "expression"
    expr: str = Field(min_length=1)

    @field_validator("expr")
    @classmethod
    def _reject_whitespace_expr(cls, value: str) -> str:
        """Rejects a whitespace-only expression.

        Raises:
            ValueError: If `expr` is empty or only whitespace.
        """
        if not value.strip():
            raise ValueError(
                f"ExpressionColumn.expr={value!r} is whitespace-only.  "
                f"Provide a real Spark SQL expression (e.g., "
                f"'quantity * unit_price' or "
                f"'concat(first_name, \" \", last_name)')."
            )
        return value


class TimestampColumn(_StrictModel):
    """Generates timestamps within a range. Bounds are inclusive, and output is
    independent of the session time zone.

    Attributes:
        strategy: Discriminator literal; always "timestamp".
        start: Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
            "YYYY-MM-DD HH:MM:SS").
        end: Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
            "YYYY-MM-DD HH:MM:SS"); must be >= start.
        distribution: Optional sampling distribution; `WeightedValues` is not
            accepted (default Uniform).
    """

    strategy: Literal["timestamp"] = "timestamp"
    start: str
    end: str
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_timestamps(self) -> TimestampColumn:
        """Validates that timestamp bounds parse, agree in tz-awareness, and are ordered.

        Raises:
            ValueError: If a bound is not a valid ISO timestamp, the bounds mix
                naive and tz-aware values, `start` > `end`, or the distribution
                is `WeightedValues`.
        """
        _reject_parametrized_normal(self.distribution, "TimestampColumn")
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
    """Emits the same literal value on every row.

    Attributes:
        strategy: Discriminator literal; always "constant".
        value: The literal value emitted on every row.
    """

    strategy: Literal["constant"] = "constant"
    value: Any


class ForeignKeyColumn(_StrictModel):
    """Marker strategy for a column generated from its `foreign_key`.

    Generation (type, distribution, null fraction, and the parent-key lookup) is
    driven by `ColumnSpec.foreign_key` at resolution time.

    Attributes:
        strategy: Discriminator literal; always "foreign_key".
    """

    strategy: Literal["foreign_key"] = "foreign_key"


class StructColumn(_StrictModel):
    """Groups child columns into a Spark struct.

    Each field is a full `ColumnSpec`; fields may be nested structs or arrays.
    Field names must be unique within the struct.

    Example JSON output::

        {"address": {"street": "123 Main St", "city": "Austin", "zip": "78701"}}

    Attributes:
        strategy: Discriminator literal; always "struct".
        fields: Non-empty, ordered list of field column specs with unique names.
    """

    strategy: Literal["struct"] = "struct"
    fields: list[ColumnSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_field_names(self) -> StructColumn:
        """Rejects duplicate field names within the struct.

        Raises:
            ValueError: If two fields share a name.
        """
        field_names = [f.name for f in self.fields]
        if len(set(field_names)) != len(field_names):
            dupes = sorted(name for name, count in Counter(field_names).items() if count > 1)
            raise ValueError(
                f"StructColumn has duplicate field names: {dupes}.  "
                f"Each field must have a unique name within the struct."
            )
        return self

    @model_validator(mode="after")
    def validate_field_strategies(self) -> StructColumn:
        """Rejects FakerColumn and ForeignKeyColumn strategies that can't be generated as struct fields.

        Raises:
            ValueError: If a field uses `FakerColumn` or `ForeignKeyColumn`.
        """
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
    """Generates a variable-length array of values.

    The array holds a random number of values generated from `element`, each with
    a unique seed. `FakerColumn` and `ForeignKeyColumn` are not allowed as
    elements (wrap them in a `StructColumn` for an array of records).

    Example JSON output::

        {"tags": ["electronics", "sale", "new"]}

    Attributes:
        strategy: Discriminator literal; always "array".
        element: Element-generation strategy; any `ColumnStrategy` except
            `FakerColumn` and `ForeignKeyColumn`. May itself be a `StructColumn`
            or `ArrayColumn`.
        min_length: Optional inclusive minimum length per row; must be >= 0
            (default 1).
        max_length: Optional inclusive maximum length per row; must be >= 1,
            >= min_length, and <= 1000 (default 5).
    """

    strategy: Literal["array"] = "array"
    element: ColumnStrategy  # forward ref resolved by model_rebuild() below
    min_length: int = 1
    max_length: int = 5

    @model_validator(mode="after")
    def validate_lengths(self) -> ArrayColumn:
        """Validates the array length bounds and element strategy.

        Raises:
            ValueError: If `min_length` < 0, `min_length` > `max_length`,
                `max_length` is 0 or exceeds the cap, or `element` is a
                `FakerColumn` or `ForeignKeyColumn`.
        """
        if self.min_length < 0:
            raise ValueError(f"min_length must be >= 0, got {self.min_length}")
        if self.min_length > self.max_length:
            raise ValueError(f"min_length ({self.min_length}) must be <= max_length ({self.max_length})")
        # max_length == 0 hits F.array() with no element exprs, which Spark types
        # ambiguously as array<nothing>/array<null> and confuses schema inference.
        if self.max_length == 0:
            raise ValueError(
                "max_length must be >= 1 (max_length == 0 always produces "
                "empty arrays with an ambiguous element type).  Omit the "
                "column if you want no array at all, or set max_length >= 1 "
                "and let min_length=0 produce sometimes-empty arrays."
            )
        if self.max_length > _MAX_ARRAY_LENGTH:
            raise ValueError(
                f"max_length ({self.max_length}) exceeds {_MAX_ARRAY_LENGTH}.  "
                f"Each array element expands to its own Spark expression at "
                f"plan time; large max_lengths stall Catalyst.  For wider "
                f"fan-out, model the data as rows or a MapType."
            )
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
    """Column data types, mapped to the equivalent Spark SQL types.

    `INTEGER` is an alias for `INT`. Alternate spellings `"integer"`, `"bool"`,
    and `"str"` are also accepted when deserializing plans.
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
        """Accepts common alternate spellings (`"integer"` -> INT, etc.)."""
        if not isinstance(value, str):
            return None
        aliases = {
            "integer": cls.INT,
            "bool": cls.BOOLEAN,
            "str": cls.STRING,
        }
        return aliases.get(value.lower())


class PrimaryKey(_StrictModel):
    """Marks one or more columns as the table's primary key.

    Composite (multi-column) primary keys are allowed, but a `ForeignKeyRef` can
    only target a single-column primary key.

    Attributes:
        columns: Non-empty, ordered list of primary-key column names with no
            duplicates. Each must match a column on the table.
    """

    columns: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_columns(self) -> PrimaryKey:
        """Rejects duplicate column names in the key.

        Raises:
            ValueError: If a column name appears more than once.
        """
        if len(set(self.columns)) != len(self.columns):
            dupes = sorted(name for name, count in Counter(self.columns).items() if count > 1)
            raise ValueError(
                f"PrimaryKey has duplicate column names: {dupes}.  "
                f"Each column may appear at most once in a composite key."
            )
        return self


class ForeignKeyRef(_StrictModel):
    """Defines a foreign-key relationship to another table's primary key.

    `ref` uses `"table.column"` syntax. Use `distribution` (e.g. `Zipf`) to skew
    children toward a small set of parents, and `null_fraction` for optional FKs.

    Attributes:
        ref: Reference to the parent primary key in `"table.column"` form. The
            parent table must exist and the column must be its single-column
            primary key.
        distribution: Optional sampling distribution over parent rows;
            `WeightedValues` is not accepted (default Uniform).
        nullable: Optional flag for whether the column is declared nullable in
            the schema; independent of `null_fraction` (default False).
        null_fraction: Optional probability in [0.0, 1.0] that a row is NULL
            instead of a parent reference (default 0.0).
    """

    ref: str
    distribution: Distribution = Uniform()
    nullable: bool = False
    null_fraction: float = 0.0

    @model_validator(mode="after")
    def validate_ref_format(self) -> ForeignKeyRef:
        """Validates the reference format, distribution, and null fraction.

        Raises:
            ValueError: If `ref` is not `"table.column"`, the distribution is a
                parametrized `Normal` or `WeightedValues`, or `null_fraction` is
                outside [0.0, 1.0].
        """
        parse_fk_ref(self.ref)
        _reject_parametrized_normal(self.distribution, "ForeignKeyRef")
        if not 0.0 <= self.null_fraction <= 1.0:
            raise ValueError(f"null_fraction must be in [0.0, 1.0], got {self.null_fraction}")
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

    A name and generation strategy must be specified. `dtype` is inferred from
    `gen` when not set. The DSL helpers in `dbldatagen.core.spec.dsl` are the
    recommended way to build a `ColumnSpec`.

    Attributes:
        name: Column name; must be a valid identifier and must not be a reserved
            internal name (e.g. "_synth_row_id").
        dtype: Optional output type; inferred from `gen` when None (default None).
        gen: The value-generation strategy for this column.
        nullable: Optional flag for whether the column is nullable; independent of
            `null_fraction` (default False).
        null_fraction: Optional probability in [0.0, 1.0] that a row is NULL
            (default 0.0).
        foreign_key: Optional foreign-key reference; when set, the column is
            generated from it and `gen` must be `ForeignKeyColumn` (default None).
        seed_from: Optional name of another column to derive the per-cell seed
            from, for correlating values across columns (default None).
        precision: Optional total number of digits for DECIMAL columns; set with
            `scale`; rejected on non-DECIMAL types (default None).
        scale: Optional number of fractional digits for DECIMAL columns; set with
            `precision` (default None).
    """

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
        """Validates the column name is a valid, unreserved identifier.

        Raises:
            ValueError: If `name` is not a valid identifier or collides with an
                engine-internal name.
        """
        if not _IDENTIFIER_RE.fullmatch(self.name):
            raise ValueError(
                f"Column name '{self.name}' is not a valid identifier (must match [A-Za-z_][A-Za-z0-9_]*)."
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
    def validate_expression_column_type(self) -> ColumnSpec:
        """Rejects a declared type on an `ExpressionColumn`. Expression types are
        always inferred by Spark. Add a cast statement in the passed expression to
        control the output data type.

        Raises:
            ValueError: If `dtype`, `precision`, or `scale` is set on an
                `ExpressionColumn`.
        """
        if not isinstance(self.gen, ExpressionColumn):
            return self
        declared = [
            field
            for field, value in (("dtype", self.dtype), ("precision", self.precision), ("scale", self.scale))
            if value is not None
        ]
        if declared:
            raise ValueError(
                f"Column '{self.name}': {', '.join(declared)} cannot be set on an "
                f"ExpressionColumn -- its output type is always inferred from the "
                f"SQL (the engine evaluates the expression as-is, with no cast).  "
                f"Cast inside the expression instead, e.g. "
                f"``cast({self.gen.expr} as <type>)``."
            )
        return self

    @model_validator(mode="after")
    def validate_decimal_precision_scale(self) -> ColumnSpec:
        """Validates decimal precision/scale and that values fit the type.

        `precision` and `scale` are valid only on decimal columns and must be set
        together; when both are unset, the engine uses Spark's `DecimalType()`
        default of (10, 0).

        Raises:
            ValueError: If precision/scale are set on a non-decimal column, only
                one is set, either is out of range, or a value does not fit.
        """
        has_precision = self.precision is not None
        has_scale = self.scale is not None
        if not (has_precision or has_scale):
            return self
        if self.dtype != DataType.DECIMAL:
            raise ValueError(
                f"Column '{self.name}': precision/scale are only valid when dtype=DECIMAL, got dtype={self.dtype}"
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
        precision = self.precision
        scale = self.scale
        if precision is None or scale is None:
            raise RuntimeError(
                f"Column '{self.name}': precision/scale narrowing invariant "
                f"violated (precision={precision}, scale={scale}).  The "
                f"precision-and-scale checks above should have raised; "
                f"reaching this line means the preceding guards were removed."
            )
        if not 1 <= precision <= 38:
            raise ValueError(f"Column '{self.name}': precision must be in [1, 38], got {precision}")
        if not 0 <= scale <= precision:
            raise ValueError(f"Column '{self.name}': scale must be in [0, precision] (0..{precision}), got {scale}")
        limit = 10 ** (precision - scale)
        max_repr = limit - 10 ** (-1 * scale)
        if isinstance(self.gen, RangeColumn):
            max_abs = max(abs(self.gen.min), abs(self.gen.max))
            if max_abs >= limit:
                raise ValueError(
                    f"Column '{self.name}': range [{self.gen.min}, {self.gen.max}] "
                    f"does not fit in decimal({precision}, {scale}) "
                    f"(max representable magnitude is {max_repr})"
                )
        elif isinstance(self.gen, ValuesColumn):
            numeric_values = [v for v in self.gen.values if isinstance(v, (int, float, Decimal))]
            if numeric_values:
                offending = max(numeric_values, key=lambda v: abs(float(v)))
                if abs(float(offending)) >= limit:
                    raise ValueError(
                        f"Column '{self.name}': value {offending} in ValuesColumn "
                        f"does not fit in decimal({precision}, {scale}) "
                        f"(max representable magnitude is {max_repr})"
                    )
        elif isinstance(self.gen, ConstantColumn):
            value = self.gen.value
            if isinstance(value, (int, float, Decimal)):
                _require_finite("value", float(value), "ConstantColumn")
                if abs(float(value)) >= limit:
                    raise ValueError(
                        f"Column '{self.name}': constant value {value} "
                        f"does not fit in decimal({precision}, {scale}) "
                        f"(max representable magnitude is {max_repr})"
                    )
        return self

    @model_validator(mode="after")
    def validate_date_dtype_strategy(self) -> ColumnSpec:
        """Rejects `dtype=DATE` on strategies that can't produce a date.

        Use `TimestampColumn` for a random date, or drop `dtype=DATE` to keep the
        strategy's native type.

        Raises:
            ValueError: If `dtype` is DATE and `gen` is a range, pattern,
                sequence, UUID, Faker, struct, or array strategy.
        """
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
        """Rejects a FakerColumn paired with a non-string dtype.

        Faker always produces StringType, so any other declared dtype is invalid.

        Raises:
            ValueError: If `gen` is a `FakerColumn` and `dtype` is set to anything
                other than STRING.
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
        """Validates the null fraction is within range.

        Raises:
            ValueError: If `null_fraction` is outside [0.0, 1.0].
        """
        if not 0.0 <= self.null_fraction <= 1.0:
            raise ValueError(f"null_fraction must be in [0.0, 1.0], got {self.null_fraction}")
        return self

    @model_validator(mode="after")
    def validate_foreign_key_strategy(self) -> ColumnSpec:
        """Validates the ForeignKeyColumn/foreign_key pairing and null fraction.

        Raises:
            ValueError: If `gen` is `ForeignKeyColumn` without a `foreign_key`,
                `foreign_key` is set with a non-FK `gen`, or the column and FK
                set conflicting and non-zero null fractions.
        """
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
    """Defines the generation specification for a single table.

    The number of rows can be passed as an integer count or a human-readable
    string like "10M".

    Attributes:
        name: Table name; must be a valid identifier. Used as the result dict key
            and as the parent name in `ForeignKeyRef.ref`.
        columns: Non-empty, ordered list of `ColumnSpec` with unique names.
        rows: Number of rows to generate. Can be an integer row count or a string
            like "10K", "5M", or "1B". Must be positive.
        primary_key: Optional `PrimaryKey`; required when another table
            references this one (default None).
        seed: Optional per-table seed; usually propagated from `DataGenPlan.seed`
            (default None).
    """

    name: str
    columns: list[ColumnSpec] = Field(min_length=1)
    rows: int | str
    primary_key: PrimaryKey | None = None
    seed: int | None = None

    @model_validator(mode="after")
    def resolve_row_count(self) -> TableSpec:
        """Normalizes the row count to a positive integer.

        Accepts an integer count or a string like `"10K"` and stores the parsed
        int back on `self.rows`.

        Raises:
            ValueError: If `rows` does not parse to a positive integer.
        """
        self.rows = TableSpec.parse_human_count(self.rows)
        if self.rows <= 0:
            raise ValueError(f"rows must be > 0, got {self.rows}")
        return self

    @model_validator(mode="after")
    def validate_name(self) -> TableSpec:
        """Validates the table name is a valid identifier.

        Raises:
            ValueError: If `name` is not a valid identifier.
        """
        if not _IDENTIFIER_RE.fullmatch(self.name):
            raise ValueError(f"Table name '{self.name}' is not a valid identifier.  Must match [a-zA-Z_][a-zA-Z0-9_]*.")
        return self

    @model_validator(mode="after")
    def validate_unique_column_names(self) -> TableSpec:
        """Rejects duplicate column names within the table.

        Raises:
            ValueError: If two columns share a name.
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
        """Rejects primary-key columns that don't exist on the table.

        Raises:
            ValueError: If a `primary_key` column is not among the table's
                columns.
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
        """Parses a human-readable row count into an integer.

        Accepts an integer (returned unchanged) or a string with a `K`, `M`, or
        `B` suffix (case-insensitive), such as `"10K"` or `"1.5B"`.

        Args:
            value: An integer row count, or a string like `"10K"`, `"5M"`, or
                `"1.5B"`.

        Returns:
            The row count as an integer.

        Raises:
            ValueError: If `value` does not parse to an integer.
        """
        if isinstance(value, int):
            return value
        suffixes = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
        s = str(value).strip().upper()
        try:
            for suffix, mult in suffixes.items():
                if s.endswith(suffix):
                    return int(float(s[: -len(suffix)]) * mult)
            return int(s)
        except ValueError:
            raise ValueError(
                f"Invalid row count '{value}'. Expected an integer or a human-readable string like '10K', '1M', '1B'."
            ) from None

    @model_validator(mode="after")
    def validate_sequence_column_overflow(self) -> TableSpec:
        """Rejects SequenceColumn configurations that overflow the available range of values.

        For each sequence column, the last value `start + (rows - 1) * step` must
        fit in int64 and, when precision/scale are set, in the declared decimal.

        Raises:
            ValueError: If a sequence column's last value overflows int64 or does
                not fit its decimal type.
        """
        long_max = 2**63 - 1
        long_min = -(2**63)
        rows = cast(int, self.rows)
        for col_spec in self.columns:
            if not isinstance(col_spec.gen, SequenceColumn):
                continue
            start = col_spec.gen.start
            step = col_spec.gen.step
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
            if col_spec.dtype == DataType.DECIMAL and col_spec.precision is not None and col_spec.scale is not None:
                decimal_limit = 10 ** (col_spec.precision - col_spec.scale)
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
    """Top-level plan describing all tables to generate. Tables are generated in
    dependency order; foreign-key references are resolved automatically.

    Attributes:
        tables: Non-empty list of `TableSpec` instances to generate.
        seed: Global seed propagated to each table without a set table seed
            (table `i` gets `seed + i`). Defaults to 42 with a `UserWarning`; set
            it explicitly in production.
        default_locale: Default Faker locale for `FakerColumn`s that don't set
            one (default "en_US").
    """

    tables: list[TableSpec] = Field(min_length=1)
    seed: int = 42
    default_locale: str = "en_US"

    @model_validator(mode="before")
    @classmethod
    def _warn_if_seed_missing(cls, data: Any) -> Any:  # noqa: ANN401 — validator signature
        """Warns when no `seed` was provided. The default seed will be used.

        Args:
            data: The raw plan data being validated.

        Returns:
            The data unchanged.
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
        """Rejects duplicate table names in the plan.

        Raises:
            ValueError: If two tables share a name.
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
        """Assigns each table a deterministic seed from the global seed when unset.

        Table `i` gets `seed + i`. Tables that already have a seed are left
        unchanged, so re-validating a dumped plan is stable.
        """
        for i, table in enumerate(self.tables):
            if table.seed is None:
                table.seed = self.seed + i
        return self


# Resolve forward references for recursive types (StructColumn, ArrayColumn)
StructColumn.model_rebuild()
ArrayColumn.model_rebuild()
ColumnSpec.model_rebuild()
