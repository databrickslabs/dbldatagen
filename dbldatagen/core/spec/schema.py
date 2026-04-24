"""dbldatagen.core.spec.schema -- Public API models."""

from __future__ import annotations

import re
import warnings
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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

# Names that the SQL Server CDC format rewrite (``rename_cdc_columns``)
# emits by renaming the raw ``__$operation`` / ``__$start_lsn`` /
# ``__$seqval`` metadata columns.  A user column with one of these names
# would silently shadow the rewritten metadata after rename, and Delta
# would carry two identically-named columns with the user's one winning
# in downstream reads.  Reserve at plan time so the collision fails
# loudly where the column is defined, not at write time with an opaque
# Delta error.
_RESERVED_CDC_RENAME_TARGETS = frozenset({"cdc_operation", "cdc_lsn", "cdc_seqval"})

# Upper bound on ArrayColumn.max_length.  Each slot materialises its own
# Spark expression tree at plan time, so Catalyst work scales linearly
# with this value (and with nested arrays/structs, multiplicatively).
# 1000 is already far beyond realistic array columns; past that, the
# user wants rows or MapType.
_MAX_ARRAY_LENGTH = 1000

# Engine granularity for null-mask generation.  Must match
# ``dbldatagen/core/engine/seed.py::_NULL_PRECISION``.  Exposed here so
# ``null_fraction`` validators can reject below-granularity values at
# plan time (before the engine would raise at generation time).  A user
# who asked for ``null_fraction=1e-5`` with precision=10000 would get
# ``int(1e-5 * 10000) == 0`` and silently emit zero NULLs -- the engine
# raises on that case, but raising one layer earlier keeps the error
# next to the ``ColumnSpec`` declaration.
_MIN_NULL_FRACTION = 1.0 / 10000


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------


class Uniform(_StrictModel):
    """Uniform distribution (default)."""

    type: Literal["uniform"] = "uniform"


class Normal(_StrictModel):
    """Normal/Gaussian distribution."""

    type: Literal["normal"] = "normal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Normal:
        if self.stddev < 0:
            raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        return self


class LogNormal(_StrictModel):
    """Log-normal distribution."""

    type: Literal["lognormal"] = "lognormal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> LogNormal:
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
    """Zipfian/power-law distribution -- common for realistic cardinality skew."""

    type: Literal["zipf"] = "zipf"
    exponent: float = 1.5

    @model_validator(mode="after")
    def validate_params(self) -> Zipf:
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
    """Exponential distribution."""

    type: Literal["exponential"] = "exponential"
    rate: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Exponential:
        if self.rate <= 0:
            raise ValueError(f"rate must be > 0, got {self.rate}")
        return self


class WeightedValues(_StrictModel):
    """Explicit weighted selection from a list of values."""

    type: Literal["weighted"] = "weighted"
    weights: dict[str, float]

    @model_validator(mode="after")
    def validate_weights(self) -> WeightedValues:
        if not self.weights:
            raise ValueError("weights must not be empty")
        if any(w < 0 for w in self.weights.values()):
            raise ValueError("weights must be non-negative")
        return self


Distribution = Annotated[
    Uniform | Normal | LogNormal | Zipf | Exponential | WeightedValues,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Column generation strategies  (discriminated union on `strategy` field)
# ---------------------------------------------------------------------------


class RangeColumn(_StrictModel):
    """Generate values from a numeric range."""

    strategy: Literal["range"] = "range"
    min: float | int = 0
    max: float | int = 100
    step: float | int | None = None
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_range(self) -> RangeColumn:
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
    """Pick from an explicit list of allowed values."""

    strategy: Literal["values"] = "values"
    values: list[Any]
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_values(self) -> ValuesColumn:
        if not self.values:
            raise ValueError("values list must not be empty")
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
    """Generate data using a Faker provider method."""

    strategy: Literal["faker"] = "faker"
    provider: str
    kwargs: dict[str, Any] = {}
    locale: str | None = None


class PatternColumn(_StrictModel):
    """Generate strings from a pattern template.

    Placeholders:
        {seq}     -- row sequence number (monotonic)
        {uuid}    -- deterministic UUID
        {alpha:N} -- N random alpha chars
        {digit:N} -- N random digits
        {hex:N}   -- N random hex chars
    Example: "ORD-{digit:4}-{alpha:3}" -> "ORD-3847-KMX"
    """

    strategy: Literal["pattern"] = "pattern"
    template: str


class SequenceColumn(_StrictModel):
    """Monotonic integer sequence.

    ``step`` may be negative for a descending sequence; only
    ``step == 0`` is rejected (it would produce a constant column;
    use ``ConstantColumn`` for that).  The sequence value at row
    ``i`` is ``start + i * step``.
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
    """Deterministic UUID generation (v5 from seed + row index)."""

    strategy: Literal["uuid"] = "uuid"


class ExpressionColumn(_StrictModel):
    """Spark SQL expression referencing other columns in the same table.

    Example: ``"quantity * unit_price"`` or ``"concat(first_name, ' ', last_name)"``

    Security note: Expressions are passed directly to ``F.expr()`` and can
    execute arbitrary Spark SQL.  Do not use ExpressionColumn with untrusted
    plan YAML in multi-tenant environments.
    """

    strategy: Literal["expression"] = "expression"
    expr: str


class TimestampColumn(_StrictModel):
    """Generate timestamps within a range."""

    strategy: Literal["timestamp"] = "timestamp"
    start: str = "2020-01-01"
    end: str = "2025-12-31"
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_timestamps(self) -> TimestampColumn:
        from datetime import datetime

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
        if parsed["start"] > parsed["end"]:
            raise ValueError(f"start ({self.start}) must be <= end ({self.end})")
        return self


class ConstantColumn(_StrictModel):
    """Every row gets the same value."""

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
    """

    strategy: Literal["foreign_key"] = "foreign_key"


class StructColumn(_StrictModel):
    """Group child columns into a Spark struct (nested object in JSON).

    Each field is a full ``ColumnSpec`` — supports range, values, faker,
    timestamps, and even nested structs.

    Example JSON output::

        {"address": {"street": "123 Main St", "city": "Austin", "zip": "78701"}}
    """

    strategy: Literal["struct"] = "struct"
    fields: list[ColumnSpec]  # forward ref resolved by model_rebuild() below

    @model_validator(mode="after")
    def validate_unique_field_names(self) -> StructColumn:
        # ``TableSpec.validate_unique_column_names`` only covers top-level
        # columns; a struct with two children named ``x`` would
        # otherwise build an invalid ``StructType`` and every
        # ``.select("addr.x")`` against the result becomes ambiguous.
        seen: set[str] = set()
        dupes: list[str] = []
        for f in self.fields:
            if f.name in seen:
                dupes.append(f.name)
            seen.add(f.name)
        if dupes:
            raise ValueError(
                f"StructColumn has duplicate field names: {sorted(set(dupes))}.  "
                f"Each field must have a unique name within the struct."
            )
        return self


class ArrayColumn(_StrictModel):
    """Generate a variable-length array of values.

    Each element is produced from *element* with a different seed offset.
    The array length per row is random in [min_length, max_length].

    Example JSON output::

        {"tags": ["electronics", "sale", "new"]}
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


# ---------------------------------------------------------------------------
# Data types  (maps to Spark SQL types)
# ---------------------------------------------------------------------------


class DataType(str, Enum):
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"

    # convenience aliases — Python Enum treats duplicate values as
    # aliases of the canonical member (``DataType.INTEGER is DataType.INT``).
    # Also accept common alternate spellings via ``_missing_`` so YAML /
    # JSON plans written with either form round-trip cleanly.
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


# ---------------------------------------------------------------------------
# Primary key and foreign key
# ---------------------------------------------------------------------------


class PrimaryKey(_StrictModel):
    """Marks a column (or set of columns) as the primary key.

    For single-column PKs, just set ``columns`` to a single-element list.
    For composite PKs, list all column names.
    """

    columns: list[str]


class ForeignKeyRef(_StrictModel):
    """Defines a foreign key relationship to another table's primary key.

    ``ref`` uses the familiar "table.column" syntax.  Use ``distribution``
    (e.g. ``Zipf``) to skew child-to-parent mapping toward a small set of
    parents; use ``null_fraction`` for optional FKs.
    """

    ref: str
    distribution: Distribution = Uniform()
    nullable: bool = False
    null_fraction: float = 0.0

    @model_validator(mode="after")
    def validate_ref_format(self) -> ForeignKeyRef:
        if "." not in self.ref:
            raise ValueError(f"ForeignKeyRef.ref='{self.ref}' must use 'table.column' format.")
        if not 0.0 <= self.null_fraction <= 1.0:
            raise ValueError(f"null_fraction must be in [0.0, 1.0], got {self.null_fraction}")
        if 0.0 < self.null_fraction < _MIN_NULL_FRACTION:
            raise ValueError(
                f"ForeignKeyRef.null_fraction={self.null_fraction} is below the "
                f"engine's {_MIN_NULL_FRACTION} granularity; ``int(f * N)`` would "
                f"round to zero and silently emit zero NULLs.  Pick a larger "
                f"fraction (>= {_MIN_NULL_FRACTION}) or raise _NULL_PRECISION in "
                f"engine/seed.py."
            )
        return self


# ---------------------------------------------------------------------------
# Column spec  -- the main column definition
# ---------------------------------------------------------------------------


class ColumnSpec(_StrictModel):
    """A single column in a table.

    At minimum, specify ``name`` and one of the strategy fields.
    The ``dtype`` is inferred from the strategy when not set explicitly.
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

        Leading-underscore names (and the specific CDC metadata
        columns ``_op``, ``_batch_id``, ``_ts``) would silently
        shadow or get shadowed by the internal columns the engine
        adds during generation — e.g. a user column named ``_ts``
        would collide with the CDC timestamp attached by
        ``_add_cdc_metadata`` and get double-counted in
        ``to_sql_server``'s ``__$seqval`` hash.  Internal-only
        markers reserved here: ``_op``, ``_batch_id``, ``_ts``,
        ``_write_batch``, ``_synth_row_id``.

        The name must also be a valid SQL / Python-like identifier
        (``[A-Za-z_][A-Za-z0-9_]*``) so it round-trips through Spark
        without backtick quoting.
        """
        # fullmatch, not match: ``match`` + ``$`` allows a trailing ``\n``
        # because ``$`` anchors before a newline in default mode.
        if not _IDENTIFIER_RE.fullmatch(self.name):
            raise ValueError(
                f"Column name '{self.name}' is not a valid identifier " f"(must match [A-Za-z_][A-Za-z0-9_]*)."
            )
        if self.name.startswith("_"):
            raise ValueError(
                f"Column name '{self.name}' starts with underscore, which is "
                f"reserved for engine-internal metadata columns "
                f"(``_op``, ``_batch_id``, ``_ts``, ``_write_batch``, "
                f"``_synth_row_id``).  Rename to a non-underscore-prefixed "
                f"identifier to avoid silent shadowing."
            )
        if self.name in _RESERVED_CDC_RENAME_TARGETS:
            raise ValueError(
                f"Column name '{self.name}' is reserved for the SQL Server "
                f"CDC format rewrite (``rename_cdc_columns`` renames "
                f"``__$operation`` / ``__$start_lsn`` / ``__$seqval`` into "
                f"``cdc_operation`` / ``cdc_lsn`` / ``cdc_seqval``).  A user "
                f"column with the same name collides silently after rename -- "
                f"rename your column to avoid the collision.  This reservation "
                f"is unconditional because a plan can be handed to any CDC "
                f"format at generation time -- rejecting only for "
                f"``format='sql_server'`` would mean a plan that validates "
                f"today fails the moment someone picks that format, with no "
                f"clear path back to the offending column."
            )
        return self

    @model_validator(mode="after")
    def validate_decimal_precision_scale(self) -> ColumnSpec:
        """Precision/scale are only meaningful for DECIMAL and must go together.

        Defaults (when both unset on a DECIMAL column) are applied at the
        engine layer as ``(18, 2)``; the schema stores ``None`` to keep
        existing serialized plans byte-identical.
        """
        has_precision = self.precision is not None
        has_scale = self.scale is not None
        if not (has_precision or has_scale):
            return self
        if self.dtype != DataType.DECIMAL:
            raise ValueError(
                f"Column '{self.name}': precision/scale are only valid when " f"dtype=DECIMAL, got dtype={self.dtype}"
            )
        if has_precision ^ has_scale:
            raise ValueError(
                f"Column '{self.name}': precision and scale must be set together "
                f"(got precision={self.precision}, scale={self.scale})"
            )
        # Narrow ``int | None`` for mypy: the XOR check above guarantees
        # both are set when we get here, but mypy doesn't track local
        # ``has_*`` flags back to the attributes.
        precision = self.precision
        scale = self.scale
        assert precision is not None and scale is not None
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
        # preserves existing behavior for pre-existing plans.
        if isinstance(self.gen, RangeColumn):
            limit = 10 ** (precision - scale)
            max_abs = max(abs(self.gen.min), abs(self.gen.max))
            if max_abs >= limit:
                max_repr = limit - 10**-scale
                raise ValueError(
                    f"Column '{self.name}': range [{self.gen.min}, {self.gen.max}] "
                    f"does not fit in decimal({precision}, {scale}) "
                    f"(max representable magnitude is {max_repr})"
                )
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
        _non_date_strategies = (RangeColumn, PatternColumn, SequenceColumn, UUIDColumn, FakerColumn)
        if self.dtype == DataType.DATE and isinstance(self.gen, _non_date_strategies):
            if isinstance(self.gen, FakerColumn):
                type_word = "strings (the Faker pool stringifies every value)"
            elif isinstance(self.gen, (RangeColumn, SequenceColumn)):
                type_word = "integers"
            else:
                type_word = "strings"
            raise ValueError(
                f"Column '{self.name}': dtype=DATE is not compatible with "
                f"{type(self.gen).__name__} (which produces {type_word}).  "
                f"Use TimestampColumn for a deterministic random date, or drop "
                f"``dtype=DATE`` to keep the strategy's native type."
            )
        # Faker's pool pandas_udf outputs StringType, and every pool
        # entry is ``str(val)``.  Any declared dtype other than STRING
        # (or None -- engine-default STRING) would be a lie about the
        # resulting column.  Reject the mismatch here rather than let
        # it ship silently.
        # DATE is caught above with its specific message; skip it here.
        _faker_compatible_dtypes = {DataType.STRING, DataType.DATE}
        if isinstance(self.gen, FakerColumn) and self.dtype is not None and self.dtype not in _faker_compatible_dtypes:
            raise ValueError(
                f"Column '{self.name}': FakerColumn always produces StringType "
                f"(the pool stringifies each value via ``str(val)``); declared "
                f"dtype={self.dtype.value} is incompatible.  Drop ``dtype`` "
                f"(or set ``dtype=DataType.STRING``) and cast downstream if you "
                f"need a different type."
            )
        return self

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
                f"fraction (>= {_MIN_NULL_FRACTION}) or raise _NULL_PRECISION in "
                f"engine/seed.py."
            )
        return self

    @model_validator(mode="after")
    def validate_foreign_key_strategy(self) -> ColumnSpec:
        # Both directions of the ForeignKeyColumn <-> foreign_key invariant:
        # the strategy and the FK spec must travel together so legacy plans
        # that used `ConstantColumn(value=None) + foreign_key` fail loudly
        # at load time instead of silently working until someone drops the
        # foreign_key and the column quietly becomes all-NULL.
        if isinstance(self.gen, ForeignKeyColumn) and self.foreign_key is None:
            raise ValueError(
                f"Column '{self.name}' uses ForeignKeyColumn strategy but has no "
                f"foreign_key set. Use the fk() helper, or set foreign_key=ForeignKeyRef(...)."
            )
        if self.foreign_key is not None and not isinstance(self.gen, ForeignKeyColumn):
            raise ValueError(
                f"Column '{self.name}' has foreign_key set but gen is "
                f"{type(self.gen).__name__}. FK columns must use "
                f"ForeignKeyColumn (or the fk() DSL helper); the "
                f"old `ConstantColumn(value=None)` placeholder is no longer accepted."
            )
        # null_fraction can be set on ColumnSpec (like every other column
        # kind) or on the ForeignKeyRef.  Previously the FK path read only
        # ForeignKeyRef.null_fraction, silently ignoring a top-level
        # null_fraction that a user had set.  Allow either source but
        # reject disagreeing non-zero values so the user intent is
        # unambiguous; planner.py resolves to ``max(...)``.
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


# ---------------------------------------------------------------------------
# Table spec
# ---------------------------------------------------------------------------


def parse_human_count(value: int | str) -> int:
    """Parse a human-readable count string like '10M', '1B', '500K' to int.

    Returns *value* unchanged if already an int.
    """
    if isinstance(value, int):
        return value
    suffixes = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    s = str(value).strip().upper()
    for suffix, mult in suffixes.items():
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)]) * mult)
    try:
        return int(s)
    except ValueError:
        raise ValueError(
            f"Invalid row count '{value}'. Expected an integer or a " f"human-readable string like '10K', '1M', '1B'."
        ) from None


class TableSpec(_StrictModel):
    """Defines one table to generate.

    ``rows`` can be an int or a string like "10M", "1B" for readability.
    """

    name: str
    columns: list[ColumnSpec]
    rows: int | str
    primary_key: PrimaryKey | None = None
    seed: int | None = None

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
    def resolve_row_count(self) -> TableSpec:
        self.rows = parse_human_count(self.rows)
        if self.rows <= 0:
            raise ValueError(f"rows must be > 0, got {self.rows}")
        return self


# ---------------------------------------------------------------------------
# Top-level plan
# ---------------------------------------------------------------------------


class DataGenPlan(_StrictModel):
    """Top-level plan describing all tables to generate.

    Tables are generated in dependency order (FK references resolved automatically).

    ``seed`` defaults to 42 for tutorial / demo convenience.  This is a
    known reproducibility trap: two independent callers who both omit
    ``seed`` will produce byte-identical data without realizing the
    reproduction is coincidental.  Constructing a ``DataGenPlan``
    without an explicit ``seed`` emits a ``UserWarning`` via the
    ``_warn_if_seed_missing`` validator so CI / log scrape catches the
    omission.  Production callers should always pass ``seed=<int>``.
    """

    tables: list[TableSpec]
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
