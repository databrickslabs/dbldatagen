"""dbldatagen.core.spec.schema -- Public API models."""

from __future__ import annotations

import re
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Upper bound on ArrayColumn.max_length.  Each slot materialises its own
# Spark expression tree at plan time, so Catalyst work scales linearly
# with this value (and with nested arrays/structs, multiplicatively).
# 1000 is already far beyond realistic array columns; past that, the
# user wants rows or MapType.
_MAX_ARRAY_LENGTH = 1000


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------


class Uniform(BaseModel):
    """Uniform distribution (default)."""

    type: Literal["uniform"] = "uniform"


class Normal(BaseModel):
    """Normal/Gaussian distribution."""

    type: Literal["normal"] = "normal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Normal:
        if self.stddev < 0:
            raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        return self


class LogNormal(BaseModel):
    """Log-normal distribution."""

    type: Literal["lognormal"] = "lognormal"
    mean: float = 0.0
    stddev: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> LogNormal:
        if self.stddev < 0:
            raise ValueError(f"stddev must be >= 0, got {self.stddev}")
        return self


class Zipf(BaseModel):
    """Zipfian/power-law distribution -- common for realistic cardinality skew."""

    type: Literal["zipf"] = "zipf"
    exponent: float = 1.5

    @model_validator(mode="after")
    def validate_params(self) -> Zipf:
        if self.exponent <= 0:
            raise ValueError(f"exponent must be > 0, got {self.exponent}")
        return self


class Exponential(BaseModel):
    """Exponential distribution."""

    type: Literal["exponential"] = "exponential"
    rate: float = 1.0

    @model_validator(mode="after")
    def validate_params(self) -> Exponential:
        if self.rate <= 0:
            raise ValueError(f"rate must be > 0, got {self.rate}")
        return self


class WeightedValues(BaseModel):
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


class RangeColumn(BaseModel):
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
        return self


class ValuesColumn(BaseModel):
    """Pick from an explicit list of allowed values."""

    strategy: Literal["values"] = "values"
    values: list[Any]
    distribution: Distribution = Uniform()

    @model_validator(mode="after")
    def validate_values(self) -> ValuesColumn:
        if not self.values:
            raise ValueError("values list must not be empty")
        return self


class FakerColumn(BaseModel):
    """Generate data using a Faker provider method."""

    strategy: Literal["faker"] = "faker"
    provider: str
    kwargs: dict[str, Any] = {}
    locale: str | None = None


class PatternColumn(BaseModel):
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


class SequenceColumn(BaseModel):
    """Monotonically increasing integer sequence."""

    strategy: Literal["sequence"] = "sequence"
    start: int = 1
    step: int = 1

    @model_validator(mode="after")
    def validate_step(self) -> SequenceColumn:
        if self.step == 0:
            raise ValueError("step must not be 0")
        return self


class UUIDColumn(BaseModel):
    """Deterministic UUID generation (v5 from seed + row index)."""

    strategy: Literal["uuid"] = "uuid"


class ExpressionColumn(BaseModel):
    """Spark SQL expression referencing other columns in the same table.

    Example: ``"quantity * unit_price"`` or ``"concat(first_name, ' ', last_name)"``

    Security note: Expressions are passed directly to ``F.expr()`` and can
    execute arbitrary Spark SQL.  Do not use ExpressionColumn with untrusted
    plan YAML in multi-tenant environments.
    """

    strategy: Literal["expression"] = "expression"
    expr: str


class TimestampColumn(BaseModel):
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


class ConstantColumn(BaseModel):
    """Every row gets the same value."""

    strategy: Literal["constant"] = "constant"
    value: Any


class ForeignKeyColumn(BaseModel):
    """Marker strategy for columns whose values are resolved from ``foreign_key``.

    The actual generation — dtype inference, distribution, null fraction,
    and the lookup into the parent table's PK — is driven by
    ``ColumnSpec.foreign_key`` at plan resolution time.  This strategy
    exists so FK columns have a real type in the ``ColumnStrategy`` union
    instead of a ``ConstantColumn(value=None)`` sentinel that would
    silently produce all-NULL output if FK resolution were ever skipped.
    """

    strategy: Literal["foreign_key"] = "foreign_key"


class StructColumn(BaseModel):
    """Group child columns into a Spark struct (nested object in JSON).

    Each field is a full ``ColumnSpec`` — supports range, values, faker,
    timestamps, and even nested structs.

    Example JSON output::

        {"address": {"street": "123 Main St", "city": "Austin", "zip": "78701"}}
    """

    strategy: Literal["struct"] = "struct"
    fields: list[ColumnSpec]  # forward ref resolved by model_rebuild() below


class ArrayColumn(BaseModel):
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

    # convenience aliases
    INTEGER = "int"


# ---------------------------------------------------------------------------
# Primary key and foreign key
# ---------------------------------------------------------------------------


class PrimaryKey(BaseModel):
    """Marks a column (or set of columns) as the primary key.

    For single-column PKs, just set ``columns`` to a single-element list.
    For composite PKs, list all column names.
    """

    columns: list[str]


class ForeignKeyRef(BaseModel):
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
        return self


# ---------------------------------------------------------------------------
# Column spec  -- the main column definition
# ---------------------------------------------------------------------------


class ColumnSpec(BaseModel):
    """A single column in a table.

    At minimum, specify ``name`` and one of the strategy fields.
    The ``dtype`` is inferred from the strategy when not set explicitly.
    """

    model_config = ConfigDict(populate_by_name=True)

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
                f"Column name '{self.name}' is not a valid identifier "
                f"(must match [A-Za-z_][A-Za-z0-9_]*)."
            )
        if self.name.startswith("_"):
            raise ValueError(
                f"Column name '{self.name}' starts with underscore, which is "
                f"reserved for engine-internal metadata columns "
                f"(``_op``, ``_batch_id``, ``_ts``, ``_write_batch``, "
                f"``_synth_row_id``).  Rename to a non-underscore-prefixed "
                f"identifier to avoid silent shadowing."
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
    def validate_null_fraction(self) -> ColumnSpec:
        if not 0.0 <= self.null_fraction <= 1.0:
            raise ValueError(f"null_fraction must be in [0.0, 1.0], got {self.null_fraction}")
        # Convenience: setting null_fraction > 0 implies nullable=True.
        # This avoids requiring users to always set both fields explicitly.
        if self.null_fraction > 0 and not self.nullable:
            self.nullable = True
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


class TableSpec(BaseModel):
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
                f"Table name '{self.name}' is not a valid identifier.  "
                f"Must match [a-zA-Z_][a-zA-Z0-9_]*."
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


class DataGenPlan(BaseModel):
    """Top-level plan describing all tables to generate.

    Tables are generated in dependency order (FK references resolved automatically).
    """

    tables: list[TableSpec]
    seed: int = 42
    default_locale: str = "en_US"

    @model_validator(mode="after")
    def propagate_seeds(self) -> DataGenPlan:
        """Assign deterministic per-table seeds from global seed when not set."""
        for i, table in enumerate(self.tables):
            if table.seed is None:
                table.seed = self.seed + i
        return self


# Resolve forward references for recursive types (StructColumn, ArrayColumn)
StructColumn.model_rebuild()
ArrayColumn.model_rebuild()
ColumnSpec.model_rebuild()
