"""dbldatagen.v1.schema -- Public API models."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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


class LogNormal(BaseModel):
    """Log-normal distribution."""

    type: Literal["lognormal"] = "lognormal"
    mean: float = 0.0
    stddev: float = 1.0


class Zipf(BaseModel):
    """Zipfian/power-law distribution -- common for realistic cardinality skew."""

    type: Literal["zipf"] = "zipf"
    exponent: float = 1.5


class Exponential(BaseModel):
    """Exponential distribution."""

    type: Literal["exponential"] = "exponential"
    rate: float = 1.0


class WeightedValues(BaseModel):
    """Explicit weighted selection from a list of values."""

    type: Literal["weighted"] = "weighted"
    weights: dict[str, float]


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


class ValuesColumn(BaseModel):
    """Pick from an explicit list of allowed values."""

    strategy: Literal["values"] = "values"
    values: list[Any]
    distribution: Distribution = Uniform()


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

        for field_name in ("start", "end"):
            val = getattr(self, field_name)
            try:
                datetime.fromisoformat(val)
            except ValueError:
                raise ValueError(
                    f"TimestampColumn.{field_name}='{val}' is not a valid ISO timestamp. "
                    f"Expected format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'."
                ) from None
        return self


class ConstantColumn(BaseModel):
    """Every row gets the same value."""

    strategy: Literal["constant"] = "constant"
    value: Any


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
        if self.min_length > self.max_length:
            raise ValueError(f"min_length ({self.min_length}) must be <= max_length ({self.max_length})")
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

    ``ref`` uses the familiar "table.column" syntax.
    ``cardinality`` controls how many child rows per parent key (default: uniform 1-5).
    """

    ref: str
    cardinality: int | tuple[int, int] | None = None
    distribution: Distribution = Uniform()
    nullable: bool = False
    null_fraction: float = 0.0

    @model_validator(mode="after")
    def validate_ref_format(self) -> ForeignKeyRef:
        if "." not in self.ref:
            raise ValueError(f"ForeignKeyRef.ref='{self.ref}' must use 'table.column' format.")
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

    @model_validator(mode="after")
    def validate_null_fraction(self) -> ColumnSpec:
        if self.null_fraction > 0 and not self.nullable:
            self.nullable = True
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
    def resolve_row_count(self) -> TableSpec:
        self.rows = parse_human_count(self.rows)
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
