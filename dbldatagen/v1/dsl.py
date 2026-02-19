"""dbldatagen.v1.dsl -- Shorthand constructors for common patterns."""

from __future__ import annotations

from dbldatagen.v1.schema import (
    ArrayColumn,
    ColumnSpec,
    ColumnStrategy,
    ConstantColumn,
    DataType,
    Distribution,
    ExpressionColumn,
    FakerColumn,
    ForeignKeyRef,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    StructColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
    Zipf,
)


# -- Primary key helpers --


def pk_auto(name: str = "id") -> ColumnSpec:
    """Auto-incrementing integer primary key."""
    return ColumnSpec(name=name, dtype=DataType.LONG, gen=SequenceColumn())


def pk_uuid(name: str = "id") -> ColumnSpec:
    """UUID primary key."""
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=UUIDColumn())


def pk_pattern(name: str, template: str) -> ColumnSpec:
    """Patterned string primary key, e.g. "CUST-{digit:6}"."""
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=PatternColumn(template=template))


# -- Foreign key helper --


def fk(
    name: str, ref: str, *, nullable: bool = False, null_fraction: float = 0.0, distribution: Distribution | None = None
) -> ColumnSpec:
    """Foreign key column referencing 'table.column'.

    The dtype and generation strategy are inferred from the referenced PK at plan resolution time.
    """
    return ColumnSpec(
        name=name,
        gen=ConstantColumn(value=None),  # placeholder; replaced at resolve time
        foreign_key=ForeignKeyRef(
            ref=ref,
            nullable=nullable,
            null_fraction=null_fraction,
            distribution=distribution or Zipf(exponent=1.2),
        ),
    )


# -- Common column shorthands --


def integer(
    name: str,
    min_val: int = 0,
    max_val: int = 100,
    seed_from: str | None = None,
    **kw,
) -> ColumnSpec:
    """Create an integer column with values in [min_val, max_val]."""
    return ColumnSpec(
        name=name,
        dtype=DataType.INT,
        gen=RangeColumn(min=min_val, max=max_val, **kw),
        seed_from=seed_from,
    )


def decimal(
    name: str,
    min_val: float = 0.0,
    max_val: float = 1000.0,
    seed_from: str | None = None,
    **kw,
) -> ColumnSpec:
    """Create a decimal (double) column with values in [min_val, max_val]."""
    return ColumnSpec(
        name=name,
        dtype=DataType.DOUBLE,
        gen=RangeColumn(min=min_val, max=max_val, **kw),
        seed_from=seed_from,
    )


def text(name: str, values: list[str], seed_from: str | None = None, **kw) -> ColumnSpec:
    """Create a string column that picks from a list of values."""
    return ColumnSpec(
        name=name,
        dtype=DataType.STRING,
        gen=ValuesColumn(values=values, **kw),
        seed_from=seed_from,
    )


def faker(
    name: str,
    provider: str,
    *,
    dtype: DataType = DataType.STRING,
    locale: str | None = None,
    seed_from: str | None = None,
    **kwargs,
) -> ColumnSpec:
    """Create a column using a Faker provider for realistic text data."""
    return ColumnSpec(
        name=name,
        dtype=dtype,
        gen=FakerColumn(provider=provider, kwargs=kwargs, locale=locale),
        seed_from=seed_from,
    )


def timestamp(
    name: str,
    start: str = "2020-01-01",
    end: str = "2025-12-31",
    seed_from: str | None = None,
    **kw,
) -> ColumnSpec:
    """Create a timestamp column with values in [start, end]."""
    return ColumnSpec(
        name=name,
        dtype=DataType.TIMESTAMP,
        gen=TimestampColumn(start=start, end=end, **kw),
        seed_from=seed_from,
    )


def pattern(name: str, template: str, seed_from: str | None = None) -> ColumnSpec:
    """Create a string column from a pattern template."""
    return ColumnSpec(
        name=name,
        dtype=DataType.STRING,
        gen=PatternColumn(template=template),
        seed_from=seed_from,
    )


def expression(name: str, expr: str, dtype: DataType | None = None) -> ColumnSpec:
    """Create a column from a Spark SQL expression."""
    return ColumnSpec(name=name, dtype=dtype, gen=ExpressionColumn(expr=expr))


# -- Nested types --


def struct(name: str, fields: list[ColumnSpec]) -> ColumnSpec:
    """Nested struct column — produces a JSON object.

    Example::

        struct("address", [
            faker("street", "street_address"),
            faker("city", "city"),
            pattern("zip", "{digit:5}"),
        ])
    """
    return ColumnSpec(name=name, gen=StructColumn(fields=fields))


def array(
    name: str,
    element: ColumnStrategy,
    min_length: int = 1,
    max_length: int = 5,
) -> ColumnSpec:
    """Variable-length array column.

    Example::

        array("tags", ValuesColumn(values=["sale", "new", "popular", "clearance"]),
              min_length=1, max_length=4)
    """
    return ColumnSpec(
        name=name,
        gen=ArrayColumn(element=element, min_length=min_length, max_length=max_length),
    )
