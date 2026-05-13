"""Lowercase factory helpers for building ``ColumnSpec`` objects.

The recommended import is::

    from dbldatagen.core.spec import dsl as datagendg

    datagendg.pk_auto("id")
    datagendg.integer("age", 0, 99)
    datagendg.decimal("price", precision=10, scale=2)
    datagendg.faker("name", provider="name")

The helpers are *not* re-exported from ``dbldatagen.core`` or
``dbldatagen.core.spec`` because several of the lowercase names would
shadow stdlib modules (``decimal``, ``array``, ``struct``) and the
PyPI ``faker`` package when flat-imported.  The ``datagendg.`` prefix keeps
every DSL call unambiguous without renaming the helpers themselves.

The pattern mirrors the established PySpark convention
(``import pyspark.sql.functions as F`` then ``F.col(...)``).  Direct
imports (``from dbldatagen.core.spec.dsl import integer``) continue to
work for callers who prefer them.
"""

from __future__ import annotations

from dbldatagen.core.spec.schema import (
    ArrayColumn,
    ColumnSpec,
    ColumnStrategy,
    ConstantColumn,
    DataType,
    Distribution,
    ExpressionColumn,
    FakerColumn,
    ForeignKeyColumn,
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
    """Auto-incrementing integer primary key (1, 2, 3, ...).

    Args:
        name: Column name. Defaults to ``"id"``.

    Returns:
        ``ColumnSpec`` with ``DataType.LONG`` and a ``SequenceColumn``
        generator. Pair with ``PrimaryKey(columns=[name])`` on the
        ``TableSpec`` to make the column part of the table's PK.
    """
    return ColumnSpec(name=name, dtype=DataType.LONG, gen=SequenceColumn())


def pk_uuid(name: str = "id") -> ColumnSpec:
    """Deterministic UUID primary key.

    Args:
        name: Column name. Defaults to ``"id"``.

    Returns:
        ``ColumnSpec`` with ``DataType.STRING`` and a ``UUIDColumn``
        generator. Output is reproducible: the same ``(plan.seed, row)``
        always yields the same UUID.
    """
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=UUIDColumn())


def pk_pattern(name: str, template: str) -> ColumnSpec:
    """Patterned string primary key.

    Args:
        name: Column name.
        template: Pattern template using core placeholders, e.g.
            ``"CUST-{digit:6}"``. Supported placeholders: ``{digit:N}``,
            ``{alpha:N}``, ``{hex:N}``, ``{seq}``, ``{uuid}``.

    Returns:
        ``ColumnSpec`` with ``DataType.STRING`` and a ``PatternColumn``
        generator.
    """
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=PatternColumn(template=template))


# -- Foreign key helper --


def fk(
    name: str,
    ref: str,
    *,
    nullable: bool = False,
    null_fraction: float = 0.0,
    distribution: Distribution | None = None,
) -> ColumnSpec:
    """Foreign key column referencing a parent table's primary key.

    The dtype and generation strategy are inferred from the referenced
    PK at plan resolution time, so the FK column always matches the
    parent column's type.

    Args:
        name: Column name on the child table.
        ref: Reference in ``"table.column"`` form, e.g.
            ``"customers.id"``.
        nullable: Whether the FK column is allowed to be NULL.
            Defaults to ``False``.
        null_fraction: When ``nullable=True``, fraction of rows
            (``[0.0, 1.0]``) that should be NULL.
        distribution: Sampling distribution over parent rows. Defaults
            to ``Zipf(exponent=1.2)`` so a small set of parents gets
            disproportionately many children, which matches typical
            production data.

    Returns:
        ``ColumnSpec`` with a ``ForeignKeyColumn`` generator and an
        attached ``ForeignKeyRef``.
    """
    return ColumnSpec(
        name=name,
        gen=ForeignKeyColumn(),
        foreign_key=ForeignKeyRef(
            ref=ref,
            nullable=nullable,
            null_fraction=null_fraction,
            distribution=distribution or Zipf(exponent=1.2),
        ),
    )


# -- Common column shorthands --


def integer(name: str, min: float | int = 0, max: float | int = 100, seed_from: str | None = None, **kw) -> ColumnSpec:
    """Build an INT column spec drawing from ``[min, max]``.

    Args:
        name: Column name.
        min: Lower bound (inclusive). Defaults to ``0``.
        max: Upper bound (inclusive). Defaults to ``100``.
        seed_from: Optional name of another column to derive the cell
            seed from -- the equivalent of v0's ``baseColumn``. When
            set, two rows with the same value in ``seed_from`` always
            produce the same value in this column.
        **kw: Extra ``RangeColumn`` fields, e.g. ``distribution`` or
            ``step``.

    Returns:
        ``ColumnSpec`` with ``DataType.INT`` and a ``RangeColumn``
        generator.
    """
    return ColumnSpec(
        name=name,
        dtype=DataType.INT,
        gen=RangeColumn(min=min, max=max, **kw),
        seed_from=seed_from,
    )


def double(
    name: str,
    min: float | int = 0.0,
    max: float | int = 1.0,
    seed_from: str | None = None,
    **kw,
) -> ColumnSpec:
    """Build a DOUBLE column spec drawing from ``[min, max]``.

    The migration target for v0's ``DoubleType()`` / ``FloatType()``.
    Use ``decimal()`` instead for fixed-precision financial values.

    Args:
        name: Column name.
        min: Lower bound (inclusive). Defaults to ``0.0``.
        max: Upper bound (inclusive). Defaults to ``1.0``.
        seed_from: Optional name of another column to derive the cell
            seed from. Same value in ``seed_from`` -> same generated
            double.
        **kw: Extra ``RangeColumn`` fields, e.g. ``distribution``.

    Returns:
        ``ColumnSpec`` with ``DataType.DOUBLE`` and a ``RangeColumn``
        generator.
    """
    return ColumnSpec(
        name=name,
        dtype=DataType.DOUBLE,
        gen=RangeColumn(min=min, max=max, **kw),
        seed_from=seed_from,
    )


def decimal(
    name: str,
    min: float | int = 0.0,
    max: float | int = 1000.0,
    seed_from: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
    **kw,
) -> ColumnSpec:
    """Build a DECIMAL column spec drawing from ``[min, max]``.

    Use this for fixed-precision financial values; use ``double()``
    for general-purpose floats.

    Args:
        name: Column name.
        min: Lower bound (inclusive). Defaults to ``0.0``.
        max: Upper bound (inclusive). Defaults to ``1000.0``.
        seed_from: Optional name of another column to derive the cell
            seed from.
        precision: Total number of digits. ``None`` uses the engine
            default ``DecimalType(18, 2)``.
        scale: Digits after the decimal point. ``None`` uses the
            engine default.  Pass both with ``precision`` to override,
            e.g. ``decimal("rate", precision=10, scale=4)``.
        **kw: Extra ``RangeColumn`` fields, e.g. ``distribution``.

    Returns:
        ``ColumnSpec`` with ``DataType.DECIMAL`` and a ``RangeColumn``
        generator.
    """
    return ColumnSpec(
        name=name,
        dtype=DataType.DECIMAL,
        gen=RangeColumn(min=min, max=max, **kw),
        seed_from=seed_from,
        precision=precision,
        scale=scale,
    )


def text(name: str, values: list[str], seed_from: str | None = None, **kw) -> ColumnSpec:
    """Build a STRING column spec drawing from a discrete value list.

    Args:
        name: Column name.
        values: Candidate string values. Each row picks one.
        seed_from: Optional name of another column to derive the cell
            seed from. Same value in ``seed_from`` -> same picked
            string.
        **kw: Extra ``ValuesColumn`` fields, e.g.
            ``distribution=WeightedValues(weights={...})`` for biased
            selection.

    Returns:
        ``ColumnSpec`` with ``DataType.STRING`` and a ``ValuesColumn``
        generator.
    """
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
    """Build a column spec backed by a Faker provider.

    Requires the ``faker`` extra (``pip install dbldatagen[core-faker]``).

    Args:
        name: Column name.
        provider: Faker method name, e.g. ``"email"``, ``"name"``,
            ``"street_address"``.
        dtype: Output type. Defaults to ``DataType.STRING``.
        locale: Optional Faker locale, e.g. ``"de_DE"``. Defaults to
            ``None`` (engine picks ``"en_US"``).
        seed_from: Optional name of another column to derive the cell
            seed from.
        **kwargs: Extra keyword args forwarded to the Faker provider
            method itself.

    Returns:
        ``ColumnSpec`` with the requested ``dtype`` and a
        ``FakerColumn`` generator.
    """
    return ColumnSpec(
        name=name,
        dtype=dtype,
        gen=FakerColumn(provider=provider, kwargs=kwargs, locale=locale),
        seed_from=seed_from,
    )


def timestamp(name: str, start: str, end: str, seed_from: str | None = None, **kw) -> ColumnSpec:
    """Build a TIMESTAMP column spec drawing from ``[start, end]``.

    Args:
        name: Column name.
        start: Inclusive lower bound, parseable as a date or timestamp
            (e.g. ``"2020-01-01"`` or ``"2020-01-01 00:00:00"``).
            Required -- no universal default makes sense for a time
            range, so callers must specify their own bounds.
        end: Inclusive upper bound, same format as ``start``.  Required.
        seed_from: Optional name of another column to derive the cell
            seed from.
        **kw: Extra ``TimestampColumn`` fields, e.g. ``distribution``.

    Returns:
        ``ColumnSpec`` with ``DataType.TIMESTAMP`` and a
        ``TimestampColumn`` generator. Output is session-TZ-independent
        (UTC epoch seconds).
    """
    return ColumnSpec(
        name=name,
        dtype=DataType.TIMESTAMP,
        gen=TimestampColumn(start=start, end=end, **kw),
        seed_from=seed_from,
    )


def pattern(name: str, template: str, seed_from: str | None = None) -> ColumnSpec:
    """Build a STRING column spec from a placeholder template.

    Args:
        name: Column name.
        template: Pattern template, e.g. ``"ORD-{digit:4}"``.
            Supported placeholders: ``{digit:N}``, ``{alpha:N}``,
            ``{hex:N}``, ``{seq}``, ``{uuid}``.
        seed_from: Optional name of another column to derive the cell
            seed from. Same value in ``seed_from`` -> same generated
            string.

    Returns:
        ``ColumnSpec`` with ``DataType.STRING`` and a ``PatternColumn``
        generator.
    """
    return ColumnSpec(
        name=name,
        dtype=DataType.STRING,
        gen=PatternColumn(template=template),
        seed_from=seed_from,
    )


def expression(name: str, expr: str, dtype: DataType | None = None) -> ColumnSpec:
    """Build a column spec computed from a Spark SQL expression.

    Args:
        name: Column name.
        expr: Spark SQL expression string referencing already-generated
            columns, e.g. ``"quantity * unit_price"``.
        dtype: Optional explicit output type. When ``None`` the type
            is inferred by Spark from the expression.

    Returns:
        ``ColumnSpec`` with the given ``dtype`` (or inferred) and an
        ``ExpressionColumn`` generator.
    """
    return ColumnSpec(name=name, dtype=dtype, gen=ExpressionColumn(expr=expr))


def constant(name: str, value: object, dtype: DataType | None = None) -> ColumnSpec:
    """Build a column spec where every row gets the same literal value.

    Useful for environment markers, version stamps, batch IDs, or any
    column with no per-row variation.

    Args:
        name: Column name.
        value: The literal value emitted on every row.  Any JSON-
            serialisable Python literal that ``F.lit`` accepts.
        dtype: Optional Spark ``DataType`` override.  When ``None``
            (default), Spark infers it from the Python type of
            ``value``.

    Returns:
        ``ColumnSpec`` with the given ``dtype`` (or inferred) and a
        ``ConstantColumn`` generator.
    """
    return ColumnSpec(name=name, dtype=dtype, gen=ConstantColumn(value=value))


# -- Nested types --


def struct(name: str, fields: list[ColumnSpec]) -> ColumnSpec:
    """Nested struct column.

    Args:
        name: Column name.
        fields: Nested ``ColumnSpec`` list defining the struct's
            fields. Each field is generated like a top-level column,
            with seeds isolated per field.

    Returns:
        ``ColumnSpec`` with a ``StructColumn`` generator. The dtype is
        derived from the field list.

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

    Args:
        name: Column name.
        element: Generator strategy for a single element, e.g.
            ``ValuesColumn(...)`` or ``RangeColumn(...)``.
        min_length: Minimum number of elements per row (inclusive).
            Defaults to ``1``.
        max_length: Maximum number of elements per row (inclusive).
            Defaults to ``5``.

    Returns:
        ``ColumnSpec`` with an ``ArrayColumn`` generator producing
        arrays of length uniformly drawn from ``[min_length, max_length]``.

    Example::

        array("tags", ValuesColumn(values=["sale", "new", "popular", "clearance"]),
              min_length=1, max_length=4)
    """
    return ColumnSpec(
        name=name,
        gen=ArrayColumn(element=element, min_length=min_length, max_length=max_length),
    )
