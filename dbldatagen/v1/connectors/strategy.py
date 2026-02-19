"""Intelligent generation strategy selection for extracted columns."""

from __future__ import annotations

from dbldatagen.v1.connectors.base import InferredColumn
from dbldatagen.v1.schema import (
    ColumnStrategy,
    ConstantColumn,
    DataType,
    FakerColumn,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


# Column-name substrings → Faker provider.
_FAKER_PATTERNS: list[tuple[tuple[str, ...], str]] = [
    (("email", "mail"), "email"),
    (("phone", "telephone", "mobile"), "phone_number"),
    (("first_name", "fname", "given_name"), "first_name"),
    (("last_name", "lname", "surname", "family_name"), "last_name"),
    (("full_name",), "name"),
    (("ip_address", "ipaddr"), "ipv4"),
    (("address", "street", "addr"), "address"),
    (("city", "town"), "city"),
    (("state", "province"), "state"),
    (("zip", "zipcode", "postal", "postcode"), "zipcode"),
    (("country",), "country"),
    (("company", "organization", "org"), "company"),
    (("url", "website"), "url"),
    (("username", "user_name", "login"), "user_name"),
]

LOW_CARDINALITY_THRESHOLD = 50


def _select_pk_strategy(column: InferredColumn) -> ColumnStrategy:
    """Return the strategy for a primary-key column."""
    if column.synth_dtype == DataType.STRING:
        if "uuid" in column.name.lower() or "guid" in column.name.lower():
            return UUIDColumn()
        return PatternColumn(template=f"{column.name.upper()}-{{digit:8}}")
    return SequenceColumn(start=1, step=1)


def _select_type_default(column: InferredColumn, default_rows: int) -> ColumnStrategy:
    """Return a strategy based on the column's data type."""
    dtype = column.synth_dtype

    _TYPE_STRATEGIES: dict[DataType, ColumnStrategy] = {
        DataType.INT: RangeColumn(min=0, max=max(10_000, default_rows * 10)),
        DataType.LONG: RangeColumn(min=0, max=max(10_000, default_rows * 10)),
        DataType.FLOAT: RangeColumn(min=0.0, max=10_000.0),
        DataType.DOUBLE: RangeColumn(min=0.0, max=10_000.0),
        DataType.DECIMAL: RangeColumn(min=0.0, max=10_000.0),
        DataType.BOOLEAN: ValuesColumn(values=[True, False]),
        DataType.DATE: TimestampColumn(start="2020-01-01", end="2025-12-31"),
        DataType.TIMESTAMP: TimestampColumn(start="2020-01-01", end="2025-12-31"),
    }

    if dtype in _TYPE_STRATEGIES:
        return _TYPE_STRATEGIES[dtype]

    if dtype == DataType.STRING:
        if column.unique:
            return UUIDColumn()
        return PatternColumn(template="{alpha:3}-{digit:4}")

    return RangeColumn(min=0, max=100)


def select_strategy(column: InferredColumn, default_rows: int = 1000) -> ColumnStrategy:
    """Choose the best generation strategy for *column*.

    Decision priority:
    1. Primary key → SequenceColumn / UUIDColumn / PatternColumn
    2. Foreign key → ConstantColumn placeholder (caller adds ForeignKeyRef)
    3. Column-name heuristics → FakerColumn
    4. Low cardinality (sampled) → ValuesColumn
    5. Type-based defaults
    """

    # 1. Primary key
    if column.is_primary_key:
        return _select_pk_strategy(column)

    # 2. Foreign key placeholder
    if column.is_foreign_key:
        return ConstantColumn(value=None)

    # 3. Name-based Faker heuristics
    name_lower = column.name.lower()
    for patterns, provider in _FAKER_PATTERNS:
        if any(p in name_lower for p in patterns):
            return FakerColumn(provider=provider)

    # 4. Low cardinality from sampling
    if column.sample_values and column.distinct_count is not None:
        if column.distinct_count <= LOW_CARDINALITY_THRESHOLD:
            distinct = sorted({str(v) for v in column.sample_values})
            return ValuesColumn(values=distinct[:LOW_CARDINALITY_THRESHOLD])

    # 5. Type-based defaults
    return _select_type_default(column, default_rows)
