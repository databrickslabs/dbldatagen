"""Map column names to dbldatagen.v1 generation strategies."""

from __future__ import annotations

import re
from collections.abc import Callable

from dbldatagen.v1.schema import (
    ColumnSpec,
    DataType,
    FakerColumn,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Rule table: (pattern, factory_fn)
# First match wins.
# ---------------------------------------------------------------------------


def _pk_auto(name: str) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.LONG, gen=SequenceColumn())


def _faker(name: str, provider: str, dtype: DataType = DataType.STRING) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=dtype, gen=FakerColumn(provider=provider))


def _integer(name: str, lo: int = 0, hi: int = 100) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.INT, gen=RangeColumn(min=lo, max=hi))


def _decimal(name: str, lo: float = 0.0, hi: float = 1000.0) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.DOUBLE, gen=RangeColumn(min=lo, max=hi))


def _timestamp(name: str) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.TIMESTAMP, gen=TimestampColumn())


def _values(name: str, vals: list[str]) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=ValuesColumn(values=vals))


def _boolean(name: str) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.BOOLEAN, gen=ValuesColumn(values=[True, False]))


def _uuid(name: str) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=UUIDColumn())


def _pattern(name: str, template: str) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.STRING, gen=PatternColumn(template=template))


# Each rule: (compiled regex, callable(col_name) -> ColumnSpec)
_NAME_RULES: list[tuple[re.Pattern[str], Callable[[str], ColumnSpec]]] = [
    # --- Boolean (before contact to prevent has_email matching _email$) ---
    (re.compile(r"^is_|^has_|_flag$|^active$|^enabled$|^verified$|^deleted$", re.I), lambda n: _boolean(n)),
    # --- Contact info (Faker) ---
    (re.compile(r"^first_name$", re.I), lambda n: _faker(n, "first_name")),
    (re.compile(r"^last_name$", re.I), lambda n: _faker(n, "last_name")),
    (re.compile(r"^email$|_email$", re.I), lambda n: _faker(n, "email")),
    (re.compile(r"^name$|_name$|^full_name$", re.I), lambda n: _faker(n, "name")),
    (re.compile(r"phone|mobile|fax", re.I), lambda n: _faker(n, "phone_number")),
    (re.compile(r"^address$|^street", re.I), lambda n: _faker(n, "street_address")),
    (re.compile(r"^city$", re.I), lambda n: _faker(n, "city")),
    (re.compile(r"^state$|^province$", re.I), lambda n: _faker(n, "state")),
    (re.compile(r"^country$", re.I), lambda n: _faker(n, "country")),
    (re.compile(r"^zip|^postal", re.I), lambda n: _faker(n, "zipcode")),
    (re.compile(r"^company$|^organization$", re.I), lambda n: _faker(n, "company")),
    (re.compile(r"^url$|^website$|^homepage$", re.I), lambda n: _faker(n, "url")),
    (re.compile(r"^ip$|^ip_address$", re.I), lambda n: _faker(n, "ipv4")),
    (re.compile(r"^user_?agent$", re.I), lambda n: _faker(n, "user_agent")),
    (re.compile(r"^username$|^user_name$", re.I), lambda n: _faker(n, "user_name")),
    # --- UUID ---
    (re.compile(r"^uuid$|^guid$", re.I), lambda n: _uuid(n)),
    # --- Temporal ---
    (re.compile(r"_at$|_date$|_time$|^created|^updated|^timestamp|^date$|_on$", re.I), lambda n: _timestamp(n)),
    # --- Money / decimal ---
    (
        re.compile(r"price|cost|amount|total|revenue|salary|fee|balance|tax|discount", re.I),
        lambda n: _decimal(n, 0.01, 9999.99),
    ),
    # --- Counts / integers ---
    (re.compile(r"quantity|qty", re.I), lambda n: _integer(n, 1, 100)),
    (re.compile(r"^age$", re.I), lambda n: _integer(n, 18, 90)),
    (re.compile(r"^year$", re.I), lambda n: _integer(n, 2000, 2026)),
    (re.compile(r"_count$|^num_|_num$", re.I), lambda n: _integer(n, 0, 1000)),
    # --- Scores / ratios ---
    (re.compile(r"rating|score", re.I), lambda n: _decimal(n, 1.0, 5.0)),
    (re.compile(r"percent|pct|ratio|fraction", re.I), lambda n: _decimal(n, 0.0, 1.0)),
    # --- Geo ---
    (re.compile(r"latitude|^lat$", re.I), lambda n: _decimal(n, -90.0, 90.0)),
    (re.compile(r"longitude|^lng$|^lon$", re.I), lambda n: _decimal(n, -180.0, 180.0)),
    # --- Dimensions ---
    (re.compile(r"weight|mass", re.I), lambda n: _decimal(n, 0.1, 1000.0)),
    (re.compile(r"height|length|width|depth", re.I), lambda n: _decimal(n, 0.1, 500.0)),
    # --- Enum-like ---
    (re.compile(r"^status$|_status$", re.I), lambda n: _values(n, ["active", "inactive", "pending"])),
    (re.compile(r"^type$|_type$|^category$|_category$", re.I), lambda n: _values(n, ["type_a", "type_b", "type_c"])),
    (re.compile(r"^tier$|^level$|^plan$", re.I), lambda n: _values(n, ["free", "basic", "premium"])),
    (re.compile(r"^gender$|^sex$", re.I), lambda n: _values(n, ["M", "F", "Other"])),
    (re.compile(r"^currency$", re.I), lambda n: _values(n, ["USD", "EUR", "GBP", "JPY"])),
    (
        re.compile(r"^payment_method$|^pay_type$", re.I),
        lambda n: _values(n, ["credit_card", "debit_card", "paypal", "bank_transfer"]),
    ),
    (re.compile(r"^order_status$", re.I), lambda n: _values(n, ["pending", "shipped", "delivered", "cancelled"])),
    # --- Text / description ---
    (
        re.compile(r"description|^desc$|summary|notes|comment|bio|body|content|^text$|^message$", re.I),
        lambda n: _faker(n, "paragraph"),
    ),
    (re.compile(r"^title$|^subject$|^headline$", re.I), lambda n: _faker(n, "sentence")),
]


def map_column_name(name: str, dtype: DataType = DataType.STRING) -> ColumnSpec:
    """Map a column name to a ColumnSpec with an appropriate generation strategy.

    Parameters
    ----------
    name : str
        Column name.
    dtype : DataType
        Pre-inferred DataType (used as fallback context).

    Returns
    -------
    ColumnSpec
    """
    for pattern, factory in _NAME_RULES:
        if pattern.search(name):
            spec = factory(name)
            return spec

    # Fallback: generate patterned strings
    return _pattern(name, f"{name}_{{digit:6}}")
