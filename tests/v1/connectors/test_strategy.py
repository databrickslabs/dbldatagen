"""Tests for smart strategy selection."""

from __future__ import annotations

import pytest

from dbldatagen.v1.connectors.base import InferredColumn
from dbldatagen.v1.connectors.strategy import select_strategy
from dbldatagen.v1.schema import (
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


def _col(**overrides) -> InferredColumn:
    """Shorthand for building an InferredColumn with defaults."""
    defaults = dict(
        name="col",
        native_type="TEXT",
        synth_dtype=DataType.STRING,
        nullable=False,
        is_primary_key=False,
        is_foreign_key=False,
        fk_references=None,
        unique=False,
        sample_values=[],
        distinct_count=None,
    )
    defaults.update(overrides)
    return InferredColumn(**defaults)


class TestPKStrategy:
    def test_numeric_pk(self):
        s = select_strategy(_col(name="id", is_primary_key=True, synth_dtype=DataType.LONG))
        assert isinstance(s, SequenceColumn)

    def test_string_pk_uuid_name(self):
        s = select_strategy(_col(name="uuid", is_primary_key=True))
        assert isinstance(s, UUIDColumn)

    def test_string_pk_guid_name(self):
        s = select_strategy(_col(name="guid_col", is_primary_key=True))
        assert isinstance(s, UUIDColumn)

    def test_string_pk_generic(self):
        s = select_strategy(_col(name="product_code", is_primary_key=True))
        assert isinstance(s, PatternColumn)
        assert "PRODUCT_CODE" in s.template


class TestFKStrategy:
    def test_fk_returns_constant_placeholder(self):
        s = select_strategy(_col(name="customer_id", is_foreign_key=True, fk_references="customers.id"))
        assert isinstance(s, ConstantColumn)
        assert s.value is None


class TestNameHeuristics:
    @pytest.mark.parametrize(
        "col_name,expected_provider",
        [
            ("email", "email"),
            ("user_email", "email"),
            ("phone", "phone_number"),
            ("mobile_phone", "phone_number"),
            ("first_name", "first_name"),
            ("last_name", "last_name"),
            ("full_name", "name"),
            ("address", "address"),
            ("city", "city"),
            ("state", "state"),
            ("zipcode", "zipcode"),
            ("country", "country"),
            ("company", "company"),
            ("url", "url"),
            ("username", "user_name"),
            ("ip_address", "ipv4"),
        ],
    )
    def test_faker_heuristic(self, col_name, expected_provider):
        s = select_strategy(_col(name=col_name))
        assert isinstance(s, FakerColumn)
        assert s.provider == expected_provider


class TestLowCardinality:
    def test_low_cardinality_uses_values(self):
        s = select_strategy(
            _col(
                name="status",
                sample_values=["active", "inactive", "pending"],
                distinct_count=3,
            )
        )
        assert isinstance(s, ValuesColumn)
        assert set(s.values) == {"active", "inactive", "pending"}

    def test_high_cardinality_skips_values(self):
        s = select_strategy(
            _col(
                name="description",
                sample_values=[f"desc_{i}" for i in range(100)],
                distinct_count=100,
            )
        )
        assert not isinstance(s, ValuesColumn)


class TestTypeDefaults:
    def test_integer(self):
        s = select_strategy(_col(name="quantity", synth_dtype=DataType.LONG))
        assert isinstance(s, RangeColumn)

    def test_double(self):
        s = select_strategy(_col(name="price", synth_dtype=DataType.DOUBLE))
        assert isinstance(s, RangeColumn)

    def test_boolean(self):
        s = select_strategy(_col(name="is_active", synth_dtype=DataType.BOOLEAN))
        assert isinstance(s, ValuesColumn)
        assert set(s.values) == {True, False}

    def test_timestamp(self):
        s = select_strategy(_col(name="created_at", synth_dtype=DataType.TIMESTAMP))
        assert isinstance(s, TimestampColumn)

    def test_date(self):
        s = select_strategy(_col(name="birth_date", synth_dtype=DataType.DATE))
        assert isinstance(s, TimestampColumn)

    def test_unique_string(self):
        s = select_strategy(_col(name="token", unique=True))
        assert isinstance(s, UUIDColumn)

    def test_generic_string(self):
        s = select_strategy(_col(name="description"))
        assert isinstance(s, PatternColumn)
