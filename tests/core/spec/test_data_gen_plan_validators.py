"""Adversarial-input tests for ``DataGenPlan`` validators."""

from __future__ import annotations

import warnings

import pytest

from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    RangeColumn,
    TableSpec,
)


def _trivial_table(name: str) -> TableSpec:
    return TableSpec(
        name=name,
        rows=10,
        columns=[ColumnSpec(name="id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9))],
    )


def test_data_gen_plan_rejects_empty_tables():
    """An empty plan has nothing to generate -- almost certainly a
    user error (``tables: []`` instead of forgetting the section).
    """
    with pytest.raises(ValueError, match="at least 1"):
        DataGenPlan(tables=[], seed=42)


def test_data_gen_plan_rejects_duplicate_table_names():
    """``resolve_plan`` builds ``{t.name: t for t in plan.tables}``
    which silently dedupes; FK lookups then point at the surviving
    table only.  Reject up front with the offending name.
    """
    a = _trivial_table("orders")
    b = _trivial_table("orders")  # duplicate is the point of this test
    with pytest.raises(ValueError, match="duplicate TableSpec names"):
        DataGenPlan(tables=[a, b], seed=42)


def test_data_gen_plan_accepts_distinct_tables():
    """Happy path -- distinct names pass."""
    plan = DataGenPlan(
        tables=[_trivial_table("orders"), _trivial_table("customers")],
        seed=42,
    )
    assert [t.name for t in plan.tables] == ["orders", "customers"]


def test_data_gen_plan_seed_default_still_warns():
    """Regression guard: the existing ``_warn_if_seed_missing``
    behaviour must keep firing after the new validators above.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        DataGenPlan(tables=[_trivial_table("orders")])
        assert any("explicit ``seed``" in str(w.message) for w in caught)
