"""Adversarial-input tests for ``TableSpec`` validators."""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataType,
    PrimaryKey,
    RangeColumn,
    TableSpec,
)


def test_table_spec_rejects_empty_columns():
    """A TableSpec with no columns has nothing to generate -- the
    engine's ``select(*[])`` would yield an empty-schema DataFrame
    with no signal that the user's plan was incomplete.
    """
    with pytest.raises(ValueError, match="at least 1"):
        TableSpec(name="t", columns=[], rows=10)


def test_table_spec_accepts_single_column():
    """Smallest happy case."""
    ts = TableSpec(
        name="t",
        rows=10,
        columns=[ColumnSpec(name="id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9))],
    )
    assert len(ts.columns) == 1


def test_table_spec_rejects_primary_key_with_missing_column():
    """``PrimaryKey.columns`` must name columns that exist in
    ``columns``.  Without this cross-check, direct ``generate_table``
    callers (outside a ``DataGenPlan``) get no model-layer signal --
    the engine eventually fails on a missing-column reference far
    from the offending declaration.
    """
    with pytest.raises(ValueError, match="don't exist"):
        TableSpec(
            name="t",
            rows=10,
            columns=[ColumnSpec(name="id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9))],
            primary_key=PrimaryKey(columns=["does_not_exist"]),
        )


def test_table_spec_rejects_primary_key_with_partial_missing_column():
    """Composite PK with one valid + one missing column.  Should reject
    on the missing one and list the offender."""
    with pytest.raises(ValueError, match=r"don't exist.*typo_col"):
        TableSpec(
            name="t",
            rows=10,
            columns=[
                ColumnSpec(name="tenant_id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9)),
                ColumnSpec(name="user_id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9)),
            ],
            primary_key=PrimaryKey(columns=["tenant_id", "typo_col"]),
        )


def test_table_spec_accepts_primary_key_with_valid_columns():
    """Happy-path: PK columns are a subset of the table's columns."""
    ts = TableSpec(
        name="t",
        rows=10,
        columns=[
            ColumnSpec(name="tenant_id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9)),
            ColumnSpec(name="user_id", dtype=DataType.INT, gen=RangeColumn(min=0, max=9)),
        ],
        primary_key=PrimaryKey(columns=["tenant_id", "user_id"]),
    )
    assert ts.primary_key is not None
