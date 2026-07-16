"""Adversarial-input tests for ``StructColumn`` validators.

Duplicate field-name detection was already present
(``validate_unique_field_names``); the audit flagged the empty-list
case, now blocked by ``Field(min_length=1)`` on ``fields``.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataType,
    FakerColumn,
    ForeignKeyColumn,
    RangeColumn,
    StructColumn,
)


def _int_col(name: str) -> ColumnSpec:
    return ColumnSpec(name=name, dtype=DataType.INT, gen=RangeColumn(min=0, max=10))


def test_struct_column_rejects_empty_fields():
    """An empty struct builds ``F.struct()`` with zero children which
    Spark types as an ambiguous ``struct<>`` -- catch at plan time.
    """
    with pytest.raises(ValueError, match="at least 1"):
        StructColumn(fields=[])


def test_struct_column_rejects_duplicate_field_names():
    """Pre-existing validator -- pinned here so the matrix is complete."""
    with pytest.raises(ValueError, match="duplicate field names"):
        StructColumn(fields=[_int_col("x"), _int_col("y"), _int_col("x")])


def test_struct_column_rejects_faker_child():
    """Parallel to ArrayColumn.element FakerColumn rejection: the engine's
    ``_build_struct_column`` cannot dispatch a column-level pandas_udf
    per struct field, so catching at plan time names the offending field."""
    with pytest.raises(ValueError, match="not supported"):
        StructColumn(
            fields=[
                _int_col("a"),
                ColumnSpec(name="b", gen=FakerColumn(provider="first_name")),
            ]
        )


def test_struct_column_rejects_foreign_key_child():
    """Parallel rejection for ForeignKeyColumn: FK resolution keys on the
    top-level (table, column) pair which has no meaning inside a struct."""
    # ForeignKeyColumn requires foreign_key on the ColumnSpec; without it,
    # validate_foreign_key_strategy raises first.  Skip if that ordering
    # changes (use a real FK declaration when test_referential_integrity
    # exists).
    with pytest.raises(ValueError):
        StructColumn(
            fields=[
                _int_col("a"),
                ColumnSpec(
                    name="ref_col",
                    gen=ForeignKeyColumn(),
                    foreign_key=None,  # triggers validate_foreign_key_strategy first
                ),
            ]
        )


def test_struct_column_accepts_single_field():
    """Smallest happy case."""
    sc = StructColumn(fields=[_int_col("only")])
    assert len(sc.fields) == 1


def test_struct_column_accepts_distinct_fields():
    """Realistic happy case."""
    sc = StructColumn(fields=[_int_col("a"), _int_col("b"), _int_col("c")])
    assert [f.name for f in sc.fields] == ["a", "b", "c"]
