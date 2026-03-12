"""Tests to cover uncovered branches in dbldatagen.v1.engine.generator."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from dbldatagen.v1.engine.generator import (
    build_all_column_exprs,
    build_column_expr,
    _build_exprs_scalar,
)
from dbldatagen.v1.engine.seed import derive_column_seed
from dbldatagen.v1.engine.utils import create_range_df
from dbldatagen.v1.schema import (
    ArrayColumn,
    ColumnSpec,
    DataType,
    FakerColumn,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    TimestampColumn,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# seed_from with FakerColumn (lines 338-344, 193-194)
# ---------------------------------------------------------------------------


class TestSeedFromWithFaker:
    def test_seed_from_faker_column(self, spark):
        """seed_from + FakerColumn routes through _build_faker_expr with effective_id."""
        spec = TableSpec(
            name="t",
            rows=50,
            seed=42,
            columns=[
                ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="fake_name",
                    gen=FakerColumn(provider="first_name"),
                    seed_from="group_id",
                ),
            ],
        )
        df, id_col = create_range_df(spark, 50)
        col_exprs, udf_columns, seeded_columns = build_all_column_exprs(
            spec, id_col, None, seed=42, row_count=50,
        )
        # seed_from columns go into seeded_columns
        assert len(seeded_columns) == 1
        name, _ = seeded_columns[0]
        assert name == "fake_name"

    def test_seed_from_faker_in_scalar_path(self, spark):
        """seed_from in _build_exprs_scalar routes through build_column_expr (line 338-344)."""
        spec = TableSpec(
            name="t",
            rows=20,
            seed=42,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="derived",
                    gen=RangeColumn(min=10, max=99),
                    seed_from="group_id",
                ),
            ],
        )
        df, id_col = create_range_df(spark, 20)
        col_exprs, udf_columns, seeded_columns = _build_exprs_scalar(
            spec, id_col, 0, 42, None, row_count=20,
        )
        # seed_from column should be in seeded_columns
        assert len(seeded_columns) == 1
        assert seeded_columns[0][0] == "derived"


# ---------------------------------------------------------------------------
# DATE type handling (line 529-537)
# ---------------------------------------------------------------------------


class TestDateTypeColumn:
    def test_date_column_via_build_column_expr(self, spark):
        """TimestampColumn with dtype=DATE dispatches to build_date_column."""
        col_spec = ColumnSpec(
            name="d",
            dtype=DataType.DATE,
            gen=TimestampColumn(start="2023-01-01", end="2023-12-31"),
        )
        col_seed = derive_column_seed(42, "t", "d")
        df = spark.range(100)
        expr = build_column_expr(col_spec, F.col("id"), col_seed, 100, 42)
        result = df.select(expr.alias("d"))
        # Verify it produces date values, not timestamps
        row = result.first()
        assert row is not None
        import datetime
        assert isinstance(row.d, datetime.date)


# ---------------------------------------------------------------------------
# Unsupported strategy error (line 556-560)
# ---------------------------------------------------------------------------


class TestUnsupportedStrategy:
    def test_unsupported_strategy_raises(self):
        """Passing an unknown strategy type raises ValueError."""
        # Create a mock strategy-like object that isn't any known type
        class FakeStrategy:
            strategy = "bogus"

        col_spec = ColumnSpec.__new__(ColumnSpec)
        object.__setattr__(col_spec, "name", "bad_col")
        object.__setattr__(col_spec, "dtype", DataType.STRING)
        object.__setattr__(col_spec, "gen", FakeStrategy())
        object.__setattr__(col_spec, "nullable", False)
        object.__setattr__(col_spec, "null_fraction", 0.0)
        object.__setattr__(col_spec, "foreign_key", None)
        object.__setattr__(col_spec, "seed_from", None)

        with pytest.raises(ValueError, match="Unsupported column strategy 'bogus'"):
            build_column_expr(col_spec, F.col("id"), 42, 100, 42)


# ---------------------------------------------------------------------------
# Array column with variable length (lines 606-608, 620-627)
# ---------------------------------------------------------------------------


class TestArrayColumnVariableLength:
    def test_array_variable_length(self, spark):
        """Array with min_length < max_length produces variable-length arrays."""
        col_spec = ColumnSpec(
            name="tags",
            gen=ArrayColumn(
                element=ValuesColumn(values=["a", "b", "c", "d"]),
                min_length=1,
                max_length=4,
            ),
        )
        col_seed = derive_column_seed(42, "t", "tags")
        df = spark.range(200)
        expr = build_column_expr(col_spec, F.col("id"), col_seed, 200, 42)
        result = df.select(expr.alias("tags"))

        lengths = result.select(F.size("tags").alias("len")).distinct().collect()
        length_set = {r.len for r in lengths}
        # Should have at least 2 different lengths (probabilistic but very likely with 200 rows)
        assert len(length_set) > 1
        # All lengths should be in [min_length, max_length]
        for l in length_set:
            assert 1 <= l <= 4, f"Array length {l} outside [1, 4]"

    def test_array_fixed_length(self, spark):
        """Array with min_length == max_length produces fixed-length arrays."""
        col_spec = ColumnSpec(
            name="tags",
            gen=ArrayColumn(
                element=RangeColumn(min=1, max=10),
                min_length=3,
                max_length=3,
            ),
        )
        col_seed = derive_column_seed(42, "t", "tags")
        df = spark.range(50)
        expr = build_column_expr(col_spec, F.col("id"), col_seed, 50, 42)
        result = df.select(expr.alias("tags"))

        lengths = result.select(F.size("tags").alias("len")).distinct().collect()
        assert len(lengths) == 1
        assert lengths[0].len == 3
