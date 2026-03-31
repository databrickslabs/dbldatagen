"""Tests covering remaining uncovered lines in generator.py.

generator.py targets:
- Line 160: FK column with no resolution falls back to lit(None)
- Line 215: _build_fk_column_expr returns None
- Lines 341-342, 354, 359-360: _build_exprs_scalar seed_from+null, FK null, null_fraction
- Lines 411-423, 433, 444-445: _build_exprs_dynamic seed_from, FK, null_fraction
- Lines 457-466: _build_write_batch_case_when
- Lines 606-608: ArrayColumn with Column seed (dynamic path)
"""

from __future__ import annotations

from pyspark.sql import functions as F

from dbldatagen.v1.engine.generator import (
    _build_exprs_dynamic,
    _build_exprs_scalar,
    _build_write_batch_case_when,
    build_all_column_exprs,
    build_column_expr,
)
from dbldatagen.v1.engine.utils import case_when_chain, create_range_df
from dbldatagen.v1.schema import (
    ArrayColumn,
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_plan(rows: int = 50) -> DataGenPlan:
    return DataGenPlan(
        tables=[
            TableSpec(
                name="items",
                rows=rows,
                primary_key=PrimaryKey(columns=["item_id"]),
                columns=[
                    ColumnSpec(name="item_id", dtype=DataType.LONG, gen=SequenceColumn(start=1)),
                    ColumnSpec(name="price", dtype=DataType.DOUBLE, gen=RangeColumn(min=1.0, max=100.0)),
                    ColumnSpec(name="status", dtype=DataType.STRING, gen=ValuesColumn(values=["A", "B", "C"])),
                ],
            ),
        ],
        seed=99,
    )


# ===================================================================
# generator.py: FK column with no resolution (line 160, 215)
# ===================================================================


class TestFKColumnNoResolution:
    def test_fk_no_resolution_falls_back_to_null(self, spark):
        """FK column without fk_resolutions => lit(None)."""
        spec = TableSpec(
            name="orders",
            rows=10,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=RangeColumn(min=1, max=100),
                    foreign_key=ForeignKeyRef(ref="customers.id"),
                ),
            ],
        )
        _df, id_col = create_range_df(spark, 10)
        col_exprs, udf_columns, _ = build_all_column_exprs(
            spec,
            id_col,
            None,
            seed=42,
            row_count=10,
        )
        # FK without resolution goes to col_exprs as lit(None)
        assert len(col_exprs) == 1
        assert len(udf_columns) == 0


# ===================================================================
# generator.py: _build_exprs_scalar with null_fraction, FK, seed_from
# (lines 340-342, 354, 358-360)
# ===================================================================


class TestBuildExprsScalarBranches:
    def test_scalar_null_fraction_on_regular_column(self, spark):
        """Lines 358-360: regular column with null_fraction > 0."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="val",
                    gen=RangeColumn(min=1, max=100),
                    null_fraction=0.5,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        col_exprs, _, _ = _build_exprs_scalar(spec, id_col, 0, 42, None, row_count=20)
        # Should have pk + val
        assert len(col_exprs) == 2

    def test_scalar_seed_from_with_null_fraction(self, spark):
        """Lines 340-342: seed_from column with null_fraction > 0."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="derived",
                    gen=RangeColumn(min=10, max=99),
                    seed_from="group_id",
                    null_fraction=0.3,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        _, _, seeded_columns = _build_exprs_scalar(spec, id_col, 0, 42, None, row_count=20)
        assert len(seeded_columns) == 1
        assert seeded_columns[0][0] == "derived"

    def test_scalar_fk_no_resolution(self, spark):
        """Line 354: FK in scalar path without resolution."""
        spec = TableSpec(
            name="t",
            rows=10,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="ref_id",
                    gen=RangeColumn(min=1, max=10),
                    foreign_key=ForeignKeyRef(ref="other.id"),
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 10)
        col_exprs, udf_cols, _ = _build_exprs_scalar(spec, id_col, 0, 42, None, row_count=10)
        # FK without resolution goes to col_exprs as lit(None)
        assert len(udf_cols) == 0
        # pk + ref_id(null)
        assert len(col_exprs) == 2


# ===================================================================
# generator.py: _build_exprs_dynamic branches
# (lines 411-423, 433, 444-445)
# ===================================================================


class TestBuildExprsDynamic:
    def test_dynamic_seed_from_with_null(self, spark):
        """Lines 410-423: seed_from in dynamic path with null_fraction."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="derived",
                    gen=RangeColumn(min=10, max=99),
                    seed_from="group_id",
                    null_fraction=0.3,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        _, _, seeded = _build_exprs_dynamic(
            spec,
            id_col,
            F.col("_wb"),
            [0, 1],
            42,
            None,
            row_count=20,
        )
        assert len(seeded) == 1
        assert seeded[0][0] == "derived"

    def test_dynamic_fk_no_resolution(self, spark):
        """Line 433: FK in dynamic path without resolution."""
        spec = TableSpec(
            name="t",
            rows=10,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="ref_id",
                    gen=RangeColumn(min=1, max=10),
                    foreign_key=ForeignKeyRef(ref="other.id"),
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 10)
        _col_exprs, udf_cols, _ = _build_exprs_dynamic(
            spec,
            id_col,
            F.col("_wb"),
            [0, 1],
            42,
            None,
            row_count=10,
        )
        assert len(udf_cols) == 0

    def test_dynamic_regular_null_fraction(self, spark):
        """Lines 443-445: regular column with null_fraction in dynamic path."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="val",
                    gen=RangeColumn(min=1, max=100),
                    null_fraction=0.5,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        col_exprs, _, _ = _build_exprs_dynamic(
            spec,
            id_col,
            F.col("_wb"),
            [0, 1],
            42,
            None,
            row_count=20,
        )
        assert len(col_exprs) == 2  # pk + val


# ===================================================================
# generator.py: _build_write_batch_case_when (lines 457-466)
# ===================================================================


class TestBuildWriteBatchCaseWhen:
    def test_single_batch_returns_expr_directly(self, spark):
        """Line 457-458: single batch returns expression directly."""
        expr = F.lit(42)
        result = _build_write_batch_case_when(F.col("wb"), [(0, expr)])
        # Should return the expression directly (no CASE WHEN)
        df = spark.range(5).withColumn("wb", F.lit(0))
        out = df.select(result.alias("v")).collect()
        assert all(r.v == 42 for r in out)

    def test_multiple_batches_builds_case_when(self, spark):
        """Lines 460-466: multiple batches build CASE WHEN chain."""
        e0 = F.lit(10)
        e1 = F.lit(20)
        e2 = F.lit(30)
        result = _build_write_batch_case_when(
            F.col("wb"),
            [(0, e0), (1, e1), (2, e2)],
        )
        df = spark.createDataFrame([(0,), (1,), (2,)], ["wb"])
        out = df.select(result.alias("v")).collect()
        values = {r.v for r in out}
        assert values == {10, 20, 30}


# ===================================================================
# utils.py: case_when_chain
# ===================================================================


class TestCaseWhenChain:
    def test_single_branch_returns_expr_directly(self, spark):
        """Single branch skips CASE WHEN, returns expression directly."""
        expr = F.lit(99)
        result = case_when_chain(F.col("disc"), [(0, expr)])
        df = spark.range(3).withColumn("disc", F.lit(0))
        out = df.select(result.alias("v")).collect()
        assert all(r.v == 99 for r in out)

    def test_two_branches(self, spark):
        """Two branches produce correct dispatch."""
        result = case_when_chain(F.col("disc"), [(0, F.lit("a")), (1, F.lit("b"))])
        df = spark.createDataFrame([(0,), (1,)], ["disc"])
        out = {r.v for r in df.select(result.alias("v")).collect()}
        assert out == {"a", "b"}

    def test_three_branches(self, spark):
        """Three branches produce correct dispatch for all values."""
        result = case_when_chain(
            F.col("disc"),
            [(0, F.lit(10)), (1, F.lit(20)), (2, F.lit(30))],
        )
        df = spark.createDataFrame([(0,), (1,), (2,)], ["disc"])
        out = df.select(result.alias("v")).collect()
        values = {r.v for r in out}
        assert values == {10, 20, 30}


# ===================================================================
# generator.py: ArrayColumn with Column seed (lines 606-608)
# ===================================================================


class TestArrayColumnWithColumnSeed:
    def test_array_column_with_column_seed(self, spark):
        """Lines 606-608: ArrayColumn where column_seed is a Column."""
        col_spec = ColumnSpec(
            name="arr",
            gen=ArrayColumn(
                element=RangeColumn(min=1, max=10),
                min_length=2,
                max_length=2,
            ),
        )
        df = spark.range(10).withColumn("_seed", F.lit(42).cast("long"))
        expr = build_column_expr(col_spec, F.col("id"), F.col("_seed"), 10, 42)
        result = df.select(expr.alias("arr")).collect()
        assert all(len(r.arr) == 2 for r in result)
