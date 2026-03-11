"""Tests for v0 → v1 DataGenerator conversion (dbldatagen.v1.compat)."""

from __future__ import annotations

import warnings

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from dbldatagen import DataGenerator
from dbldatagen.v1 import generate
from dbldatagen.v1.compat import from_data_generator
from dbldatagen.v1.schema import (
    DataGenPlan,
    DataType,
    ExpressionColumn,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    TimestampColumn,
    ValuesColumn,
    WeightedValues,
)


# ---------------------------------------------------------------------------
# Basic conversion
# ---------------------------------------------------------------------------


class TestBasicConversion:
    def test_returns_data_gen_plan(self, spark):
        dg = DataGenerator(spark, rows=100, name="t").withColumn("x", IntegerType(), minValue=1, maxValue=10)
        plan = from_data_generator(dg)
        assert isinstance(plan, DataGenPlan)

    def test_table_name_from_generator(self, spark):
        dg = DataGenerator(spark, rows=100, name="users").withColumn("x", IntegerType())
        plan = from_data_generator(dg)
        assert plan.tables[0].name == "users"

    def test_table_name_override(self, spark):
        dg = DataGenerator(spark, rows=100, name="users").withColumn("x", IntegerType())
        plan = from_data_generator(dg, table_name="customers")
        assert plan.tables[0].name == "customers"

    def test_row_count_preserved(self, spark):
        dg = DataGenerator(spark, rows=5000, name="t").withColumn("x", IntegerType())
        plan = from_data_generator(dg)
        assert plan.tables[0].rows == 5000

    def test_seed_from_generator(self, spark):
        dg = DataGenerator(spark, rows=100, name="t", randomSeed=123).withColumn("x", IntegerType())
        plan = from_data_generator(dg)
        assert plan.seed == 123

    def test_seed_override(self, spark):
        dg = DataGenerator(spark, rows=100, name="t", randomSeed=123).withColumn("x", IntegerType())
        plan = from_data_generator(dg, seed=999)
        assert plan.seed == 999

    def test_has_primary_key(self, spark):
        dg = DataGenerator(spark, rows=100, name="t").withColumn("x", IntegerType())
        plan = from_data_generator(dg)
        assert plan.tables[0].primary_key is not None
        assert plan.tables[0].primary_key.columns == ["id"]

    def test_seed_column_converted_to_sequence(self, spark):
        dg = DataGenerator(spark, rows=100, name="t").withColumn("x", IntegerType())
        plan = from_data_generator(dg)
        id_col = next(c for c in plan.tables[0].columns if c.name == "id")
        assert isinstance(id_col.gen, SequenceColumn)


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class TestTypeMapping:
    def test_integer_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", IntegerType(), minValue=1, maxValue=10)
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.dtype == DataType.INT

    def test_long_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", LongType(), minValue=1, maxValue=10)
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.dtype == DataType.LONG

    def test_float_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", FloatType(), minValue=0.0, maxValue=1.0)
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.dtype == DataType.FLOAT

    def test_double_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", DoubleType(), minValue=0.0, maxValue=100.0)
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.dtype == DataType.DOUBLE

    def test_string_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", StringType(), values=["a", "b"])
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.dtype == DataType.STRING

    def test_boolean_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", BooleanType())
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.dtype == DataType.BOOLEAN
        assert isinstance(col.gen, ValuesColumn)
        assert col.gen.values == [True, False]

    def test_timestamp_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "ts", TimestampType(), begin="2020-01-01 00:00:00", end="2025-12-31 23:59:59"
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "ts")
        assert col.dtype == DataType.TIMESTAMP
        assert isinstance(col.gen, TimestampColumn)

    def test_date_type(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("d", DateType(), begin="2020-01-01", end="2025-12-31")
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "d")
        assert col.dtype == DataType.DATE
        assert isinstance(col.gen, TimestampColumn)


# ---------------------------------------------------------------------------
# Generation strategies
# ---------------------------------------------------------------------------


class TestRangeColumn:
    def test_int_range(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", IntegerType(), minValue=5, maxValue=50)
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 5
        assert col.gen.max == 50

    def test_double_range(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("x", DoubleType(), minValue=0.0, maxValue=1.0)
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 0.0
        assert col.gen.max == 1.0


class TestValuesColumn:
    def test_string_values(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "status", StringType(), values=["active", "inactive", "pending"]
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "status")
        assert isinstance(col.gen, ValuesColumn)
        assert col.gen.values == ["active", "inactive", "pending"]

    def test_weighted_values(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "tier", StringType(), values=["free", "basic", "premium"], weights=[70, 20, 10]
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "tier")
        assert isinstance(col.gen, ValuesColumn)
        assert isinstance(col.gen.distribution, WeightedValues)
        assert col.gen.distribution.weights["free"] == 70


class TestExpressionColumn:
    def test_expr_converted(self, spark):
        dg = (
            DataGenerator(spark, rows=10, name="t")
            .withColumn("a", IntegerType(), minValue=1, maxValue=10)
            .withColumn("b", IntegerType(), expr="a * 2")
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "b")
        assert isinstance(col.gen, ExpressionColumn)
        assert col.gen.expr == "a * 2"


class TestPatternColumn:
    def test_template_converted(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("code", StringType(), template=r"ABC-\d{4}")
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "code")
        assert isinstance(col.gen, PatternColumn)

    def test_prefix_converted(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn("code", StringType(), prefix="ORD-")
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "code")
        assert isinstance(col.gen, PatternColumn)
        assert "ORD-" in col.gen.template


# ---------------------------------------------------------------------------
# Nullable / null fraction
# ---------------------------------------------------------------------------


class TestNullHandling:
    def test_percent_nulls_converted(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "x", IntegerType(), minValue=1, maxValue=10, percentNulls=0.2
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert col.nullable is True
        assert col.null_fraction == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# Warnings for unsupported features
# ---------------------------------------------------------------------------


class TestBaseColumnToSeedFrom:
    def test_base_column_maps_to_seed_from(self, spark):
        dg = (
            DataGenerator(spark, rows=10, name="t")
            .withColumn("a", IntegerType(), minValue=1, maxValue=10)
            .withColumn("b", IntegerType(), baseColumn="a", minValue=1, maxValue=5)
        )
        plan = from_data_generator(dg)
        col_b = next(c for c in plan.tables[0].columns if c.name == "b")
        assert col_b.seed_from == "a"

    def test_base_column_list_takes_first(self, spark):
        dg = (
            DataGenerator(spark, rows=10, name="t")
            .withColumn("a", IntegerType(), minValue=1, maxValue=10)
            .withColumn("c", IntegerType(), minValue=1, maxValue=10)
            .withColumn("b", IntegerType(), baseColumn=["a", "c"], minValue=1, maxValue=5)  # type: ignore[arg-type]
        )
        plan = from_data_generator(dg)
        col_b = next(c for c in plan.tables[0].columns if c.name == "b")
        assert col_b.seed_from == "a"

    def test_seed_from_generates_correlated_data(self, spark):
        """Same base value → same derived value when using seed_from."""
        dg = (
            DataGenerator(spark, rows=200, name="t")
            .withColumn("group_id", IntegerType(), minValue=1, maxValue=5)
            .withColumn("label", StringType(), baseColumn="group_id", values=["A", "B", "C", "D"])
        )
        plan = from_data_generator(dg)
        result = generate(spark, plan)
        df = result["t"]

        # Same group_id should map to same label
        distinct = df.select("group_id", "label").distinct()
        groups = df.select("group_id").distinct().count()
        assert distinct.count() == groups


class TestNumColumnsExpansion:
    def test_num_columns_expands(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "feature", IntegerType(), minValue=0, maxValue=100, numColumns=3
        )
        plan = from_data_generator(dg)
        feature_cols = [c for c in plan.tables[0].columns if c.name.startswith("feature_")]
        assert len(feature_cols) == 3
        assert [c.name for c in feature_cols] == ["feature_0", "feature_1", "feature_2"]

    def test_num_columns_generates(self, spark):
        dg = DataGenerator(spark, rows=50, name="t").withColumn(
            "val", IntegerType(), minValue=0, maxValue=10, numColumns=4
        )
        plan = from_data_generator(dg)
        result = generate(spark, plan)
        df = result["t"]
        assert "val_0" in df.columns
        assert "val_1" in df.columns
        assert "val_2" in df.columns
        assert "val_3" in df.columns
        assert df.count() == 50


class TestUniqueValuesAdjustment:
    def test_unique_values_adjusts_range(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "x", IntegerType(), minValue=0, maxValue=1000, uniqueValues=10
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert isinstance(col.gen, RangeColumn)
        # max should be adjusted: 0 + (10-1) * 1 = 9
        assert col.gen.max == 9.0


class TestDistributionMapping:
    def test_normal_distribution_maps(self, spark):
        from dbldatagen.distributions import Normal as V0Normal

        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "x", DoubleType(), minValue=0.0, maxValue=100.0, distribution=V0Normal(50.0, 10.0)
        )
        plan = from_data_generator(dg)
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert isinstance(col.gen, RangeColumn)
        from dbldatagen.v1.schema import Normal as V1Normal

        assert isinstance(col.gen.distribution, V1Normal)


# ---------------------------------------------------------------------------
# Warnings for truly unsupported features
# ---------------------------------------------------------------------------


class TestUnsupportedWarnings:

    def test_unique_values_adjusted_warns(self, spark):
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "x", IntegerType(), minValue=1, maxValue=100, uniqueValues=50
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            plan = from_data_generator(dg)
            uv_warnings = [x for x in w if "uniqueValues" in str(x.message)]
            assert len(uv_warnings) > 0
        # Also verify the range was adjusted
        col = next(c for c in plan.tables[0].columns if c.name == "x")
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.max == 50.0  # 1 + (50-1) * 1 = 50

    def test_constraints_warns(self, spark):
        from dbldatagen import SqlExpr

        dg = (
            DataGenerator(spark, rows=10, name="t")
            .withColumn("x", IntegerType(), minValue=1, maxValue=10)
            .withConstraint(SqlExpr("x > 0"))
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            from_data_generator(dg)
            constraint_warnings = [x for x in w if "constraint" in str(x.message)]
            assert len(constraint_warnings) > 0


# ---------------------------------------------------------------------------
# End-to-end: convert and generate
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_simple_generate(self, spark):
        """Convert a v0 spec, generate with v1, verify output."""
        dg = (
            DataGenerator(spark, rows=500, name="orders")
            .withColumn("order_id", LongType(), minValue=1, maxValue=10000)
            .withColumn("status", StringType(), values=["pending", "shipped", "delivered"])
            .withColumn("amount", DoubleType(), minValue=10.0, maxValue=500.0)
        )
        plan = from_data_generator(dg)
        result = generate(spark, plan)

        assert "orders" in result
        df = result["orders"]
        assert df.count() == 500
        assert set(df.columns) == {"id", "order_id", "status", "amount"}

    def test_generate_with_nulls(self, spark):
        """Columns with percentNulls produce actual nulls in v1."""
        dg = DataGenerator(spark, rows=200, name="t").withColumn(
            "x", IntegerType(), minValue=1, maxValue=100, percentNulls=0.3
        )
        plan = from_data_generator(dg)
        result = generate(spark, plan)
        df = result["t"]

        null_count = df.filter("x IS NULL").count()
        assert null_count > 0, "Expected some nulls from percentNulls=0.3"

    def test_generate_with_values(self, spark):
        """Values-based columns produce valid output."""
        dg = DataGenerator(spark, rows=100, name="t").withColumn("color", StringType(), values=["red", "green", "blue"])
        plan = from_data_generator(dg)
        result = generate(spark, plan)
        df = result["t"]

        distinct = {r.color for r in df.select("color").distinct().collect()}
        assert distinct.issubset({"red", "green", "blue"})

    def test_generate_with_expression(self, spark):
        """Expression columns evaluate correctly."""
        dg = (
            DataGenerator(spark, rows=50, name="t")
            .withColumn("a", IntegerType(), minValue=10, maxValue=10)
            .withColumn("b", IntegerType(), expr="id + 1")
        )
        plan = from_data_generator(dg)
        result = generate(spark, plan)
        df = result["t"]

        # id starts at 0, b should be id + 1
        row = df.filter("id = 0").first()
        assert row is not None
        assert row.b == 1

    def test_determinism(self, spark):
        """Same v0 spec → same v1 plan → same output."""
        dg = DataGenerator(spark, rows=100, name="t", randomSeed=42).withColumn(
            "x", IntegerType(), minValue=1, maxValue=100
        )
        plan1 = from_data_generator(dg)
        plan2 = from_data_generator(dg)

        r1 = generate(spark, plan1)["t"].orderBy("id").collect()
        r2 = generate(spark, plan2)["t"].orderBy("id").collect()

        assert len(r1) == len(r2)
        for row1, row2 in zip(r1, r2):
            assert row1.asDict() == row2.asDict()
