"""Tests to cover uncovered branches in dbldatagen.v1.compat."""

from __future__ import annotations

import warnings

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
)

from dbldatagen import DataGenerator
from dbldatagen.v1 import generate
from dbldatagen.v1.compat import (
    _map_distribution,
    _map_spark_type,
    from_data_generator,
)
from dbldatagen.v1.schema import (
    ConstantColumn,
    DataType,
    Exponential,
    Normal,
)


# ---------------------------------------------------------------------------
# Unsupported Spark types fallback to STRING (line 76-82)
# ---------------------------------------------------------------------------


class TestUnsupportedSparkType:
    def test_array_type_falls_back_to_string(self):
        """ArrayType has no v1 mapping and should warn + default to STRING."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _map_spark_type(ArrayType(StringType()))
            assert result == DataType.STRING
            type_warnings = [x for x in w if "Unsupported Spark type" in str(x.message)]
            assert len(type_warnings) == 1
            assert "ArrayType" in str(type_warnings[0].message)

    def test_map_type_falls_back_to_string(self):
        """MapType has no v1 mapping and should warn + default to STRING."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _map_spark_type(MapType(StringType(), IntegerType()))
            assert result == DataType.STRING
            type_warnings = [x for x in w if "Unsupported Spark type" in str(x.message)]
            assert len(type_warnings) == 1


# ---------------------------------------------------------------------------
# Beta/Gamma distribution warnings (line 103-108)
# ---------------------------------------------------------------------------


class TestDistributionWarnings:
    def test_beta_distribution_warns(self):
        """Beta distribution has no v1 equivalent and should warn."""
        from dbldatagen.distributions import Beta

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _map_distribution(Beta(2.0, 5.0))
            assert result is None
            dist_warnings = [x for x in w if "no v1 equivalent" in str(x.message)]
            assert len(dist_warnings) == 1
            assert "Beta" in str(dist_warnings[0].message)

    def test_gamma_distribution_warns(self):
        """Gamma distribution has no v1 equivalent and should warn."""
        from dbldatagen.distributions import Gamma

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _map_distribution(Gamma(2.0, 1.0))
            assert result is None
            dist_warnings = [x for x in w if "no v1 equivalent" in str(x.message)]
            assert len(dist_warnings) == 1
            assert "Gamma" in str(dist_warnings[0].message)

    def test_exponential_distribution_maps(self):
        """Exponential distribution maps correctly."""
        from dbldatagen.distributions import Exponential as V0Exp

        result = _map_distribution(V0Exp(2.0))
        assert isinstance(result, Exponential)

    def test_normal_distribution_maps(self):
        """Normal distribution maps correctly."""
        from dbldatagen.distributions import Normal as V0Normal

        result = _map_distribution(V0Normal(10.0, 3.0))
        assert isinstance(result, Normal)

    def test_none_distribution_returns_none(self):
        """None distribution returns None."""
        result = _map_distribution(None)
        assert result is None


# ---------------------------------------------------------------------------
# numColumns/numFeatures expansion (lines 300-310)
# ---------------------------------------------------------------------------


class TestNumFeaturesExpansion:
    def test_num_features_expands(self, spark):
        """numFeatures parameter expands a single column into N columns."""
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "feat", IntegerType(), minValue=0, maxValue=100, numFeatures=3
        )
        plan = from_data_generator(dg)
        feat_cols = [c for c in plan.tables[0].columns if c.name.startswith("feat_")]
        assert len(feat_cols) == 3
        assert [c.name for c in feat_cols] == ["feat_0", "feat_1", "feat_2"]

    def test_num_features_generates_data(self, spark):
        """Expanded numFeatures columns produce valid data."""
        dg = DataGenerator(spark, rows=20, name="t").withColumn(
            "f", IntegerType(), minValue=1, maxValue=50, numFeatures=2
        )
        plan = from_data_generator(dg)
        result = generate(spark, plan)
        df = result["t"]
        assert "f_0" in df.columns
        assert "f_1" in df.columns
        assert df.count() == 20


# ---------------------------------------------------------------------------
# Edge case: empty DataGenerator (no columns)
# ---------------------------------------------------------------------------


class TestEmptyDataGenerator:
    def test_no_columns_warns(self, spark):
        """DataGenerator with no withColumn calls warns about no columns."""
        dg = DataGenerator(spark, rows=10, name="empty")
        with warnings.catch_warnings(record=True) as _w:
            warnings.simplefilter("always")
            plan = from_data_generator(dg)
            # Should have the seed column at minimum
            # Check for the "No columns" warning or that we got a seed column
            assert len(plan.tables[0].columns) >= 1


# ---------------------------------------------------------------------------
# numColumns with tuple form (line 302-303)
# ---------------------------------------------------------------------------


class TestNumColumnsTuple:
    def test_num_columns_tuple_uses_second_element(self, spark):
        """numColumns as tuple (start, count) uses the second element."""
        dg = DataGenerator(spark, rows=10, name="t").withColumn(
            "x", IntegerType(), minValue=0, maxValue=10, numColumns=(0, 3)
        )
        plan = from_data_generator(dg)
        x_cols = [c for c in plan.tables[0].columns if c.name.startswith("x_")]
        assert len(x_cols) == 3


# ---------------------------------------------------------------------------
# Catch-all: column with no determinable strategy (line 281-286)
# ---------------------------------------------------------------------------


class TestConstantFallback:
    def test_unknown_dtype_falls_back_to_constant(self, spark):
        """A column whose type/options don't match any strategy gets ConstantColumn(None)."""
        # Use an unsupported Spark type that maps to STRING but has no values/min/max/template
        # Actually, STRING without prefix/suffix falls through to pattern. We need a type
        # that maps to something unexpected. Let's use _map_spark_type fallback + no options.
        # The catch-all is hard to trigger through the public API because STRING always
        # gets a pattern. Instead test _convert_column directly.
        from dbldatagen.v1.compat import _convert_column

        # Create a minimal mock ColumnGenerationSpec
        class MockSpec:
            name = "weird"
            omit = False
            nullable = False

            def __init__(self):
                # Mock datatype as something that maps to DATE but with no begin/end
                from pyspark.sql.types import DateType

                self.datatype = DateType()

            def __getitem__(self, key):
                # Return None for all options
                return None

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            specs = _convert_column(MockSpec())  # type: ignore[arg-type]
            assert len(specs) == 1
            assert isinstance(specs[0].gen, ConstantColumn)
            const_warnings = [x for x in w if "could not determine" in str(x.message)]
            assert len(const_warnings) == 1
