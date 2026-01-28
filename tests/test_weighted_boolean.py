import pytest

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("weighted boolean tests")


class TestWeightedBoolean:
    """Test weighted values with boolean type."""

    def test_weighted_boolean_values_random(self):
        """Test that weighted boolean values generate correctly with random=True.

        This test verifies that using values=[True, False] with weights
        works correctly and produces the expected distribution.
        """
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_boolean", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn("active", "boolean", values=[True, False], weights=[0.85, 0.15], random=True)
        )

        # This should not raise an exception
        df = ds.build()

        # Verify the data was generated
        count = df.count()
        assert count == 10000

        # Verify we have both True and False values
        distinct_values = df.select("active").distinct().collect()
        values = {row.active for row in distinct_values}
        assert values == {True, False}

        # Verify distribution is approximately correct (within 10% tolerance)
        true_count = df.filter("active = true").count()
        true_ratio = true_count / count
        assert true_ratio == pytest.approx(0.85, abs=0.1)

    def test_weighted_boolean_normalized_weights(self):
        """Test boolean weighted values when weights sum to 1.0.

        This tests the fix for divide-by-zero when sum(weights) = 1.0.
        """
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_boolean_norm", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn("flag", "boolean", values=[True, False], weights=[0.7, 0.3], random=True)
        )

        df = ds.build()
        count = df.count()
        assert count == 10000

        # Verify distribution
        true_count = df.filter("flag = true").count()
        true_ratio = true_count / count
        assert true_ratio == pytest.approx(0.7, abs=0.1)


class TestWeightedNumeric:
    """Test weighted values with numeric types to ensure ELSE clause works correctly."""

    def test_weighted_integer_values_random(self):
        """Test that weighted integer values generate correctly with random=True."""
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_integer", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn("status", "integer", values=[1, 2, 3], weights=[0.5, 0.3, 0.2], random=True)
        )

        df = ds.build()
        count = df.count()
        assert count == 10000

        # Verify we have all expected values
        distinct_values = df.select("status").distinct().collect()
        values = {row.status for row in distinct_values}
        assert values == {1, 2, 3}

        # Verify distribution is approximately correct
        count_1 = df.filter("status = 1").count()
        ratio_1 = count_1 / count
        assert ratio_1 == pytest.approx(0.5, abs=0.1)

    def test_weighted_float_values_random(self):
        """Test that weighted float values generate correctly with random=True."""
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_float", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn("rate", "float", values=[1.5, 2.5, 3.5], weights=[0.5, 0.3, 0.2], random=True)
        )

        df = ds.build()
        count = df.count()
        assert count == 10000

    def test_weighted_double_values_random(self):
        """Test that weighted double values generate correctly with random=True."""
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_double", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn("amount", "double", values=[100.0, 200.0, 300.0], weights=[0.6, 0.3, 0.1], random=True)
        )

        df = ds.build()
        count = df.count()
        assert count == 10000


class TestWeightedStringStillWorks:
    """Ensure string weighted values still work after the fix."""

    def test_weighted_string_values_random(self):
        """Test that weighted string values still generate correctly after the fix."""
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_string", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn(
                "region", "string", values=["North", "South", "East", "West"], weights=[0.4, 0.3, 0.2, 0.1], random=True
            )
        )

        df = ds.build()
        count = df.count()
        assert count == 10000

        # Verify we have all expected values
        distinct_values = df.select("region").distinct().collect()
        values = {row.region for row in distinct_values}
        assert values == {"North", "South", "East", "West"}

        # Verify distribution is approximately correct
        north_count = df.filter("region = 'North'").count()
        north_ratio = north_count / count
        assert north_ratio == pytest.approx(0.4, abs=0.1)


class TestWeightedSingleQuoteEscaping:
    """Test that single quotes in values are properly escaped."""

    def test_string_values_with_single_quotes(self):
        """Test that string values containing single quotes work correctly.

        Values like "O'Brien" need to have their single quotes escaped
        in the generated SQL expression.
        """
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_quotes", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn(
                "name",
                "string",
                values=["John", "O'Brien", "D'Angelo", "Plain"],
                weights=[0.4, 0.3, 0.2, 0.1],
                random=True,
            )
        )

        # This should not raise a SQL syntax error
        df = ds.build()
        count = df.count()
        assert count == 10000

        # Verify we have all expected values including those with quotes
        distinct_values = df.select("name").distinct().collect()
        values = {row.name for row in distinct_values}
        assert values == {"John", "O'Brien", "D'Angelo", "Plain"}

        # Verify O'Brien appears in the data (not escaped version)
        obrien_count = df.filter("name = \"O'Brien\"").count()
        assert obrien_count > 0, "O'Brien should appear in generated data"


class TestWeightedNormalizedWeights:
    """Test weighted values where weights sum to exactly 1.0 (normalized weights)."""

    def test_string_with_normalized_weights_random(self):
        """Test that string weighted values with weights summing to 1.0 work correctly.

        This tests the fix for:
        1. ELSE clause bug where literal '{values[-1]}' was output instead of actual value
        2. Divide-by-zero when scale (sum of weights) equals 1.0
        """
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_normalized", rows=10000, partitions=4)
            .withIdOutput()
            .withColumn("gender", "string", values=["M", "F"], weights=[0.48, 0.52], random=True)
            .withColumn(
                "marital_status",
                "string",
                values=["Single", "Married", "Divorced", "Widowed"],
                weights=[0.30, 0.50, 0.15, 0.05],
                random=True,
            )
        )

        df = ds.build()
        count = df.count()
        assert count == 10000

        # Check no literal {values[-1]} in data
        bad_gender = df.filter("gender = '{values[-1]}'").count()
        bad_marital = df.filter("marital_status = '{values[-1]}'").count()
        assert bad_gender == 0, "Found literal '{values[-1]}' in gender column"
        assert bad_marital == 0, "Found literal '{values[-1]}' in marital_status column"

        # Verify we have expected values
        gender_values = {row.gender for row in df.select("gender").distinct().collect()}
        assert gender_values == {"M", "F"}

        marital_values = {row.marital_status for row in df.select("marital_status").distinct().collect()}
        assert marital_values == {"Single", "Married", "Divorced", "Widowed"}

        # Verify gender distribution is approximately correct
        f_count = df.filter("gender = 'F'").count()
        f_ratio = f_count / count
        assert f_ratio == pytest.approx(0.52, abs=0.1)

        # Verify marital status distribution is approximately correct
        married_count = df.filter("marital_status = 'Married'").count()
        married_ratio = married_count / count
        assert married_ratio == pytest.approx(0.50, abs=0.1)


class TestWeightedNonRandom:
    """Test weighted values WITHOUT random=True to cover non-random code path."""

    def test_weighted_string_non_random_normalized_weights(self):
        """Test non-random weighted values when weights sum to <= 1.0.

        This covers the scale_factor = 1000000 branch in column_generation_spec.py
        when weights_sum <= 1.0 and random=False.
        """
        # Use enough rows to get some distribution with deterministic mapping
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_nonrandom_norm", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("status", "string", values=["A", "B", "C"], weights=[0.5, 0.3, 0.2])
        )

        df = ds.build()
        count = df.count()
        assert count == 100000

        # Verify we can generate data without errors
        distinct_values = df.select("status").distinct().collect()
        values = {row.status for row in distinct_values}
        # With non-random, distribution depends on row IDs, but should have valid values
        assert values.issubset({"A", "B", "C"})

    def test_weighted_string_non_random_integer_weights(self):
        """Test non-random weighted values when weights sum to > 1.0.

        This covers the scale_factor = weights_sum branch in column_generation_spec.py
        when weights_sum > 1.0 and random=False.
        """
        # Use integer weights that sum to > 1
        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_nonrandom_int", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("category", "string", values=["X", "Y", "Z"], weights=[50, 30, 20])
        )

        df = ds.build()
        count = df.count()
        assert count == 100000

        # Verify we can generate data without errors
        distinct_values = df.select("category").distinct().collect()
        values = {row.category for row in distinct_values}
        assert values == {"X", "Y", "Z"}


class TestScaledExpressionValidation:
    """Test defensive validation in _getScaledIntSQLExpression."""

    def test_scale_one_with_normalize_raises_value_error(self):
        """Test that scale=1 with normalize=True raises ValueError.

        When normalize=True, the code divides by (scale - 1). If scale=1,
        this would result in division by zero. Rather than silently returning
        NULL via try_divide, we should raise an explicit ValueError.
        """
        from dbldatagen.column_generation_spec import ColumnGenerationSpec

        # Create a minimal column spec to test the internal method
        spec = ColumnGenerationSpec(name="test_col", colType="string")

        # Calling with scale=1 and normalize=True should raise ValueError
        # because divisor = (1 - 1) = 0
        with pytest.raises(ValueError, match="scale.*normalize.*divisor"):
            spec._getScaledIntSQLExpression(
                col_name="test_col",
                scale=1,
                base_columns=["id"],
                normalize=True,
            )
