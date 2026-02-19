"""Tests for the pool-based Faker column generation."""

from __future__ import annotations

from pyspark.sql import functions as F

from dbldatagen.v1.engine.columns.faker_pool import build_faker_column
from dbldatagen.v1.engine.seed import derive_column_seed


class TestFakerName:
    def test_faker_name_non_null(self, spark):
        """Generate names; verify all are non-null strings."""
        col_seed = derive_column_seed(42, "users", "name")
        df = spark.range(500).select(build_faker_column(F.col("id"), col_seed, "name").alias("name"))

        null_count = df.filter(F.col("name").isNull()).count()
        assert null_count == 0

        # Verify they're non-empty strings
        empty_count = df.filter(F.length(F.col("name")) == 0).count()
        assert empty_count == 0


class TestFakerDeterminism:
    def test_faker_determinism(self, spark):
        """Same seed produces the same names."""
        col_seed = derive_column_seed(42, "users", "name")
        df = spark.range(100)

        run1 = [r.name for r in df.select(build_faker_column(F.col("id"), col_seed, "name").alias("name")).collect()]
        run2 = [r.name for r in df.select(build_faker_column(F.col("id"), col_seed, "name").alias("name")).collect()]
        assert run1 == run2


class TestFakerPoolVariety:
    def test_faker_pool_variety(self, spark):
        """With pool_size=1000, verify reasonable cardinality in output."""
        col_seed = derive_column_seed(42, "users", "name")
        df = spark.range(5000).select(build_faker_column(F.col("id"), col_seed, "name", pool_size=1000).alias("name"))
        distinct_count = df.distinct().count()
        # With 1000 pool entries and 5000 rows, we should see many distinct values
        assert distinct_count >= 100, f"Expected variety but got only {distinct_count} distinct names"
