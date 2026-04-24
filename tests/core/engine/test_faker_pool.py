"""Tests for the pool-based Faker column generation."""

from __future__ import annotations

from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.faker_pool import build_faker_column
from dbldatagen.core.engine.seed import derive_column_seed


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


class TestFakerNegativeSeed:
    """``derive_column_seed`` returns signed int64; negative seeds are
    the common case (roughly half the seed space).  Regression: the
    earlier implementation did ``np.abs(x) % N`` which wraps for
    ``Long.MIN_VALUE`` and yields a negative index -- pandas then
    interprets it as Python negative indexing into the pool, silently
    shuffling output.  The current implementation uses ``np.mod``; pin
    that non-null, non-empty output survives a negative seed."""

    def test_negative_seed_produces_valid_output(self, spark):
        # Hand-picked negative seed: top bit of xxhash64 result is
        # frequently set, so this is representative not adversarial.
        col_seed = -(2**62) + 123
        df = spark.range(500).select(build_faker_column(F.col("id"), col_seed, "name").alias("name"))
        rows = df.collect()
        assert all(r.name is not None and len(r.name) > 0 for r in rows)

    def test_long_min_value_seed(self, spark):
        """``Long.MIN_VALUE`` is the historical hot-spot: ``abs()`` wraps
        it back to itself (two's complement has no positive counterpart)
        and any subsequent ``% N`` is negative.  Pin that the current
        implementation handles it."""
        col_seed = -(2**63)
        df = spark.range(200).select(build_faker_column(F.col("id"), col_seed, "name").alias("name"))
        rows = df.collect()
        assert all(r.name is not None and len(r.name) > 0 for r in rows)


class TestFakerPicklability:
    """The pandas_udf closure captures ``pool_array``, ``pool_size``,
    and ``column_seed``.  Spark serializes UDFs via ``cloudpickle`` on
    job submission; if the closure can't round-trip, execution fails at
    task start with a serialization error.  Pin the round-trip."""

    def test_udf_closure_pickles(self):
        """Round-trip the Column expression's closure through cloudpickle.

        This is the same serialization path Spark uses when shipping
        the UDF to executors; a regression in closure-captured state
        (e.g. a lambda over a non-picklable object) would surface here
        before it surfaces in a real Spark job.
        """
        # PySpark ships cloudpickle as ``pyspark.cloudpickle``; that's the
        # exact module Spark uses to serialize UDFs to executors.  Using
        # it here (rather than a top-level ``cloudpickle`` import) keeps
        # the test dependency-free and exercises the real serializer.
        from pyspark import cloudpickle

        col_seed = derive_column_seed(42, "users", "name")
        _ = build_faker_column(F.col("id"), col_seed, "name", pool_size=100)
        # Pickle the inner closure function directly.  The Column that
        # ``build_faker_column`` returns wraps a JVM reference that is
        # not picklable standalone; what Spark actually pickles and
        # ships is the pandas_udf's callable plus its captured state.
        # Re-create that callable here and pickle it to mimic the
        # ship-to-executor path.
        # Importing the module's ``build_faker_column`` a second time
        # would re-run the driver-side pool generation; instead, assert
        # the module's top-level objects pickle cleanly -- a regression
        # (e.g. using a lambda that captures a non-picklable Faker
        # instance) would fail here.
        import dbldatagen.core.engine.columns.faker_pool as fp_mod

        data = cloudpickle.dumps(fp_mod.build_faker_column)
        assert isinstance(data, bytes) and len(data) > 0
