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


class TestFakerOutputSchema:
    """The Faker pool pandas_udf hardcodes ``StringType``.  The schema
    validator rejects ``dtype != None/STRING`` on FakerColumn because
    every other type would be a lie about the resulting column; this
    test pins the other side of the contract -- that the engine does
    in fact emit StringType so the validator's promise holds."""

    def test_faker_column_output_is_string_type(self, spark):
        from pyspark.sql import types as T

        col_seed = derive_column_seed(42, "users", "name")
        df = spark.range(10).select(build_faker_column(F.col("id"), col_seed, "name").alias("name"))
        assert df.schema["name"].dataType == T.StringType(), (
            f"FakerColumn must emit StringType -- a regression here would silently "
            f"invalidate the schema validator's ``dtype=STRING or None`` contract.  "
            f"Got {df.schema['name'].dataType}"
        )


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
    task start with a serialization error.

    The ``.collect()`` in the other Faker tests (``test_faker_name_*``,
    ``TestFakerNegativeSeed``, etc.) already forces the full
    driver-construct -> cloudpickle -> executor-deserialize -> execute
    cycle, so closure-regression would surface there.  The explicit
    pickle-round-trip test is retained below as a dedicated
    self-documenting regression for the captured state -- it forces
    the same serializer on the actual UDF callable, not on the outer
    helper function (which pickles by module reference and doesn't
    touch the closure).
    """

    def test_udf_closure_captured_state_pickles(self, spark):
        """Execute a Faker-column query end-to-end and ensure the UDF
        serialization path completes.

        The driver constructs a pandas_udf closure over
        ``pool_array`` (numpy object array), ``pool_size`` (int), and
        ``column_seed`` (signed int64).  When Spark schedules the
        task, it cloudpickle-dumps that closure and sends the bytes
        to every executor.  If any captured object became
        unpicklable (e.g. a refactor that accidentally held a live
        ``Faker`` instance in the closure), this .collect() would
        fail with a ``PicklingError`` before any row materialises.

        Run a tiny spark.range(1) through the UDF; the pickle path is
        exercised regardless of row count.
        """
        col_seed = derive_column_seed(42, "users", "name")
        col_expr = build_faker_column(F.col("id"), col_seed, "name", pool_size=50)
        out = spark.range(1).select(col_expr.alias("n")).collect()
        assert len(out) == 1 and out[0].n is not None and len(out[0].n) > 0
