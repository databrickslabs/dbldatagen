"""Tests for the dbldatagen.v1 generation engine.

Uses a real local Spark session to verify determinism, bounds, uniqueness,
and correctness of all column generation strategies.
"""

from __future__ import annotations

import re

import numpy as np
from pyspark.sql import functions as F

from dbldatagen.v1.engine.columns.numeric import build_range_column
from dbldatagen.v1.engine.columns.pk import (
    build_formatted_pk,
    build_sequential_pk,
    feistel_permute_batch,
)
from dbldatagen.v1.engine.columns.string import build_pattern_column, build_values_column
from dbldatagen.v1.engine.columns.temporal import build_timestamp_column
from dbldatagen.v1.engine.columns.uuid import build_uuid_column
from dbldatagen.v1.engine.generator import generate_table
from dbldatagen.v1.engine.seed import cell_seed_expr, derive_column_seed, null_mask_expr
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Seed determinism
# ---------------------------------------------------------------------------


class TestSeedDeterminism:
    def test_derive_column_seed_determinism(self):
        """Same inputs always produce the same column seed."""
        s1 = derive_column_seed(42, "orders", "amount")
        s2 = derive_column_seed(42, "orders", "amount")
        assert s1 == s2

    def test_derive_column_seed_varies(self):
        """Different column names produce different seeds."""
        s1 = derive_column_seed(42, "orders", "amount")
        s2 = derive_column_seed(42, "orders", "status")
        assert s1 != s2

    def test_cell_seed_determinism(self, spark):
        """Cell seeds for 1000 rows are identical across two runs."""
        col_seed = derive_column_seed(42, "t", "c")
        df = spark.range(1000)

        run1 = df.select(cell_seed_expr(col_seed, "id").alias("seed")).collect()
        run2 = df.select(cell_seed_expr(col_seed, "id").alias("seed")).collect()

        seeds1 = [r.seed for r in run1]
        seeds2 = [r.seed for r in run2]
        assert seeds1 == seeds2


# ---------------------------------------------------------------------------
# Numeric columns
# ---------------------------------------------------------------------------


class TestRangeColumn:
    def test_bounds(self, spark):
        """All generated values fall within [min, max]."""
        col_seed = derive_column_seed(42, "t", "val")
        df = spark.range(5000).select(build_range_column("id", col_seed, 10, 99).alias("val"))
        stats = df.agg(F.min("val").alias("lo"), F.max("val").alias("hi")).first()
        assert stats.lo >= 10
        assert stats.hi <= 99

    def test_determinism(self, spark):
        """Same seed produces identical values."""
        col_seed = derive_column_seed(42, "t", "val")
        df = spark.range(500)
        run1 = [r.val for r in df.select(build_range_column("id", col_seed, 0, 1000).alias("val")).collect()]
        run2 = [r.val for r in df.select(build_range_column("id", col_seed, 0, 1000).alias("val")).collect()]
        assert run1 == run2

    def test_single_value_range(self, spark):
        """Range where min == max returns that single value."""
        col_seed = derive_column_seed(42, "t", "val")
        df = spark.range(100).select(build_range_column("id", col_seed, 42, 42).alias("val"))
        distinct = df.distinct().collect()
        assert len(distinct) == 1
        assert distinct[0].val == 42

    def test_float_range(self, spark):
        """Float-type range produces values in the continuous interval."""
        col_seed = derive_column_seed(42, "t", "price")
        df = spark.range(1000).select(
            build_range_column("id", col_seed, 0.0, 100.0, dtype=DataType.DOUBLE).alias("price")
        )
        stats = df.agg(F.min("price").alias("lo"), F.max("price").alias("hi")).first()
        assert stats.lo >= 0.0
        assert stats.hi <= 100.0


# ---------------------------------------------------------------------------
# Values column
# ---------------------------------------------------------------------------


class TestValuesColumn:
    def test_only_allowed_values(self, spark):
        """Generated values are always from the provided list."""
        allowed = ["active", "inactive", "pending"]
        col_seed = derive_column_seed(42, "t", "status")
        df = spark.range(2000).select(build_values_column("id", col_seed, allowed).alias("status"))
        actual = {r.status for r in df.distinct().collect()}
        assert actual.issubset(set(allowed))

    def test_single_value_list(self, spark):
        """Single-element list always returns that element."""
        col_seed = derive_column_seed(42, "t", "status")
        df = spark.range(100).select(build_values_column("id", col_seed, ["only"]).alias("status"))
        vals = {r.status for r in df.distinct().collect()}
        assert vals == {"only"}


# ---------------------------------------------------------------------------
# Timestamp column
# ---------------------------------------------------------------------------


class TestTimestampColumn:
    def test_bounds(self, spark):
        """Timestamps fall within [start, end] (UTC epoch range)."""
        col_seed = derive_column_seed(42, "t", "ts")
        df = spark.range(2000).select(build_timestamp_column("id", col_seed, "2023-01-01", "2023-12-31").alias("ts"))
        # Compare using epoch seconds to avoid local-timezone display issues
        stats = df.agg(
            F.min(F.unix_timestamp("ts")).alias("lo"),
            F.max(F.unix_timestamp("ts")).alias("hi"),
        ).first()
        start_epoch = 1672531200  # 2023-01-01 00:00:00 UTC
        end_epoch = 1703980800  # 2023-12-31 00:00:00 UTC
        assert stats.lo >= start_epoch
        assert stats.hi <= end_epoch


# ---------------------------------------------------------------------------
# Sequential PK
# ---------------------------------------------------------------------------


class TestSequentialPK:
    def test_sequential(self, spark):
        """Sequential PK is unique, starts at the given value, and uses the step."""
        df = spark.range(100).select(build_sequential_pk("id", start=1, step=1).alias("pk"))
        rows = sorted([r.pk for r in df.collect()])
        assert rows == list(range(1, 101))

    def test_custom_start_step(self, spark):
        """Custom start and step produce the expected sequence."""
        df = spark.range(5).select(build_sequential_pk("id", start=10, step=5).alias("pk"))
        rows = sorted([r.pk for r in df.collect()])
        assert rows == [10, 15, 20, 25, 30]


# ---------------------------------------------------------------------------
# Feistel permutation (pure NumPy)
# ---------------------------------------------------------------------------


class TestFeistelPermutation:
    def test_uniqueness(self):
        """Feistel permutation of [0, N) yields all unique values in [0, N)."""
        N = 1000
        indices = np.arange(N, dtype=np.int64)
        result = feistel_permute_batch(indices, N, seed=42)
        assert len(set(result)) == N
        assert result.min() >= 0
        assert result.max() < N

    def test_determinism(self):
        """Same seed produces the same permutation."""
        N = 500
        indices = np.arange(N, dtype=np.int64)
        r1 = feistel_permute_batch(indices, N, seed=99)
        r2 = feistel_permute_batch(indices, N, seed=99)
        np.testing.assert_array_equal(r1, r2)

    def test_different_seed(self):
        """Different seeds produce different permutations."""
        N = 500
        indices = np.arange(N, dtype=np.int64)
        r1 = feistel_permute_batch(indices, N, seed=1)
        r2 = feistel_permute_batch(indices, N, seed=2)
        # Very unlikely to be identical for different seeds
        assert not np.array_equal(r1, r2)

    def test_non_power_of_two(self):
        """Handles domain sizes that are not powers of two."""
        for N in [7, 13, 100, 1023, 1025]:
            indices = np.arange(N, dtype=np.int64)
            result = feistel_permute_batch(indices, N, seed=42)
            assert len(set(result)) == N, f"Non-unique results for N={N}"
            assert result.min() >= 0
            assert result.max() < N


# ---------------------------------------------------------------------------
# Pattern column
# ---------------------------------------------------------------------------


class TestPatternColumn:
    def test_digit_pattern(self, spark):
        """Pattern 'ORD-{digit:4}' produces correctly formatted strings."""
        col_seed = derive_column_seed(42, "t", "order_id")
        df = spark.range(200).select(build_pattern_column("id", col_seed, "ORD-{digit:4}").alias("oid"))
        values = [r.oid for r in df.collect()]
        for v in values:
            assert v.startswith("ORD-"), f"Bad prefix: {v}"
            suffix = v[4:]
            assert len(suffix) == 4, f"Bad digit width: {v}"
            assert suffix.isdigit(), f"Non-digit suffix: {v}"

    def test_seq_pattern(self, spark):
        """Pattern '{seq:4}' produces zero-padded sequential IDs."""
        col_seed = derive_column_seed(42, "t", "store_id")
        df = spark.range(5).select(build_pattern_column("id", col_seed, "STORE-{seq:4}").alias("sid"))
        values = [r.sid for r in df.collect()]
        assert values == ["STORE-0001", "STORE-0002", "STORE-0003", "STORE-0004", "STORE-0005"]

    def test_seq_pattern_with_d_suffix(self, spark):
        """Pattern '{seq:06d}' (Python-style format) also works."""
        col_seed = derive_column_seed(42, "t", "product_id")
        df = spark.range(3).select(build_pattern_column("id", col_seed, "PROD-{seq:06d}").alias("pid"))
        values = [r.pid for r in df.collect()]
        assert values == ["PROD-000001", "PROD-000002", "PROD-000003"]

    def test_alpha_pattern(self, spark):
        """Pattern '{alpha:3}' produces 3 uppercase letters."""
        col_seed = derive_column_seed(42, "t", "code")
        df = spark.range(100).select(build_pattern_column("id", col_seed, "{alpha:3}").alias("code"))
        values = [r.code for r in df.collect()]
        for v in values:
            assert len(v) == 3, f"Bad length: {v}"
            assert v.isalpha() and v.isupper(), f"Not uppercase alpha: {v}"


# ---------------------------------------------------------------------------
# UUID column
# ---------------------------------------------------------------------------

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class TestUUIDColumn:
    def test_format(self, spark):
        """UUIDs match the standard format."""
        col_seed = derive_column_seed(42, "t", "uid")
        df = spark.range(200).select(build_uuid_column("id", col_seed).alias("uid"))
        values = [r.uid for r in df.collect()]
        for v in values:
            assert UUID_RE.match(v), f"Bad UUID format: {v}"

    def test_uniqueness(self, spark):
        """UUIDs are unique across rows."""
        col_seed = derive_column_seed(42, "t", "uid")
        df = spark.range(1000).select(build_uuid_column("id", col_seed).alias("uid"))
        total = df.count()
        distinct = df.distinct().count()
        assert distinct == total

    def test_determinism(self, spark):
        """Same seed produces the same UUIDs."""
        col_seed = derive_column_seed(42, "t", "uid")
        df = spark.range(100)
        run1 = [r.uid for r in df.select(build_uuid_column("id", col_seed).alias("uid")).collect()]
        run2 = [r.uid for r in df.select(build_uuid_column("id", col_seed).alias("uid")).collect()]
        assert run1 == run2


# ---------------------------------------------------------------------------
# Null injection
# ---------------------------------------------------------------------------


class TestNullInjection:
    def test_approximate_null_fraction(self, spark):
        """Null injection produces approximately the expected fraction of NULLs."""
        col_seed = derive_column_seed(42, "t", "val")
        n = 10000
        df = spark.range(n).select(
            F.when(
                null_mask_expr(col_seed, "id", 0.3),
                F.lit(None),
            )
            .otherwise(F.lit(1))
            .alias("val")
        )
        null_count = df.filter(F.col("val").isNull()).count()
        fraction = null_count / n
        # Allow a reasonable tolerance
        assert 0.2 < fraction < 0.4, f"Null fraction {fraction} outside expected range"

    def test_zero_null_fraction(self, spark):
        """null_fraction=0 produces no NULLs."""
        col_seed = derive_column_seed(42, "t", "val")
        df = spark.range(1000).select(
            F.when(null_mask_expr(col_seed, "id", 0.0), F.lit(None)).otherwise(F.lit(1)).alias("val")
        )
        null_count = df.filter(F.col("val").isNull()).count()
        assert null_count == 0


# ---------------------------------------------------------------------------
# generate_table integration
# ---------------------------------------------------------------------------


class TestGenerateTable:
    def test_simple_table(self, spark):
        """Generate a table with mixed column types and verify schema + row count."""
        spec = TableSpec(
            name="test_table",
            rows=500,
            seed=42,
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="amount",
                    dtype=DataType.LONG,
                    gen=RangeColumn(min=100, max=9999),
                ),
                ColumnSpec(
                    name="status",
                    gen=ValuesColumn(values=["open", "closed", "pending"]),
                ),
                ColumnSpec(
                    name="created_at",
                    gen=TimestampColumn(start="2023-01-01", end="2023-12-31"),
                ),
                ColumnSpec(name="uid", gen=UUIDColumn()),
                ColumnSpec(name="region", gen=ConstantColumn(value="US")),
            ],
        )
        df = generate_table(spark, spec)

        assert df.count() == 500
        col_names = set(df.columns)
        expected = {"order_id", "amount", "status", "created_at", "uid", "region"}
        assert expected == col_names

    def test_determinism(self, spark):
        """Two calls with the same spec produce identical DataFrames."""
        spec = TableSpec(
            name="det_test",
            rows=200,
            seed=99,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn()),
                ColumnSpec(name="val", gen=RangeColumn(min=0, max=100)),
            ],
        )
        df1 = generate_table(spark, spec).orderBy("pk").collect()
        df2 = generate_table(spark, spec).orderBy("pk").collect()

        vals1 = [(r.pk, r.val) for r in df1]
        vals2 = [(r.pk, r.val) for r in df2]
        assert vals1 == vals2

    def test_null_injection(self, spark):
        """Columns with null_fraction > 0 contain some NULLs."""
        spec = TableSpec(
            name="null_test",
            rows=5000,
            seed=42,
            columns=[
                ColumnSpec(
                    name="val",
                    gen=RangeColumn(min=0, max=100),
                    null_fraction=0.25,
                ),
            ],
        )
        df = generate_table(spark, spec)
        null_count = df.filter(F.col("val").isNull()).count()
        fraction = null_count / 5000
        assert 0.15 < fraction < 0.35, f"Unexpected null fraction: {fraction}"

    def test_expression_column(self, spark):
        """Expression columns reference other columns in the same table."""
        spec = TableSpec(
            name="expr_test",
            rows=100,
            seed=42,
            columns=[
                ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                ColumnSpec(name="b", gen=ConstantColumn(value=2)),
                ColumnSpec(name="c", gen=ExpressionColumn(expr="a * b")),
            ],
        )
        df = generate_table(spark, spec)
        rows = df.collect()
        for r in rows:
            assert r.c == r.a * r.b, f"Expression mismatch: {r.a} * {r.b} != {r.c}"

    def test_formatted_pk(self, spark):
        """Formatted PK template produces correct output."""
        df = spark.range(5).select(build_formatted_pk("id", "CUST-{digit:6}").alias("pk"))
        values = [r.pk for r in df.collect()]
        expected = [f"CUST-{str(i + 1).zfill(6)}" for i in range(5)]
        assert sorted(values) == sorted(expected)
