"""Tests covering uncovered lines in dbldatagen.core.engine.columns.

Targets:
- temporal.py: lines 26, 48, 70, 75, 80
- string.py: lines 43, 97-98, 104, 107, 157, 168-170
- numeric.py: lines 60, 83-86
- uuid.py: lines 29-30
"""

from __future__ import annotations

import datetime
import re

import pytest
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.numeric import build_range_column
from dbldatagen.core.engine.columns.string import (
    build_pattern_column,
    build_values_column,
)
from dbldatagen.core.engine.columns.temporal import (
    _parse_epoch,
    build_date_column,
    build_timestamp_column,
)
from dbldatagen.core.engine.columns.uuid import build_uuid_column
from dbldatagen.core.spec.schema import DataType, Normal


# ===================================================================
# temporal.py coverage
# ===================================================================


class TestParseEpochError:
    """Line 26: _parse_epoch raises ValueError on unparseable strings."""

    def test_unparseable_datetime_raises(self):
        with pytest.raises(ValueError, match="Cannot parse datetime string"):
            _parse_epoch("not-a-date")

    def test_partial_date_raises(self):
        with pytest.raises(ValueError, match="Cannot parse datetime string"):
            _parse_epoch("2023-13-45")


class TestTimestampDegenerateRange:
    """Line 48: end_epoch <= start_epoch returns a literal timestamp."""

    def test_end_before_start_returns_literal(self, spark):
        df = spark.range(5)
        col = build_timestamp_column(F.col("id"), 42, start="2025-01-01", end="2020-01-01")
        result = df.select(col.alias("ts")).collect()
        assert all(r.ts is not None for r in result)

    def test_same_start_end_returns_literal(self, spark):
        df = spark.range(5)
        col = build_timestamp_column(F.col("id"), 42, start="2023-06-15", end="2023-06-15")
        result = df.select(col.alias("ts")).collect()
        assert all(r.ts is not None for r in result)


class TestDateColumnStringId:
    """Line 70: build_date_column with string id_col."""

    def test_string_id_col(self, spark):
        df = spark.range(10).withColumnRenamed("id", "row_id")
        col = build_date_column("row_id", 42, start="2023-01-01", end="2023-12-31")
        result = df.select(col.alias("d")).collect()
        assert all(isinstance(r.d, datetime.date) for r in result)


class TestDateDegenerateRange:
    """Line 75: end <= start returns literal date."""

    def test_end_before_start_returns_literal(self, spark):
        df = spark.range(5)
        col = build_date_column(F.col("id"), 42, start="2025-01-01", end="2020-01-01")
        result = df.select(col.alias("d")).collect()
        assert all(r.d is not None for r in result)


class TestDateRangeDaysZero:
    """Line 80: range_days <= 0 returns literal date (same-day range)."""

    def test_same_day_returns_literal(self, spark):
        # Start and end are the same day but parsed with different formats
        # that give slightly different epoch seconds but 0 full days difference
        df = spark.range(5)
        # Use times that are within the same day (< 86400 seconds apart)
        col = build_date_column(
            F.col("id"),
            42,
            start="2023-06-15 00:00:00",
            end="2023-06-15 23:59:59",
        )
        result = df.select(col.alias("d")).collect()
        assert all(r.d is not None for r in result)


# ===================================================================
# string.py coverage
# ===================================================================


class TestValuesColumnEmpty:
    """Line 43: empty values list returns lit(None)."""

    def test_empty_values_returns_null(self, spark):
        df = spark.range(5)
        col = build_values_column(F.col("id"), 42, [])
        result = df.select(col.alias("v")).collect()
        assert all(r.v is None for r in result)


class TestPatternHex:
    """Lines 97-98: {hex:N} placeholder in pattern column."""

    def test_hex_placeholder(self, spark):
        df = spark.range(20)
        col = build_pattern_column(F.col("id"), 42, "HX-{hex:6}")
        result = df.select(col.alias("p")).collect()
        for r in result:
            assert r.p.startswith("HX-")
            hex_part = r.p[3:]
            assert len(hex_part) == 6
            assert all(c in "0123456789abcdef" for c in hex_part)


class TestPatternTrailingLiteral:
    """Line 104: trailing literal text after last placeholder."""

    def test_trailing_literal(self, spark):
        df = spark.range(10)
        col = build_pattern_column(F.col("id"), 42, "{digit:3}-END")
        result = df.select(col.alias("p")).collect()
        for r in result:
            assert r.p.endswith("-END")
            assert len(r.p) == 7  # 3 digits + "-END"


class TestPatternNoPlaceholders:
    """Line 107: template with no placeholders returns lit(template)."""

    def test_no_placeholders_returns_literal(self, spark):
        df = spark.range(5)
        col = build_pattern_column(F.col("id"), 42, "STATIC-VALUE")
        result = df.select(col.alias("p")).collect()
        assert all(r.p == "STATIC-VALUE" for r in result)


class TestRandomAlphaSingleChar:
    """Line 157: _random_alpha with width=1 returns single char (no concat)."""

    def test_single_alpha_char(self, spark):
        df = spark.range(10)
        col = build_pattern_column(F.col("id"), 42, "{alpha:1}")
        result = df.select(col.alias("p")).collect()
        for r in result:
            assert len(r.p) == 1
            assert r.p.isupper()


class TestRandomHexGeneration:
    """Lines 168-170: _random_hex generation."""

    def test_hex_values_correct_width(self, spark):
        df = spark.range(30)
        col = build_pattern_column(F.col("id"), 42, "{hex:8}")
        result = df.select(col.alias("p")).collect()
        for r in result:
            assert len(r.p) == 8
            assert all(c in "0123456789abcdef" for c in r.p)


# ===================================================================
# numeric.py coverage
# ===================================================================


class TestIntegerRangeDegenerate:
    """Line 60: range_size <= 0 returns lit(min_val)."""

    def test_max_less_than_min_returns_literal(self, spark):
        df = spark.range(5)
        col = build_range_column(F.col("id"), 42, min_val=10, max_val=5, dtype=DataType.INT)
        result = df.select(col.alias("v")).collect()
        assert all(r.v == 10 for r in result)

    def test_equal_min_max_returns_literal(self, spark):
        """When min == max, range_size = max - min + 1 = 1, so not degenerate.
        But min > max is degenerate (range_size = 0)."""
        df = spark.range(5)
        col = build_range_column(F.col("id"), 42, min_val=7, max_val=6, dtype=DataType.INT)
        result = df.select(col.alias("v")).collect()
        assert all(r.v == 7 for r in result)


class TestFloatRangeNormalDistribution:
    """Lines 83-86: float range with Normal distribution."""

    def test_normal_distribution_float_range(self, spark):
        df = spark.range(200)
        col = build_range_column(
            F.col("id"),
            42,
            min_val=0.0,
            max_val=100.0,
            distribution=Normal(),
            dtype=DataType.DOUBLE,
        )
        result = df.select(col.alias("v")).collect()
        values = [r.v for r in result]
        # All values should be within [0, 100] (clamped)
        assert all(0.0 <= v <= 100.0 for v in values)
        # Normal distribution should center around the middle
        mean = sum(values) / len(values)
        assert 30.0 < mean < 70.0, f"Mean {mean} seems off for Normal in [0, 100]"


class TestDecimalPrecisionScale:
    """build_range_column with dtype=DECIMAL respects precision/scale."""

    def test_default_precision_scale_is_18_2(self, spark):
        """When precision/scale are None, decimal type is DecimalType(18, 2)."""
        df = spark.range(5)
        col = build_range_column(F.col("id"), 42, min_val=0.0, max_val=1000.0, dtype=DataType.DECIMAL)
        result = df.select(col.alias("v"))
        field = result.schema["v"]
        assert field.dataType.typeName() == "decimal"
        assert field.dataType.precision == 18
        assert field.dataType.scale == 2

    def test_custom_precision_scale_applied(self, spark):
        """DecimalType(10, 4) is produced when precision=10, scale=4."""
        df = spark.range(5)
        col = build_range_column(
            F.col("id"), 42, min_val=0.0, max_val=1.0, dtype=DataType.DECIMAL,
            precision=10, scale=4,
        )
        result = df.select(col.alias("v"))
        field = result.schema["v"]
        assert field.dataType.precision == 10
        assert field.dataType.scale == 4

    def test_values_rounded_to_scale(self, spark):
        """Rows are rounded/padded to exactly ``scale`` decimal places.

        Spark's ``cast(DecimalType(p, s))`` pads to exactly ``s`` fractional
        digits (``Decimal('0.5000')`` not ``Decimal('0.5')``).  Assert
        ``exponent == -scale`` exactly — a regression that kept the old
        hard-coded scale=2 would produce exponent=-2 and fail this test.
        """
        from decimal import Decimal

        df = spark.range(50)
        col = build_range_column(
            F.col("id"), 42, min_val=0.0, max_val=1.0, dtype=DataType.DECIMAL,
            precision=10, scale=4,
        )
        rows = df.select(col.alias("v")).collect()
        for r in rows:
            assert isinstance(r.v, Decimal)
            _, _, exponent = r.v.as_tuple()
            assert exponent == -4, (
                f"value {r.v} has exponent {exponent}, expected -4 "
                f"(would be -2 if the old hard-coded scale leaked through)"
            )

    def test_high_precision_decimal_38_8(self, spark):
        """Crypto-scale decimal(38, 8) works end-to-end."""
        df = spark.range(5)
        col = build_range_column(
            F.col("id"), 42, min_val=0.0, max_val=1_000_000.0, dtype=DataType.DECIMAL,
            precision=38, scale=8,
        )
        result = df.select(col.alias("v"))
        assert result.schema["v"].dataType.precision == 38
        assert result.schema["v"].dataType.scale == 8


# ===================================================================
# pmod sweep: F.abs(x) % n → F.pmod(x, n)
# ===================================================================


class TestPmodForLongMinValueSweep:
    """Regression tests for the F.abs(x) % n → F.pmod(x, n) migration.

    The old pattern broke at Long.MIN_VALUE: ``abs(-2**63)`` overflows
    (raises ARITHMETIC_OVERFLOW under ANSI, silently stays negative
    without), and Spark's ``%`` on a negative dividend returns a negative
    remainder — either way the ``[0, N)`` invariant downstream code
    assumed could be violated.  pmod fixes both by computing
    ``((x % n) + n) % n`` with no abs.
    """

    def test_uniform_sample_at_long_min_value_under_ansi(self, spark, ansi_enabled):
        """Passes Long.MIN_VALUE directly through ``uniform_sample``.

        Exercises the first post-sweep site (``distributions.py:uniform_sample``).
        Before the fix, ``F.abs(Long.MIN_VALUE) % n`` under ANSI raised
        ARITHMETIC_OVERFLOW — this test's query would have failed to
        complete.  After the sweep, ``pmod`` returns a value in [0, n).
        """
        from dbldatagen.core.engine.distributions import uniform_sample

        long_min = -(2**63)
        n = 10000
        df = spark.range(5).withColumn("h", F.lit(long_min).cast("long"))
        rows = df.select(uniform_sample(F.col("h"), n).alias("r")).collect()
        for r in rows:
            assert 0 <= r.r < n, f"uniform_sample produced out-of-range {r.r}"

    def test_null_mask_runs_under_ansi_mode(self, spark, ansi_enabled):
        """null_mask_expr used to be ``abs(null_hash) % N < threshold``.

        Under ANSI any row whose xxhash64 yielded Long.MIN_VALUE would
        raise ARITHMETIC_OVERFLOW.  With the pmod sweep the expression
        is total over all signed-64 inputs, so a mask evaluated with
        ANSI enabled must run without raising.
        """
        from dbldatagen.core.engine.seed import null_mask_expr

        df = spark.range(10000)
        mask = null_mask_expr(column_seed=42, id_col="id", null_fraction=0.3)
        null_count = df.select(mask.alias("is_null")).filter("is_null").count()
        # Not asserting an exact ratio (stat variance); just that
        # the query ran to completion without ANSI overflow.
        assert 0 < null_count < 10000, f"unexpected null_count {null_count}"


# ===================================================================
# uuid.py coverage
# ===================================================================


class TestUUIDColumnSeedAsColumn:
    """Lines 29-30: build_uuid_column with column_seed as a Column (not int)."""

    def test_uuid_with_column_seed(self, spark):
        df = spark.range(20).withColumn("_seed", F.lit(42).cast("long"))
        col = build_uuid_column(F.col("id"), F.col("_seed"))
        result = df.select(col.alias("uuid")).collect()
        uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        for r in result:
            assert uuid_re.match(r.uuid), f"Invalid UUID format: {r.uuid}"

    def test_uuid_with_string_id_col(self, spark):
        """Also covers line 25-26: string id_col branch."""
        df = spark.range(10).withColumnRenamed("id", "row_id")
        col = build_uuid_column("row_id", 42)
        result = df.select(col.alias("uuid")).collect()
        uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        for r in result:
            assert uuid_re.match(r.uuid), f"Invalid UUID format: {r.uuid}"

    def test_uuid_seed_at_long_max_produces_valid_uuids(self, spark):
        """column_seed == Long.MAX_VALUE (int branch) must not overflow seed + 1.

        Without the to_signed64 clamp, ``column_seed + 1`` = 2**63, which is
        out of signed-64 range — F.lit/cast("long") rejects it before any rows
        are produced.  With the clamp, MAX wraps to MIN and rows are produced
        as valid UUIDs.
        """
        long_max = 2**63 - 1
        df = spark.range(20)
        col = build_uuid_column("id", long_max)
        rows = df.select(col.alias("u")).collect()
        uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        for r in rows:
            assert uuid_re.match(r.u), f"Invalid UUID format: {r.u}"

    def test_uuid_column_seed_at_long_max_produces_valid_uuids(self, spark, ansi_enabled):
        """Column-typed column_seed carrying Long.MAX_VALUE must also wrap cleanly.

        ANSI is toggled on (via the ``ansi_enabled`` fixture) so the bug
        actually reproduces: under ANSI, a naive ``seed + 1`` on a MAX
        row raises ARITHMETIC_OVERFLOW in Catalyst.  In non-ANSI mode
        (Spark 3.x default) the add silently wraps, which would hide the
        fix.
        """
        long_max = 2**63 - 1
        df = spark.range(20).withColumn("_seed", F.lit(long_max).cast("long"))
        col = build_uuid_column(F.col("id"), F.col("_seed"))
        rows = df.select(col.alias("u")).collect()
        uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        for r in rows:
            assert uuid_re.match(r.u), f"Invalid UUID format: {r.u}"
