"""Tests covering uncovered lines in dbldatagen.v1.engine.columns.

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

from dbldatagen.v1.engine.columns.numeric import build_range_column
from dbldatagen.v1.engine.columns.string import (
    build_pattern_column,
    build_values_column,
)
from dbldatagen.v1.engine.columns.temporal import (
    _parse_epoch,
    build_date_column,
    build_timestamp_column,
)
from dbldatagen.v1.engine.columns.uuid import build_uuid_column
from dbldatagen.v1.schema import DataType, Normal


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
