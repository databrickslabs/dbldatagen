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
import sys

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


class TestParseEpochSchemaParity:
    """Pin that ``_parse_epoch`` accepts the same strings the schema's
    ``TimestampColumn.validate_timestamps`` accepts (both call
    ``datetime.fromisoformat``), and that TZ offsets resolve to the
    correct UTC epoch instead of being reinterpreted as UTC wall-clock.

    Previously ``_parse_epoch`` used ``strptime`` against three fixed
    formats with no ``%z`` and no fractional seconds, so plans like
    ``TimestampColumn(start="2024-01-15T10:00:00+02:00", ...)`` passed
    schema validation and then raised at materialisation.  The parity
    fix landed in commit 72bc82f; this class is the regression pin.
    """

    def test_fractional_seconds_accepted(self):
        # Six-digit fractional second works on Python 3.10+ (the project
        # minimum); the single-digit form ``.5`` and ``Z`` suffix require
        # 3.11+ and are pinned in the gated test below.
        assert _parse_epoch("2024-01-15T10:00:00.123456") == 1_705_312_800

    def test_naive_datetime_treated_as_utc(self):
        # Reference value: a bare datetime with no offset is treated as
        # UTC so the engine remains session-TZ-independent.
        assert _parse_epoch("2024-01-15T10:00:00") == 1_705_312_800

    def test_tz_aware_offset_converted_to_utc_epoch(self):
        # 10:00 at +02:00 is 08:00 UTC — epoch 1_705_305_600, NOT
        # 1_705_312_800 (which would mean the offset was silently
        # dropped via ``.replace(tzinfo=utc)``).  This pins the
        # tz-aware branch's correctness.
        assert _parse_epoch("2024-01-15T10:00:00+02:00") == 1_705_305_600

    @pytest.mark.skipif(
        sys.version_info < (3, 11),
        reason="datetime.fromisoformat accepts the 'Z' suffix only on Python 3.11+",
    )
    def test_z_suffix_treated_as_utc(self):
        assert _parse_epoch("2024-01-15T10:00:00Z") == 1_705_312_800


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

    def test_date_is_session_tz_independent(self, spark):
        """``build_date_column`` must produce identical dates regardless of
        ``spark.sql.session.timeZone``.  Previously ``F.from_unixtime(s).cast('date')``
        picked the session-TZ wall-clock date, so midnight-UTC rows
        landed on different dates in UTC vs ``America/Los_Angeles``.
        The fix routes through ``F.date_add(epoch_date, days)``, which
        is TZ-agnostic.
        """
        key = "spark.sql.session.timeZone"
        prev = spark.conf.get(key, None)
        try:
            spark.conf.set(key, "UTC")
            df = spark.range(10)
            d_utc = [
                str(r.v)
                for r in df.select(
                    build_date_column(F.col("id"), 42, start="2024-01-01", end="2024-01-02").alias("v")
                ).collect()
            ]
            spark.conf.set(key, "America/Los_Angeles")
            d_la = [
                str(r.v)
                for r in df.select(
                    build_date_column(F.col("id"), 42, start="2024-01-01", end="2024-01-02").alias("v")
                ).collect()
            ]
            assert d_utc == d_la, f"date diverges across sessions: UTC={d_utc}, LA={d_la}"
        finally:
            if prev is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, prev)

    def test_range_beyond_int32_seconds_produces_valid_timestamps(self, spark, ansi_enabled):
        """A 200-year range is ~6.3e9 seconds, past the int32 ceiling.

        The range passes through ``apply_distribution`` → ``F.pmod(seed,
        F.lit(n))`` and ``F.least(idx, F.lit(n-1))``; both must stay in
        long arithmetic all the way through.  PySpark auto-promotes
        ``F.lit(int > 2**31)`` to LongType, so the expression composes
        cleanly under ANSI — this test pins that behavior so any
        regression that forces IntegerType fails here instead of
        silently wrapping row values.
        """
        # 50 rows (not 10): the test is deterministic at this seed today,
        # but the assertion is loose enough that a future change to seed
        # derivation could land all rows inside the 1970-2070 band and
        # break the test deterministically.  101 of 201 calendar years
        # fall in that band, so P(all 50 in band) = (101/201)^50 ≈
        # 1.1e-15 under any reasonable uniform draw — effectively zero,
        # and robust to seed-algo changes.
        df = spark.range(50)
        col = build_timestamp_column(F.col("id"), 42, start="1900-01-01", end="2100-12-31")
        rows = df.select(col.alias("ts")).collect()
        years = {r.ts.year for r in rows}
        # Draw spans more than one century — rules out silent wrap into 1970.
        assert min(years) < 1970 or max(years) > 2070, f"timestamp range collapsed: years={years}"


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

    def test_alpha_seed_xor_clamps_to_signed64(self, spark, ansi_enabled):
        """``_random_alpha`` XORs ``column_seed`` with ``(idx+1) * GOLDEN_RATIO_HASH``.

        For the Column seed branch the XOR is in Spark SQL and stays in
        signed-64 by construction; the int branch can exceed signed-64
        when ``column_seed`` is negative and the idx-scaled constant
        is large, and previously raised at ``F.lit`` because Python
        ints that don't fit in signed-long are rejected.  The int
        branch now passes through ``to_signed64`` before ``F.lit`` so
        any column_seed produces a valid signed-64 mixed seed.
        """
        from dbldatagen.core.engine.columns.string import _random_alpha

        # idx=10**10 forces the XOR result outside signed-64 without the clamp.
        df = spark.range(5)
        col = _random_alpha(F.col("id"), -(2**63), idx=10**10, width=3)
        rows = df.select(col.alias("p")).collect()
        for r in rows:
            assert len(r.p) == 3
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
    """Engine rejects validator-bypass min > max with ValueError."""

    def test_max_less_than_min_raises(self, spark):
        df = spark.range(5)
        with pytest.raises(ValueError, match="non-positive"):
            df.select(
                build_range_column(F.col("id"), 42, min_val=10, max_val=5, dtype=DataType.INT).alias("v")
            ).collect()

    def test_min_equal_max_plus_one_raises(self, spark):
        """``min == max + 1`` gives ``range_size = 0`` and also raises."""
        df = spark.range(5)
        with pytest.raises(ValueError, match="non-positive"):
            df.select(
                build_range_column(F.col("id"), 42, min_val=7, max_val=6, dtype=DataType.INT).alias("v")
            ).collect()


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

    def test_default_precision_scale_is_10_0(self, spark):
        """When precision/scale are None, decimal type matches Spark's DecimalType() default of (10, 0)."""
        df = spark.range(5)
        col = build_range_column(F.col("id"), 42, min_val=0.0, max_val=1000.0, dtype=DataType.DECIMAL)
        result = df.select(col.alias("v"))
        field = result.schema["v"]
        assert field.dataType.typeName() == "decimal"
        assert field.dataType.precision == 10
        assert field.dataType.scale == 0

    def test_custom_precision_scale_applied(self, spark):
        """DecimalType(10, 4) is produced when precision=10, scale=4."""
        df = spark.range(5)
        col = build_range_column(
            F.col("id"),
            42,
            min_val=0.0,
            max_val=1.0,
            dtype=DataType.DECIMAL,
            precision=10,
            scale=4,
        )
        result = df.select(col.alias("v"))
        field = result.schema["v"]
        assert field.dataType.precision == 10
        assert field.dataType.scale == 4

    def test_values_rounded_to_scale(self, spark):
        """Rows are rounded/padded to exactly ``scale`` decimal places.

        Spark's ``cast(DecimalType(p, s))`` pads to exactly ``s`` fractional
        digits (``Decimal('0.5000')`` not ``Decimal('0.5')``).  Assert
        ``exponent == -scale`` exactly — a regression that fell back to the
        default scale=0 would produce exponent=0 and fail this test.
        """
        from decimal import Decimal

        df = spark.range(50)
        col = build_range_column(
            F.col("id"),
            42,
            min_val=0.0,
            max_val=1.0,
            dtype=DataType.DECIMAL,
            precision=10,
            scale=4,
        )
        rows = df.select(col.alias("v")).collect()
        for r in rows:
            assert isinstance(r.v, Decimal)
            _, _, exponent = r.v.as_tuple()
            assert exponent == -4, (
                f"value {r.v} has exponent {exponent}, expected -4 " f"(would be 0 if the default scale leaked through)"
            )

    def test_high_precision_decimal_38_8(self, spark):
        """Crypto-scale decimal(38, 8) works end-to-end."""
        df = spark.range(5)
        col = build_range_column(
            F.col("id"),
            42,
            min_val=0.0,
            max_val=1_000_000.0,
            dtype=DataType.DECIMAL,
            precision=38,
            scale=8,
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


class TestUUIDColumnEdgeCases:
    """Edge cases on ``build_uuid_column``: string id_col and Long.MAX seed."""

    def test_uuid_with_string_id_col(self, spark):
        """String id_col branch (line 25-26)."""
        df = spark.range(10).withColumnRenamed("id", "row_id")
        col = build_uuid_column("row_id", 42)
        result = df.select(col.alias("uuid")).collect()
        uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        for r in result:
            assert uuid_re.match(r.uuid), f"Invalid UUID format: {r.uuid}"

    def test_uuid_seed_at_long_max_produces_valid_uuids(self, spark):
        """column_seed == Long.MAX_VALUE must not overflow seed + 1.

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

    def test_uuid_values_are_deterministic(self, spark):
        """Pin specific UUIDs for known ``(column_seed, id)`` pairs.

        ``build_uuid_column`` advertises reproducibility as part of its
        public contract: the same ``(column_seed, id)`` pair must yield
        the same UUID across runs, Spark versions, and refactors.  The
        format-only regex checks above don't catch a silent value
        regression -- e.g. swapping the hi/lo halves, reordering the
        bit slices, or changing from arithmetic to logical right shift
        would still pass a shape check.

        These pins were generated from the current implementation and
        cover three seeds: 42 (arbitrary), 0 (edge), Long.MAX (the
        wraparound regression case).  Any refactor of ``build_uuid_column``
        must reproduce these byte-for-byte.
        """
        expected = {
            42: {
                0: "55e286bc-ca92-6f97-0cb3-254fe7d7e63d",
                1: "54f51447-3c2a-606a-fb05-fa24aef6d6ef",
                2: "c193988c-86d5-e3fd-ad33-317ce387e634",
                3: "eef0d506-d5e9-40d8-488d-8677907c22aa",
                4: "c4edb456-7e3e-e763-4406-4aef13f31e7a",
            },
            0: {
                0: "80534700-ba5d-177e-e12e-325b86032959",
                1: "72eec4cc-5475-1688-af9b-ef9d3437f546",
                2: "5fea6284-a215-1548-8e3a-bf45d1b1ac3a",
                3: "e0e259f3-a9a9-d270-676c-0e9821e2b81d",
                4: "523ac03b-8c02-3737-6682-89f42e1130c3",
            },
            (2**63 - 1): {
                0: "a243b662-5419-7ba6-9080-74cc17197c65",
                1: "4b99220f-309b-f090-1227-b5d613ad8ea4",
                2: "24bf6c01-c0ee-397a-78c7-e676e7f75dfb",
                3: "62af3824-7770-5ab2-63c8-0a52ed16df8a",
                4: "de73f140-7ca1-3c69-c660-22a68c5bbc30",
            },
        }
        for seed, id_to_uuid in expected.items():
            df = spark.range(5).withColumnRenamed("id", "row_id")
            col = build_uuid_column("row_id", seed)
            rows = df.select("row_id", col.alias("u")).collect()
            actual = {r.row_id: r.u for r in rows}
            assert actual == id_to_uuid, (
                f"UUID determinism regression at seed={seed}: "
                f"expected {id_to_uuid}, got {actual}"
            )
