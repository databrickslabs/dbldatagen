"""Tests for the dbldatagen.core generation engine.

Uses a real local Spark session to verify determinism, bounds, uniqueness,
and correctness of all column generation strategies.
"""

from __future__ import annotations

import re

import pytest
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.numeric import build_range_column
from dbldatagen.core.engine.columns.pk import build_sequential_pk
from dbldatagen.core.engine.columns.string import build_pattern_column, build_values_column
from dbldatagen.core.engine.columns.temporal import build_timestamp_column
from dbldatagen.core.engine.columns.uuid import build_uuid_column
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.seed import cell_seed_expr, derive_column_seed, null_mask_expr, to_signed64
from dbldatagen.core.spec.schema import (
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

    def test_derive_column_seed_pinned_values(self):
        """Pin specific seeds for known ``(global_seed, table, column)`` triples.

        ``derive_column_seed`` is the foundation of dbldatagen's reproducibility
        contract: every column seed in every plan flows from here, and any
        change to its output cascades into every value the engine generates.
        The other tests prove determinism and variance but not byte
        equivalence -- a refactor that swapped multipliers, changed the
        mask, or reordered the table/column hash steps would still pass
        those checks.

        These pins were generated from the current implementation and
        cover global-seed edge cases (0, Long.MIN, Long.MAX), empty
        strings, unicode, and a long-string case to exercise the
        polynomial mix.  Any refactor of ``derive_column_seed`` must
        reproduce these byte-for-byte.
        """
        expected = {
            (42, "orders", "amount"): -6611133535135442573,
            (42, "orders", "status"): -6611133533874842473,
            (42, "customers", "id"): 1640369763616038596,
            (0, "orders", "amount"): 8431482345212582905,
            (1, "", ""): 1,
            (2**63 - 1, "orders", "amount"): -3068981324068604432,
            (-(2**63), "orders", "amount"): -791889691642192903,
            (42, "unicode_table", "col_with_emoji"): 662861529137711238,
            (42, "t", "c"): 52565,
            (42, "a_very_long_table_name_for_polynomial_mix_coverage", "a_very_long_column_name_for_polynomial_mix_coverage"): 3752365923030754514,
        }
        for (gs, table, column), pinned in expected.items():
            actual = derive_column_seed(gs, table, column)
            assert actual == pinned, (
                f"derive_column_seed determinism regression at "
                f"({gs}, {table!r}, {column!r}): expected {pinned}, got {actual}"
            )

    def test_cell_seed_determinism(self, spark):
        """Cell seeds for 1000 rows are identical across two runs."""
        col_seed = derive_column_seed(42, "t", "c")
        df = spark.range(1000)

        run1 = df.select(cell_seed_expr(col_seed, "id").alias("seed")).collect()
        run2 = df.select(cell_seed_expr(col_seed, "id").alias("seed")).collect()

        seeds1 = [r.seed for r in run1]
        seeds2 = [r.seed for r in run2]
        assert seeds1 == seeds2


class TestToSigned64:
    """Pin two's-complement wrap behavior at the signed-64 boundary."""

    def test_to_signed64_edge_cases(self):
        """Pin ``to_signed64`` output across the signed-64 boundary.

        ``to_signed64`` truncates an arbitrary Python int to the JVM
        long range ``[-2**63, 2**63 - 1]`` by reinterpreting the low
        64 bits as two's complement.  It's used in every seeded code
        path (derive_column_seed, UUID generation, length-seed mix),
        so any refactor that changed wrap behavior at the boundary
        would cascade through every downstream value.

        Pins cover: identity-on-in-range, Long.MIN/MAX exactness, the
        exact-2^63 overflow point, 2^64 wrap-to-zero, several-step
        overflow on both sides, and very-large Python ints
        (which only Python supports natively).
        """
        expected = {
            0: 0,
            1: 1,
            -1: -1,
            2**63 - 1: 2**63 - 1,            # Long.MAX, identity
            -(2**63): -(2**63),              # Long.MIN, identity
            2**63: -(2**63),                 # first overflow → wraps to Long.MIN
            2**63 + 1: -(2**63) + 1,         # one past overflow
            -(2**63) - 1: 2**63 - 1,         # underflow → wraps to Long.MAX
            -(2**63) - 2: 2**63 - 2,
            2**64: 0,                        # full-period wrap
            2**64 + 5: 5,
            10**30: 5076944270305263616,     # very large positive
            -(10**30): -5076944270305263616,
            0xFFFFFFFFFFFFFFFF: -1,          # all-ones mask
            0x8000000000000000: -(2**63),    # exact sign bit
        }
        for n, pinned in expected.items():
            actual = to_signed64(n)
            assert actual == pinned, (
                f"to_signed64 boundary regression at n={n}: "
                f"expected {pinned}, got {actual}"
            )


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

    def test_descending_step(self, spark):
        """Negative step produces a descending sequence.

        ``SequenceColumn`` validator accepts ``step < 0``; pin the
        engine-side behaviour so the allowed schema stays aligned
        with what the engine actually emits.
        """
        df = spark.range(5).select(build_sequential_pk("id", start=100, step=-3).alias("pk"))
        rows = [r.pk for r in df.orderBy("pk", ascending=False).collect()]
        assert rows == [100, 97, 94, 91, 88]


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

    def test_digit_pattern_is_uniform(self, spark):
        """Digits should be uniformly distributed, not Benford-biased.

        Regression for: ``F.substring(F.abs(seed).cast("string"), 1, w)``
        takes *leading* decimal digits of an int64, which Benford-biases
        toward 1 and 2 (1 alone appears ~30% of the time). ``pmod``
        gives trailing uniform bits instead.
        """
        col_seed = derive_column_seed(42, "t", "pk")
        n_rows = 10_000
        df = spark.range(n_rows).select(build_pattern_column("id", col_seed, "{digit:1}").alias("d"))
        values = [r.d for r in df.collect()]

        # No '-' leaked from Long.MIN_VALUE wraparound.
        assert all(
            v.isdigit() for v in values
        ), f"non-digit char in output: {[v for v in values if not v.isdigit()][:5]}"

        # Each digit 0-9 should hit ~10%. Benford would give 1→30%, 9→4.6%.
        # Tolerance is +/-3% (generous ~3 stddev on n=10_000 binomial).
        counts = {d: values.count(d) for d in "0123456789"}
        expected = n_rows / 10
        for d, c in counts.items():
            assert abs(c - expected) < 0.03 * n_rows, f"digit {d} appeared {c} times (expected ~{expected})"

    def test_seq_pattern(self, spark):
        """Pattern '{seq:4}' produces zero-padded sequential IDs."""
        col_seed = derive_column_seed(42, "t", "store_id")
        df = spark.range(5).select(build_pattern_column("id", col_seed, "STORE-{seq:4}").alias("sid"))
        values = [r.sid for r in df.collect()]
        assert values == ["STORE-0001", "STORE-0002", "STORE-0003", "STORE-0004", "STORE-0005"]

    def test_malformed_placeholder_passes_through_as_literal(self, spark):
        """Placeholders with a trailing non-digit character (``{seq:06d}``,
        ``{digit:4a}``, ``{alpha:3x}``, ``{seq:y}``) do not match
        ``_PLACEHOLDER_RE`` and are emitted verbatim into the output
        string.

        Pins the removal of an undocumented trailing ``[a-z]?`` group
        from the placeholder regex that previously silently consumed
        the letter, masquerading as Python-style format-spec support
        (``{seq:06d}`` produced the same output as ``{seq:6}`` because
        the ``d`` was dropped and ``lpad`` always pads with ``"0"``).
        Literal pass-through is the intended failure mode: a malformed
        placeholder appears in the data so the typo is visible, rather
        than being silently re-interpreted.
        """
        col_seed = derive_column_seed(42, "t", "p")
        for template in ("PROD-{seq:06d}", "X-{digit:4a}", "{alpha:3x}", "Y-{seq:y}"):
            df = spark.range(2).select(build_pattern_column("id", col_seed, template).alias("v"))
            values = [r.v for r in df.collect()]
            assert all(v == template for v in values), (
                f"template {template!r} should pass through as literal; got {values}"
            )

    def test_alpha_pattern(self, spark):
        """Pattern '{alpha:3}' produces 3 uppercase letters."""
        col_seed = derive_column_seed(42, "t", "code")
        df = spark.range(100).select(build_pattern_column("id", col_seed, "{alpha:3}").alias("code"))
        values = [r.code for r in df.collect()]
        for v in values:
            assert len(v) == 3, f"Bad length: {v}"
            assert v.isalpha() and v.isupper(), f"Not uppercase alpha: {v}"

    def test_hex_pattern_is_uniform(self, spark):
        """Hex chars should be uniformly distributed across 0-9a-f.

        Regression for: ``F.substring(F.hex(F.abs(seed)), 1, w)`` takes
        the *leading* hex nibble of a positive int64 — whose top bit is
        always clear, so nibbles ``8-f`` never appear as the leading
        hex char of ``abs(seed)``. ``pmod(seed, 16**w)`` gives the
        uniform trailing hex bits.
        """
        col_seed = derive_column_seed(42, "t", "pk")
        n_rows = 10_000
        df = spark.range(n_rows).select(build_pattern_column("id", col_seed, "{hex:1}").alias("h"))
        values = [r.h for r in df.collect()]

        counts = {c: values.count(c) for c in "0123456789abcdef"}
        expected = n_rows / 16
        for c, cnt in counts.items():
            assert abs(cnt - expected) < 0.03 * n_rows, f"hex {c} appeared {cnt} times (expected ~{expected})"

    def test_digit_width_cap(self):
        """{digit:N} with N > 18 is rejected up-front (10**19 overflows int64)."""
        with pytest.raises(ValueError, match=r"digit:N.*width must be <= 18"):
            build_pattern_column("id", 0, "{digit:19}")

    def test_hex_width_cap(self):
        """{hex:N} with N > 15 is rejected up-front (16**16 overflows int64)."""
        with pytest.raises(ValueError, match=r"hex:N.*width must be <= 15"):
            build_pattern_column("id", 0, "{hex:16}")

    def test_alpha_width_cap(self):
        """{alpha:N} with N > 64 is rejected — alpha materialises one
        xxhash64 + substring expression per character, so width
        directly controls per-row Catalyst plan size."""
        with pytest.raises(ValueError, match=r"alpha:N.*width must be <= 64"):
            build_pattern_column("id", 0, "{alpha:65}")

    def test_alpha_at_cap_accepted(self):
        """Boundary: {alpha:64} is exactly at the cap and must pass."""
        # No assertion on value content -- this only pins that the
        # boundary is inclusive (matches the existing digit/hex caps,
        # which reject ``> _MAX``, not ``>= _MAX``).
        build_pattern_column("id", 0, "{alpha:64}")

    def test_seq_width_cap(self):
        """{seq:N} with N > 24 is rejected — F.lpad emits width-char
        strings per row, so unbounded width is a per-row size DoS."""
        with pytest.raises(ValueError, match=r"seq:N.*width must be <= 24"):
            build_pattern_column("id", 0, "{seq:25}")

    def test_seq_at_cap_accepted(self):
        """Boundary: {seq:24} is exactly at the cap and must pass."""
        build_pattern_column("id", 0, "{seq:24}")

    def test_uuid_placeholder_emits_uuid(self, spark):
        """``{uuid}`` produces a 36-char UUID inside a template.

        Regression: the placeholder regex accepted ``uuid`` but the
        dispatcher had no handler, so ``USER-{uuid}-x`` silently
        collapsed to ``USER--x`` (the pos cursor advanced past the
        match but nothing was appended).
        """
        col_seed = derive_column_seed(42, "t", "pk")
        df = spark.range(20).select(build_pattern_column("id", col_seed, "USER-{uuid}-x").alias("v"))
        values = [r.v for r in df.collect()]
        # Template yields 36-char UUID body + 'USER-' (5) + '-x' (2) = 43 chars.
        assert all(len(v) == 43 for v in values), f"got lens {sorted({len(v) for v in values})}"
        assert all(v.startswith("USER-") and v.endswith("-x") for v in values)
        uuid_bodies = [v[5:-2] for v in values]
        assert all(UUID_RE.fullmatch(u) for u in uuid_bodies)
        # Each row's UUID should be distinct (deterministic but unique).
        assert len(set(uuid_bodies)) == len(uuid_bodies)

    def test_multiple_uuids_in_template_are_distinct(self, spark):
        """Two ``{uuid}`` placeholders in the same template must produce
        different UUIDs on the same row; otherwise a naive handler that
        forwarded the same seed twice would emit the same UUID both
        positions (defeating the point of having two placeholders)."""
        col_seed = derive_column_seed(42, "t", "pk")
        df = spark.range(10).select(build_pattern_column("id", col_seed, "{uuid}:{uuid}").alias("v"))
        for row in df.collect():
            left, right = row.v.split(":")
            assert UUID_RE.fullmatch(left) and UUID_RE.fullmatch(right)
            assert left != right, f"two {{uuid}}s collided at row: {row.v}"

    def test_uuid_rejects_width_modifier(self):
        """``{uuid:8}`` is almost certainly a user bug (they want
        ``{hex:8}`` or ``{alpha:8}``).  Reject rather than silently
        produce a full 36-char UUID that ignores the 8."""
        with pytest.raises(ValueError, match=r"\{uuid\} does not accept a width modifier"):
            build_pattern_column("id", 0, "{uuid:8}")


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


# ---------------------------------------------------------------------------
# ExpressionColumn — broader evaluation coverage
# ---------------------------------------------------------------------------


class TestExpressionColumnEvaluation:
    """End-to-end evaluation of non-trivial ``ExpressionColumn`` shapes.

    The existing ``test_expression_column`` only exercises ``a * b``.
    These cases pin three realistic patterns: ``CASE WHEN``, ``cast``,
    and window functions (which depends on the ``_SQL_KEYWORDS``
    window-function fix landing in commit 16e1a43).
    """

    def test_case_when(self, spark):
        spec = TableSpec(
            name="case_when_test",
            rows=200,
            seed=42,
            columns=[
                ColumnSpec(name="a", gen=RangeColumn(min=0, max=100)),
                ColumnSpec(
                    name="band",
                    gen=ExpressionColumn(expr="case when a < 50 then 'low' else 'high' end"),
                ),
            ],
        )
        df = generate_table(spark, spec)
        rows = df.collect()
        assert {r.band for r in rows} == {"low", "high"}, "expected exactly two bands"
        for r in rows:
            assert r.band == ("low" if r.a < 50 else "high"), f"{r.a} band={r.band}"

    def test_cast_promotes_dtype(self, spark):
        """``cast(a as bigint) * 1000000`` materialises as a long column."""
        import pyspark.sql.types as T

        spec = TableSpec(
            name="cast_test",
            rows=50,
            seed=42,
            columns=[
                ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                ColumnSpec(name="big", gen=ExpressionColumn(expr="cast(a as bigint) * 1000000")),
            ],
        )
        df = generate_table(spark, spec)
        big_field = df.schema["big"]
        assert isinstance(big_field.dataType, T.LongType)
        rows = df.collect()
        for r in rows:
            assert r.big == r.a * 1_000_000

    def test_window_function(self, spark):
        """``row_number() over (order by ...)`` is now a valid ExpressionColumn.

        Pins the engine-side counterpart of the plan-time keyword fix
        (commit 16e1a43): if ``_SQL_KEYWORDS`` regresses on window
        tokens, this test fails at plan-resolution time before Spark
        ever runs.

        Note: the window clause must not reference another column being
        defined in the same projection -- Spark rejects lateral column
        alias references inside window expressions
        (UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW).  Using a
        constant ``1`` for the order keeps the expression
        self-contained.
        """
        spec = TableSpec(
            name="window_test",
            rows=100,
            seed=42,
            columns=[
                ColumnSpec(name="bucket", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="rn",
                    gen=ExpressionColumn(expr="row_number() over (order by 1)"),
                ),
            ],
        )
        df = generate_table(spark, spec)
        rows = df.collect()
        assert len(rows) == 100
        # row_number() over (order by 1) emits the trivial 1..N sequence
        assert {r.rn for r in rows} == set(range(1, 101))
