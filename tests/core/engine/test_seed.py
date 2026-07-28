"""Unit tests for dbldatagen.core.engine.seed primitives.

Pure-Python boundary tests — no Spark session needed.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.engine.seed import derive_column_seed, null_mask_expr, seed_xor, to_signed64


LONG_MAX = 2**63 - 1
LONG_MIN = -(2**63)
LONG_MASK = 0xFFFFFFFFFFFFFFFF


class TestToSigned64:
    """to_signed64 is documented to accept arbitrary Python ints and return a
    signed 64-bit int.  These tests pin the wrapping behavior at boundaries."""

    @pytest.mark.parametrize(
        "n, expected",
        [
            (0, 0),
            (1, 1),
            (-1, -1),
            (LONG_MAX, LONG_MAX),
            (LONG_MIN, LONG_MIN),
            # Positive overflow: 2**63 wraps to Long.MIN.
            (2**63, LONG_MIN),
            (2**63 + 1, LONG_MIN + 1),
            # Full 2**64 wraps back to 0.
            (2**64, 0),
            (2**64 + 5, 5),
            # Very large positive: masked to low 64 bits, then signed.
            (LONG_MASK, -1),
            # Negative beyond Long.MIN: wraps via two's-complement arithmetic.
            (LONG_MIN - 1, LONG_MAX),
            (-(2**64), 0),
        ],
    )
    def test_boundary_values(self, n, expected):
        assert to_signed64(n) == expected

    def test_result_always_in_signed64_range(self):
        """Fuzz a few odd inputs and verify the result fits in signed 64 bits."""
        for n in [0, 1, -1, 2**100, -(2**100), 2**63, LONG_MIN - 7, LONG_MAX + 9]:
            r = to_signed64(n)
            assert LONG_MIN <= r <= LONG_MAX, f"to_signed64({n}) = {r} out of range"

    def test_idempotent_on_in_range_ints(self):
        """Values already in signed-64 range are returned unchanged."""
        for n in [0, 1, -1, 42, -42, LONG_MAX, LONG_MIN, LONG_MAX - 1, LONG_MIN + 1]:
            assert to_signed64(n) == n

    def test_one_below_long_min_wraps_to_long_max(self):
        """-(2**63) - 1 is one below signed-64 min; two's-complement wraps to LONG_MAX.

        Same value as ``LONG_MIN - 1`` in the parametrized suite; the literal
        spelling is preserved here to match how the boundary is commonly
        reasoned about ("one past the negative end").
        """
        assert to_signed64(-(2**63) - 1) == LONG_MAX
        assert to_signed64(LONG_MIN - 1) == LONG_MAX


class TestDeriveColumnSeed:
    """derive_column_seed composes to_signed64 — verify the output is in range
    and deterministic."""

    def test_result_in_signed64_range(self):
        r = derive_column_seed(42, "orders", "customer_id")
        assert LONG_MIN <= r <= LONG_MAX

    def test_determinism(self):
        assert derive_column_seed(42, "t", "c") == derive_column_seed(42, "t", "c")

    def test_different_inputs_differ(self):
        a = derive_column_seed(42, "t", "c1")
        b = derive_column_seed(42, "t", "c2")
        assert a != b


class TestSeedXor:
    """seed_xor derives a decorrelated sub-seed by XORing a column seed with a
    constant, then clamping into signed-64 range."""

    @pytest.mark.parametrize(
        "column_seed, constant",
        [
            (0, 0),
            (42, 0xDEADBEEF),
            (-1, 0x9E3779B9),
            (LONG_MAX, 0xDEADBEEF),
            (LONG_MIN, 0xDEADBEEF),
        ],
    )
    def test_matches_to_signed64_of_xor(self, column_seed, constant):
        """seed_xor is defined as to_signed64(column_seed ^ constant)."""
        assert seed_xor(column_seed, constant) == to_signed64(column_seed ^ constant)

    def test_xor_with_zero_is_identity_clamp(self):
        """XOR with 0 leaves the bits unchanged, returning to_signed64(column_seed)."""
        for column_seed in [0, 1, -1, 42, LONG_MAX, LONG_MIN]:
            assert seed_xor(column_seed, 0) == to_signed64(column_seed)

    def test_result_always_in_signed64_range(self):
        """The XOR of a signed-64 seed with a large constant can exceed signed-64;
        seed_xor must clamp it back so F.lit accepts the result."""
        for column_seed in [LONG_MIN, LONG_MAX, -1, 0, 7]:
            for constant in [0, 0xDEADBEEF, 0x9E3779B9, 10**18, (2**32) * 0x9E3779B9]:
                r = seed_xor(column_seed, constant)
                assert LONG_MIN <= r <= LONG_MAX, f"seed_xor({column_seed}, {constant}) = {r} out of range"

    def test_distinct_constants_decorrelate(self):
        """A fixed seed XORed with distinct constants yields distinct sub-seeds."""
        column_seed = derive_column_seed(42, "orders", "amount")
        sub_seeds = {seed_xor(column_seed, c) for c in (0xDEADBEEF, 0x9E3779B9, 2 * 0x9E3779B9, 3 * 0x9E3779B9)}
        assert len(sub_seeds) == 4

    def test_determinism(self):
        assert seed_xor(123, 0xDEADBEEF) == seed_xor(123, 0xDEADBEEF)


class TestNullMaskBoundaries:
    """``null_mask_expr`` has two short-circuit boundaries worth pinning:
    ``f <= 0`` (no nulls) and ``f >= 1`` (all nulls).  In between, a row is
    NULL when its uniform draw falls below ``f``."""

    def test_zero_returns_false_literal(self, spark):
        from pyspark.sql import functions as F

        mask = null_mask_expr(42, F.col("id"), 0.0)
        df = spark.range(5).select(mask.alias("m"))
        assert all(not r.m for r in df.collect())

    def test_one_returns_true_literal(self, spark):
        """``null_fraction >= 1`` short-circuits to ``F.lit(True)``.

        Introspect the Column's SQL to confirm the short-circuit fired: the
        full hash path contains ``pmod`` / ``xxhash64``, a literal-True path
        doesn't.
        """
        from pyspark.sql import functions as F

        mask = null_mask_expr(42, F.col("id"), 1.0)
        df = spark.range(5).select(mask.alias("m"))
        assert all(r.m for r in df.collect())
        # The short-circuit path returns ``F.lit(True)`` directly; its
        # SQL doesn't reference ``pmod`` or ``xxhash64``.  Without the
        # short-circuit, the fallback path would emit those functions.
        sql = str(mask)
        assert "pmod" not in sql.lower(), f"expected short-circuit lit; got {sql}"
        assert "xxhash64" not in sql.lower(), f"expected short-circuit lit; got {sql}"

    def test_tiny_fraction_accepted(self, spark):
        """A very small fraction is accepted without error and yields a valid
        boolean mask -- the uniform comparison has no granularity floor."""
        from pyspark.sql import functions as F

        mask = null_mask_expr(42, F.col("id"), 1e-6)
        df = spark.range(100).select(mask.alias("m"))
        assert all(isinstance(r.m, bool) for r in df.collect())

    def test_99999_fraction_produces_near_all_nulls(self, spark):
        """At 0.9999, nearly every row's uniform draw falls below the
        fraction, so almost all rows are NULL.  Statistical check."""
        from pyspark.sql import functions as F

        mask = null_mask_expr(42, F.col("id"), 0.9999)
        n = 10_000
        count_null = spark.range(n).select(mask.alias("m")).filter(F.col("m")).count()
        # Expect ~99.99% True; allow slack for hash distribution.
        assert count_null >= int(n * 0.99)
