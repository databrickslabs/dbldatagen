"""Unit tests for dbldatagen.core.engine.seed primitives.

Pure-Python boundary tests — no Spark session needed.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.engine.seed import (
    compute_batch_seed,
    derive_column_seed,
    to_signed64,
)


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


class TestComputeBatchSeed:
    """compute_batch_seed wraps through to_signed64 for non-zero batch_id."""

    def test_batch_zero_returns_global(self):
        assert compute_batch_seed(12345, 0) == 12345

    def test_result_in_signed64_range_at_large_batch(self):
        # A batch_id large enough to push global_seed past Long.MAX.
        r = compute_batch_seed(LONG_MAX - 100, 1_000_000)
        assert LONG_MIN <= r <= LONG_MAX

    def test_determinism(self):
        assert compute_batch_seed(7, 3) == compute_batch_seed(7, 3)
