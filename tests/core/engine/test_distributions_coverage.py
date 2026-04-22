"""Tests for dbldatagen.core.engine.distributions — targeting uncovered lines.

Covers: uniform_sample edge cases, weighted_sample_expr with zero weights,
normal_sample_expr, zipf_sample_expr (n<=1, exponent<=1), exponential_sample_expr,
apply_distribution dispatcher for all distribution types, and _array_index helper.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from dbldatagen.core.engine.distributions import (
    _array_index,
    apply_distribution,
    exponential_sample_expr,
    lognormal_sample_expr,
    normal_sample_expr,
    uniform_sample,
    weighted_sample_expr,
    zipf_sample_expr,
)
from dbldatagen.core.spec.schema import (
    Exponential,
    LogNormal,
    Normal,
    Uniform,
    WeightedValues,
    Zipf,
)


# ---------------------------------------------------------------------------
# uniform_sample
# ---------------------------------------------------------------------------


class TestUniformSample:
    def test_n_zero_raises(self, spark):
        """Line 35: n <= 0 raises ValueError."""
        with pytest.raises(ValueError, match="n must be positive"):
            uniform_sample(F.col("id"), 0)

    def test_n_negative_raises(self, spark):
        """Line 35: negative n also raises."""
        with pytest.raises(ValueError, match="n must be positive"):
            uniform_sample(F.col("id"), -5)

    def test_n_one_returns_literal_zero(self, spark):
        """Line 37: n == 1 returns lit(0)."""
        df = spark.range(5).select(uniform_sample(F.col("id"), 1).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(v == 0 for v in values)

    def test_basic_uniform(self, spark):
        """Line 38: general case returns values in [0, n)."""
        n = 10
        df = spark.range(100).select(uniform_sample(F.col("id"), n).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)


# ---------------------------------------------------------------------------
# weighted_sample_expr
# ---------------------------------------------------------------------------


class TestWeightedSampleExpr:
    def test_zero_weights_falls_back_to_array_index(self, spark):
        """Line 55: total weight <= 0 falls back to _array_index."""
        values = ["a", "b", "c"]
        weights = {"a": 0.0, "b": 0.0, "c": 0.0}
        df = spark.range(20).select(weighted_sample_expr(F.col("id"), values, weights).alias("val"))
        results = [row.val for row in df.collect()]
        assert all(v in values for v in results)

    def test_normal_weighted_selection(self, spark):
        """Weighted selection returns only values with positive weight."""
        values = ["x", "y", "z"]
        weights = {"x": 1.0, "y": 0.0, "z": 0.0}
        df = spark.range(50).select(weighted_sample_expr(F.col("id"), values, weights).alias("val"))
        results = [row.val for row in df.collect()]
        # All results should be "x" since only "x" has weight
        assert all(v == "x" for v in results)


# ---------------------------------------------------------------------------
# normal_sample_expr
# ---------------------------------------------------------------------------


class TestNormalSampleExpr:
    def test_returns_values(self, spark):
        """Lines 94-103: normal sampling produces finite values around the mean."""
        df = spark.range(200).select(normal_sample_expr(F.col("id"), mean=50.0, stddev=10.0).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(v is not None for v in values)
        # Values should be roughly centred around 50
        avg = sum(values) / len(values)
        assert 20.0 < avg < 80.0


# ---------------------------------------------------------------------------
# zipf_sample_expr
# ---------------------------------------------------------------------------


class TestZipfSampleExpr:
    def test_n_one_returns_zero(self, spark):
        """Line 114: n <= 1 returns lit(0)."""
        df = spark.range(5).select(zipf_sample_expr(F.col("id"), n=1).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(v == 0 for v in values)

    def test_exponent_lte_one(self, spark):
        """Line 120: exponent <= 1.0 uses log-based fallback."""
        n = 20
        df = spark.range(100).select(zipf_sample_expr(F.col("id"), n=n, exponent=0.8).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_default_exponent(self, spark):
        """Standard zipf with default exponent produces valid indices."""
        n = 50
        df = spark.range(100).select(zipf_sample_expr(F.col("id"), n=n, exponent=1.5).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)


# ---------------------------------------------------------------------------
# exponential_sample_expr
# ---------------------------------------------------------------------------


class TestExponentialSampleExpr:
    def test_n_one_returns_zero(self, spark):
        """Lines 138-139: n <= 1 returns lit(0)."""
        df = spark.range(5).select(exponential_sample_expr(F.col("id"), n=1).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(v == 0 for v in values)

    def test_n_zero_returns_zero(self, spark):
        """n=0 also returns lit(0)."""
        df = spark.range(3).select(exponential_sample_expr(F.col("id"), n=0).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(v == 0 for v in values)

    def test_basic_exponential(self, spark):
        """Lines 140-145: general case returns values in [0, n)."""
        n = 30
        df = spark.range(200).select(exponential_sample_expr(F.col("id"), n=n, rate=1.0).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)


# ---------------------------------------------------------------------------
# apply_distribution dispatcher
# ---------------------------------------------------------------------------


class TestApplyDistribution:
    def test_none_distribution(self, spark):
        """Line 163: None -> uniform."""
        n = 10
        df = spark.range(50).select(apply_distribution(F.col("id"), n, None).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_uniform_distribution(self, spark):
        """Line 163: Uniform -> uniform."""
        n = 10
        df = spark.range(50).select(apply_distribution(F.col("id"), n, Uniform()).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_zipf_distribution(self, spark):
        """Line 165: Zipf dispatches to zipf_sample_expr."""
        n = 20
        df = spark.range(50).select(apply_distribution(F.col("id"), n, Zipf(exponent=1.5)).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_exponential_distribution(self, spark):
        """Lines 166-167: Exponential dispatches to exponential_sample_expr."""
        n = 20
        df = spark.range(50).select(apply_distribution(F.col("id"), n, Exponential(rate=2.0)).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_normal_distribution(self, spark):
        """Lines 168-172: Normal mapped to [0, n)."""
        n = 100
        df = spark.range(200).select(apply_distribution(F.col("id"), n, Normal()).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_lognormal_distribution(self, spark):
        """LogNormal dispatches to lognormal_sample_expr."""
        n = 20
        df = spark.range(50).select(apply_distribution(F.col("id"), n, LogNormal(stddev=1.5)).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_lognormal_is_not_exponential(self, spark):
        """Regression: LogNormal used to dispatch to exponential_sample_expr.

        With identical params the two distributions must produce observably
        different samples — otherwise the public LogNormal type in the
        pydantic Distribution union is silently a second Exponential.
        """
        n = 50
        seed = F.xxhash64(F.col("id"))
        df = spark.range(500).select(
            apply_distribution(seed, n, LogNormal(mean=0.0, stddev=1.0)).alias("ln"),
            apply_distribution(seed, n, Exponential(rate=1.0)).alias("ex"),
        )
        rows = df.collect()
        ln_values = [r.ln for r in rows]
        ex_values = [r.ex for r in rows]
        # Disagreement fraction: identical samplers would give 0%.
        mismatches = sum(1 for a, b in zip(ln_values, ex_values) if a != b)
        assert mismatches / len(rows) > 0.5, (
            f"LogNormal and Exponential agreed on {len(rows) - mismatches}/{len(rows)} draws; "
            "LogNormal is likely still dispatching to the exponential sampler."
        )

    def test_weighted_values_distribution(self, spark):
        """Lines 176-178: WeightedValues falls back to uniform."""
        n = 10
        df = spark.range(50).select(
            apply_distribution(F.col("id"), n, WeightedValues(weights={"a": 1.0, "b": 2.0})).alias("val")
        )
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)


# ---------------------------------------------------------------------------
# lognormal_sample_expr
# ---------------------------------------------------------------------------


class TestLogNormalSampleExpr:
    def test_n_one_returns_zero(self, spark):
        df = spark.range(5).select(lognormal_sample_expr(F.col("id"), n=1).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(v == 0 for v in values)

    def test_basic_lognormal(self, spark):
        n = 50
        seed = F.xxhash64(F.col("id"))
        df = spark.range(300).select(lognormal_sample_expr(seed, n=n).alias("val"))
        values = [row.val for row in df.collect()]
        assert all(0 <= v < n for v in values)

    def test_lognormal_right_skewed(self, spark):
        """Log-normal is right-skewed: the lower half of [0, n) holds most mass."""
        n = 100
        seed = F.xxhash64(F.col("id"))
        df = spark.range(1000).select(lognormal_sample_expr(seed, n=n).alias("val"))
        values = [row.val for row in df.collect()]
        lower_half = sum(1 for v in values if v < n // 2)
        assert (
            lower_half / len(values) > 0.6
        ), f"Expected >60% of lognormal draws in the lower half, got {lower_half}/{len(values)}"


# ---------------------------------------------------------------------------
# Exponential shape regression (no modulo wraparound)
# ---------------------------------------------------------------------------


class TestExponentialShape:
    def test_exponential_no_tail_wraparound(self, spark):
        """Regression: the sampler used to return F.abs(idx) % n, wrapping
        tail draws back into early bins. With proper clipping, the last
        bin absorbs the entire right tail (P(x >= threshold)) and is
        therefore heavier than the adjacent bin. With wraparound the tail
        mass is sprayed into early bins instead, so the last bin only
        holds its thin natural slice and is lighter than its neighbours.
        """
        n = 20
        seed = F.xxhash64(F.col("id"))
        df = spark.range(10000).select(exponential_sample_expr(seed, n=n, rate=1.0).alias("val"))
        values = [row.val for row in df.collect()]
        counts = [0] * n
        for v in values:
            counts[v] += 1
        # Bin 0 should be the mode either way
        assert counts[0] == max(counts), f"bin 0 is not the mode: counts={counts}"
        # Under clipping the last bin ≈ 4x its wrapped counterpart
        assert counts[-1] > counts[-2], (
            f"Last bin count ({counts[-1]}) did not exceed bin n-2 ({counts[-2]}); "
            f"expected the last bin to absorb tail mass under clipping. "
            f"Likely a modulo-wraparound regression. Full histogram: {counts}"
        )


# ---------------------------------------------------------------------------
# _array_index helper
# ---------------------------------------------------------------------------


class TestArrayIndex:
    def test_picks_from_values(self, spark):
        """Lines 190-192: _array_index selects elements from list."""
        values = ["alpha", "beta", "gamma"]
        df = spark.range(30).select(_array_index(F.col("id"), values).alias("val"))
        results = [row.val for row in df.collect()]
        assert all(v in values for v in results)
        # With 30 rows and 3 values, expect at least 2 distinct values
        assert len(set(results)) >= 2
