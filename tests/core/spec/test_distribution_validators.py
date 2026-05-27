"""Adversarial-input tests for ``Distribution`` discriminated-union members.

NaN / Inf comparisons short-circuit to False, so ``stddev < 0`` /
``rate <= 0`` / ``exponent <= 1`` accept NaN.  The engine then ships
``F.lit(nan)`` into the Spark plan and every sampled row materialises
as NaN.  These tests pin the schema-side rejection.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import (
    Exponential,
    LogNormal,
    Normal,
    WeightedValues,
    Zipf,
)


@pytest.mark.parametrize(
    "kwargs, err_substring",
    [
        ({"mean": float("nan")}, "is not finite"),
        ({"stddev": float("nan")}, "is not finite"),
        ({"mean": float("inf")}, "is not finite"),
        ({"stddev": -1.0}, "stddev must be >= 0"),
    ],
)
def test_normal_rejects(kwargs, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        Normal(**kwargs)


@pytest.mark.parametrize(
    "kwargs, err_substring",
    [
        ({"mean": float("nan")}, "is not finite"),
        ({"stddev": float("nan")}, "is not finite"),
        ({"mean": float("inf")}, "is not finite"),
        ({"mean": 200.0}, "must be in"),
    ],
)
def test_lognormal_rejects(kwargs, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        LogNormal(**kwargs)


@pytest.mark.parametrize(
    "kwargs, err_substring",
    [
        ({"exponent": float("nan")}, "is not finite"),
        ({"exponent": float("inf")}, "is not finite"),
        ({"exponent": 1.0}, "must be > 1"),
        ({"exponent": 0.5}, "must be > 1"),
    ],
)
def test_zipf_rejects(kwargs, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        Zipf(**kwargs)


@pytest.mark.parametrize(
    "kwargs, err_substring",
    [
        ({"rate": float("nan")}, "is not finite"),
        ({"rate": float("inf")}, "is not finite"),
        ({"rate": 0.0}, "must be > 0"),
        ({"rate": -1.0}, "must be > 0"),
    ],
)
def test_exponential_rejects(kwargs, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        Exponential(**kwargs)


@pytest.mark.parametrize(
    "weights, err_substring",
    [
        ({}, "must not be empty"),
        ({"a": float("nan")}, "is not finite"),
        ({"a": float("inf"), "b": 1.0}, "is not finite"),
        ({"a": -1.0}, "non-negative"),
    ],
)
def test_weighted_values_rejects(weights, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        WeightedValues(weights=weights)


def test_distributions_accept_normal_values():
    """Happy-path smoke -- the new NaN/Inf gate doesn't reject
    legitimate finite values."""
    Normal(mean=0.0, stddev=1.0)
    LogNormal(mean=0.0, stddev=1.0)
    Zipf(exponent=1.5)
    Exponential(rate=1.0)
    WeightedValues(weights={"a": 1.0, "b": 2.0, "c": 0.0})
