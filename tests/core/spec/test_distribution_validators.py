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
    ForeignKeyRef,
    LogNormal,
    Normal,
    RangeColumn,
    TimestampColumn,
    ValuesColumn,
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


# ---------------------------------------------------------------------------
# Normal mean/stddev are value-space: honored on numeric RangeColumn,
# rejected on hosts with no usable float value space (timestamps, FK,
# value lists).  Bare ``Normal()`` (auto-center) is always allowed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "host_factory, host_name",
    [
        (lambda dist: TimestampColumn(start="2023-01-01", end="2023-12-31", distribution=dist), "TimestampColumn"),
        (lambda dist: ForeignKeyRef(ref="parent.id", distribution=dist), "ForeignKeyRef"),
        (lambda dist: ValuesColumn(values=["a", "b", "c"], distribution=dist), "ValuesColumn"),
    ],
)
@pytest.mark.parametrize(
    "dist",
    [Normal(mean=5.0), Normal(stddev=2.0), Normal(mean=5.0, stddev=2.0)],
)
def test_parametrized_normal_rejected_on_non_numeric_hosts(host_factory, host_name, dist):
    with pytest.raises(ValueError, match="does not support Normal with explicit mean/stddev"):
        host_factory(dist)


@pytest.mark.parametrize(
    "host_factory",
    [
        lambda: TimestampColumn(start="2023-01-01", end="2023-12-31", distribution=Normal()),
        lambda: ForeignKeyRef(ref="parent.id", distribution=Normal()),
        lambda: ValuesColumn(values=["a", "b", "c"], distribution=Normal()),
    ],
)
def test_bare_normal_allowed_on_non_numeric_hosts(host_factory):
    # Auto-centered Normal() carries no value-space params, so it is fine.
    host_factory()


@pytest.mark.parametrize(
    "dist",
    [Normal(), Normal(mean=40.0), Normal(stddev=12.0), Normal(mean=40.0, stddev=12.0)],
)
def test_numeric_range_accepts_normal_with_or_without_params(dist):
    # RangeColumn honors value-space mean/stddev, so every form is accepted.
    RangeColumn(min=0, max=100, distribution=dist)
