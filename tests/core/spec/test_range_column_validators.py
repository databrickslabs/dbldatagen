"""Adversarial-input tests for ``RangeColumn`` validators.

Per the ``dbldatagen-engine`` A1 convention: every touched Pydantic
model in ``core/spec/`` gets one of these test files asserting the
validator's *rejection set*.  The happy-path tests live elsewhere;
these pin the bad inputs that must raise.
"""

from __future__ import annotations

import math

import pytest

from dbldatagen.core.spec.schema import RangeColumn


@pytest.mark.parametrize(
    "bad_kwargs, err_substring",
    [
        # NaN/Inf — silent-bypass shape: NaN comparisons return False so
        # ``min > max`` and ``step <= 0`` checks miss.
        ({"min": float("nan"), "max": 10}, "is not finite"),
        ({"min": 0, "max": float("nan")}, "is not finite"),
        ({"min": 0, "max": 10, "step": float("nan")}, "is not finite"),
        ({"min": float("inf"), "max": 10}, "is not finite"),
        ({"min": 0, "max": float("inf")}, "is not finite"),
        ({"min": 0, "max": 10, "step": float("-inf")}, "is not finite"),
        # min > max — pre-existing check, kept here so the matrix is
        # complete and a future regression on either side breaks loudly.
        ({"min": 100, "max": 0}, "must be <= max"),
        # step <= 0
        ({"min": 0, "max": 10, "step": 0}, "step must be > 0"),
        ({"min": 0, "max": 10, "step": -1}, "step must be > 0"),
        # Integer-range overflow — also pre-existing; included to lock
        # the contract surface.
        ({"min": -(2**63), "max": 2**63 - 1}, "overflows"),
    ],
)
def test_range_column_rejects(bad_kwargs, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        RangeColumn(**bad_kwargs)


def test_range_column_accepts_finite_floats():
    """Happy-path smoke: finite floats including 0.0, negatives, and
    bounds at the magnitude that would have been masked by a permissive
    NaN check are still accepted."""
    rc = RangeColumn(min=-1.5, max=1.5, step=0.1)
    assert math.isfinite(rc.min) and math.isfinite(rc.max)
