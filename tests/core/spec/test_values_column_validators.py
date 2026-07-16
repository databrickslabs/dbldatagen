"""Adversarial-input tests for ``ValuesColumn`` validators."""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import ValuesColumn


@pytest.mark.parametrize(
    "bad_values, err_substring",
    [
        ([], "must not be empty"),
        # Hashable duplicates — the silent-probability-mass shape.
        (["a", "a", "b"], "duplicate"),
        ([1, 2, 1], "duplicate"),
        ([True, True], "duplicate"),
        # Mixed-type duplicates after ``str()`` -- the engine keys
        # WeightedValues by ``str(v)``, so ``1`` and ``"1"`` collide
        # downstream.  Our duplicate check uses ``set()`` first
        # (hashable distinct) so ``[1, "1"]`` passes hash-distinct but
        # would still produce keying collisions later -- not flagged
        # here today; documented as a known limitation in the test
        # below.
    ],
)
def test_values_column_rejects(bad_values, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        ValuesColumn(values=bad_values)


def test_values_column_unhashable_duplicates():
    """Unhashable duplicates (lists/dicts) are caught via ``repr()``
    comparison rather than ``set()``.  Two identical dict literals
    in a YAML plan land as two equal dicts in ``values``; the user
    almost certainly meant one of them, not double-weight.
    """
    with pytest.raises(ValueError, match="duplicate"):
        ValuesColumn(values=[{"a": 1}, {"a": 1}])


def test_values_column_accepts_distinct():
    """Happy path: distinct values pass."""
    vc = ValuesColumn(values=["a", "b", "c"])
    assert len(vc.values) == 3
