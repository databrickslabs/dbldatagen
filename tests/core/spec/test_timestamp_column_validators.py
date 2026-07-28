"""Adversarial-input tests for ``TimestampColumn`` validators."""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import TimestampColumn


@pytest.mark.parametrize(
    "start, end, err_substring",
    [
        # Bad ISO format
        ("not-a-date", "2024-12-31", "not a valid ISO timestamp"),
        ("2024-01-01", "garbage", "not a valid ISO timestamp"),
        # start > end (pre-existing check; included in matrix)
        ("2024-12-31", "2024-01-01", "must be <= end"),
        # Mixed tz-awareness -- previously raised an uncaught TypeError
        # from the start > end comparison; now caught explicitly with a
        # ValueError naming both bounds.
        ("2024-01-01", "2024-01-01T00:00:00+00:00", "differ in tz-awareness"),
        ("2024-01-01T00:00:00+00:00", "2024-12-31", "differ in tz-awareness"),
        ("2024-01-01T00:00:00", "2024-01-01T00:00:00+05:30", "differ in tz-awareness"),
    ],
)
def test_timestamp_column_rejects(start, end, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        TimestampColumn(start=start, end=end)


def test_timestamp_column_accepts_both_naive():
    ts = TimestampColumn(start="2024-01-01", end="2024-12-31")
    assert ts.start == "2024-01-01"


def test_timestamp_column_accepts_both_aware():
    ts = TimestampColumn(
        start="2024-01-01T00:00:00+00:00",
        end="2024-12-31T23:59:59+00:00",
    )
    assert ts.start.endswith("+00:00")
