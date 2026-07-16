"""Date and timestamp column generation.

Maps a deterministic per-row seed into an epoch range and casts the result to a
Spark date or timestamp. Output is independent of the session time zone.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.distributions import apply_distribution
from dbldatagen.core.engine.seed import cell_seed_expr
from dbldatagen.core.spec.schema import Distribution


def _parse_epoch(datetime_str: str) -> int:
    """Parses a datetime string to UTC epoch seconds.

    Accepts any string `datetime.fromisoformat` accepts. Naive datetimes are
    treated as UTC; tz-aware datetimes resolve to the correct UTC epoch.

    Args:
        datetime_str: An ISO-8601 date or datetime string.

    Returns:
        The UTC epoch in seconds.

    Raises:
        ValueError: If `datetime_str` is not a valid ISO-8601 datetime.
    """
    try:
        parsed = datetime.fromisoformat(datetime_str)
    except ValueError as exc:
        raise ValueError(f"Cannot parse datetime string: {datetime_str!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def build_timestamp_column(
    id_column: Column | str,
    column_seed: int,
    start: str,
    end: str,
    distribution: Distribution | None = None,
) -> Column:
    """Builds a timestamp column with values sampled in the inclusive range [start, end].

    Both bounds are inclusive, matching `RangeColumn`; `end` is a reachable value.
    Output is independent of the session time zone (values are sampled in UTC
    epoch seconds).

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        column_seed: Per-column seed.
        start: Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
            "YYYY-MM-DD HH:MM:SS").
        end: Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
            "YYYY-MM-DD HH:MM:SS").
        distribution: Optional sampling distribution over the range
            (default None, meaning uniform).

    Returns:
        A Spark timestamp `Column` of sampled values.

    Raises:
        ValueError: If `start` or `end` is not a valid ISO-8601 datetime.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)

    start_epoch = _parse_epoch(start)
    end_epoch = _parse_epoch(end)
    if end_epoch <= start_epoch:
        return F.lit(start_epoch).cast("long").cast("timestamp")

    # +1 second so end is inclusive: sample over [0, end - start].
    second_count = end_epoch - start_epoch + 1
    seed_col = cell_seed_expr(column_seed, id_column)
    index = apply_distribution(seed_col, second_count, distribution)
    epoch_col = (index + F.lit(start_epoch)).cast("long")
    return epoch_col.cast("timestamp")


def build_date_column(
    id_column: Column | str,
    column_seed: int,
    start: str,
    end: str,
    distribution: Distribution | None = None,
) -> Column:
    """Builds a date column with values sampled in the inclusive range [start, end].

    Both calendar dates are inclusive, matching `RangeColumn`; the `end` date is a
    reachable value. Output is independent of the session time zone (dates are
    computed from days since the UTC epoch).

    Args:
        id_column: Row-id column, given as a `Column` or column name.
        column_seed: Per-column seed.
        start: Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
            "YYYY-MM-DD HH:MM:SS"); any time component is truncated to the date.
        end: Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
            "YYYY-MM-DD HH:MM:SS"); any time component is truncated to the date.
        distribution: Optional sampling distribution over the range
            (default None, meaning uniform).

    Returns:
        A Spark date `Column` of sampled values.

    Raises:
        ValueError: If `start` or `end` is not a valid ISO-8601 datetime.
    """
    if isinstance(id_column, str):
        id_column = F.col(id_column)

    start_days = _parse_epoch(start) // 86400
    end_days = _parse_epoch(end) // 86400
    if end_days <= start_days:
        return _days_to_date(start_days)

    # +1 day so the end date is inclusive: sample over [0, end_days - start_days].
    day_count = end_days - start_days + 1
    seed_col = cell_seed_expr(column_seed, id_column)
    index = apply_distribution(seed_col, day_count, distribution)
    return _days_to_date(index + F.lit(start_days).cast("long"))


def _days_to_date(days: Column | int) -> Column:
    """Converts days-since-epoch to a Spark date column.

    Args:
        days: Number of days since 1970-01-01, as a `Column` or int.

    Returns:
        A Spark date `Column`.
    """
    epoch_date = F.lit("1970-01-01").cast("date")
    days_col = F.lit(days).cast("int") if isinstance(days, int) else days.cast("int")
    return F.date_add(epoch_date, days_col)
