---
sidebar_label: temporal
title: dbldatagen.core.engine.columns.temporal
---

Date and timestamp column generation.

Maps a deterministic per-row seed into an epoch range and casts the result to a
Spark date or timestamp. Output is independent of the session time zone.

### build\_timestamp\_column

```python
def build_timestamp_column(id_column: Column | str,
                           column_seed: int,
                           start: str,
                           end: str,
                           distribution: Distribution | None = None) -> Column
```

Builds a timestamp column with values sampled in the inclusive range [start, end].

Both bounds are inclusive, matching `RangeColumn`; `end` is a reachable value.
Output is independent of the session time zone (values are sampled in UTC
epoch seconds).

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `column_seed` - Per-column seed.
- `start` - Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS").
- `end` - Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS").
- `distribution` - Optional sampling distribution over the range
  (default None, meaning uniform).
  

**Returns**:

  A Spark timestamp `Column` of sampled values.
  

**Raises**:

- `ValueError` - If `start` or `end` is not a valid ISO-8601 datetime.

### build\_date\_column

```python
def build_date_column(id_column: Column | str,
                      column_seed: int,
                      start: str,
                      end: str,
                      distribution: Distribution | None = None) -> Column
```

Builds a date column with values sampled in the inclusive range [start, end].

Both calendar dates are inclusive, matching `RangeColumn`; the `end` date is a
reachable value. Output is independent of the session time zone (dates are
computed from days since the UTC epoch).

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `column_seed` - Per-column seed.
- `start` - Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS"); any time component is truncated to the date.
- `end` - Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS"); any time component is truncated to the date.
- `distribution` - Optional sampling distribution over the range
  (default None, meaning uniform).
  

**Returns**:

  A Spark date `Column` of sampled values.
  

**Raises**:

- `ValueError` - If `start` or `end` is not a valid ISO-8601 datetime.

