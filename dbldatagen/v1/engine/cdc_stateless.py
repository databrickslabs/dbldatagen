"""Stateless CDC lifecycle engine using Modular Recurrence.

Replaces the stateful ``TableState`` class with pure mathematical functions
that derive row lifecycle from the row's private index ``k`` and batch
number ``t``.  Zero driver-side state -- all computation is O(1) per row.

The "Three Clocks" model:
    birth_tick(k)  -- batch when row k is created (0 for initial rows)
    death_tick(k)  -- batch when row k is deleted (inf if no deletes)
    update_due(k, t) -- whether row k should be updated at batch t

See design/cdc_design_v4.md for the full specification.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.v1.engine.utils import split_with_remainder


# ---------------------------------------------------------------------------
# Period computation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CDCPeriods:
    """Pre-computed CDC timing parameters for a table.

    Attributes:
        inserts_per_batch: number of new rows per batch
        deletes_per_batch: number of deaths per batch (0 if no deletes)
        updates_per_batch: number of updates per batch (0 if no updates)
        death_period: stride between deaths in k-space (inf if no deletes)
        update_period: batches between updates for a given row (inf if no updates)
    """

    inserts_per_batch: int
    deletes_per_batch: int
    updates_per_batch: int
    death_period: int | float
    update_period: int | float


def compute_periods(
    initial_rows: int,
    batch_size: int,
    insert_weight: float,
    update_weight: float,
    delete_weight: float,
    *,
    min_life: int = 1,
) -> CDCPeriods:
    """Compute CDC timing periods from configuration.

    Parameters
    ----------
    initial_rows : int
        Number of rows in the initial snapshot (batch 0).
    batch_size : int
        Total operations per batch.
    insert_weight, update_weight, delete_weight : float
        Relative operation weights (will be normalised).
    min_life : int
        Minimum number of batches a row must live before dying.

    Returns
    -------
    CDCPeriods
        Pre-computed timing parameters.
    """
    total_weight = insert_weight + update_weight + delete_weight
    if total_weight <= 0:
        raise ValueError("At least one operation weight must be positive")

    inserts_per_batch, updates_per_batch, deletes_per_batch = split_with_remainder(
        batch_size,
        (insert_weight, update_weight, delete_weight),
    )

    # death_period: how many k-values apart deaths are spaced
    # If we delete D rows per batch, and the universe grows, we stride
    # through k-space with period = ceil(initial_rows / D).
    # Using initial_rows as the base for period calculation keeps the
    # stride constant across batches.
    death_period: int | float
    if deletes_per_batch > 0:
        death_period = max(1, initial_rows // deletes_per_batch)
    else:
        death_period = math.inf

    # update_period: how many batches between updates for a given row
    update_period: int | float
    if updates_per_batch > 0 and initial_rows > 0:
        update_period = max(1, initial_rows // updates_per_batch)
    else:
        update_period = math.inf

    return CDCPeriods(
        inserts_per_batch=inserts_per_batch,
        deletes_per_batch=deletes_per_batch,
        updates_per_batch=updates_per_batch,
        death_period=death_period,
        update_period=update_period,
    )


# ---------------------------------------------------------------------------
# Birth / Death / Update tick functions (pure Python, O(1) per row)
# ---------------------------------------------------------------------------


def birth_tick(k: int, initial_rows: int, inserts_per_batch: int) -> int:
    """Return the batch number when row *k* is created.

    - k < initial_rows -> batch 0 (initial snapshot)
    - k >= initial_rows -> computed from insert rate
    """
    if k < initial_rows:
        return 0
    if inserts_per_batch <= 0:
        raise ValueError(f"Row k={k} >= initial_rows={initial_rows} but " f"inserts_per_batch={inserts_per_batch} <= 0")
    return ((k - initial_rows) // inserts_per_batch) + 1


def death_tick(
    k: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    min_life: int = 1,
) -> int | float:
    """Return the batch number when row *k* dies.

    Returns ``math.inf`` if the death_period is infinite (no deletes).

    The formula spaces deaths using modular arithmetic on k:
        T_death(k) = T_birth(k) + min_life + (k % death_period)

    This ensures:
    - Every row lives at least ``min_life`` batches
    - Deaths are spread deterministically across batches
    - The mapping is O(1) per row
    """
    if math.isinf(death_period):
        return math.inf
    dp = int(death_period)
    if dp <= 0:
        return math.inf
    t_birth = birth_tick(k, initial_rows, inserts_per_batch)
    return t_birth + min_life + (k % dp)


def is_alive(
    k: int,
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    *,
    min_life: int = 1,
) -> bool:
    """Return True if row *k* is alive at batch *batch_n*.

    A row is alive during [birth_tick, death_tick).
    """
    t_birth = birth_tick(k, initial_rows, inserts_per_batch)
    if batch_n < t_birth:
        return False
    t_death = death_tick(k, initial_rows, inserts_per_batch, death_period, min_life)
    return batch_n < t_death


def update_due(
    k: int,
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    *,
    update_period: int | float,
    min_life: int = 1,
    update_window: int | None = None,
) -> bool:
    """Return True if row *k* should be updated at batch *batch_n*.

    Conditions:
    1. Row must be alive at batch_n
    2. Must not be the birth batch (no update on creation)
    3. (batch_n + k) % update_period == 0 (staggered across rows)
    4. Row must NOT be dying at batch_n (delete supersedes update)
    5. If update_window is set, row age must be <= update_window

    The staggering by k ensures that updates are spread evenly across
    batches rather than all rows updating at the same time.
    """
    if math.isinf(update_period):
        return False
    up = int(update_period)
    if up <= 0:
        return False

    t_birth = birth_tick(k, initial_rows, inserts_per_batch)
    if batch_n <= t_birth:
        return False

    t_death = death_tick(k, initial_rows, inserts_per_batch, death_period, min_life)
    if batch_n >= t_death:
        return False

    if update_window is not None:
        age = batch_n - t_birth
        if age > update_window:
            return False

    return (batch_n + k) % up == 0


def pre_image_batch(
    k: int,
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    update_period: int | float,
) -> int:
    """Return the batch_id of row *k*'s previous state before this event.

    Finds the most recent batch at which row *k* was written (via insert
    or update) before *batch_n*.  Updates are staggered by k:
    row k updates when ``(t + k) % update_period == 0``.

    Returns birth_tick if no update occurred before batch_n.
    """
    t_birth = birth_tick(k, initial_rows, inserts_per_batch)
    if math.isinf(update_period):
        return t_birth
    up = int(update_period)
    if up <= 0:
        return t_birth

    # Find the most recent batch < batch_n where (t + k) % up == 0
    # The update cadence hits at t where t ≡ -k (mod up)
    # i.e., t = -k mod up, -k mod up + up, -k mod up + 2*up, ...
    # The largest such t < batch_n is:
    remainder = (batch_n + k) % up
    if remainder == 0:
        # batch_n itself is an update batch; previous was up batches ago
        prev = batch_n - up
    else:
        prev = batch_n - remainder

    if prev <= t_birth:
        return t_birth
    return prev


# ---------------------------------------------------------------------------
# Range / index helpers for batch generation
# ---------------------------------------------------------------------------


def insert_range(
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
) -> tuple[int, int]:
    """Return (start_k, end_k) for rows born at batch *batch_n*.

    For batch 0, returns (0, initial_rows).
    For batch n >= 1, returns the range of new k values.
    """
    if batch_n == 0:
        return (0, initial_rows)
    if inserts_per_batch <= 0:
        return (0, 0)  # No inserts
    start_k = initial_rows + (batch_n - 1) * inserts_per_batch
    end_k = start_k + inserts_per_batch
    return (start_k, end_k)


def max_k_at_batch(
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
) -> int:
    """Return the exclusive upper bound on k values that exist at batch *batch_n*.

    This is the total number of rows ever created up to and including batch_n.
    """
    if batch_n <= 0:
        return initial_rows
    return initial_rows + batch_n * inserts_per_batch


def delete_indices_at_batch(
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    min_life: int = 1,
) -> list[int]:
    """Return all k values whose death_tick equals *batch_n* (brute-force).

    This is an O(N) brute-force implementation used only in tests for
    verification against the fast version (``delete_indices_at_batch_fast``).
    Production code should use ``delete_indices_at_batch_fast`` instead.
    """
    if math.isinf(death_period) or batch_n <= 0:
        return []
    dp = int(death_period)
    if dp <= 0:
        return []

    upper_k = max_k_at_batch(batch_n, initial_rows, inserts_per_batch)
    result = []

    for k in range(upper_k):
        if death_tick(k, initial_rows, inserts_per_batch, dp, min_life) == batch_n:
            result.append(k)

    return result


def delete_indices_at_batch_fast(
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    min_life: int = 1,
) -> list[int]:
    """Optimised version using stride scanning.

    For each possible birth tick t_b, compute the required k % dp value
    and scan at stride dp.  Much faster than brute-force for large tables.
    """
    if math.isinf(death_period) or batch_n <= 0:
        return []
    dp = int(death_period)
    if dp <= 0:
        return []

    upper_k = max_k_at_batch(batch_n, initial_rows, inserts_per_batch)
    result = []

    # For initial rows (t_birth = 0): death_tick = min_life + (k % dp)
    # So k % dp == batch_n - min_life
    # For inserted rows at batch t_b: death_tick = t_b + min_life + (k % dp)
    # So k % dp == batch_n - t_b - min_life

    # Determine which birth ticks could produce deaths at batch_n
    # t_birth can be 0..batch_n (rows must be born before or at batch_n)
    # but death_tick >= t_birth + min_life, so t_birth <= batch_n - min_life
    max_birth = batch_n - min_life
    if max_birth < 0:
        return []

    # Iterate over possible birth ticks
    for t_b in range(0, max_birth + 1):
        target_mod = batch_n - t_b - min_life
        if target_mod < 0 or target_mod >= dp:
            continue

        # k values born at t_b with k % dp == target_mod
        if t_b == 0:
            k_start = 0
            k_end = initial_rows
        else:
            if inserts_per_batch <= 0:
                continue
            k_start = initial_rows + (t_b - 1) * inserts_per_batch
            k_end = k_start + inserts_per_batch

        k_end = min(k_end, upper_k)

        # Find first k >= k_start with k % dp == target_mod
        if k_start <= 0:
            first_k = target_mod
        else:
            remainder = k_start % dp
            if remainder <= target_mod:
                first_k = k_start + (target_mod - remainder)
            else:
                first_k = k_start + (dp - remainder + target_mod)

        # Stride through valid k values
        k = first_k
        while k < k_end:
            result.append(k)
            k += dp

    result.sort()
    return result


def update_indices_at_batch(
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    update_period: int | float,
    *,
    min_life: int = 1,
    update_window: int | None = None,
) -> list[int]:
    """Return all k values that should be updated at *batch_n*.

    Updates are staggered by k: row k updates when ``(batch_n + k) % up == 0``.
    Excludes rows that are dying at batch_n (delete supersedes update)
    and rows not yet born.

    If *update_window* is set, only rows with age <= update_window are eligible.
    """
    if math.isinf(update_period) or batch_n <= 0:
        return []
    up = int(update_period)
    if up <= 0:
        return []

    upper_k = max_k_at_batch(batch_n, initial_rows, inserts_per_batch)
    result = []

    # We need k values where (batch_n + k) % up == 0
    # i.e., k % up == (-batch_n) % up == (up - batch_n % up) % up
    target_mod = (up - (batch_n % up)) % up

    # Start from first k with k % up == target_mod
    first_k = target_mod

    for k in range(first_k, upper_k, up):
        # Must be born before batch_n (not at batch_n)
        t_birth = birth_tick(k, initial_rows, inserts_per_batch)
        if batch_n <= t_birth:
            continue

        # Must be alive (not dead yet)
        t_death = death_tick(k, initial_rows, inserts_per_batch, death_period, min_life)
        if batch_n >= t_death:
            continue

        # Update window: only recent rows are eligible
        if update_window is not None:
            age = batch_n - t_birth
            if age > update_window:
                continue

        result.append(k)

    result.sort()
    return result


# ---------------------------------------------------------------------------
# Seed computation
# ---------------------------------------------------------------------------


def batch_timestamp_str(start_timestamp: str, batch_interval_seconds: int, batch_id: int) -> str:
    """Compute the ISO-formatted timestamp string for a given batch.

    Shared by both CDC and ingest generators.
    """
    base = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
    ts = base + timedelta(seconds=batch_id * batch_interval_seconds)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Spark column expression builders
# ---------------------------------------------------------------------------


def _safe_import_spark() -> tuple:
    """Return PySpark functions and Column type."""
    return F, Column


def birth_tick_expr(id_col: Column | str, initial_rows: int, inserts_per_batch: int) -> Column:
    """Spark column expression for birth_tick of a row.

    Parameters
    ----------
    id_col : Column or str
        The column containing the private index k.
    initial_rows : int
        Number of rows in the initial snapshot.
    inserts_per_batch : int
        Number of inserts per batch.

    Returns
    -------
    Column
        Spark SQL expression evaluating to the birth batch number.
    """

    if isinstance(id_col, str):
        id_col = F.col(id_col)

    # k < initial_rows -> 0
    # k >= initial_rows -> ((k - initial_rows) // inserts_per_batch) + 1
    if inserts_per_batch <= 0:
        return F.lit(0).cast("long")

    return F.when(
        id_col < F.lit(initial_rows).cast("long"),
        F.lit(0).cast("long"),
    ).otherwise(
        (
            F.floor((id_col - F.lit(initial_rows).cast("long")) / F.lit(inserts_per_batch).cast("long"))
            + F.lit(1).cast("long")
        ).cast("long")
    )


def death_tick_expr(
    id_col: Column | str,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    min_life: int = 1,
) -> Column:
    """Spark column expression for death_tick of a row.

    Returns a very large long value (2^62) when death_period is infinite,
    since Spark doesn't support float('inf') in long columns.
    """

    if isinstance(id_col, str):
        id_col = F.col(id_col)

    INF_SUBSTITUTE = 2**62  # Large enough to never be reached

    if math.isinf(death_period):
        return F.lit(INF_SUBSTITUTE).cast("long")

    dp = int(death_period)
    if dp <= 0:
        return F.lit(INF_SUBSTITUTE).cast("long")

    t_birth = birth_tick_expr(id_col, initial_rows, inserts_per_batch)

    # death_tick = birth_tick + min_life + (k % death_period)
    return (t_birth + F.lit(min_life).cast("long") + (id_col % F.lit(dp).cast("long"))).cast("long")


def is_alive_expr(
    id_col: Column | str,
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    *,
    min_life: int = 1,
) -> Column:
    """Spark column expression: True if row is alive at *batch_n*.

    Evaluates: birth_tick(k) <= batch_n < death_tick(k)
    """

    if isinstance(id_col, str):
        id_col = F.col(id_col)

    t_birth = birth_tick_expr(id_col, initial_rows, inserts_per_batch)
    t_death = death_tick_expr(id_col, initial_rows, inserts_per_batch, death_period, min_life)

    batch_lit = F.lit(batch_n).cast("long")
    return (t_birth <= batch_lit) & (batch_lit < t_death)


def update_due_expr(
    id_col: Column | str,
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    death_period: int | float,
    *,
    update_period: int | float,
    min_life: int = 1,
    update_window: int | None = None,
) -> Column:
    """Spark column expression: True if row should be updated at *batch_n*.

    Conditions:
    1. alive at batch_n
    2. age > 0 (not birth batch)
    3. (batch_n + k) % update_period == 0 (staggered across rows)
    4. not dying at batch_n (delete supersedes)
    5. if update_window is set, age <= update_window
    """

    if isinstance(id_col, str):
        id_col = F.col(id_col)

    if math.isinf(update_period):
        return F.lit(False)

    up = int(update_period)
    if up <= 0:
        return F.lit(False)

    t_birth = birth_tick_expr(id_col, initial_rows, inserts_per_batch)
    t_death = death_tick_expr(id_col, initial_rows, inserts_per_batch, death_period, min_life)

    batch_lit = F.lit(batch_n).cast("long")
    age = batch_lit - t_birth

    alive = (t_birth <= batch_lit) & (batch_lit < t_death)
    not_birth = age > F.lit(0).cast("long")
    # Stagger updates by k: (batch_n + k) % up == 0
    periodic = ((batch_lit + id_col) % F.lit(up).cast("long")) == F.lit(0).cast("long")
    not_dying = batch_lit != t_death  # redundant with alive check, but explicit

    result = alive & not_birth & periodic & not_dying

    if update_window is not None:
        recency = age <= F.lit(update_window).cast("long")
        result = result & recency

    return result


def pre_image_batch_expr(
    id_col: Column | str,
    batch_n: int,
    initial_rows: int,
    inserts_per_batch: int,
    update_period: int | float,
) -> Column:
    """Spark column expression for pre_image_batch of a row.

    Returns the batch_id of the row's previous state before *batch_n*.
    Finds the most recent batch where ``(t + k) % update_period == 0``
    before *batch_n*, or the birth_tick if no such batch exists.

    Parameters
    ----------
    id_col : Column or str
        Column containing the private index k.
    batch_n : int
        Current batch number.
    initial_rows : int
        Number of rows in the initial snapshot.
    inserts_per_batch : int
        Number of inserts per batch.
    update_period : int | float
        Batches between updates for a given row.

    Returns
    -------
    Column
        Spark SQL expression evaluating to the pre-image batch number.
    """

    if isinstance(id_col, str):
        id_col = F.col(id_col)

    t_birth = birth_tick_expr(id_col, initial_rows, inserts_per_batch)

    if math.isinf(update_period):
        return t_birth

    up = int(update_period)
    if up <= 0:
        return t_birth

    batch_lit = F.lit(batch_n).cast("long")
    up_lit = F.lit(up).cast("long")

    # remainder = (batch_n + k) % up
    remainder = (batch_lit + id_col) % up_lit
    # if remainder == 0: prev = batch_n - up; else: prev = batch_n - remainder
    prev = (
        F.when(
            remainder == F.lit(0).cast("long"),
            batch_lit - up_lit,
        )
        .otherwise(
            batch_lit - remainder,
        )
        .cast("long")
    )

    # return max(prev, t_birth)
    return F.greatest(prev, t_birth).cast("long")
