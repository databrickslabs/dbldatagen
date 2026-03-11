"""Tests for the stateless CDC lifecycle engine (Modular Recurrence).

Covers period computation, tick calculations, edge cases (zero weights,
boundary batches), collision handling (delete supersedes update), and
batch seed computation.
"""

from __future__ import annotations

import math

import pytest

from dbldatagen.v1.engine.cdc_stateless import (
    birth_tick,
    compute_periods,
    death_tick,
    delete_indices_at_batch,
    delete_indices_at_batch_fast,
    insert_range,
    is_alive,
    max_k_at_batch,
    pre_image_batch,
    update_due,
    update_indices_at_batch,
)
from dbldatagen.v1.engine.seed import compute_batch_seed


# ===================================================================
# compute_periods
# ===================================================================


class TestComputePeriods:
    """Tests for compute_periods()."""

    def test_basic_weights(self):
        """Standard 3:5:2 weights with 100M rows and 100K batch size."""
        p = compute_periods(100_000_000, 100_000, 3.0, 5.0, 2.0)
        assert p.inserts_per_batch == 30_000
        assert p.updates_per_batch == 50_000
        assert p.deletes_per_batch == 20_000
        # death_period = 100M / 20K = 5000
        assert p.death_period == 5000
        # update_period = 100M / 50K = 2000
        assert p.update_period == 2000

    def test_equal_weights(self):
        """Equal weights split evenly."""
        p = compute_periods(1000, 30, 1.0, 1.0, 1.0)
        assert p.inserts_per_batch == 10
        assert p.updates_per_batch == 10
        assert p.deletes_per_batch == 10
        assert p.inserts_per_batch + p.updates_per_batch + p.deletes_per_batch == 30

    def test_zero_delete_weight(self):
        """When delete weight is 0, death_period is infinity."""
        p = compute_periods(1000, 100, 5.0, 5.0, 0.0)
        assert p.deletes_per_batch == 0
        assert math.isinf(p.death_period)
        assert p.inserts_per_batch == 50
        assert p.updates_per_batch == 50

    def test_zero_update_weight(self):
        """When update weight is 0, update_period is infinity."""
        p = compute_periods(1000, 100, 5.0, 0.0, 5.0)
        assert p.updates_per_batch == 0
        assert math.isinf(p.update_period)
        assert p.inserts_per_batch == 50
        assert p.deletes_per_batch == 50

    def test_zero_insert_weight(self):
        """When insert weight is 0, inserts_per_batch is 0."""
        p = compute_periods(1000, 100, 0.0, 5.0, 5.0)
        assert p.inserts_per_batch == 0
        assert p.updates_per_batch == 50
        assert p.deletes_per_batch == 50

    def test_all_inserts(self):
        """Only inserts: no updates or deletes."""
        p = compute_periods(1000, 100, 1.0, 0.0, 0.0)
        assert p.inserts_per_batch == 100
        assert p.updates_per_batch == 0
        assert p.deletes_per_batch == 0
        assert math.isinf(p.death_period)
        assert math.isinf(p.update_period)

    def test_all_weights_zero_raises(self):
        """All zero weights should raise ValueError."""
        with pytest.raises(ValueError, match="positive"):
            compute_periods(1000, 100, 0.0, 0.0, 0.0)

    def test_remainder_assignment(self):
        """Remainder goes to the largest non-zero weight."""
        # Weights 1:1:1 with batch_size=10 -> 3+3+3=9, remainder 1
        p = compute_periods(1000, 10, 1.0, 1.0, 1.0)
        total = p.inserts_per_batch + p.updates_per_batch + p.deletes_per_batch
        assert total == 10

    def test_remainder_with_zero_delete(self):
        """Remainder must NOT go to deletes when delete weight is 0."""
        p = compute_periods(1000, 10, 1.0, 1.0, 0.0)
        assert p.deletes_per_batch == 0
        total = p.inserts_per_batch + p.updates_per_batch + p.deletes_per_batch
        assert total == 10

    def test_small_initial_rows(self):
        """Small initial_rows with large batch causes death_period of 1."""
        p = compute_periods(10, 100, 1.0, 1.0, 1.0)
        assert p.death_period >= 1
        assert p.update_period >= 1


# ===================================================================
# birth_tick
# ===================================================================


class TestBirthTick:
    """Tests for birth_tick()."""

    def test_initial_rows(self):
        """All initial rows are born at batch 0."""
        for k in range(100):
            assert birth_tick(k, 100, 10) == 0

    def test_first_batch_inserts(self):
        """First batch of inserts (k = 100..109) born at batch 1."""
        for k in range(100, 110):
            assert birth_tick(k, 100, 10) == 1

    def test_second_batch_inserts(self):
        """Second batch of inserts (k = 110..119) born at batch 2."""
        for k in range(110, 120):
            assert birth_tick(k, 100, 10) == 2

    def test_large_k(self):
        """Birth tick for a row far in the future."""
        # k = 100 + 99*10 = 1090, should be born at batch 100
        assert birth_tick(1090, 100, 10) == 100

    def test_zero_inserts_raises(self):
        """k >= initial_rows with 0 inserts_per_batch raises."""
        with pytest.raises(ValueError, match="inserts_per_batch"):
            birth_tick(100, 100, 0)

    def test_boundary_k(self):
        """k = initial_rows is the first inserted row, born at batch 1."""
        assert birth_tick(100, 100, 10) == 1

    def test_k_zero(self):
        """k = 0 is always born at batch 0."""
        assert birth_tick(0, 100, 10) == 0
        assert birth_tick(0, 1, 1) == 0


# ===================================================================
# death_tick
# ===================================================================


class TestDeathTick:
    """Tests for death_tick()."""

    def test_infinite_death_period(self):
        """Infinite death period means the row never dies."""
        assert math.isinf(death_tick(0, 100, 10, math.inf))

    def test_zero_death_period(self):
        """Zero death period treated as infinite (no deletes)."""
        assert math.isinf(death_tick(0, 100, 10, 0))

    def test_initial_row_death(self):
        """Initial row with death_period=10, min_life=3.
        death_tick(k=0) = 0 + 3 + (0 % 10) = 3
        death_tick(k=5) = 0 + 3 + (5 % 10) = 8
        """
        assert death_tick(0, 100, 10, 10, min_life=3) == 3
        assert death_tick(5, 100, 10, 10, min_life=3) == 8
        assert death_tick(9, 100, 10, 10, min_life=3) == 12

    def test_inserted_row_death(self):
        """Inserted row born at batch 1.
        k=100, initial=100, inserts=10, dp=10, min_life=3
        birth=1, death=1+3+(100%10)=4
        """
        assert death_tick(100, 100, 10, 10, min_life=3) == 4

    def test_min_life_guarantee(self):
        """Every row lives at least min_life batches."""
        for k in range(200):
            t_b = birth_tick(k, 100, 10)
            t_d = death_tick(k, 100, 10, 10, min_life=3)
            if not math.isinf(t_d):
                assert t_d >= t_b + 3, f"k={k}: born at {t_b}, dies at {t_d}"

    def test_death_tick_uniqueness_per_k(self):
        """Each k should have a unique death tick assignment."""
        deaths: dict[float, list[int]] = {}
        for k in range(100):
            t = death_tick(k, 100, 10, 10, min_life=3)
            deaths.setdefault(t, []).append(k)
        # Not all deaths should be at the same batch (some spread expected)
        assert len(deaths) > 1


# ===================================================================
# is_alive
# ===================================================================


class TestIsAlive:
    """Tests for is_alive()."""

    def test_alive_at_birth(self):
        """Row is alive at its birth batch."""
        assert is_alive(0, 0, 100, 10, 10, min_life=3)
        assert is_alive(100, 1, 100, 10, 10, min_life=3)

    def test_not_alive_before_birth(self):
        """Row is not alive before its birth batch."""
        assert not is_alive(100, 0, 100, 10, 10, min_life=3)

    def test_alive_between_birth_and_death(self):
        """Row is alive between birth and death."""
        # k=0: born 0, dies 3
        assert is_alive(0, 0, 100, 10, 10, min_life=3)
        assert is_alive(0, 1, 100, 10, 10, min_life=3)
        assert is_alive(0, 2, 100, 10, 10, min_life=3)
        # dead at batch 3
        assert not is_alive(0, 3, 100, 10, 10, min_life=3)

    def test_alive_no_deletes(self):
        """Row with infinite death period is always alive (after birth)."""
        assert is_alive(0, 0, 100, 10, math.inf)
        assert is_alive(0, 1000, 100, 10, math.inf)

    def test_not_alive_at_death(self):
        """Row is NOT alive at its death batch (half-open interval)."""
        # k=0: born 0, dies 3
        assert not is_alive(0, 3, 100, 10, 10, min_life=3)


# ===================================================================
# update_due
# ===================================================================


class TestUpdateDue:
    """Tests for update_due()."""

    def test_no_update_at_birth(self):
        """No update on the birth batch."""
        assert not update_due(0, 0, 100, 10, 10, 5, min_life=3)

    def test_update_at_period(self):
        """Row k=0 with update_period=5: updated at batch 5, 10, etc.
        (as long as alive)
        """
        # k=0: born 0, dies 3 (dp=10, min_life=3 -> death=0+3+0=3)
        # update_period=5, but row dies at 3, so no updates
        assert not update_due(0, 5, 100, 10, 10, 5, min_life=3)

    def test_update_for_long_lived_row(self):
        """Row k=5 with death_tick=8: can get updated at batch 5."""
        # k=5: born 0, death=0+3+(5%10)=8, update_period=5
        assert update_due(5, 5, 100, 10, 10, 5, min_life=3)
        # But not at batch 8 (death)
        assert not update_due(5, 8, 100, 10, 10, 5, min_life=3)

    def test_infinite_update_period(self):
        """Infinite update period means no updates ever."""
        assert not update_due(0, 5, 100, 10, 10, math.inf)

    def test_delete_supersedes_update(self):
        """If a row dies at batch t, it should NOT be updated at t."""
        # k=0: born 0, death=3 (dp=10, min_life=3)
        # Even if update_period=1, no update at death batch
        assert not update_due(0, 3, 100, 10, 10, 1, min_life=3)

    def test_update_period_1(self):
        """With update_period=1, row is updated every batch (while alive)."""
        # k=5: born 0, death=8
        assert not update_due(5, 0, 100, 10, 10, 1, min_life=3)  # birth
        assert update_due(5, 1, 100, 10, 10, 1, min_life=3)
        assert update_due(5, 7, 100, 10, 10, 1, min_life=3)
        assert not update_due(5, 8, 100, 10, 10, 1, min_life=3)  # death

    def test_dead_rows_not_updated(self):
        """Rows past their death tick are not updated."""
        assert not update_due(0, 100, 100, 10, 10, 5, min_life=3)


# ===================================================================
# pre_image_batch
# ===================================================================


class TestPreImageBatch:
    """Tests for pre_image_batch()."""

    def test_first_update_returns_birth(self):
        """First update: pre-image is the birth batch."""
        # k=5, update_period=5, birth=0, first update at batch 5
        assert pre_image_batch(5, 5, 100, 10, 5) == 0

    def test_subsequent_update(self):
        """Second update: pre-image is batch_n - update_period."""
        # k=5, update_period=5, updated at batch 10 -> pre-image is batch 5
        assert pre_image_batch(5, 10, 100, 10, 5) == 5

    def test_infinite_update_period(self):
        """Infinite update period: pre-image is always birth."""
        assert pre_image_batch(0, 5, 100, 10, math.inf) == 0

    def test_inserted_row_pre_image(self):
        """Inserted row: pre-image of first update is birth."""
        # k=110, initial=100, inserts=10 -> born at batch 2
        # With staggered updates: (t + k) % up == 0
        # k=110, up=5: 110 % 5 = 0, so updates at t where t%5==0
        # First update after birth (batch 2) is at batch 5
        # Pre-image at batch 5 = birth = 2
        assert pre_image_batch(110, 5, 100, 10, 5) == 2


# ===================================================================
# insert_range
# ===================================================================


class TestInsertRange:
    """Tests for insert_range()."""

    def test_batch_zero(self):
        """Batch 0 returns initial rows."""
        assert insert_range(0, 100, 10) == (0, 100)

    def test_batch_one(self):
        """Batch 1: first inserts."""
        assert insert_range(1, 100, 10) == (100, 110)

    def test_batch_two(self):
        """Batch 2: second inserts."""
        assert insert_range(2, 100, 10) == (110, 120)

    def test_zero_inserts(self):
        """No inserts returns empty range."""
        assert insert_range(1, 100, 0) == (0, 0)

    def test_range_continuity(self):
        """Insert ranges should be contiguous."""
        _, end1 = insert_range(1, 100, 10)
        start2, _ = insert_range(2, 100, 10)
        assert end1 == start2


# ===================================================================
# max_k_at_batch
# ===================================================================


class TestMaxKAtBatch:
    """Tests for max_k_at_batch()."""

    def test_batch_zero(self):
        """At batch 0, max_k = initial_rows."""
        assert max_k_at_batch(0, 100, 10) == 100

    def test_batch_one(self):
        """At batch 1, max_k = initial_rows + inserts."""
        assert max_k_at_batch(1, 100, 10) == 110

    def test_multiple_batches(self):
        """Multiple batches accumulate inserts."""
        assert max_k_at_batch(5, 100, 10) == 150


# ===================================================================
# delete_indices_at_batch / delete_indices_at_batch_fast
# ===================================================================


class TestDeleteIndices:
    """Tests for delete index computation."""

    def test_no_deletes_when_infinite_period(self):
        """Infinite death period -> no deletes."""
        assert delete_indices_at_batch(1, 100, 10, math.inf) == []
        assert delete_indices_at_batch_fast(1, 100, 10, math.inf) == []

    def test_no_deletes_at_batch_zero(self):
        """No deletes at batch 0."""
        assert delete_indices_at_batch(0, 100, 10, 10) == []
        assert delete_indices_at_batch_fast(0, 100, 10, 10) == []

    def test_deletes_before_min_life(self):
        """No deletes before min_life batches have passed."""
        assert delete_indices_at_batch(1, 100, 10, 10, min_life=3) == []
        assert delete_indices_at_batch(2, 100, 10, 10, min_life=3) == []
        assert delete_indices_at_batch_fast(1, 100, 10, 10, min_life=3) == []
        assert delete_indices_at_batch_fast(2, 100, 10, 10, min_life=3) == []

    def test_brute_force_matches_fast(self):
        """Brute-force and fast implementations must agree."""
        initial = 50
        ins = 5
        dp = 10
        for batch_n in range(1, 20):
            brute = delete_indices_at_batch(batch_n, initial, ins, dp, min_life=3)
            fast = delete_indices_at_batch_fast(batch_n, initial, ins, dp, min_life=3)
            assert brute == fast, f"batch {batch_n}: brute={brute}, fast={fast}"

    def test_each_delete_has_correct_death_tick(self):
        """Every k in the delete list must have death_tick == batch_n."""
        initial = 50
        ins = 5
        dp = 10
        for batch_n in range(1, 20):
            deletes = delete_indices_at_batch_fast(batch_n, initial, ins, dp, min_life=3)
            for k in deletes:
                assert (
                    death_tick(k, initial, ins, dp, min_life=3) == batch_n
                ), f"k={k} at batch {batch_n}: death_tick={death_tick(k, initial, ins, dp, min_life=3)}"

    def test_deletes_are_sorted(self):
        """Delete indices should be returned in sorted order."""
        deletes = delete_indices_at_batch_fast(5, 100, 10, 10, min_life=3)
        assert deletes == sorted(deletes)


# ===================================================================
# update_indices_at_batch
# ===================================================================


class TestUpdateIndices:
    """Tests for update_indices_at_batch()."""

    def test_no_updates_when_infinite_period(self):
        """Infinite update period -> no updates."""
        assert update_indices_at_batch(1, 100, 10, 10, math.inf) == []

    def test_no_updates_at_batch_zero(self):
        """No updates at batch 0."""
        assert update_indices_at_batch(0, 100, 10, 10, 5) == []

    def test_updates_exclude_dying_rows(self):
        """Rows dying at batch_n must not appear in updates."""
        initial = 50
        ins = 5
        dp = 10
        up = 1  # update every batch -> easy to trigger collision
        for batch_n in range(1, 20):
            updates = update_indices_at_batch(batch_n, initial, ins, dp, up, min_life=3)
            deletes = delete_indices_at_batch_fast(batch_n, initial, ins, dp, min_life=3)
            overlap = set(updates) & set(deletes)
            assert len(overlap) == 0, f"batch {batch_n}: overlap={overlap}"

    def test_updates_are_alive(self):
        """Every row in the update list must be alive."""
        initial = 50
        ins = 5
        dp = 10
        up = 5
        for batch_n in range(1, 20):
            updates = update_indices_at_batch(batch_n, initial, ins, dp, up, min_life=3)
            for k in updates:
                assert is_alive(k, batch_n, initial, ins, dp, min_life=3), f"k={k} not alive at batch {batch_n}"

    def test_updates_are_sorted(self):
        """Update indices should be returned in sorted order."""
        updates = update_indices_at_batch(10, 100, 10, 10, 5, min_life=3)
        assert updates == sorted(updates)


# ===================================================================
# Disjoint guarantee
# ===================================================================


class TestDisjointGuarantee:
    """Ensure insert, update, and delete sets are disjoint at each batch."""

    def test_disjoint_across_many_batches(self):
        """For several batches, verify no row appears in multiple streams."""
        initial = 100
        ins = 10
        dp = 20
        up = 5
        min_life = 3

        for batch_n in range(1, 30):
            ins_start, ins_end = insert_range(batch_n, initial, ins)
            inserts = set(range(ins_start, ins_end))
            updates = set(update_indices_at_batch(batch_n, initial, ins, dp, up, min_life))
            deletes = set(delete_indices_at_batch_fast(batch_n, initial, ins, dp, min_life))

            # Inserts should never overlap with updates or deletes
            # (inserts are brand new rows, not yet alive before this batch)
            assert len(inserts & updates) == 0, f"batch {batch_n}: insert/update overlap"
            assert len(inserts & deletes) == 0, f"batch {batch_n}: insert/delete overlap"
            # Updates and deletes must be disjoint
            assert len(updates & deletes) == 0, f"batch {batch_n}: update/delete overlap"


# ===================================================================
# compute_batch_seed
# ===================================================================


class TestComputeBatchSeed:
    """Tests for compute_batch_seed()."""

    def test_batch_zero_returns_global(self):
        """Batch 0 returns the global seed directly."""
        assert compute_batch_seed(42, 0) == 42

    def test_batch_one(self):
        """Batch 1 returns global_seed + 10000."""
        assert compute_batch_seed(42, 1) == 10042

    def test_deterministic(self):
        """Same inputs produce same output."""
        assert compute_batch_seed(42, 5) == compute_batch_seed(42, 5)

    def test_different_batches_different_seeds(self):
        """Different batches produce different seeds."""
        seeds = {compute_batch_seed(42, b) for b in range(100)}
        assert len(seeds) == 100

    def test_negative_seed_wrapping(self):
        """Large seeds wrap to signed 64-bit correctly."""
        seed = 2**63 - 5000  # Just below max signed
        result = compute_batch_seed(seed, 1)
        # After adding 10000 and wrapping
        assert -(2**63) <= result < 2**63


# ===================================================================
# Lifecycle consistency
# ===================================================================


class TestLifecycleConsistency:
    """End-to-end tests verifying lifecycle invariants across batches."""

    def test_row_lifecycle_progression(self):
        """A row is born, lives, and dies in the correct order."""
        initial = 100
        ins = 10
        dp = 20
        min_life = 3

        for k in range(initial):
            t_b = birth_tick(k, initial, ins)
            t_d = death_tick(k, initial, ins, dp, min_life)
            assert t_b == 0
            if not math.isinf(t_d):
                assert t_d >= t_b + min_life
                # Alive from birth to death-1
                for t in range(t_b, min(t_b + 50, int(t_d))):
                    assert is_alive(k, t, initial, ins, dp, min_life)
                # Dead at death tick
                assert not is_alive(k, int(t_d), initial, ins, dp, min_life)

    def test_no_orphan_updates(self):
        """Updates only happen for rows that are alive."""
        initial = 50
        ins = 5
        dp = 10
        up = 3
        min_life = 3

        for batch_n in range(1, 25):
            updates = update_indices_at_batch(batch_n, initial, ins, dp, up, min_life)
            for k in updates:
                assert is_alive(k, batch_n, initial, ins, dp, min_life)
                assert update_due(k, batch_n, initial, ins, dp, up, min_life)

    def test_inserted_rows_have_correct_birth(self):
        """Rows created in batch n have birth_tick == n."""
        initial = 100
        ins = 10
        for batch_n in range(0, 10):
            start_k, end_k = insert_range(batch_n, initial, ins)
            for k in range(start_k, end_k):
                assert birth_tick(k, initial, ins) == batch_n

    def test_all_initial_rows_eventually_die(self):
        """With finite death_period, all initial rows have finite death_tick."""
        initial = 50
        dp = 10
        min_life = 3
        for k in range(initial):
            t_d = death_tick(k, initial, 5, dp, min_life)
            assert not math.isinf(t_d), f"k={k} has infinite death_tick"

    def test_pre_image_chain(self):
        """Pre-image chain links back to birth correctly."""
        initial = 100
        ins = 10
        up = 5
        k = 5  # born at 0

        # First update at batch 5
        assert pre_image_batch(k, 5, initial, ins, up) == 0
        # Second update at batch 10
        assert pre_image_batch(k, 10, initial, ins, up) == 5
        # Third update at batch 15
        assert pre_image_batch(k, 15, initial, ins, up) == 10
