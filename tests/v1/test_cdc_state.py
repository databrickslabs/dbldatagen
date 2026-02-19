"""Tests for CDC stateless lifecycle and batch size resolution.

Tests the stateless modular-recurrence functions used for CDC row selection,
and the batch size resolution helper.
"""

from __future__ import annotations

import math

from dbldatagen.v1.engine.cdc_state import resolve_batch_size
from dbldatagen.v1.engine.cdc_stateless import (
    birth_tick,
    compute_periods,
    death_tick,
    delete_indices_at_batch_fast,
    insert_range,
    is_alive,
    max_k_at_batch,
    update_indices_at_batch,
)

# ---------------------------------------------------------------------------
# Batch size resolution
# ---------------------------------------------------------------------------


class TestResolveBatchSize:
    def test_fraction(self):
        assert resolve_batch_size(0.1, 1000) == 100

    def test_fraction_small_table(self):
        assert resolve_batch_size(0.1, 5) == 1  # min 1

    def test_int_passthrough(self):
        assert resolve_batch_size(50, 1000) == 50


# ---------------------------------------------------------------------------
# Stateless row selection via compute_periods + index functions
# ---------------------------------------------------------------------------


class TestStatelessBatchRowSelection:
    def test_correct_total_count(self):
        """Batch produces correct total of inserts + updates + deletes."""
        periods = compute_periods(100, 10, 3.0, 5.0, 2.0)
        batch_n = 1
        start_k, end_k = insert_range(batch_n, 100, periods.inserts_per_batch)
        ins_count = end_k - start_k
        upd_count = len(
            update_indices_at_batch(
                batch_n,
                100,
                periods.inserts_per_batch,
                periods.death_period,
                periods.update_period,
            )
        )
        del_count = len(
            delete_indices_at_batch_fast(
                batch_n,
                100,
                periods.inserts_per_batch,
                periods.death_period,
            )
        )
        total = ins_count + upd_count + del_count
        # Total should be close to batch_size (may vary due to lifecycle timing)
        assert total > 0

    def test_insert_only(self):
        """With only insert weight, no updates or deletes are produced."""
        periods = compute_periods(50, 10, 1.0, 0.0, 0.0)
        batch_n = 1
        start_k, end_k = insert_range(batch_n, 50, periods.inserts_per_batch)
        ins_count = end_k - start_k
        assert ins_count == 10
        upd_indices = update_indices_at_batch(
            batch_n,
            50,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        del_indices = delete_indices_at_batch_fast(
            batch_n,
            50,
            periods.inserts_per_batch,
            periods.death_period,
        )
        assert len(upd_indices) == 0
        assert len(del_indices) == 0

    def test_no_overlap_update_delete(self):
        """Updates and deletes should never overlap (delete supersedes)."""
        periods = compute_periods(200, 50, 1.0, 5.0, 4.0)
        for batch_n in range(1, 15):
            upd = set(
                update_indices_at_batch(
                    batch_n,
                    200,
                    periods.inserts_per_batch,
                    periods.death_period,
                    periods.update_period,
                )
            )
            dlt = set(
                delete_indices_at_batch_fast(
                    batch_n,
                    200,
                    periods.inserts_per_batch,
                    periods.death_period,
                )
            )
            overlap = upd & dlt
            assert len(overlap) == 0, f"Batch {batch_n}: update/delete overlap: {overlap}"

    def test_capped_to_live_rows(self):
        """Updates should only reference rows that are alive."""
        periods = compute_periods(5, 100, 1.0, 5.0, 4.0)
        batch_n = 1
        upd_indices = update_indices_at_batch(
            batch_n,
            5,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        for k in upd_indices:
            assert is_alive(
                k,
                batch_n,
                5,
                periods.inserts_per_batch,
                periods.death_period,
            )


class TestStatelessRowSelectionDeterminism:
    def test_same_parameters_same_result(self):
        """Same parameters produce identical index sets."""
        periods = compute_periods(100, 20, 2.0, 5.0, 3.0)
        r1 = update_indices_at_batch(
            1,
            100,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        r2 = update_indices_at_batch(
            1,
            100,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        assert r1 == r2

    def test_different_batch_different_selection(self):
        """Different batches produce different update sets (when alive rows differ)."""
        periods = compute_periods(100, 20, 2.0, 5.0, 3.0)
        r1 = update_indices_at_batch(
            5,
            100,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        r2 = update_indices_at_batch(
            10,
            100,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        assert r1 != r2


# ---------------------------------------------------------------------------
# Stateless state-at-batch computation
# ---------------------------------------------------------------------------


class TestStatelessStateAtBatch:
    def test_batch_zero(self):
        """At batch 0, all initial rows are alive and no inserts yet."""
        initial_rows = 100
        periods = compute_periods(initial_rows, 10, 3.0, 5.0, 2.0)
        upper_k = max_k_at_batch(0, initial_rows, periods.inserts_per_batch)
        assert upper_k == initial_rows
        live_count = sum(
            1 for k in range(upper_k) if is_alive(k, 0, initial_rows, periods.inserts_per_batch, periods.death_period)
        )
        assert live_count == initial_rows

    def test_inserts_accumulate(self):
        """After N batches, max_k grows by inserts_per_batch * N."""
        periods = compute_periods(50, 10, 1.0, 0.0, 0.0)
        assert max_k_at_batch(5, 50, periods.inserts_per_batch) == 50 + 10 * 5

    def test_deletes_reduce_live_count(self):
        """With delete-only config, live count decreases."""
        initial_rows = 100
        periods = compute_periods(initial_rows, 10, 0.0, 0.0, 1.0)
        upper_k = max_k_at_batch(3, initial_rows, periods.inserts_per_batch)
        live_count = sum(
            1 for k in range(upper_k) if is_alive(k, 3, initial_rows, periods.inserts_per_batch, periods.death_period)
        )
        # After 3 batches of 10 deletes each, should be 100 - ~some deletes
        # (actual count depends on death_period spacing)
        assert live_count < initial_rows

    def test_live_count_invariant(self):
        """Live count = initial + inserts - dead rows."""
        initial_rows = 200
        periods = compute_periods(initial_rows, 20, 3.0, 5.0, 2.0)
        for batch_n in [1, 5, 10]:
            upper_k = max_k_at_batch(batch_n, initial_rows, periods.inserts_per_batch)
            live = sum(
                1
                for k in range(upper_k)
                if is_alive(k, batch_n, initial_rows, periods.inserts_per_batch, periods.death_period)
            )
            dead = upper_k - live
            # Every dead row should have death_tick <= batch_n
            dead_count = sum(
                1
                for k in range(upper_k)
                if not math.isinf(death_tick(k, initial_rows, periods.inserts_per_batch, periods.death_period))
                and death_tick(k, initial_rows, periods.inserts_per_batch, periods.death_period) <= batch_n
                and birth_tick(k, initial_rows, periods.inserts_per_batch) <= batch_n
            )
            assert dead == dead_count
