"""Tests for large-scale stateless CDC lifecycle functions.

Exercises the modular-recurrence engine with large row counts to verify
that performance and correctness hold at scale.  Replaces the old
memmap-backed TableState tests with pure stateless equivalents.
"""

from __future__ import annotations

import pytest

from dbldatagen.v1.cdc_schema import CDCTableConfig
from dbldatagen.v1.engine.cdc_stateless import (
    birth_tick,
    compute_periods,
    death_tick,
    delete_indices_at_batch_fast,
    is_alive,
    max_k_at_batch,
    pre_image_batch,
    update_indices_at_batch,
)
from dbldatagen.v1.engine.seed import compute_batch_seed

# Large row count -- exercises the fast stride-scanning paths
LARGE_ROWS = 100_000


# ---------------------------------------------------------------------------
# Large-scale lifecycle correctness
# ---------------------------------------------------------------------------


class TestLargeScaleLifecycle:
    def test_all_initial_rows_alive_at_batch_zero(self):
        """At batch 0, all initial rows are alive."""
        periods = compute_periods(LARGE_ROWS, 1000, 3.0, 5.0, 2.0)
        upper_k = max_k_at_batch(0, LARGE_ROWS, periods.inserts_per_batch)
        assert upper_k == LARGE_ROWS
        # Spot-check a sample of rows
        for k in [0, 1, LARGE_ROWS // 2, LARGE_ROWS - 1]:
            assert is_alive(k, 0, LARGE_ROWS, periods.inserts_per_batch, periods.death_period)

    def test_inserts_accumulate_large(self):
        """After N batches, max_k grows correctly."""
        periods = compute_periods(LARGE_ROWS, 1000, 1.0, 0.0, 0.0)
        for n in [1, 5, 10, 50]:
            expected = LARGE_ROWS + periods.inserts_per_batch * n
            assert max_k_at_batch(n, LARGE_ROWS, periods.inserts_per_batch) == expected

    def test_deletes_reduce_live_count(self):
        """With deletes enabled, live count decreases over batches."""
        periods = compute_periods(LARGE_ROWS, 1000, 0.0, 0.0, 1.0)
        # After enough batches, some rows should be dead
        batch_n = 10
        upper_k = max_k_at_batch(batch_n, LARGE_ROWS, periods.inserts_per_batch)
        del_indices = delete_indices_at_batch_fast(
            batch_n,
            LARGE_ROWS,
            periods.inserts_per_batch,
            periods.death_period,
        )
        # There should be deletions
        assert len(del_indices) > 0


# ---------------------------------------------------------------------------
# Large-scale delete index correctness
# ---------------------------------------------------------------------------


class TestLargeScaleDeletes:
    def test_delete_indices_fast_matches_brute_force(self):
        """Fast stride-scan produces same results as brute force for a sample."""
        # Use a smaller row count for brute-force comparison
        rows = 500
        periods = compute_periods(rows, 50, 3.0, 5.0, 2.0)
        for batch_n in [3, 5, 8]:
            fast = delete_indices_at_batch_fast(
                batch_n,
                rows,
                periods.inserts_per_batch,
                periods.death_period,
            )
            # Brute force
            upper_k = max_k_at_batch(batch_n, rows, periods.inserts_per_batch)
            brute = [
                k
                for k in range(upper_k)
                if death_tick(k, rows, periods.inserts_per_batch, periods.death_period) == batch_n
            ]
            assert fast == brute, f"Mismatch at batch {batch_n}"

    def test_deleted_rows_not_in_updates(self):
        """Rows dying at a batch should never appear in the update set."""
        periods = compute_periods(LARGE_ROWS, 1000, 3.0, 5.0, 2.0)
        for batch_n in [5, 10, 15]:
            upd = set(
                update_indices_at_batch(
                    batch_n,
                    LARGE_ROWS,
                    periods.inserts_per_batch,
                    periods.death_period,
                    periods.update_period,
                )
            )
            dlt = set(
                delete_indices_at_batch_fast(
                    batch_n,
                    LARGE_ROWS,
                    periods.inserts_per_batch,
                    periods.death_period,
                )
            )
            overlap = upd & dlt
            assert len(overlap) == 0, f"Batch {batch_n}: overlap {len(overlap)} rows"

    def test_dead_rows_stay_dead(self):
        """Once a row dies, it never appears as alive in later batches."""
        periods = compute_periods(1000, 100, 3.0, 5.0, 2.0)
        # Find rows that die at batch 5
        dead_at_5 = delete_indices_at_batch_fast(
            5,
            1000,
            periods.inserts_per_batch,
            periods.death_period,
        )
        # Verify they're dead at batch 6, 7, 10
        for k in dead_at_5[:10]:  # Sample
            for later in [6, 7, 10, 20]:
                assert not is_alive(
                    k,
                    later,
                    1000,
                    periods.inserts_per_batch,
                    periods.death_period,
                ), f"Row {k} should be dead at batch {later}"


# ---------------------------------------------------------------------------
# Large-scale update index correctness
# ---------------------------------------------------------------------------


class TestLargeScaleUpdates:
    def test_updates_only_alive_rows(self):
        """All updated rows must be alive at the batch."""
        periods = compute_periods(LARGE_ROWS, 1000, 3.0, 5.0, 2.0)
        for batch_n in [5, 10]:
            upd_indices = update_indices_at_batch(
                batch_n,
                LARGE_ROWS,
                periods.inserts_per_batch,
                periods.death_period,
                periods.update_period,
            )
            # Sample check (full check too slow for 100k)
            for k in upd_indices[:100]:
                assert is_alive(
                    k,
                    batch_n,
                    LARGE_ROWS,
                    periods.inserts_per_batch,
                    periods.death_period,
                ), f"Row {k} updated at batch {batch_n} but not alive"

    def test_update_determinism(self):
        """Same parameters always produce the same update set."""
        periods = compute_periods(LARGE_ROWS, 1000, 3.0, 5.0, 2.0)
        r1 = update_indices_at_batch(
            7,
            LARGE_ROWS,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        r2 = update_indices_at_batch(
            7,
            LARGE_ROWS,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        assert r1 == r2


# ---------------------------------------------------------------------------
# Pre-image batch correctness
# ---------------------------------------------------------------------------


class TestLargeScalePreImage:
    def test_pre_image_within_bounds(self):
        """pre_image_batch returns a value between birth and batch_n."""
        periods = compute_periods(LARGE_ROWS, 1000, 3.0, 5.0, 2.0)
        batch_n = 10
        upd_indices = update_indices_at_batch(
            batch_n,
            LARGE_ROWS,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        for k in upd_indices[:100]:
            pib = pre_image_batch(
                k,
                batch_n,
                LARGE_ROWS,
                periods.inserts_per_batch,
                periods.update_period,
            )
            tb = birth_tick(k, LARGE_ROWS, periods.inserts_per_batch)
            assert pib >= tb, f"pre_image_batch {pib} < birth {tb} for k={k}"
            assert pib < batch_n, f"pre_image_batch {pib} >= batch_n {batch_n} for k={k}"

    def test_pre_image_first_update_is_birth(self):
        """For the first update of a row, pre_image_batch == birth_tick."""
        periods = compute_periods(1000, 100, 3.0, 5.0, 2.0)
        up = int(periods.update_period)
        if up <= 0:
            pytest.skip("No updates in this configuration")
        # Batch = up means initial rows get their first update
        batch_n = up
        upd_indices = update_indices_at_batch(
            batch_n,
            1000,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
        )
        for k in upd_indices[:50]:
            if k < 1000:  # Initial rows, birth=0
                pib = pre_image_batch(
                    k,
                    batch_n,
                    1000,
                    periods.inserts_per_batch,
                    periods.update_period,
                )
                assert pib == 0, f"First update pre_image should be 0, got {pib}"


# ---------------------------------------------------------------------------
# Batch seed computation
# ---------------------------------------------------------------------------


class TestLargeScaleBatchSeed:
    def test_batch_zero_is_global_seed(self):
        assert compute_batch_seed(42, 0) == 42

    def test_different_batches_different_seeds(self):
        seeds = {compute_batch_seed(42, b) for b in range(100)}
        assert len(seeds) == 100

    def test_signed_64_bit_wrapping(self):
        """Large seeds wrap correctly to signed 64-bit."""
        seed = 2**63 - 5
        result = compute_batch_seed(seed, 1)
        assert -(2**63) <= result < 2**63


# ---------------------------------------------------------------------------
# Precompute CDC plans at scale
# ---------------------------------------------------------------------------


class TestPrecomputePlansLargeScale:
    def test_basic_precompute(self):
        from dbldatagen.v1.cdc import precompute_cdc_plans
        from dbldatagen.v1.cdc_schema import CDCPlan
        from dbldatagen.v1.schema import (
            ColumnSpec,
            DataGenPlan,
            DataType,
            PrimaryKey,
            SequenceColumn,
            TableSpec,
        )

        table = TableSpec(
            name="items",
            rows=10_000,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ],
        )
        plan = CDCPlan(
            base_plan=DataGenPlan(seed=42, tables=[table]),
            num_batches=10,
        )
        result = precompute_cdc_plans(plan)
        assert "items" in result
        assert len(result["items"]) == 10
        for bp in result["items"]:
            assert bp.batch_id >= 1
            assert bp.batch_size > 0
            # All update/delete indices should have row_last_write entries
            for idx in bp.update_indices:
                assert int(idx) in bp.row_last_write
            for idx in bp.delete_indices:
                assert int(idx) in bp.row_last_write

    def test_precompute_consistency_across_batches(self):
        """Rows deleted in one batch should not appear in later batches."""
        from dbldatagen.v1.cdc import precompute_cdc_plans
        from dbldatagen.v1.cdc_schema import CDCPlan
        from dbldatagen.v1.schema import (
            ColumnSpec,
            DataGenPlan,
            DataType,
            PrimaryKey,
            SequenceColumn,
            TableSpec,
        )

        table = TableSpec(
            name="t",
            rows=500,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ],
        )
        config = CDCTableConfig(batch_size=50)
        plan = CDCPlan(
            base_plan=DataGenPlan(seed=42, tables=[table]),
            num_batches=10,
            table_configs={"t": config},
        )
        plans = precompute_cdc_plans(plan)["t"]

        # Collect all deleted row indices across all batches
        deleted_ever: set[int] = set()
        for bp in plans:
            # No deleted row should appear in updates of this or later batches
            upd_set = set(int(i) for i in bp.update_indices)
            overlap = deleted_ever & upd_set
            assert len(overlap) == 0, f"Batch {bp.batch_id}: previously deleted rows in updates: {overlap}"
            deleted_ever.update(int(i) for i in bp.delete_indices)
