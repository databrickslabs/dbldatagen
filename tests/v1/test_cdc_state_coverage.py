"""Tests covering uncovered lines in dbldatagen.v1.engine.cdc_state.

Pure-Python / numpy unit tests -- no Spark required.
"""

from __future__ import annotations

import numpy as np
import pytest

from dbldatagen.v1.cdc_schema import BatchPlan, CDCTableConfig, OperationWeights
from dbldatagen.v1.engine.cdc_state import (
    MEMMAP_THRESHOLD,
    TableState,
    _chunked_position_mapper,
    _rejection_position_mapper,
    advance_table_state,
    compute_table_state_at_batch,
    create_batch_plan,
    map_positions_to_absolute,
    resolve_batch_size,
    select_rows_for_batch,
)


# ---------------------------------------------------------------------------
# TableState: basic construction and properties (lines 60-61, 69, 141-154)
# ---------------------------------------------------------------------------


class TestTableStateBasics:
    def test_small_table_no_memmap(self):
        """Tables below threshold stay in-memory."""
        state = TableState(initial_rows=100)
        assert not state._using_memmap
        assert state.total_rows_ever == 100
        assert state.live_row_count == 100
        assert not state.has_deletes

    def test_total_rows_ever_includes_inserts(self):
        """total_rows_ever = initial + cumulative_inserts (line 142)."""
        state = TableState(initial_rows=50, cumulative_inserts=30)
        assert state.total_rows_ever == 80

    def test_live_row_count_minus_deletes(self):
        """live_row_count subtracts deleted indices (lines 146-148)."""
        state = TableState(initial_rows=100)
        state.deleted_indices.add(5)
        state.deleted_indices.add(10)
        assert state.live_row_count == 98

    def test_has_deletes_true(self):
        """has_deletes returns True when deleted_indices non-empty (lines 152-154)."""
        state = TableState(initial_rows=10)
        assert not state.has_deletes
        state.deleted_indices.add(3)
        assert state.has_deletes


# ---------------------------------------------------------------------------
# Memmap lifecycle (lines 60-61, 69, 73-82, 91-122, 126-131, 134)
# ---------------------------------------------------------------------------


class TestMemmapLifecycle:
    def test_init_memmap_above_threshold(self):
        """Tables >= MEMMAP_THRESHOLD use memmap (lines 60-61, 73-82)."""
        state = TableState(initial_rows=MEMMAP_THRESHOLD)
        try:
            assert state._using_memmap
            assert state._memmap_dir is not None
            assert state._memmap_dir.exists()
            assert state._row_last_write is not None
            assert state._deleted_flags is not None
            expected_max = int(MEMMAP_THRESHOLD * 1.2) + 1000
            assert state._max_rows == expected_max
        finally:
            state.cleanup()

    def test_cleanup_removes_files(self):
        """cleanup() removes memmap dir and nulls references (lines 126-131)."""
        state = TableState(initial_rows=MEMMAP_THRESHOLD)
        memmap_dir = state._memmap_dir
        assert memmap_dir is not None and memmap_dir.exists()
        state.cleanup()
        assert not memmap_dir.exists()
        assert state._memmap_dir is None
        assert state._row_last_write is None
        assert state._deleted_flags is None

    def test_cleanup_idempotent(self):
        """Calling cleanup twice is safe (lines 126-131)."""
        state = TableState(initial_rows=MEMMAP_THRESHOLD)
        state.cleanup()
        state.cleanup()  # should not raise

    def test_del_calls_cleanup(self):
        """__del__ delegates to cleanup (line 134)."""
        state = TableState(initial_rows=MEMMAP_THRESHOLD)
        memmap_dir = state._memmap_dir
        assert memmap_dir is not None
        state.__del__()
        assert not memmap_dir.exists()

    def test_grow_memmaps(self):
        """_grow_memmaps expands arrays and preserves data (lines 91-122)."""
        # Use a small threshold workaround: manually init memmap on a small state
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            # Write some data before growing
            state._row_last_write[0] = 5
            state._row_last_write[99] = 7
            state._deleted_flags[50] = 1

            old_max = state._max_rows
            state._grow_memmaps(old_max + 1000)
            assert state._max_rows > old_max
            # Data preserved
            assert state._row_last_write[0] == 5
            assert state._row_last_write[99] == 7
            assert state._deleted_flags[50] == 1
            # New slots are zero
            assert state._row_last_write[old_max + 500] == 0
            assert state._deleted_flags[old_max + 500] == 0
        finally:
            state.cleanup()

    def test_memmap_live_row_count(self):
        """live_row_count uses _delete_count for memmap (lines 146-148)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            assert state.live_row_count == 100
            state._deleted_flags[10] = 1
            state._delete_count = 1
            assert state.live_row_count == 99
        finally:
            state.cleanup()

    def test_memmap_has_deletes(self):
        """has_deletes uses _delete_count for memmap (lines 152-154)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            assert not state.has_deletes
            state._delete_count = 1
            assert state.has_deletes
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# last_write_batch (lines 162-164)
# ---------------------------------------------------------------------------


class TestLastWriteBatch:
    def test_returns_zero_when_no_array(self):
        """When _row_last_write is None, returns 0 (line 162-163)."""
        state = TableState(initial_rows=10)
        assert state.last_write_batch(0) == 0
        assert state.last_write_batch(9) == 0

    def test_returns_zero_for_out_of_range_index(self):
        """Out-of-range index returns 0 (line 162-163)."""
        state = TableState(initial_rows=10)
        state._row_last_write = np.zeros(5, dtype=np.int16)
        assert state.last_write_batch(10) == 0

    def test_returns_recorded_batch(self):
        """Returns recorded batch id (line 164)."""
        state = TableState(initial_rows=10)
        state._row_last_write = np.zeros(10, dtype=np.int16)
        state._row_last_write[3] = 5
        assert state.last_write_batch(3) == 5


# ---------------------------------------------------------------------------
# record_updates -- in-memory path (lines 168-187)
# ---------------------------------------------------------------------------


class TestRecordUpdatesInMemory:
    def test_empty_indices_noop(self):
        """Empty indices array is a no-op (line 168-169)."""
        state = TableState(initial_rows=10)
        state.record_updates(np.array([], dtype=np.int64), 1)
        assert state._row_last_write is None

    def test_creates_array_on_first_call(self):
        """First update allocates _row_last_write (line 181-182)."""
        state = TableState(initial_rows=10)
        state.record_updates(np.array([3, 7], dtype=np.int64), 2)
        assert state._row_last_write is not None
        assert state._row_last_write[3] == 2
        assert state._row_last_write[7] == 2
        assert state._row_last_write[0] == 0

    def test_grows_array_when_needed(self):
        """Array grows when total_rows_ever exceeds current length (lines 183-186)."""
        state = TableState(initial_rows=5)
        state.record_updates(np.array([0], dtype=np.int64), 1)
        assert len(state._row_last_write) == 5

        state.cumulative_inserts = 10  # total_rows_ever = 15
        state.record_updates(np.array([12], dtype=np.int64), 2)
        assert len(state._row_last_write) >= 15
        assert state._row_last_write[12] == 2
        # Old data preserved
        assert state._row_last_write[0] == 1


# ---------------------------------------------------------------------------
# record_updates -- memmap path (lines 171-177)
# ---------------------------------------------------------------------------


class TestRecordUpdatesMemmap:
    def test_memmap_record_updates(self):
        """Updates on memmap-backed state (lines 171-177)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            state.record_updates(np.array([10, 20], dtype=np.int64), 3)
            assert state._row_last_write[10] == 3
            assert state._row_last_write[20] == 3
        finally:
            state.cleanup()

    def test_memmap_record_updates_triggers_grow(self):
        """Memmap grow is triggered when total_rows_ever > _max_rows (lines 172-174)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            # Force total_rows_ever to exceed _max_rows
            state.cumulative_inserts = state._max_rows + 100
            state.record_updates(np.array([0], dtype=np.int64), 1)
            assert state._max_rows >= state.total_rows_ever
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# record_delete (lines 191-197)
# ---------------------------------------------------------------------------


class TestRecordDelete:
    def test_in_memory_delete(self):
        """In-memory delete adds to deleted_indices (line 197)."""
        state = TableState(initial_rows=10)
        state.record_delete(5)
        assert 5 in state.deleted_indices

    def test_memmap_delete(self):
        """Memmap delete sets flag and increments count (lines 191-195)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            state.record_delete(42)
            assert state._deleted_flags[42] == 1
            assert state._delete_count == 1
            # Deleting same row again doesn't double-count
            state.record_delete(42)
            assert state._delete_count == 1
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# get_live_indices (lines 205-212)
# ---------------------------------------------------------------------------


class TestGetLiveIndices:
    def test_no_deletes(self):
        """Without deletes, returns arange (line 207-208)."""
        state = TableState(initial_rows=10)
        live = state.get_live_indices()
        np.testing.assert_array_equal(live, np.arange(10))

    def test_with_deletes(self):
        """With deletes, removes deleted indices (lines 209-212)."""
        state = TableState(initial_rows=10)
        state.deleted_indices = {2, 5, 8}
        live = state.get_live_indices()
        expected = np.array([0, 1, 3, 4, 6, 7, 9], dtype=np.int64)
        np.testing.assert_array_equal(live, expected)

    def test_memmap_delegates_to_chunked(self):
        """Memmap path calls _get_live_indices_chunked (line 205-206)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            state._deleted_flags[10] = 1
            state._deleted_flags[50] = 1
            state._delete_count = 2
            live = state.get_live_indices()
            assert len(live) == 98
            assert 10 not in live
            assert 50 not in live
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# _get_live_indices_chunked (lines 216-233)
# ---------------------------------------------------------------------------


class TestGetLiveIndicesChunked:
    def test_no_deletes_chunked(self):
        """No deletes in memmap returns full arange (line 220-221)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            live = state._get_live_indices_chunked()
            np.testing.assert_array_equal(live, np.arange(100))
        finally:
            state.cleanup()

    def test_with_deletes_chunked(self):
        """Deletes are excluded in chunked scanner (lines 222-233)."""
        state = TableState(initial_rows=50)
        state._init_memmap()
        try:
            for i in [5, 15, 25]:
                state._deleted_flags[i] = 1
                state._delete_count += 1
            live = state._get_live_indices_chunked()
            assert len(live) == 47
            for i in [5, 15, 25]:
                assert i not in live
        finally:
            state.cleanup()

    def test_rows_beyond_flags_are_live(self):
        """Rows beyond _deleted_flags length are treated as live (lines 229-230)."""
        state = TableState(initial_rows=50)
        state._init_memmap()
        try:
            # Simulate inserts beyond the flags array
            state.cumulative_inserts = state._max_rows + 10
            state._deleted_flags[0] = 1
            state._delete_count = 1
            live = state._get_live_indices_chunked()
            total_expected = state.total_rows_ever - 1
            assert len(live) == total_expected
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# copy (lines 241-253)
# ---------------------------------------------------------------------------


class TestTableStateCopy:
    def test_copy_basic(self):
        """Copy produces independent state (lines 246-253)."""
        state = TableState(initial_rows=20, cumulative_inserts=5)
        state.deleted_indices = {3, 7}
        state.record_updates(np.array([1, 4], dtype=np.int64), 2)
        copied = state.copy()

        assert copied.initial_rows == 20
        assert copied.cumulative_inserts == 5
        assert copied.deleted_indices == {3, 7}
        assert copied._row_last_write is not None
        assert copied._row_last_write[1] == 2
        # Mutating copy doesn't affect original
        copied.deleted_indices.add(0)
        assert 0 not in state.deleted_indices

    def test_copy_without_writes(self):
        """Copy when _row_last_write is None (line 251-252)."""
        state = TableState(initial_rows=10)
        copied = state.copy()
        assert copied._row_last_write is None

    def test_copy_raises_for_memmap(self):
        """Copy raises NotImplementedError for memmap state (line 242-245)."""
        state = TableState(initial_rows=100)
        state._init_memmap()
        try:
            with pytest.raises(NotImplementedError, match="memmap-backed"):
                state.copy()
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# map_positions_to_absolute (lines 275-283)
# ---------------------------------------------------------------------------


class TestMapPositionsToAbsolute:
    def test_no_deletes_returns_copy(self):
        """Without deletes, returns positions as-is (line 275-276)."""
        state = TableState(initial_rows=20)
        positions = np.array([0, 5, 10], dtype=np.int64)
        result = map_positions_to_absolute(state, positions)
        np.testing.assert_array_equal(result, positions)
        # Verify it's a copy, not the same object
        assert result is not positions

    def test_in_memory_with_deletes(self):
        """In-memory table with deletes maps through live_indices (lines 281-283)."""
        state = TableState(initial_rows=10)
        state.deleted_indices = {2, 5}
        # live indices: [0,1,3,4,6,7,8,9]
        positions = np.array([0, 2, 5], dtype=np.int64)
        result = map_positions_to_absolute(state, positions)
        expected = np.array([0, 3, 7], dtype=np.int64)
        np.testing.assert_array_equal(result, expected)

    def test_memmap_with_deletes_delegates(self):
        """Memmap table with deletes uses chunked mapper (lines 278-279)."""
        state = TableState(initial_rows=50)
        state._init_memmap()
        try:
            state._deleted_flags[10] = 1
            state._delete_count = 1
            # 49 live rows, request position 0 and 48
            positions = np.array([0, 48], dtype=np.int64)
            result = map_positions_to_absolute(state, positions)
            assert len(result) == 2
            assert result[0] == 0
            # Position 48 should map to index 49 (since 10 is deleted)
            assert result[1] == 49
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# _rejection_position_mapper (lines 298-341)
# ---------------------------------------------------------------------------


class TestRejectionPositionMapper:
    def test_basic_mapping(self):
        """Block-indexed binary search maps positions correctly (lines 298-341)."""
        state = TableState(initial_rows=50)
        state._init_memmap()
        try:
            # Delete indices 5, 10, 15
            for i in [5, 10, 15]:
                state._deleted_flags[i] = 1
                state._delete_count += 1
            # live: 47 rows
            positions = np.array([0, 1, 46], dtype=np.int64)
            result = _rejection_position_mapper(state, positions)
            assert len(result) == 3
            # Position 0 → absolute 0
            assert result[0] == 0
            # Position 1 → absolute 1
            assert result[1] == 1
            # Position 46 → last live row = 49
            assert result[2] == 49
        finally:
            state.cleanup()

    def test_preserves_order(self):
        """Output preserves input order even when positions unsorted (lines 338-341)."""
        state = TableState(initial_rows=20)
        state._init_memmap()
        try:
            state._deleted_flags[5] = 1
            state._delete_count = 1
            # Request positions in reverse order
            positions = np.array([18, 10, 0], dtype=np.int64)
            result = _rejection_position_mapper(state, positions)
            # Re-run with sorted to verify values match
            sorted_pos = np.array([0, 10, 18], dtype=np.int64)
            sorted_result = _rejection_position_mapper(state, sorted_pos)
            assert result[0] == sorted_result[2]
            assert result[1] == sorted_result[1]
            assert result[2] == sorted_result[0]
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# _chunked_position_mapper (lines 357-415)
# ---------------------------------------------------------------------------


class TestChunkedPositionMapper:
    def test_basic_mapping(self):
        """Chunked mapper resolves positions through deleted flags (lines 357-415)."""
        state = TableState(initial_rows=30)
        state._init_memmap()
        try:
            state._deleted_flags[3] = 1
            state._deleted_flags[7] = 1
            state._delete_count = 2
            # 28 live rows
            positions = np.array([0, 1, 2, 27], dtype=np.int64)
            result = _chunked_position_mapper(state, positions)
            assert result[0] == 0
            assert result[1] == 1
            assert result[2] == 2
            assert result[3] == 29  # last live row

        finally:
            state.cleanup()

    def test_rows_beyond_flags(self):
        """Phase 2: rows beyond _deleted_flags are all live (lines 391-400)."""
        state = TableState(initial_rows=20)
        state._init_memmap()
        try:
            state.cumulative_inserts = state._max_rows + 50
            state._deleted_flags[0] = 1
            state._delete_count = 1
            total_live = state.live_row_count
            # Request last position
            positions = np.array([total_live - 1], dtype=np.int64)
            result = _chunked_position_mapper(state, positions)
            assert result[0] == state.total_rows_ever - 1
        finally:
            state.cleanup()

    def test_unresolved_raises(self):
        """Unresolved positions raise RuntimeError (lines 403-410)."""
        state = TableState(initial_rows=10)
        state._init_memmap()
        try:
            # Request position beyond live range
            positions = np.array([100], dtype=np.int64)
            with pytest.raises(RuntimeError, match="unresolved"):
                _chunked_position_mapper(state, positions)
        finally:
            state.cleanup()

    def test_preserves_order(self):
        """Output preserves original order (lines 412-415)."""
        state = TableState(initial_rows=20)
        state._init_memmap()
        try:
            state._deleted_flags[5] = 1
            state._delete_count = 1
            positions = np.array([18, 5, 0], dtype=np.int64)
            result = _chunked_position_mapper(state, positions)
            # position 0 → index 0
            assert result[2] == 0
            # Confirm all results are valid indices
            assert all(0 <= r < state.total_rows_ever for r in result)
        finally:
            state.cleanup()


# ---------------------------------------------------------------------------
# select_rows_for_batch (lines 441-496)
# ---------------------------------------------------------------------------


class TestSelectRowsForBatch:
    def test_zero_live_rows(self):
        """All rows deleted → only inserts returned (line 442-443)."""
        state = TableState(initial_rows=5)
        state.deleted_indices = set(range(5))
        updates, deletes, inserts = select_rows_for_batch(
            "t", 1, state, 10, OperationWeights(insert=1, update=1, delete=1), 42
        )
        assert len(updates) == 0
        assert len(deletes) == 0
        assert inserts == 10

    def test_insert_only_weights(self):
        """With only insert weight, no updates/deletes (line 458-459)."""
        state = TableState(initial_rows=100)
        updates, deletes, inserts = select_rows_for_batch(
            "t", 1, state, 20, OperationWeights(insert=1, update=0, delete=0), 42
        )
        assert len(updates) == 0
        assert len(deletes) == 0
        assert inserts == 20

    def test_mixed_operations(self):
        """Mixed weights produce updates, deletes, and inserts (lines 441-496)."""
        state = TableState(initial_rows=100)
        updates, deletes, inserts = select_rows_for_batch(
            "t", 1, state, 30, OperationWeights(insert=3, update=5, delete=2), 42
        )
        assert len(updates) > 0
        assert len(deletes) > 0
        assert inserts > 0
        assert len(updates) + len(deletes) + inserts == 30

    def test_indices_within_bounds(self):
        """All returned indices are within [0, total_rows_ever) (lines 478-494)."""
        state = TableState(initial_rows=50)
        updates, deletes, _inserts = select_rows_for_batch(
            "t", 1, state, 20, OperationWeights(insert=3, update=5, delete=2), 42
        )
        total = state.total_rows_ever
        for idx in updates:
            assert 0 <= idx < total
        for idx in deletes:
            assert 0 <= idx < total

    def test_with_prior_deletes(self):
        """Handles state that already has deletes (lines 469-471)."""
        state = TableState(initial_rows=50)
        state.deleted_indices = {0, 1, 2, 3, 4}
        updates, deletes, _inserts = select_rows_for_batch(
            "t", 1, state, 15, OperationWeights(insert=3, update=5, delete=2), 42
        )
        # All update/delete indices should be live (not in deleted set)
        for idx in updates:
            assert idx not in state.deleted_indices
        for idx in deletes:
            assert idx not in state.deleted_indices

    def test_deterministic(self):
        """Same inputs produce same outputs."""
        state = TableState(initial_rows=100)
        args = ("t", 1, state, 30, OperationWeights(insert=3, update=5, delete=2), 42)
        u1, d1, i1 = select_rows_for_batch(*args)
        u2, d2, i2 = select_rows_for_batch(*args)
        np.testing.assert_array_equal(u1, u2)
        np.testing.assert_array_equal(d1, d2)
        assert i1 == i2


# ---------------------------------------------------------------------------
# resolve_batch_size (lines 507-509)
# ---------------------------------------------------------------------------


class TestResolveBatchSize:
    def test_string_spec(self):
        """String spec calls parse_human_count (lines 507-509)."""
        result = resolve_batch_size("100", 1000)
        assert result == 100

    def test_float_fraction(self):
        """Float < 1.0 treated as fraction (line 510-511)."""
        assert resolve_batch_size(0.5, 200) == 100

    def test_float_fraction_minimum_one(self):
        """Fraction on tiny table returns at least 1."""
        assert resolve_batch_size(0.01, 5) == 1

    def test_int_passthrough(self):
        """Integer passed through directly (line 512)."""
        assert resolve_batch_size(50, 1000) == 50


# ---------------------------------------------------------------------------
# advance_table_state (lines 527-539)
# ---------------------------------------------------------------------------


class TestAdvanceTableState:
    def test_advance_modifies_state(self):
        """advance_table_state applies one batch in place (lines 527-539)."""
        state = TableState(initial_rows=100)
        config = CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size=20,
        )
        advance_table_state(state, "orders", 1, config, 42)
        # State should have changed
        assert state.cumulative_inserts > 0 or state.has_deletes or state._row_last_write is not None

    def test_advance_accumulates_inserts(self):
        """Inserts accumulate over multiple batches."""
        state = TableState(initial_rows=100)
        config = CDCTableConfig(
            operations=OperationWeights(insert=1, update=0, delete=0),
            batch_size=10,
        )
        advance_table_state(state, "t", 1, config, 42)
        assert state.cumulative_inserts == 10
        advance_table_state(state, "t", 2, config, 42)
        assert state.cumulative_inserts == 20

    def test_advance_records_deletes(self):
        """Deletes are recorded in state."""
        state = TableState(initial_rows=100)
        config = CDCTableConfig(
            operations=OperationWeights(insert=0, update=0, delete=1),
            batch_size=10,
        )
        advance_table_state(state, "t", 1, config, 42)
        assert state.has_deletes
        assert len(state.deleted_indices) > 0

    def test_advance_records_updates(self):
        """Updates set _row_last_write entries."""
        state = TableState(initial_rows=100)
        config = CDCTableConfig(
            operations=OperationWeights(insert=0, update=1, delete=0),
            batch_size=10,
        )
        advance_table_state(state, "t", 1, config, 42)
        assert state._row_last_write is not None
        # At least some rows should have batch_id=1
        assert np.any(state._row_last_write == 1)


# ---------------------------------------------------------------------------
# create_batch_plan (lines 555-572)
# ---------------------------------------------------------------------------


class TestCreateBatchPlan:
    def test_creates_valid_plan(self):
        """create_batch_plan returns a BatchPlan with correct fields (lines 555-582)."""
        state = TableState(initial_rows=50)
        config = CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size=15,
        )
        plan = create_batch_plan("orders", 1, state, config, 42, timestamp="2025-01-01T00:00:00Z")
        assert isinstance(plan, BatchPlan)
        assert plan.batch_id == 1
        assert plan.table_name == "orders"
        assert plan.batch_size == 15
        assert plan.timestamp == "2025-01-01T00:00:00Z"
        assert plan.insert_start_index == 50
        assert plan.insert_count + len(plan.update_indices) + len(plan.delete_indices) == 15

    def test_row_last_write_populated(self):
        """row_last_write contains entries for update/delete indices (lines 566-570)."""
        state = TableState(initial_rows=100)
        # Pre-record some writes
        state.record_updates(np.array([10, 20, 30], dtype=np.int64), 1)
        state.cumulative_inserts = 0

        config = CDCTableConfig(
            operations=OperationWeights(insert=0, update=1, delete=0),
            batch_size=10,
        )
        plan = create_batch_plan("t", 2, state, config, 42)
        # Every index in update_indices should have an entry
        for idx in plan.update_indices:
            assert int(idx) in plan.row_last_write

    def test_insert_only_plan(self):
        """Insert-only plan has no updates or deletes."""
        state = TableState(initial_rows=50)
        config = CDCTableConfig(
            operations=OperationWeights(insert=1, update=0, delete=0),
            batch_size=10,
        )
        plan = create_batch_plan("t", 1, state, config, 42)
        assert len(plan.update_indices) == 0
        assert len(plan.delete_indices) == 0
        assert plan.insert_count == 10


# ---------------------------------------------------------------------------
# compute_table_state_at_batch (lines 596-601)
# ---------------------------------------------------------------------------


class TestComputeTableStateAtBatch:
    def test_batch_zero(self):
        """At batch 0, state is just initial rows (line 596-601)."""
        config = CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size=10,
        )
        state = compute_table_state_at_batch("t", 100, config, 0, 42)
        assert state.initial_rows == 100
        assert state.cumulative_inserts == 0
        assert state.live_row_count == 100

    def test_multiple_batches(self):
        """Replaying multiple batches produces consistent state (lines 596-601)."""
        config = CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size=20,
        )
        state = compute_table_state_at_batch("orders", 50, config, 5, 42)
        assert state.initial_rows == 50
        # After 5 batches of insert-heavy ops, should have inserts
        assert state.cumulative_inserts > 0
        assert state.live_row_count > 0

    def test_deterministic(self):
        """Same inputs produce same final state."""
        config = CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size=10,
        )
        s1 = compute_table_state_at_batch("t", 100, config, 3, 42)
        s2 = compute_table_state_at_batch("t", 100, config, 3, 42)
        assert s1.cumulative_inserts == s2.cumulative_inserts
        assert s1.deleted_indices == s2.deleted_indices
        assert s1.live_row_count == s2.live_row_count

    def test_incremental_matches_replay(self):
        """Advancing state step-by-step matches compute_table_state_at_batch."""
        config = CDCTableConfig(
            operations=OperationWeights(insert=3, update=5, delete=2),
            batch_size=10,
        )
        # Manual advance
        state = TableState(initial_rows=100)
        for b in range(1, 4):
            advance_table_state(state, "t", b, config, 42)

        # Replay
        replayed = compute_table_state_at_batch("t", 100, config, 3, 42)

        assert state.cumulative_inserts == replayed.cumulative_inserts
        assert state.deleted_indices == replayed.deleted_indices
        assert state.live_row_count == replayed.live_row_count
