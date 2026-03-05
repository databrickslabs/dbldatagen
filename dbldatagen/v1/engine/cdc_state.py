"""Stateful tracking of table rows across CDC batches.

Computes which rows are live, inserted, updated, and deleted at each batch
using only seed-based metadata -- no DataFrames materialised.

For tables exceeding ``MEMMAP_THRESHOLD`` rows, state is stored in
memory-mapped files (``np.memmap``) so the OS manages paging.  This
allows billion-row tables to run with ~1.5 GB of resident RAM instead
of 50+ GB.
"""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from dbldatagen.v1.cdc_schema import BatchPlan, CDCTableConfig, OperationWeights
from dbldatagen.v1.engine.columns.pk import feistel_permute_batch
from dbldatagen.v1.engine.seed import derive_column_seed


# ---------------------------------------------------------------------------
# Threshold for switching to disk-backed (memmap) state.
# Below this, everything stays in RAM (existing fast path).
# ---------------------------------------------------------------------------
MEMMAP_THRESHOLD = 10_000_000  # 10 M rows


@dataclass
class TableState:
    """Logical state of a table at a point in the CDC stream.

    Uses a numpy int16 array for ``_row_last_write`` instead of a Python
    dict, eliminating millions of Python objects and GC pressure on the
    driver.  Supports up to 32 767 batches with int16.

    For tables with ``initial_rows >= MEMMAP_THRESHOLD``, arrays are
    backed by memory-mapped files so the OS manages paging.
    """

    initial_rows: int
    cumulative_inserts: int = 0
    deleted_indices: set[int] = field(default_factory=set)
    # numpy int16 array: row_index → last batch_id that wrote it (0 = initial).
    # Allocated lazily on first update to avoid wasting memory on
    # insert-only plans.
    _row_last_write: np.ndarray | None = field(default=None, repr=False, init=False)

    # --- Disk-backed (memmap) fields ---
    _memmap_dir: Path | None = field(default=None, repr=False, init=False)
    _deleted_flags: np.ndarray | None = field(default=None, repr=False, init=False)
    _delete_count: int = field(default=0, repr=False, init=False)
    _max_rows: int = field(default=0, repr=False, init=False)

    def __post_init__(self) -> None:
        if self.initial_rows >= MEMMAP_THRESHOLD:
            self._init_memmap()

    # ------------------------------------------------------------------
    # Memmap lifecycle
    # ------------------------------------------------------------------

    @property
    def _using_memmap(self) -> bool:
        return self._memmap_dir is not None

    def _init_memmap(self) -> None:
        """Initialise memory-mapped files for large-table state."""
        self._memmap_dir = Path(tempfile.mkdtemp(prefix="synth_cdc_"))
        # Pre-allocate 20 % headroom for inserts
        self._max_rows = int(self.initial_rows * 1.2) + 1000
        self._row_last_write = np.memmap(
            self._memmap_dir / "last_write.bin",
            dtype=np.int16,
            mode="w+",
            shape=(self._max_rows,),
        )
        self._deleted_flags = np.memmap(
            self._memmap_dir / "deleted.bin",
            dtype=np.uint8,
            mode="w+",
            shape=(self._max_rows,),
        )

    def _grow_memmaps(self, needed: int) -> None:
        """Grow memmap files to accommodate *needed* rows."""
        new_max = max(needed, int(self._max_rows * 1.5))
        gen = getattr(self, "_memmap_gen", 0) + 1
        self._memmap_gen = gen

        # Grow _row_last_write
        old_lw = self._row_last_write
        lw_path = self._memmap_dir / f"last_write_{gen}.bin"
        new_lw = np.memmap(lw_path, dtype=np.int16, mode="w+", shape=(new_max,))
        new_lw[: self._max_rows] = old_lw[: self._max_rows]
        new_lw.flush()
        old_lw_path = getattr(old_lw, "filename", None)
        del old_lw
        self._row_last_write = new_lw
        if old_lw_path and Path(old_lw_path) != lw_path:
            Path(old_lw_path).unlink(missing_ok=True)

        # Grow _deleted_flags
        old_df = self._deleted_flags
        df_path = self._memmap_dir / f"deleted_{gen}.bin"
        new_df = np.memmap(df_path, dtype=np.uint8, mode="w+", shape=(new_max,))
        new_df[: self._max_rows] = old_df[: self._max_rows]
        new_df.flush()
        old_df_path = getattr(old_df, "filename", None)
        del old_df
        self._deleted_flags = new_df
        if old_df_path and Path(old_df_path) != df_path:
            Path(old_df_path).unlink(missing_ok=True)

        self._max_rows = new_max

    def cleanup(self) -> None:
        """Explicitly remove memmap files.  Called by ``__del__`` too."""
        if self._memmap_dir is not None and self._memmap_dir.exists():
            # Release memmap references so the OS can unlink
            self._row_last_write = None
            self._deleted_flags = None
            shutil.rmtree(self._memmap_dir, ignore_errors=True)
            self._memmap_dir = None

    def __del__(self) -> None:
        self.cleanup()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def total_rows_ever(self) -> int:
        return self.initial_rows + self.cumulative_inserts

    @property
    def live_row_count(self) -> int:
        if self._using_memmap:
            return self.total_rows_ever - self._delete_count
        return self.total_rows_ever - len(self.deleted_indices)

    @property
    def has_deletes(self) -> bool:
        if self._using_memmap:
            return self._delete_count > 0
        return len(self.deleted_indices) > 0

    # ------------------------------------------------------------------
    # Read / write operations
    # ------------------------------------------------------------------

    def last_write_batch(self, row_index: int) -> int:
        """Return the most recent batch that wrote *row_index* (0 = initial)."""
        if self._row_last_write is None or row_index >= len(self._row_last_write):
            return 0
        return int(self._row_last_write[row_index])

    def record_updates(self, indices: np.ndarray, batch_id: int) -> None:
        """Record that *indices* were last written at *batch_id*."""
        if len(indices) == 0:
            return

        if self._using_memmap:
            needed = self.total_rows_ever
            if needed > self._max_rows:
                self._grow_memmaps(needed)
            self._row_last_write[indices.astype(np.int64)] = np.int16(batch_id)
            return

        # In-memory path (unchanged)
        needed = self.total_rows_ever
        if self._row_last_write is None:
            self._row_last_write = np.zeros(needed, dtype=np.int16)
        elif len(self._row_last_write) < needed:
            new = np.zeros(needed, dtype=np.int16)
            new[: len(self._row_last_write)] = self._row_last_write
            self._row_last_write = new
        self._row_last_write[indices.astype(np.int64)] = np.int16(batch_id)

    def record_delete(self, row_index: int) -> None:
        """Record that *row_index* has been deleted."""
        if self._using_memmap:
            if self._deleted_flags[row_index] == 0:
                self._deleted_flags[row_index] = 1
                self._delete_count += 1
        else:
            self.deleted_indices.add(row_index)

    # ------------------------------------------------------------------
    # Live-index retrieval
    # ------------------------------------------------------------------

    def get_live_indices(self) -> np.ndarray:
        """Return sorted array of all live row indices."""
        if self._using_memmap:
            return self._get_live_indices_chunked()
        if not self.has_deletes:
            return np.arange(self.total_rows_ever, dtype=np.int64)
        all_ids = np.arange(self.total_rows_ever, dtype=np.int64)
        deleted = np.fromiter(self.deleted_indices, dtype=np.int64, count=len(self.deleted_indices))
        deleted.sort()
        return np.setdiff1d(all_ids, deleted, assume_unique=True)

    def _get_live_indices_chunked(self) -> np.ndarray:
        """Chunked scanner for memmap state.  Memory: O(chunk_size)."""
        n = self.total_rows_ever
        flags_len = len(self._deleted_flags) if self._deleted_flags is not None else 0
        scan_end = min(n, flags_len)
        chunk_size = 10_000_000
        if not self.has_deletes:
            return np.arange(n, dtype=np.int64)
        live_chunks: list[np.ndarray] = []
        for start in range(0, scan_end, chunk_size):
            end = min(start + chunk_size, scan_end)
            mask = self._deleted_flags[start:end] == 0
            live_chunks.append(np.where(mask)[0].astype(np.int64) + start)
        # Rows beyond _deleted_flags are recent inserts — all live
        if scan_end < n:
            live_chunks.append(np.arange(scan_end, n, dtype=np.int64))
        if live_chunks:
            return np.concatenate(live_chunks)
        return np.array([], dtype=np.int64)

    # ------------------------------------------------------------------
    # Copy
    # ------------------------------------------------------------------

    def copy(self) -> TableState:
        """Efficient copy without deepcopy overhead."""
        if self._using_memmap:
            raise NotImplementedError(
                "copy() is not supported for memmap-backed state. "
                "Use advance_table_state() for incremental computation."
            )
        new = TableState(
            initial_rows=self.initial_rows,
            cumulative_inserts=self.cumulative_inserts,
            deleted_indices=self.deleted_indices.copy(),
        )
        if self._row_last_write is not None:
            new._row_last_write = self._row_last_write.copy()
        return new


# ---------------------------------------------------------------------------
# Chunked position-to-absolute mapper
# ---------------------------------------------------------------------------


def map_positions_to_absolute(
    state: TableState,
    positions: np.ndarray,
) -> np.ndarray:
    """Map live-set positions to absolute row indices.

    For tables without deletes, positions *are* absolute indices.
    For memmap-backed tables with deletes and low delete ratio, uses
    rejection sampling — O(len(positions)) flag lookups instead of
    scanning the full array.
    For memmap-backed tables with high delete ratio (>50%), falls back
    to chunked scanning.
    For small in-memory tables, uses the full ``get_live_indices()``.
    """
    if not state.has_deletes:
        return positions.copy()

    if state._using_memmap:
        return _chunked_position_mapper(state, positions)

    # Small in-memory table — full array is fine
    live = state.get_live_indices()
    return live[positions]


def _rejection_position_mapper(
    state: TableState,
    positions: np.ndarray,
) -> np.ndarray:
    """Map live-set positions via block-indexed binary search.

    Builds a prefix live-count at 1M-row block boundaries (only ~260
    entries for 258M rows), then binary-searches each position into the
    right block and scans only that 1M block.  Total work is
    O(n/block_size + len(positions) * block_size) which for 950K
    positions in a 258M-row table scans ~50-100 blocks instead of all 258.
    """
    n = state.total_rows_ever
    flags = state._deleted_flags

    # Build prefix live-count at block boundaries — O(n) but only once
    block_size = 1_000_000
    n_blocks = (n + block_size - 1) // block_size
    prefix_live = np.empty(n_blocks + 1, dtype=np.int64)
    prefix_live[0] = 0
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        prefix_live[i + 1] = prefix_live[i] + int(np.sum(flags[start:end] == 0))

    # Sort positions for cache-friendly scanning; group by block
    sort_order = np.argsort(positions)
    sorted_pos = positions[sort_order]

    # Vectorized binary search: find which block each position belongs to
    block_indices = np.searchsorted(prefix_live[1:], sorted_pos, side="right")

    result = np.empty(len(positions), dtype=np.int64)

    # Process positions block-by-block (only load each block once)
    current_block = -1
    live_mask = None
    block_start = 0

    for idx in range(len(sorted_pos)):
        bi = int(block_indices[idx])
        if bi != current_block:
            current_block = bi
            block_start = bi * block_size
            block_end = min(block_start + block_size, n)
            live_mask = np.where(flags[block_start:block_end] == 0)[0]

        local_offset = int(sorted_pos[idx] - prefix_live[bi])
        result[idx] = block_start + live_mask[local_offset]

    # Restore original order
    inverse = np.empty_like(sort_order)
    inverse[sort_order] = np.arange(len(sort_order))
    return result[inverse]


def _chunked_position_mapper(
    state: TableState,
    positions: np.ndarray,
) -> np.ndarray:
    """Map positions through live-set using chunked scanning of deleted_flags.

    Scans 10M rows at a time, accumulating live-row count, and resolves
    each position to its absolute index without building the full
    live-index array.

    Rows beyond the ``_deleted_flags`` array are treated as live (they are
    recently inserted rows that cannot have been deleted yet).
    """
    n = state.total_rows_ever
    flags_len = len(state._deleted_flags) if state._deleted_flags is not None else 0
    scan_end = min(n, flags_len)
    chunk_size = 10_000_000

    # Sort positions for single-pass scanning
    sort_order = np.argsort(positions)
    sorted_pos = positions[sort_order]

    result = np.full(len(positions), -1, dtype=np.int64)
    pos_cursor = 0
    live_count = 0

    # Phase 1: scan _deleted_flags for rows [0, scan_end)
    for start in range(0, scan_end, chunk_size):
        if pos_cursor >= len(sorted_pos):
            break
        end = min(start + chunk_size, scan_end)
        mask = state._deleted_flags[start:end] == 0
        live_in_chunk = np.where(mask)[0].astype(np.int64) + start
        chunk_live = len(live_in_chunk)

        # Resolve all positions that fall within this chunk's live range
        while pos_cursor < len(sorted_pos):
            p = sorted_pos[pos_cursor]
            if p < live_count + chunk_live:
                result[pos_cursor] = live_in_chunk[p - live_count]
                pos_cursor += 1
            else:
                break
        live_count += chunk_live

    # Phase 2: rows [scan_end, n) are beyond _deleted_flags — all live
    if pos_cursor < len(sorted_pos) and scan_end < n:
        extra_live = n - scan_end
        while pos_cursor < len(sorted_pos):
            p = sorted_pos[pos_cursor]
            if p < live_count + extra_live:
                result[pos_cursor] = scan_end + (p - live_count)
                pos_cursor += 1
            else:
                break
        live_count += extra_live

    # Validate: all positions must have been resolved
    if pos_cursor < len(sorted_pos):
        unresolved = len(sorted_pos) - pos_cursor
        raise RuntimeError(
            f"_chunked_position_mapper: {unresolved}/{len(sorted_pos)} positions "
            f"unresolved. live_count={live_count}, expected n_live="
            f"{state.live_row_count}, total_rows_ever={n}, "
            f"_delete_count={state._delete_count}, max_pos={int(sorted_pos[-1])}"
        )

    # Restore original order
    inverse = np.empty_like(sort_order)
    inverse[sort_order] = np.arange(len(sort_order))
    return result[inverse]


# ---------------------------------------------------------------------------
# Row selection
# ---------------------------------------------------------------------------


def select_rows_for_batch(
    table_name: str,
    batch_id: int,
    state: TableState,
    batch_size: int,
    op_weights: OperationWeights,
    global_seed: int,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Determine which rows are updated, deleted, and how many are inserted.

    Returns ``(update_row_indices, delete_row_indices, insert_count)`` where
    update/delete indices are *absolute* row indices (not positions in the
    live array).

    Accepts a ``TableState`` directly.  When the table has no deletes,
    avoids allocating the full ``live_indices`` array (which can be
    hundreds of millions of entries for large tables).
    """
    n_live = state.live_row_count
    if n_live == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64), batch_size

    from dbldatagen.v1.engine.utils import split_with_remainder

    ins_frac, upd_frac, del_frac = op_weights.fractions
    insert_count, update_count, delete_count = split_with_remainder(
        batch_size,
        (ins_frac, upd_frac, del_frac),
    )

    # Cap to available live rows
    change_count = min(update_count + delete_count, n_live)
    update_count = min(update_count, change_count)
    delete_count = min(delete_count, change_count - update_count)

    if change_count == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64), insert_count

    # Deterministic permutation of positions in live_indices
    batch_seed = derive_column_seed(global_seed, table_name, f"__batch_{batch_id}")
    positions = np.arange(change_count, dtype=np.int64)
    perm = feistel_permute_batch(positions, n_live, batch_seed)

    update_positions = perm[:update_count]
    delete_positions = perm[update_count : update_count + delete_count]

    if state.has_deletes:
        update_abs = map_positions_to_absolute(state, update_positions)
        delete_abs = map_positions_to_absolute(state, delete_positions)
    else:
        # No deletes: live indices are 0..n-1, positions ARE absolute
        update_abs = update_positions
        delete_abs = delete_positions

    # Safety: validate all indices are within bounds
    total = state.total_rows_ever
    if len(update_abs) > 0:
        u_max = int(update_abs.max())
        if u_max >= total or int(update_abs.min()) < 0:
            raise RuntimeError(
                f"select_rows_for_batch: update index out of range "
                f"[0, {total}): min={int(update_abs.min())}, max={u_max}, "
                f"batch={batch_id}, n_live={n_live}"
            )
    if len(delete_abs) > 0:
        d_max = int(delete_abs.max())
        if d_max >= total or int(delete_abs.min()) < 0:
            raise RuntimeError(
                f"select_rows_for_batch: delete index out of range "
                f"[0, {total}): min={int(delete_abs.min())}, max={d_max}, "
                f"batch={batch_id}, n_live={n_live}"
            )

    return update_abs, delete_abs, insert_count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_batch_size(batch_size_spec: int | float, initial_rows: int) -> int:
    """Convert a batch_size spec (int or float fraction) to an absolute count."""
    if isinstance(batch_size_spec, float) and batch_size_spec < 1.0:
        return max(1, int(initial_rows * batch_size_spec))
    return int(batch_size_spec)


def advance_table_state(
    state: TableState,
    table_name: str,
    batch_id: int,
    config: CDCTableConfig,
    global_seed: int,
) -> None:
    """Advance *state* forward by one batch IN PLACE.

    Applies batch *batch_id* to the state, recording updates, deletes,
    and inserts.  Used for incremental state computation.
    """
    bs = resolve_batch_size(config.batch_size, state.initial_rows)
    updates, deletes, inserts = select_rows_for_batch(
        table_name,
        batch_id,
        state,
        bs,
        config.operations,
        global_seed,
    )
    state.record_updates(updates, batch_id)
    for idx in deletes:
        state.record_delete(int(idx))
    state.cumulative_inserts += inserts


def create_batch_plan(
    table_name: str,
    batch_id: int,
    state: TableState,
    config: CDCTableConfig,
    global_seed: int,
    timestamp: str = "",
) -> BatchPlan:
    """Create a ``BatchPlan`` for one batch from the current state.

    Extracts the minimal information needed for generation, including
    a sparse ``row_last_write`` mapping for only the update + delete rows.
    """
    batch_size = resolve_batch_size(config.batch_size, state.initial_rows)
    update_indices, delete_indices, insert_count = select_rows_for_batch(
        table_name,
        batch_id,
        state,
        batch_size,
        config.operations,
        global_seed,
    )
    insert_start_index = state.total_rows_ever

    row_last_write: dict[int, int] = {}
    for idx in update_indices:
        row_last_write[int(idx)] = state.last_write_batch(int(idx))
    for idx in delete_indices:
        row_last_write[int(idx)] = state.last_write_batch(int(idx))

    return BatchPlan(
        batch_id=batch_id,
        table_name=table_name,
        update_indices=update_indices,
        delete_indices=delete_indices,
        insert_count=insert_count,
        insert_start_index=insert_start_index,
        row_last_write=row_last_write,
        batch_size=batch_size,
        timestamp=timestamp,
    )


def compute_table_state_at_batch(
    table_name: str,
    initial_rows: int,
    config: CDCTableConfig,
    target_batch: int,
    global_seed: int,
) -> TableState:
    """Replay metadata forward to compute the table state at *target_batch*.

    Does NOT materialise any DataFrames -- pure Python computation.
    """
    state = TableState(initial_rows=initial_rows)

    for b in range(1, target_batch + 1):
        advance_table_state(state, table_name, b, config, global_seed)

    return state
