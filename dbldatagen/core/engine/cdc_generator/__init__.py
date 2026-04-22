"""CDC batch generation — stateless three-stream architecture.

This package is split into per-path modules:

- ``_common``       : shared helpers (metadata columns, period derivation,
                      FK delete guard, write-batch column application)
- ``single_batch``  : ``generate_cdc_batch_for_table`` and its per-stream helpers
                      (one batch per call)
- ``fused``         : ``generate_fused_deletes`` / ``generate_fused_updates``
                      (many batches per scan)
- ``bulk``          : ``generate_bulk_inserts`` (many batches of inserts per scan)

Every public symbol is re-exported here so existing imports
(``from dbldatagen.core.engine.cdc_generator import ...``) keep working.
"""

from __future__ import annotations

from dbldatagen.core.engine.cdc_generator._common import (
    RawBatchResult,
    _add_cdc_metadata,
    _apply_columns_with_write_batch,
    _precompute_write_batches,
    _table_has_faker_columns,
    _table_has_fk_dependents,
    apply_fk_delete_guard,
    batch_timestamp,
    compute_periods_from_config,
)
from dbldatagen.core.engine.cdc_generator.bulk import (
    _build_case_when_column,
    _build_case_when_fk,
    _build_case_when_lit,
    generate_bulk_inserts,
)
from dbldatagen.core.engine.cdc_generator.fused import (
    _build_fused_output,
    generate_fused_deletes,
    generate_fused_updates,
)
from dbldatagen.core.engine.cdc_generator.single_batch import (
    _generate_delete_stream,
    _generate_delete_stream_native,
    _generate_insert_stream,
    _generate_pre_image_for_indices,
    _generate_update_after_stream,
    _generate_update_after_stream_native,
    _generate_update_before_stream,
    _generate_update_before_stream_native,
    generate_cdc_batch_for_table,
    generate_for_indices,
    generate_initial_snapshot,
)


__all__ = [
    "RawBatchResult",
    "_add_cdc_metadata",
    "_apply_columns_with_write_batch",
    "_build_case_when_column",
    "_build_case_when_fk",
    "_build_case_when_lit",
    "_build_fused_output",
    "_generate_delete_stream",
    "_generate_delete_stream_native",
    "_generate_insert_stream",
    "_generate_pre_image_for_indices",
    "_generate_update_after_stream",
    "_generate_update_after_stream_native",
    "_generate_update_before_stream",
    "_generate_update_before_stream_native",
    "_precompute_write_batches",
    "_table_has_faker_columns",
    "_table_has_fk_dependents",
    "apply_fk_delete_guard",
    "batch_timestamp",
    "compute_periods_from_config",
    "generate_bulk_inserts",
    "generate_cdc_batch_for_table",
    "generate_for_indices",
    "generate_fused_deletes",
    "generate_fused_updates",
    "generate_initial_snapshot",
]
