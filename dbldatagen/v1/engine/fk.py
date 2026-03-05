"""Foreign key generation with guaranteed referential integrity.

The core insight: we reconstruct what the parent PK generator would have
produced for a given row index, without materializing the parent table.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.v1.engine.columns.string import build_pattern_column
from dbldatagen.v1.engine.columns.uuid import build_uuid_column
from dbldatagen.v1.engine.distributions import apply_distribution
from dbldatagen.v1.engine.planner import FKResolution, PKMetadata
from dbldatagen.v1.engine.seed import cell_seed_expr, null_mask_expr


def build_fk_column(
    id_col: Column,
    column_seed: int | Column,
    fk_resolution: FKResolution,
) -> Column:
    """Generate FK values that are guaranteed to be valid parent PKs.

    Algorithm:
    1. cell_seed = xxhash64(column_seed, id)
    2. parent_index = distribution_sample(cell_seed, N_parent)
    3. fk_value = reconstruct_parent_pk(parent_index, pk_metadata)
    """
    meta = fk_resolution.parent_meta
    distribution = fk_resolution.distribution
    null_fraction = fk_resolution.null_fraction

    # Step 1: Derive per-cell seed
    seed_col = cell_seed_expr(column_seed, id_col)

    # Step 2: Map seed to a parent row index via distribution
    parent_index = apply_distribution(seed_col, meta.row_count, distribution)

    # Step 3: Reconstruct the parent PK value from the index
    fk_value = _reconstruct_parent_pk(parent_index, meta)

    # Step 4: Apply null injection if needed
    if null_fraction > 0:
        is_null = null_mask_expr(column_seed, id_col, null_fraction)
        fk_value = F.when(is_null, F.lit(None)).otherwise(fk_value)

    return fk_value


def _reconstruct_parent_pk(parent_index_col: Column, meta: PKMetadata) -> Column:
    """Given a parent row index, reconstruct the PK value that the parent
    generator would produce for that row.

    For sequential PKs: parent_index * step + start
    For pattern PKs: apply pattern formatting
    For UUID PKs: apply UUID generation function
    """
    if meta.pk_type == "sequence":
        return (parent_index_col * F.lit(meta.pk_step) + F.lit(meta.pk_start)).cast("long")

    if meta.pk_type == "pattern":
        # Pattern PKs use build_pattern_column with the parent's column seed
        # parent_index_col acts as the "id" the parent would have used
        return build_pattern_column(parent_index_col, meta.pk_seed, meta.pk_template)

    if meta.pk_type == "uuid":
        # UUID PKs use xxhash64(column_seed, id) and xxhash64(column_seed+1, id)
        # We need to reconstruct the UUID from the parent_index
        return build_uuid_column(parent_index_col, meta.pk_seed)

    # Fallback: treat as sequential
    return parent_index_col.cast("long")
