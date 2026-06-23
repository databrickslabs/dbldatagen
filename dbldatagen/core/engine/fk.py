"""Foreign key generation with guaranteed referential integrity.

Generates child FK values by reconstructing the primary key the parent table
would have produced for a chosen row index. Every FK value matches a real parent
key, and the parent table never has to be materialized to look it up.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.string import build_pattern_column
from dbldatagen.core.engine.columns.uuid import build_uuid_column
from dbldatagen.core.engine.distributions import apply_distribution
from dbldatagen.core.engine.planner import FKResolution, PKMetadata
from dbldatagen.core.engine.seed import cell_seed_expr, null_mask_expr
from dbldatagen.core.spec.schema import ColumnSpec


def build_fk_column(
    id_column: Column,
    column_seed: int,
    fk_resolution: FKResolution,
) -> Column:
    """Builds a column of foreign-key values that are valid parent keys.

    Derives a per-row seed, samples a parent row index using the configured
    distribution, and reconstructs the parent's primary key for that index. When
    the null fraction is positive, a fraction of rows are emitted as NULL.

    Args:
        id_column: Row-id column.
        column_seed: Per-column seed for the FK column.
        fk_resolution: Resolved FK information carrying the parent key metadata,
            sampling distribution, and null fraction.

    Returns:
        A Spark `Column` whose type matches the parent key (long for
        sequence/UUID keys, string for pattern keys).
    """
    parent_metadata = fk_resolution.parent_metadata
    distribution = fk_resolution.distribution
    null_fraction = fk_resolution.null_fraction

    cell_seed = cell_seed_expr(column_seed, id_column)
    parent_index = apply_distribution(cell_seed, parent_metadata.row_count, distribution)
    fk_value = _reconstruct_parent_pk(parent_index, parent_metadata)

    if null_fraction > 0:
        is_null = null_mask_expr(column_seed, id_column, null_fraction)
        fk_value = F.when(is_null, F.lit(None)).otherwise(fk_value)

    return fk_value


def _reconstruct_parent_pk(parent_index_col: Column, parent_metadata: PKMetadata) -> Column:
    """Reconstructs the parent primary key value for a given row index.

    Mirrors how each parent key strategy generates values: `index * step + start`
    for sequence keys, pattern formatting for pattern keys, and UUID generation
    for UUID keys.

    Args:
        parent_index_col: Column of parent row indices.
        parent_metadata: Parent primary key metadata.

    Returns:
        A Spark `Column` of reconstructed parent key values.

    Raises:
        ValueError: If a pattern key has no template, or `pk_type` is not one of
            "sequence", "pattern", or "uuid".
    """
    if parent_metadata.pk_type == "sequence":
        return (parent_index_col * F.lit(parent_metadata.pk_step) + F.lit(parent_metadata.pk_start)).cast("long")

    if parent_metadata.pk_type == "pattern":
        if parent_metadata.pk_template is None:
            raise ValueError(
                f"pattern PK for '{parent_metadata.table_name}.{parent_metadata.pk_column}' has no template -- "
                f"planner.extract_pk_metadata should have populated it; invariant bypassed"
            )
        return build_pattern_column(parent_index_col, parent_metadata.pk_seed, parent_metadata.pk_template)

    if parent_metadata.pk_type == "uuid":
        return build_uuid_column(parent_index_col, parent_metadata.pk_seed)

    # _validate_primary_keys rejects non-{sequence, pattern, uuid} PK strategies
    # at plan time; this raise surfaces a hand-constructed PKMetadata with a bogus
    # pk_type rather than silently emitting wrong FK values.
    raise ValueError(
        f"_reconstruct_parent_pk received unknown pk_type='{parent_metadata.pk_type}' "
        f"for '{parent_metadata.table_name}.{parent_metadata.pk_column}'.  Expected one of "
        f"'sequence', 'pattern', 'uuid' — the plan-time validator "
        f"``_validate_primary_keys`` should have rejected this."
    )


def build_fk_column_expr(
    col_spec: ColumnSpec,
    table_name: str,
    id_column: Column,
    column_seed: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None,
) -> tuple[str, Column]:
    """Builds an FK column expression, raising if its resolution is missing.

    Used by the engine's column-building loop. A missing resolution is an error
    rather than a silent all-NULL column, so calling `generate_table` directly
    requires passing a `ResolvedPlan` that includes this column's FK.

    Args:
        col_spec: The FK column's spec.
        table_name: Name of the table that owns the column.
        id_column: Row-id column.
        column_seed: Per-column seed.
        fk_resolutions: Map of `(table, column)` to its `FKResolution`, or None.

    Returns:
        A tuple `(column_name, expr)` for the FK column.

    Raises:
        TypeError: If no `FKResolution` exists for this column.
    """
    fk_key = (table_name, col_spec.name)
    if fk_resolutions is None or fk_key not in fk_resolutions:
        raise TypeError(
            f"FK column '{table_name}.{col_spec.name}' has no FKResolution — "
            f"caller must resolve the plan (via ``resolve_plan`` / ``generate``) "
            f"before reaching ``build_column_expr``.  Calling ``generate_table`` "
            f"directly requires passing a ``ResolvedPlan`` that includes this "
            f"column's FK."
        )
    fk_expr = build_fk_column(id_column, column_seed, fk_resolutions[fk_key])
    return (col_spec.name, fk_expr)
