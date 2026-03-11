"""Streaming DataFrame generation via Spark's rate source.

Produces a streaming DataFrame with the same column expressions as batch
generation, but using ``spark.readStream.format("rate")`` instead of
``spark.range(N)``.  The rate source emits ``(timestamp, value)`` where
``value`` is a monotonically increasing long — same role as the batch id.

Supports all column types including FK columns (when ``parent_specs`` is
provided) except Feistel/random-unique PKs which require a fixed domain size.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.v1.engine.generator import build_all_column_exprs
from dbldatagen.v1.engine.planner import FKResolution
from dbldatagen.v1.engine.utils import get_pk_columns
from dbldatagen.v1.schema import (
    PatternColumn,
    SequenceColumn,
    TableSpec,
    UUIDColumn,
)


def generate_stream(
    spark: SparkSession,
    table_spec: TableSpec,
    *,
    rows_per_second: int = 1000,
    num_partitions: int | None = None,
    parent_specs: dict[str, TableSpec] | None = None,
) -> DataFrame:
    """Generate a single streaming DataFrame from a TableSpec.

    Uses Spark's ``rate`` source to produce an unbounded stream of rows.
    All column generation strategies are supported **except**:

    - Feistel / random-unique PKs (require a fixed domain size N)

    FK columns are supported when ``parent_specs`` is provided with the
    parent table definitions.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_spec:
        Table definition.  The ``rows`` field is ignored (streaming is unbounded).
    rows_per_second:
        Rate source throughput (default 1000 rows/sec).
    num_partitions:
        Number of streaming partitions (default: Spark decides).
    parent_specs:
        Optional dict mapping parent table names to their ``TableSpec``
        definitions.  Required when the streaming table has FK columns.
        Parent tables are not materialised — only their PK metadata
        (row count, PK strategy, seed) is used to generate valid FK values.

    Returns
    -------
    A streaming DataFrame with all specified columns.
    """
    _validate_streaming_spec(table_spec, parent_specs)

    global_seed = table_spec.seed if table_spec.seed is not None else 42

    # Resolve FK metadata if parent_specs provided
    fk_resolutions: dict[tuple[str, str], FKResolution] = {}
    if parent_specs:
        fk_resolutions = _resolve_streaming_fk(table_spec, parent_specs, global_seed)

    # 1. Streaming base DataFrame
    reader = spark.readStream.format("rate")
    reader = reader.option("rowsPerSecond", rows_per_second)
    if num_partitions is not None:
        reader = reader.option("numPartitions", num_partitions)
    df = reader.load().withColumnRenamed("value", "_synth_row_id")
    id_col = F.col("_synth_row_id")

    # 2. Build column expressions (same logic as batch generate_table)
    col_exprs, udf_columns, seeded_columns = build_all_column_exprs(
        table_spec,
        id_col,
        fk_resolutions or None,
        seed=global_seed,
        row_count=0,
    )

    # 3. Flat select + withColumn phases + drop _synth_row_id
    from dbldatagen.v1.engine.utils import apply_column_phases

    return apply_column_phases(df, id_col, col_exprs, udf_columns, seeded_columns)


def _resolve_streaming_fk(
    table_spec: TableSpec,
    parent_specs: dict[str, TableSpec],
    global_seed: int,
) -> dict[tuple[str, str], FKResolution]:
    """Build FKResolution objects for FK columns using parent_specs metadata.

    Reuses ``_extract_pk_metadata`` from the planner to derive parent PK
    metadata, then constructs ``FKResolution`` objects identical to those
    produced by ``resolve_plan`` in batch mode.
    """
    from dbldatagen.v1.engine.planner import _extract_pk_metadata

    resolutions: dict[tuple[str, str], FKResolution] = {}
    for col_spec in table_spec.columns:
        if col_spec.foreign_key is None:
            continue

        parent_table_name, parent_col_name = col_spec.foreign_key.ref.split(".", 1)
        parent_table = parent_specs[parent_table_name]

        # Find the parent PK column
        parent_col = next(c for c in parent_table.columns if c.name == parent_col_name)

        parent_meta = _extract_pk_metadata(parent_table, parent_col, global_seed)

        resolutions[(table_spec.name, col_spec.name)] = FKResolution(
            child_table=table_spec.name,
            child_column=col_spec.name,
            parent_meta=parent_meta,
            distribution=col_spec.foreign_key.distribution,
            null_fraction=col_spec.foreign_key.null_fraction,
        )
    return resolutions


def _validate_streaming_spec(
    table_spec: TableSpec,
    parent_specs: dict[str, TableSpec] | None = None,
) -> None:
    """Reject column strategies incompatible with streaming."""
    pk_cols = get_pk_columns(table_spec)

    for col_spec in table_spec.columns:
        # FK columns require parent_specs
        if col_spec.foreign_key is not None:
            if parent_specs is None:
                raise ValueError(
                    f"Column '{col_spec.name}': streaming mode requires "
                    f"parent_specs for FK columns. Pass parent table "
                    f"definitions via parent_specs={{...}}."
                )
            parent_table_name, parent_col_name = col_spec.foreign_key.ref.split(".", 1)
            if parent_table_name not in parent_specs:
                raise ValueError(
                    f"Column '{col_spec.name}': parent table "
                    f"'{parent_table_name}' not found in parent_specs. "
                    f"Available: {list(parent_specs.keys())}."
                )
            # Validate referenced column exists and is a PK
            parent_table = parent_specs[parent_table_name]
            parent_col_names = {c.name for c in parent_table.columns}
            if parent_col_name not in parent_col_names:
                raise ValueError(
                    f"Column '{col_spec.name}': parent column "
                    f"'{parent_col_name}' not found in table "
                    f"'{parent_table_name}'."
                )
            if parent_table.primary_key is None or parent_col_name not in parent_table.primary_key.columns:
                raise ValueError(
                    f"Column '{col_spec.name}': referenced column "
                    f"'{parent_col_name}' is not a primary key of "
                    f"'{parent_table_name}'."
                )
            continue

        # Reject Feistel/random-unique PKs — they need a fixed domain size N
        if col_spec.name in pk_cols:
            gen = col_spec.gen
            if not isinstance(gen, (SequenceColumn, PatternColumn, UUIDColumn)):
                raise ValueError(
                    f"Column '{col_spec.name}': random-unique (Feistel) PKs require a "
                    f"fixed row count and are not supported in streaming mode. "
                    f"Use sequence, pattern, or uuid PK strategies instead."
                )
