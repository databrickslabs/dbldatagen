"""dbldatagen.v1.cdc_dsl -- Shorthand constructors for CDC plans.

Usage::

    from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

    plan = cdc_plan(
        base,
        num_batches=10,
        format="delta_cdf",
        customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbldatagen.v1.cdc_schema import (
    CDCFormat,
    CDCPlan,
    CDCTableConfig,
    MutationSpec,
    OperationWeights,
)
from dbldatagen.v1.schema import DataGenPlan


if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def ops(insert: float = 3.0, update: float = 5.0, delete: float = 2.0) -> OperationWeights:
    """Shorthand for OperationWeights."""
    return OperationWeights(insert=insert, update=update, delete=delete)


def mutations(columns: list[str] | None = None, fraction: float = 0.5) -> MutationSpec:
    """Shorthand for MutationSpec."""
    return MutationSpec(columns=columns, fraction=fraction)


def cdc_config(
    batch_size: int | float | str = 0.1,
    operations: OperationWeights | None = None,
    mutations_spec: MutationSpec | None = None,
) -> CDCTableConfig:
    """Build a CDCTableConfig with convenient defaults."""
    return CDCTableConfig(
        batch_size=batch_size,
        operations=operations or OperationWeights(),
        mutations=mutations_spec or MutationSpec(),
    )


def cdc_plan(
    base: DataGenPlan,
    num_batches: int = 5,
    format: str | CDCFormat = CDCFormat.RAW,
    batch_interval_seconds: int = 3600,
    start_timestamp: str = "2025-01-01T00:00:00Z",
    cdc_tables: list[str] | None = None,
    **table_configs: CDCTableConfig,
) -> CDCPlan:
    """Build a CDCPlan with per-table configs as keyword arguments.

    Example::

        plan = cdc_plan(
            base,
            num_batches=10,
            format="delta_cdf",
            customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
            orders=cdc_config(batch_size=0.10),
        )
    """
    fmt = CDCFormat(format) if isinstance(format, str) else format
    return CDCPlan(
        base_plan=base,
        num_batches=num_batches,
        format=fmt,
        table_configs=table_configs,
        batch_interval_seconds=batch_interval_seconds,
        start_timestamp=start_timestamp,
        cdc_tables=cdc_tables or [],
    )


# ---------------------------------------------------------------------------
# Column rename helpers
# ---------------------------------------------------------------------------

# Format → {original_name: clean_name}
_FORMAT_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "sql_server": {
        "__$operation": "cdc_operation",
        "__$start_lsn": "cdc_lsn",
        "__$seqval": "cdc_seqval",
    },
}


def rename_cdc_columns(df: DataFrame, format: str = "sql_server") -> DataFrame:
    """Rename CDC metadata columns to clean, SQL-friendly names.

    Usage::

        from dbldatagen.v1.cdc_dsl import rename_cdc_columns

        stream = generate_cdc_bulk(spark, plan)
        initial = rename_cdc_columns(stream.initial["my_table"])
        initial.write.format("delta").saveAsTable(uc_table)

    For ``sql_server`` format, renames:
      - ``__$operation`` → ``cdc_operation``
      - ``__$start_lsn``  → ``cdc_lsn``
      - ``__$seqval``     → ``cdc_seqval``
    """
    renames = _FORMAT_COLUMN_RENAMES.get(format, {})
    for old_name, new_name in renames.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------


def append_only_config(batch_size: int | float | str = 0.1) -> CDCTableConfig:
    """Preset: inserts only, no updates or deletes."""
    return CDCTableConfig(
        batch_size=batch_size,
        operations=OperationWeights(insert=1, update=0, delete=0),
    )


def high_churn_config(batch_size: int | float | str = 0.2) -> CDCTableConfig:
    """Preset: high update/delete rate — good for SCD2 stress testing."""
    return CDCTableConfig(
        batch_size=batch_size,
        operations=OperationWeights(insert=1, update=6, delete=3),
    )


def scd2_config(batch_size: int | float | str = 0.1) -> CDCTableConfig:
    """Preset: moderate updates, few inserts, rare deletes."""
    return CDCTableConfig(
        batch_size=batch_size,
        operations=OperationWeights(insert=1, update=8, delete=1),
    )
