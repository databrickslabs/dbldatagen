"""Public CDC generation API: generate_cdc, generate_cdc_bulk, generate_cdc_batch, write_cdc_to_delta."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc._common import CDCStream, _auto_chunk_size, _normalize_plan
from dbldatagen.core.engine.cdc.chunking import _generate_chunk_for_table
from dbldatagen.core.engine.cdc.formats import apply_format
from dbldatagen.core.engine.cdc.single_batch import (
    generate_cdc_batch_for_table,
    generate_initial_snapshot,
)
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import resolve_plan
from dbldatagen.core.engine.utils import _LazyList
from dbldatagen.core.spec.cdc_dsl import rename_cdc_columns
from dbldatagen.core.spec.cdc_schema import CDCFormat, CDCPlan
from dbldatagen.core.spec.schema import DataGenPlan


logger = logging.getLogger(__name__)

# Strict identifier pattern — prevents backtick/dot injection in SQL quoting.
# Matches the table-name rule enforced elsewhere in the codebase
# (see dbldatagen.core.spec.schema._IDENTIFIER_RE).
_UC_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass(frozen=True)
class DeltaWriteResult:
    """Per-table result of :func:`write_cdc_to_delta`.

    Attributes
    ----------
    uc_table :
        Fully qualified, backtick-quoted UC table name
        (e.g. ``` `cat`.`sch`.`orders` ```).
    delta_version :
        Final Delta version written. Version 0 is the initial
        snapshot; each chunk appends one more version.
    """

    uc_table: str
    delta_version: int


def _validate_uc_identifier(name: str, kind: str) -> None:
    """Reject identifiers that could escape the backtick quoting.

    Allows ``[A-Za-z_][A-Za-z0-9_]*`` only — same rule as table-name
    validation elsewhere in the codebase.  UC does accept additional
    characters in quoted form, but this project uses the strict rule
    consistently so users hit one policy, not two.
    """
    if not _UC_IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"invalid UC {kind} identifier {name!r}: must match {_UC_IDENTIFIER_RE.pattern}")


def _make_lazy_batch_list(
    spark: SparkSession,
    plan: CDCPlan,
    fmt_name: str,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that generates CDC batches on-demand."""

    def _gen(index: int) -> dict[str, DataFrame]:
        return _generate_batch(spark, plan, index + 1, fmt_name)

    return _LazyList(plan.num_batches, _gen)


def _make_lazy_chunk_list(
    spark: SparkSession,
    plan: CDCPlan,
    fmt_name: str,
    chunk_size: int,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that groups CDC batches into larger chunks."""
    num_chunks = -(-plan.num_batches // chunk_size)  # ceil division

    def _build_chunk(chunk_index: int) -> dict[str, DataFrame]:
        """Build one chunk: bulk insert fusion + per-batch updates/deletes."""
        start_batch = chunk_index * chunk_size + 1  # batch_ids are 1-based
        end_batch = min(start_batch + chunk_size, plan.num_batches + 1)
        batch_ids = list(range(start_batch, end_batch))

        chunk_tables: dict[str, DataFrame] = {}
        for table_name in plan.cdc_tables:
            chunk_df = _generate_chunk_for_table(
                spark,
                plan,
                table_name,
                batch_ids,
                fmt_name,
            )
            if chunk_df is not None:
                chunk_tables[table_name] = chunk_df
            else:
                table_map = {t.name: t for t in plan.base_plan.tables}
                resolved = resolve_plan(plan.base_plan)
                chunk_tables[table_name] = generate_table(
                    spark,
                    table_map[table_name],
                    resolved,
                ).limit(0)

        return chunk_tables

    return _LazyList(num_chunks, _build_chunk)


def generate_cdc(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    format: str | CDCFormat | None = None,
) -> CDCStream:
    """Generate a complete CDC stream.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        Either a ``CDCPlan`` (full config) or a ``DataGenPlan`` (uses
        defaults for all CDC settings).
    num_batches :
        Number of change batches.  Overrides ``plan.num_batches`` if
        the first argument is a ``CDCPlan``.  Required if passing a
        ``DataGenPlan``.
    format :
        Output format.  One of 'raw', 'delta_cdf', 'sql_server',
        'debezium'.  Defaults to 'raw'.

    Returns
    -------
    CDCStream with initial snapshot and lazy per-batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, format)

    fmt_name = plan.format.value

    initial_raw = generate_initial_snapshot(spark, plan)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _make_lazy_batch_list(spark, plan, fmt_name)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_bulk(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    format: str | CDCFormat | None = None,
    chunk_size: int | None = None,
) -> CDCStream:
    """Generate a CDC stream optimised for large-scale cluster execution.

    Like ``generate_cdc()``, but batches are grouped into chunks so each
    Spark job processes millions of rows instead of thousands.  This
    allows Spark to utilise all cluster nodes via its native parallelism.

    Usage is identical to ``generate_cdc()``::

        stream = generate_cdc_bulk(spark, plan)
        stream.initial["table"].write.format("delta").save(...)
        for chunk in stream.batches:
            chunk["table"].write.format("delta").mode("append").save(...)

    Each iteration of the loop yields a dict of DataFrames containing
    multiple logical batches unioned together.  The ``_batch_id`` /
    ``_commit_version`` column still distinguishes individual batches
    within each chunk.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        Either a ``CDCPlan`` or a ``DataGenPlan``.
    num_batches :
        Override for ``plan.num_batches``.
    format :
        Output format override.
    chunk_size :
        Number of logical batches per chunk.  Auto-calculated if None
        (targets ~20M rows per chunk for good cluster utilisation).

    Returns
    -------
    CDCStream with initial snapshot and chunked batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, format)

    fmt_name = plan.format.value

    if chunk_size is None:
        chunk_size = _auto_chunk_size(plan)

    initial_raw = generate_initial_snapshot(spark, plan)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _make_lazy_chunk_list(spark, plan, fmt_name, chunk_size)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_batch(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    batch_id: int,
    format: str | CDCFormat | None = None,
) -> dict[str, DataFrame]:
    """Generate a single CDC batch independently.

    Useful for generating batch N without materialising batches 1..N-1.
    The state is replayed via metadata, not DataFrames.
    """
    if isinstance(plan_or_base, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_base)
    else:
        plan = plan_or_base

    if format is not None:
        fmt = CDCFormat(format) if isinstance(format, str) else format
        plan = plan.model_copy(update={"format": fmt})

    fmt_name = plan.format.value
    return _generate_batch(spark, plan, batch_id, fmt_name)


def _generate_batch(
    spark: SparkSession,
    plan: CDCPlan,
    batch_id: int,
    fmt_name: str,
) -> dict[str, DataFrame]:
    """Generate one batch for all CDC tables, applying the output format."""
    resolved = resolve_plan(plan.base_plan)
    batch_tables: dict[str, DataFrame] = {}

    for table_name in plan.cdc_tables:
        result = generate_cdc_batch_for_table(spark, plan, table_name, batch_id, resolved_plan=resolved)
        combined = result.to_dataframe()

        if combined is not None:
            batch_tables[table_name] = apply_format(combined, fmt_name)
        else:
            # Empty batch — can happen when min_life delays deletes
            table_map = {t.name: t for t in plan.base_plan.tables}
            empty = generate_table(spark, table_map[table_name], resolved).limit(0)
            empty = (
                empty.withColumn("_op", F.lit("I"))
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_ts", F.lit("1970-01-01 00:00:00").cast("timestamp"))
            )
            batch_tables[table_name] = apply_format(empty, fmt_name)

    return batch_tables


def write_cdc_to_delta(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    *,
    catalog: str,
    schema: str,
    num_batches: int | None = None,
    format: str | CDCFormat | None = None,
    chunk_size: int | None = None,
) -> dict[str, DeltaWriteResult]:
    """Generate a CDC stream and write it to Delta tables in one call.

    Each table in the plan is written to ``{catalog}.{schema}.{table_name}``.
    The initial snapshot is written as Delta version 0, then each chunk
    is appended as one additional version.

    For the ``sql_server`` format, CDC metadata columns (``__$operation``,
    ``__$start_lsn``, ``__$seqval``) are renamed to SQL-friendly names
    (``cdc_operation``, ``cdc_lsn``, ``cdc_seqval``).  Other formats are
    written as-is.

    ``catalog``, ``schema``, and every CDC table name must match
    ``[A-Za-z_][A-Za-z0-9_]*`` and are validated before any SQL is
    issued, so a backtick or dot in the input cannot escape the
    quoting.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        A ``CDCPlan`` or ``DataGenPlan``.
    catalog :
        Unity Catalog catalog name (must match ``[A-Za-z_][A-Za-z0-9_]*``).
    schema :
        Unity Catalog schema name (must match ``[A-Za-z_][A-Za-z0-9_]*``).
    num_batches :
        Override for ``plan.num_batches``.
    format :
        Output format override.
    chunk_size :
        Batches per Delta version.  Default ``None`` = auto-calculate
        targeting ~20M rows per chunk for good cluster utilisation.
        Use ``1`` for single-batch versions (best for CDC verification).

    Returns
    -------
    dict mapping ``table_name`` -> :class:`DeltaWriteResult`
    (fully qualified UC table + final Delta version).
    """
    _validate_uc_identifier(catalog, "catalog")
    _validate_uc_identifier(schema, "schema")

    plan = _normalize_plan(plan_or_base, num_batches, format)
    for table_name in plan.cdc_tables:
        _validate_uc_identifier(table_name, "table")

    fmt_name = plan.format.value
    stream = generate_cdc_bulk(spark, plan, chunk_size=chunk_size)

    uc_tables: dict[str, str] = {}
    for table_name in plan.cdc_tables:
        uc_table = f"`{catalog}`.`{schema}`.`{table_name}`"
        uc_tables[table_name] = uc_table

        # Initial snapshot
        spark.sql(f"DROP TABLE IF EXISTS {uc_table}")
        initial_df = rename_cdc_columns(stream.initial[table_name], fmt_name)
        initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(uc_table)
        logger.info("%s: v0 -> %s", table_name, uc_table)

    # Batches (chunked)
    n = len(stream.batches)
    for chunk_idx, chunk in enumerate(stream.batches):
        for table_name in plan.cdc_tables:
            if table_name in chunk:
                rename_cdc_columns(chunk[table_name], fmt_name).write.format("delta").mode("append").saveAsTable(
                    uc_tables[table_name]
                )
        logger.info("chunk %d/%d written", chunk_idx + 1, n)

    logger.info("Done: %d Delta versions per table.", n + 1)
    return {name: DeltaWriteResult(uc_table=uc, delta_version=n) for name, uc in uc_tables.items()}
