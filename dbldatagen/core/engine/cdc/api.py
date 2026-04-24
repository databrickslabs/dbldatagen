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
from dbldatagen.core.engine.planner import ResolvedPlan, resolve_plan
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
        The actual final Delta log version of the table **after** all
        writes for this invocation have committed, read via
        ``DESCRIBE HISTORY``.  Unlike the first-run-only mapping
        "v0 = snapshot, v1..vn = chunks", this value is correct even
        when the target already existed (``saveAsTable(mode="overwrite")``
        appends a new version rather than resetting the log) or when
        some chunks skipped the table (tables with zero CDC rows in a
        chunk don't advance that chunk's version).  Safe to use for
        time-travel.
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
    resolved_plan: ResolvedPlan,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that generates CDC batches on-demand.

    Reuses the single ``resolved_plan`` that ``generate_cdc`` resolved
    once at entry — without this, ``_generate_batch`` would re-walk
    the plan graph and revalidate FKs for every lazy-list access.
    """

    def _gen(index: int) -> dict[str, DataFrame]:
        return _generate_batch(spark, plan, index + 1, fmt_name, resolved_plan=resolved_plan)

    return _LazyList(plan.num_batches, _gen)


def _make_lazy_chunk_list(
    spark: SparkSession,
    plan: CDCPlan,
    fmt_name: str,
    chunk_size: int,
    resolved_plan: ResolvedPlan,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that groups CDC batches into larger chunks.

    ``resolved_plan`` is threaded through so the per-chunk per-table
    calls to ``_generate_chunk_for_table`` don't each re-resolve the
    plan — at 100 tables x 100 chunks that was 10,000 topological
    sorts + FK validations on the driver.
    """
    num_chunks = -(-plan.num_batches // chunk_size)  # ceil division

    def _build_chunk(chunk_index: int) -> dict[str, DataFrame]:
        """Build one chunk: bulk insert fusion + per-batch updates/deletes.

        When a table has no CDC rows in this chunk,
        ``_generate_chunk_for_table`` returns ``None`` and the key is
        omitted from the returned dict.  Consumers should check
        ``table_name in chunk`` before accessing -- the previous
        behavior filled missing slots with a ``.limit(0)`` DataFrame,
        which made every chunk write an empty Delta append for every
        table (inflating the Delta log and confusing CDF consumers
        reading by version).
        """
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
                resolved_plan=resolved_plan,
            )
            if chunk_df is not None:
                chunk_tables[table_name] = chunk_df
            # else: omit.  Callers guard with ``if table_name in chunk``.

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
        Output format.  One of 'raw', 'delta_cdf', or 'sql_server'.
        Defaults to 'raw'.

    Returns
    -------
    CDCStream with initial snapshot and lazy per-batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, format)

    fmt_name = plan.format.value

    # Resolve the plan once and thread it through every path — per-batch
    # re-resolution was a hidden O(num_batches) driver cost.
    resolved = resolve_plan(plan.base_plan)

    initial_raw = generate_initial_snapshot(spark, plan, resolved_plan=resolved)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _make_lazy_batch_list(spark, plan, fmt_name, resolved_plan=resolved)
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
            if "table" in chunk:  # chunk may omit tables with no changes
                chunk["table"].write.format("delta").mode("append").save(...)

    Each iteration of the loop yields a dict of DataFrames containing
    multiple logical batches unioned together.  A table with zero CDC
    rows in a given chunk is **omitted from the chunk dict** (not
    included as an empty DataFrame) -- guard the access with
    ``if name in chunk`` or ``chunk.get(name)`` so you don't write
    empty appends.  The ``_batch_id`` / ``_commit_version`` column
    still distinguishes individual batches within each chunk.

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

    # Resolve once and reuse across every chunk x table.
    resolved = resolve_plan(plan.base_plan)

    initial_raw = generate_initial_snapshot(spark, plan, resolved_plan=resolved)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _make_lazy_chunk_list(spark, plan, fmt_name, chunk_size, resolved_plan=resolved)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_batch(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    batch_id: int,
    format: str | CDCFormat | None = None,
    resolved_plan: ResolvedPlan | None = None,
) -> dict[str, DataFrame]:
    """Generate a single CDC batch independently.

    Useful for generating batch N without materialising batches 1..N-1.
    The state is replayed via metadata, not DataFrames.

    ``batch_id`` must be in ``[1, plan.num_batches]``.  ``batch_id=0``
    is the initial snapshot and is produced by ``generate_cdc`` /
    ``generate_cdc_bulk`` (not by this function) — passing 0 or a
    value outside the plan's batch range used to return an
    empty-but-formatted DataFrame, silently masking the mistake.

    ``resolved_plan`` is optional.  Pass a pre-computed ``ResolvedPlan``
    (from ``resolve_plan(plan.base_plan)``) to skip the internal
    resolution -- useful when iterating over batches in a loop.  Must
    be built from the base plan of the same ``plan_or_base`` argument;
    mismatches produce undefined FK resolution.
    """
    if isinstance(plan_or_base, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_base)
    else:
        plan = plan_or_base

    if format is not None:
        fmt = CDCFormat(format) if isinstance(format, str) else format
        plan = plan.model_copy(update={"format": fmt})

    if not 1 <= batch_id <= plan.num_batches:
        raise ValueError(
            f"batch_id must be in [1, {plan.num_batches}], got {batch_id}.  "
            f"batch_id=0 is the initial snapshot (obtain via ``generate_cdc`` "
            f"or ``generate_cdc_bulk`` and read ``.initial``)."
        )

    fmt_name = plan.format.value
    # Name-based compatibility check mirrors ``generate_table``:
    # ``resolved_plan`` must have been produced from a plan whose
    # tables cover the CDC-eligible set; otherwise FK resolution is
    # undefined (the resolved's ``fk_resolutions`` key space won't
    # line up with ``plan.cdc_tables``).  Identity is too strict
    # because the CDCPlan wraps a DataGenPlan whose identity the
    # caller may not track; match on the sorted set of table names.
    if resolved_plan is not None:
        resolved_names = {t.name for t in resolved_plan.plan.tables}
        plan_names = {t.name for t in plan.base_plan.tables}
        if resolved_names != plan_names:
            raise ValueError(
                f"resolved_plan was produced from a plan with tables "
                f"{sorted(resolved_names)}, but this CDCPlan has "
                f"{sorted(plan_names)}.  Pass a ResolvedPlan built "
                f"from ``plan.base_plan``, or drop the kwarg."
            )
    # Resolve once and thread through -- mirrors the pattern in
    # ``generate_cdc`` / ``generate_cdc_bulk``.  When the caller passes
    # ``resolved_plan`` (a loop over batches), reuse it and skip the
    # driver-side plan walk + FK validation; otherwise resolve once
    # here so ``_generate_batch`` doesn't re-resolve via its kwarg
    # default.
    resolved = resolved_plan if resolved_plan is not None else resolve_plan(plan.base_plan)
    return _generate_batch(spark, plan, batch_id, fmt_name, resolved_plan=resolved)


def _generate_batch(
    spark: SparkSession,
    plan: CDCPlan,
    batch_id: int,
    fmt_name: str,
    resolved_plan: ResolvedPlan | None = None,
) -> dict[str, DataFrame]:
    """Generate one batch for all CDC tables, applying the output format.

    ``resolved_plan`` is threaded from ``generate_cdc`` /
    ``generate_cdc_batch`` so we don't re-walk the plan graph and
    revalidate FKs for every batch access.
    """
    resolved = resolved_plan if resolved_plan is not None else resolve_plan(plan.base_plan)
    batch_tables: dict[str, DataFrame] = {}

    for table_name in plan.cdc_tables:
        result = generate_cdc_batch_for_table(spark, plan, table_name, batch_id, resolved_plan=resolved)
        combined = result.to_dataframe()

        if combined is not None:
            batch_tables[table_name] = apply_format(combined, fmt_name)
        else:
            # Empty batch — can happen when min_life delays deletes.
            # Use this batch's real timestamp (not a 1970 sentinel) so
            # ``_ts`` in the output is always session-TZ-independent and
            # consistent with non-empty batches; downstream SQL Server
            # CDF / Delta CDF consumers that filter on `_ts` don't see a
            # spurious epoch-0 row.
            from dbldatagen.core.engine.cdc._common import batch_timestamp

            table_map = {t.name: t for t in plan.base_plan.tables}
            empty = generate_table(spark, table_map[table_name], resolved).limit(0)
            empty_ts_epoch = batch_timestamp(plan, batch_id)
            empty = (
                empty.withColumn("_op", F.lit("I"))
                # ``_batch_id`` is long everywhere else -- keep the
                # cast explicit on every ``F.lit`` emission site.
                .withColumn("_batch_id", F.lit(batch_id).cast("long")).withColumn(
                    "_ts", F.lit(empty_ts_epoch).cast("long").cast("timestamp")
                )
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

    Atomicity caveat: writes are **per-table, per-chunk, not
    transactional end-to-end**.  A mid-run failure (Spark executor loss,
    driver interrupt, network partition) after the initial snapshot
    succeeds leaves the target catalog with the new initial snapshot
    plus whatever chunks completed before the failure; subsequent
    reruns overwrite the initial snapshot but do not roll back partial
    chunk appends of the previous run.  If you need all-or-nothing
    semantics, write to a staging namespace and promote via a single
    ``CREATE TABLE ... AS`` after the run completes.

    The initial snapshot write uses Delta's ``.mode("overwrite")`` with
    ``overwriteSchema=true``, which atomically replaces any pre-existing
    table at the target path in a single transaction -- the previous
    data is preserved if the new write fails.  Earlier code issued an
    explicit ``DROP TABLE IF EXISTS`` before the write, which meant a
    crash between DROP and write left the target empty; that DROP has
    been removed.

    Delta-only target: if ``uc_table`` already exists as a non-Delta
    table (Parquet, CSV, view, external non-Delta path), the Delta
    overwrite raises -- previously the explicit DROP would have
    replaced it with a fresh Delta table.  Callers who need to
    convert an existing non-Delta target should issue the DROP
    themselves before calling ``write_cdc_to_delta``.

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

        # Initial snapshot.  ``mode("overwrite") + overwriteSchema=true``
        # atomically replaces any existing Delta table at ``uc_table`` --
        # if this write fails, the pre-existing data is preserved.  An
        # earlier ``DROP TABLE IF EXISTS`` before the write made the
        # replacement non-atomic (DROP succeeds, then write fails, target
        # left empty).  Removed.
        initial_df = rename_cdc_columns(stream.initial[table_name], fmt_name)
        initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(uc_table)
        logger.info("%s: v0 -> %s", table_name, uc_table)

    # Batches (chunked).  ``if table_name in chunk`` is the load-bearing
    # guard: ``_make_lazy_chunk_list`` omits a table from a chunk dict
    # when that table had zero CDC rows in the chunk, so the skip is
    # real (not a no-op over an empty-but-present DataFrame like the
    # earlier ``.limit(0)`` fallback was).  Skipping a table in a
    # chunk means its Delta log does NOT advance for that chunk -- the
    # final ``delta_version`` per table reflects only the chunks that
    # actually wrote rows.
    n = len(stream.batches)
    for chunk_idx, chunk in enumerate(stream.batches):
        for table_name in plan.cdc_tables:
            if table_name in chunk:
                rename_cdc_columns(chunk[table_name], fmt_name).write.format("delta").mode("append").saveAsTable(
                    uc_tables[table_name]
                )
        logger.info("chunk %d/%d written", chunk_idx + 1, n)

    # Report the actual committed Delta version per table via
    # ``DESCRIBE HISTORY``.  The old code returned ``delta_version=n``
    # (just the chunk count) which lies on:
    #   (a) reruns -- ``saveAsTable(mode="overwrite")`` APPENDS a new
    #       version to an existing Delta log (doesn't reset to 0), so
    #       first-run "v0 snapshot + v1..vn chunks" no longer holds on
    #       the second run, and ``n`` points at a historical commit
    #       rather than the final one.
    #   (b) tables with no changes in a chunk -- those chunks are now
    #       skipped (Bug 2 fix), so a table's final version is
    #       ``initial_version + count(chunks with rows)``, not n.
    # ``DESCRIBE HISTORY ... LIMIT 1`` reads the most-recent row (Delta
    # orders history descending by version), which is the final
    # committed version after all appends land.
    results: dict[str, DeltaWriteResult] = {}
    for table_name, uc in uc_tables.items():
        final_version = spark.sql(f"DESCRIBE HISTORY {uc} LIMIT 1").select("version").collect()[0][0]
        results[table_name] = DeltaWriteResult(uc_table=uc, delta_version=int(final_version))
    logger.info("Done: final per-table Delta versions: %s", {k: v.delta_version for k, v in results.items()})
    return results
