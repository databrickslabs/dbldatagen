"""Shared CDC helpers: CDCStream dataclass, plan normalisation, chunk sizing."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from dbldatagen.core.engine.utils import _LazyList, resolve_batch_size
from dbldatagen.core.spec.cdc_schema import CDCFormat, CDCPlan
from dbldatagen.core.spec.schema import DataGenPlan


@dataclass
class CDCStream:
    """Complete CDC stream: initial snapshot + batches of changes.

    Attributes
    ----------
    initial : dict[str, DataFrame]
        Full table snapshots at batch 0 (all ops = insert).
    batches :
        Lazy list of batch dicts ``{table_name: DataFrame}``.
        Supports indexing, iteration, and ``len()``.
        Batches are generated on first access and cached.
    plan : CDCPlan
        The plan used to generate this stream.
    """

    initial: dict[str, DataFrame] = field(default_factory=dict)
    batches: _LazyList[dict[str, DataFrame]] | list[dict[str, DataFrame]] = field(default_factory=list)
    plan: CDCPlan | None = None


def _normalize_plan(
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None,
    format: str | CDCFormat | None,
) -> CDCPlan:
    """Normalise input to a CDCPlan, applying overrides."""
    if isinstance(plan_or_base, DataGenPlan):
        kwargs: dict = {"base_plan": plan_or_base}
        if num_batches is not None:
            kwargs["num_batches"] = num_batches
        if format is not None:
            kwargs["format"] = CDCFormat(format) if isinstance(format, str) else format
        return CDCPlan(**kwargs)
    else:
        plan = plan_or_base
        if num_batches is not None:
            plan = plan.model_copy(update={"num_batches": num_batches})
        if format is not None:
            fmt = CDCFormat(format) if isinstance(format, str) else format
            plan = plan.model_copy(update={"format": fmt})
        return plan


# Target rows per chunk for auto-sizing: large enough for cluster
# utilization, small enough to avoid OOM on wide schemas.
_AUTO_CHUNK_TARGET_ROWS = 20_000_000


def _auto_chunk_size(plan: CDCPlan) -> int:
    """Pick chunk_size so each chunk has ~_AUTO_CHUNK_TARGET_ROWS rows."""
    target_rows = _AUTO_CHUNK_TARGET_ROWS
    max_rows_per_batch = 1
    for table_name in plan.cdc_tables:
        config = plan.config_for(table_name)
        table_spec = next(t for t in plan.base_plan.tables if t.name == table_name)
        batch_size = resolve_batch_size(config.batch_size, int(table_spec.rows))
        _ins_frac, upd_frac, _del_frac = config.operations.fractions
        # Each batch produces: batch_size + update_before_images
        rows = batch_size + int(batch_size * upd_frac)
        max_rows_per_batch = max(max_rows_per_batch, rows)
    chunk = max(1, target_rows // max(max_rows_per_batch, 1))
    return min(chunk, plan.num_batches)
