"""Driver-side test oracle: compute the expected table state at a given batch.

Not suitable for production use — iterates every row index on the driver.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from dbldatagen.core.engine.cdc._common import CDCStream
from dbldatagen.core.engine.cdc_generator import (
    apply_fk_delete_guard,
    compute_periods_from_config,
    generate_for_indices,
)
from dbldatagen.core.engine.cdc_stateless import is_alive, max_k_at_batch
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import resolve_plan
from dbldatagen.core.engine.seed import compute_batch_seed
from dbldatagen.core.engine.utils import resolve_batch_size, union_all
from dbldatagen.core.spec.cdc_schema import CDCPlan
from dbldatagen.core.spec.schema import DataGenPlan


_MAX_EXPECTED_STATE_ROWS = 100_000


def generate_expected_state(
    spark: SparkSession,
    plan_or_stream: CDCPlan | CDCStream | DataGenPlan,
    table_name: str,
    batch_id: int,
) -> DataFrame:
    """Generate the expected table state at a given batch (test oracle).

    .. warning:: Driver-side O(N) — for test use only.

        Iterates every row index on the driver.  Raises ``ValueError``
        if the table has more than 100K initial rows.  For production
        snapshots at scale, use Spark-native generation paths.
    """
    if isinstance(plan_or_stream, CDCStream):
        plan = plan_or_stream.plan
    elif isinstance(plan_or_stream, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_stream)
    else:
        plan = plan_or_stream

    if plan is not None:
        table_map = {t.name: t for t in plan.base_plan.tables}
        if table_name in table_map:
            row_count = int(table_map[table_name].rows)
            if row_count > _MAX_EXPECTED_STATE_ROWS:
                raise ValueError(
                    f"generate_expected_state is a driver-side O(N) test oracle and "
                    f"is not suitable for {row_count:,} rows (max {_MAX_EXPECTED_STATE_ROWS:,}). "
                    f"Use Spark-native generation paths for large tables."
                )

    return _generate_expected_state_driver(spark, plan_or_stream, table_name, batch_id)


def _generate_expected_state_driver(
    spark: SparkSession,
    plan_or_stream: CDCPlan | CDCStream | DataGenPlan,
    table_name: str,
    batch_id: int,
) -> DataFrame:
    """Internal driver-side implementation of generate_expected_state."""
    if isinstance(plan_or_stream, CDCStream):
        plan = plan_or_stream.plan
    elif isinstance(plan_or_stream, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_stream)
    else:
        plan = plan_or_stream

    if plan is None:
        raise ValueError("CDCStream.plan must be set before calling generate_expected_state")
    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    config = plan.config_for(table_name)
    global_seed = table_spec.seed if table_spec.seed is not None else plan.base_plan.seed
    initial_rows = int(table_spec.rows)

    # Apply FK parent delete guard
    config = apply_fk_delete_guard(plan, table_name, config)

    resolved = resolve_plan(plan.base_plan)

    batch_size = resolve_batch_size(config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, config)

    upper_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
    live_indices = [
        k
        for k in range(upper_k)
        if is_alive(
            k,
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            config.min_life,
        )
    ]

    if len(live_indices) == 0:
        full = generate_table(spark, table_spec, resolved)
        return full.limit(0)

    import math

    from dbldatagen.core.engine.cdc_stateless import birth_tick, update_due

    batch_groups: dict[int, list[int]] = {}
    for k in live_indices:
        if update_due(
            k,
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            periods.update_period,
            config.min_life,
            update_window=config.update_window,
        ):
            batch_groups.setdefault(batch_id, []).append(k)
        else:
            t_birth = birth_tick(k, initial_rows, periods.inserts_per_batch)
            if math.isinf(periods.update_period) or int(periods.update_period) <= 0:
                batch_groups.setdefault(t_birth, []).append(k)
            else:
                up = int(periods.update_period)
                remainder = (batch_id + k) % up
                last_update = batch_id - remainder
                if last_update <= t_birth:
                    batch_groups.setdefault(t_birth, []).append(k)
                else:
                    batch_groups.setdefault(last_update, []).append(k)

    dfs = []
    for write_batch, indices in batch_groups.items():
        seed = compute_batch_seed(global_seed, write_batch)
        df = generate_for_indices(spark, table_spec, resolved, indices, seed)
        dfs.append(df)

    return union_all(dfs)


__all__ = ["generate_expected_state"]
