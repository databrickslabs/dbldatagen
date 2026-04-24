"""dbldatagen.core.spec.cdc_schema -- Pydantic models for CDC data generation."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import Field, model_validator

from dbldatagen.core.spec.schema import DataGenPlan, _StrictModel, parse_human_count


class CDCFormat(str, Enum):
    """Supported CDC output formats.

    Debezium is not yet supported — the earlier approximation silently
    dropped update before-images, which is structurally wrong for real
    Debezium consumers.  The enum member was removed (rather than kept
    as a value that raises at runtime) so misuse fails at plan
    construction.  Use ``RAW`` (full UB/U/D event log) or ``DELTA_CDF``
    (Delta Change Data Feed) until a faithful nested before/after
    implementation lands.

    .. warning::

       ``SQL_SERVER`` deviates from the real SQL Server CDC contract
       on ``__$seqval``: this generator emits
       ``xxhash64(_batch_id, *data_cols)``, not a monotonic rank
       within ``__$start_lsn``.  Ordering ``ORDER BY __$start_lsn,
       __$seqval`` inside a batch is effectively random.  Consumers
       that use seqval only for row-identity / dedup are unaffected;
       consumers relying on strict within-transaction ordering must
       either tolerate the randomisation, rank downstream, or pick
       ``RAW`` / ``DELTA_CDF``.  See
       ``dbldatagen/core/engine/cdc/formats.py::to_sql_server`` for
       the cost/benefit reasoning behind the rank-less design.
    """

    RAW = "raw"
    SQL_SERVER = "sql_server"
    DELTA_CDF = "delta_cdf"


class OperationWeights(_StrictModel):
    """Controls the mix of insert/update/delete operations per batch.

    Weights are relative and normalised internally.
    """

    insert: float = 3.0
    update: float = 5.0
    delete: float = 2.0

    @model_validator(mode="after")
    def validate_positive(self) -> OperationWeights:
        if self.insert < 0 or self.update < 0 or self.delete < 0:
            raise ValueError("Operation weights must be non-negative")
        if self.insert + self.update + self.delete <= 0:
            raise ValueError("At least one operation weight must be positive")
        return self

    @property
    def fractions(self) -> tuple[float, float, float]:
        total = self.insert + self.update + self.delete
        return self.insert / total, self.update / total, self.delete / total


class MutationSpec(_StrictModel):
    """Controls which columns mutate on updates.

    If *columns* is None, all non-PK / non-FK columns are eligible.
    *fraction* controls what proportion of eligible columns change per row.
    """

    columns: list[str] | None = None
    fraction: float = 0.5

    @model_validator(mode="after")
    def validate_fraction(self) -> MutationSpec:
        if not 0.0 <= self.fraction <= 1.0:
            raise ValueError(f"fraction must be in [0.0, 1.0], got {self.fraction}")
        return self


class CDCTableConfig(_StrictModel):
    """Per-table CDC configuration."""

    operations: OperationWeights = OperationWeights()
    batch_size: int | float | str = 0.1
    mutations: MutationSpec = MutationSpec()
    min_life: int = 3
    update_window: int | None = None

    @model_validator(mode="after")
    def resolve_batch_size(self) -> CDCTableConfig:
        if isinstance(self.batch_size, str):
            self.batch_size = parse_human_count(self.batch_size)
        if isinstance(self.batch_size, float) and self.batch_size > 1.0:
            raise ValueError(
                f"batch_size={self.batch_size}: float values must be <= 1.0 "
                f"(interpreted as a fraction of initial rows). "
                f"Use an integer for absolute batch sizes."
            )
        if self.min_life < 1:
            # ``min_life == 0`` allows a row to be "born dead":
            # ``death_tick(k) = t_birth + 0 + (k % death_period)`` can
            # equal ``t_birth``, and ``is_alive`` in the stateless engine
            # uses ``batch_n < t_death``, so the freshly-inserted row
            # would never appear as alive in any batch.  The CDC stream
            # would emit an I event followed by no U / UB / D -- opaque
            # and almost certainly not what the user wanted.  Require at
            # least 1 so every inserted row is alive for its birth batch.
            raise ValueError(
                f"min_life must be >= 1, got {self.min_life}.  "
                f"min_life=0 would let a row be born dead (I event with no "
                f"subsequent U/D because the row's death_tick == birth_tick)."
            )
        if self.update_window is not None and self.update_window <= 0:
            raise ValueError(f"update_window must be > 0, got {self.update_window}")
        return self


class CDCPlan(_StrictModel):
    """Top-level plan for generating a CDC stream.

    Wraps a ``DataGenPlan`` and adds temporal / change semantics.
    """

    base_plan: DataGenPlan
    num_batches: int = 5
    table_configs: dict[str, CDCTableConfig] = Field(default_factory=dict)
    default_config: CDCTableConfig = CDCTableConfig()
    format: CDCFormat = CDCFormat.RAW
    batch_interval_seconds: int = 3600
    start_timestamp: str = "2025-01-01T00:00:00Z"
    cdc_tables: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_start_timestamp(self) -> CDCPlan:
        """Reject malformed or TZ-naive ``start_timestamp`` at plan time.

        ``batch_timestamp_epoch`` parses this string with
        ``datetime.fromisoformat`` and calls ``.timestamp()`` to get
        UTC epoch seconds.  If the string lacks a timezone suffix
        (``Z`` / ``+HH:MM`` / ``-HH:MM``), ``fromisoformat`` returns a
        *naive* datetime and ``.timestamp()`` silently interprets it
        as the driver's local TZ -- breaking the "UTC epoch"
        contract the rest of the engine relies on.  The default
        (``2025-01-01T00:00:00Z``) is TZ-aware, so this only bit
        callers who overrode with a naive ISO string.  Reject at the
        validator so the TZ contract is enforced before any Spark
        call.
        """
        try:
            parsed = datetime.fromisoformat(self.start_timestamp.replace("Z", "+00:00"))
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"CDCPlan.start_timestamp='{self.start_timestamp}' is not a "
                f"valid ISO-8601 timestamp.  Use a format like "
                f"'2025-01-01T00:00:00Z' or '2025-01-01T00:00:00+00:00'."
            ) from exc
        if parsed.tzinfo is None:
            raise ValueError(
                f"CDCPlan.start_timestamp='{self.start_timestamp}' is a naive "
                f"ISO-8601 datetime (no timezone suffix).  ``batch_timestamp_epoch`` "
                f"converts it via ``.timestamp()`` which would silently use the "
                f"driver's local timezone, violating the engine's UTC epoch "
                f"contract.  Append ``Z`` (UTC) or an explicit offset like "
                f"``+00:00`` / ``-05:00``."
            )
        return self

    @model_validator(mode="after")
    def validate_table_refs(self) -> CDCPlan:
        valid = {t.name for t in self.base_plan.tables}
        for name in self.table_configs:
            if name not in valid:
                raise ValueError(f"CDC config references unknown table '{name}'")
        for name in self.cdc_tables:
            if name not in valid:
                raise ValueError(f"cdc_tables references unknown table '{name}'")
        # Validate mutation columns exist in their table
        table_map = {t.name: t for t in self.base_plan.tables}
        for table_name, config in self.table_configs.items():
            if config.mutations.columns is None:
                continue
            col_names = {c.name for c in table_map[table_name].columns}
            for mut_col in config.mutations.columns:
                if mut_col not in col_names:
                    raise ValueError(
                        f"Mutation column '{mut_col}' not found in table "
                        f"'{table_name}'. Available: {sorted(col_names)}"
                    )
        return self

    @model_validator(mode="after")
    def populate_cdc_tables(self) -> CDCPlan:
        if not self.cdc_tables:
            self.cdc_tables = [t.name for t in self.base_plan.tables]
        return self

    @model_validator(mode="after")
    def _reject_cross_cdc_foreign_keys(self) -> CDCPlan:
        """Reject FK references between two CDC tables.

        The engine's FK integrity guarantee (``apply_fk_delete_guard`` in
        ``cdc/_common.py``) only holds when the FK parent is
        not also being mutated per batch: it disables deletes on parents
        of CDC children, but PK reconstruction in ``fk.build_fk_column``
        uses the plan-time row count and pk_seed, which don't reflect
        per-batch insert/update lifecycles on a CDC parent.  Letting
        such plans through today silently produces dangling child FKs
        (the child references a parent index that was born, mutated, or
        deleted at a different point in the CDC timeline than the
        child's current row).

        This validator raises at plan construction so the failure is
        loud and actionable.  Static parents (``parent`` is in
        ``base_plan.tables`` but NOT in ``cdc_tables``) are fine —
        static parents aren't mutated per batch, so the child's
        plan-time snapshot of the parent PK is correct.
        """
        cdc_table_names = set(self.cdc_tables) if self.cdc_tables else {t.name for t in self.base_plan.tables}
        offending: list[tuple[str, str, str]] = []
        for table_spec in self.base_plan.tables:
            if table_spec.name not in cdc_table_names:
                continue
            for col_spec in table_spec.columns:
                if col_spec.foreign_key is None:
                    continue
                parent_name = col_spec.foreign_key.ref.split(".", 1)[0]
                if parent_name in cdc_table_names:
                    offending.append((table_spec.name, col_spec.name, parent_name))
        if offending:
            items = ", ".join(f"{t}.{c} -> {p}" for t, c, p in offending)
            raise ValueError(
                f"CDCPlan rejects cross-CDC foreign keys: {items}.  "
                f"Multi-table CDC with FKs between CDC tables is not yet "
                f"supported — the FK generator would dangle under mutation.  "
                f"Remove the parent from cdc_tables (make it a static "
                f"dimension), or drop the FK and use a non-FK column for "
                f"now.  Track the feature request in a follow-up issue."
            )
        return self

    @model_validator(mode="after")
    def _validate_num_batches(self) -> CDCPlan:
        if self.num_batches <= 0:
            raise ValueError(f"num_batches must be > 0, got {self.num_batches}")
        return self

    def config_for(self, table_name: str) -> CDCTableConfig:
        """Return the CDC config for *table_name*, falling back to defaults."""
        return self.table_configs.get(table_name, self.default_config)
