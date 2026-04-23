"""dbldatagen.core.spec.cdc_schema -- Pydantic models for CDC data generation."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, model_validator

from dbldatagen.core.spec.schema import DataGenPlan, parse_human_count


class CDCFormat(str, Enum):
    """Supported CDC output formats.

    Debezium is not yet supported — the earlier approximation silently
    dropped update before-images, which is structurally wrong for real
    Debezium consumers.  The enum member was removed (rather than kept
    as a value that raises at runtime) so misuse fails at plan
    construction.  Use ``RAW`` (full UB/U/D event log) or ``DELTA_CDF``
    (Delta Change Data Feed) until a faithful nested before/after
    implementation lands.
    """

    RAW = "raw"
    SQL_SERVER = "sql_server"
    DELTA_CDF = "delta_cdf"


class OperationWeights(BaseModel):
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


class MutationSpec(BaseModel):
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


class CDCTableConfig(BaseModel):
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
        if self.min_life < 0:
            raise ValueError(f"min_life must be >= 0, got {self.min_life}")
        if self.update_window is not None and self.update_window <= 0:
            raise ValueError(f"update_window must be > 0, got {self.update_window}")
        return self


class CDCPlan(BaseModel):
    """Top-level plan for generating a CDC stream.

    Wraps a ``DataGenPlan`` and adds temporal / change semantics.
    """

    base_plan: DataGenPlan
    num_batches: int = 5
    table_configs: dict[str, CDCTableConfig] = {}
    default_config: CDCTableConfig = CDCTableConfig()
    format: CDCFormat = CDCFormat.RAW
    batch_interval_seconds: int = 3600
    start_timestamp: str = "2025-01-01T00:00:00Z"
    cdc_tables: list[str] = []

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
