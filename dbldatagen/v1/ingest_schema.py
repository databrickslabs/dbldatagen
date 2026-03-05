"""dbldatagen.v1.ingest_schema -- Pydantic models for full-row ingestion simulation."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, model_validator

from dbldatagen.v1.cdc_schema import MutationSpec
from dbldatagen.v1.schema import DataGenPlan, parse_human_count


class IngestMode(str, Enum):
    """Output mode for ingestion batches."""

    INCREMENTAL = "incremental"  # Each batch = new + changed rows only
    SNAPSHOT = "snapshot"  # Each batch = all live rows


class IngestStrategy(str, Enum):
    """Generation strategy for ingestion data."""

    SYNTHETIC = "synthetic"  # Pure synthetic, no Delta reads (fast)
    DELTA = "delta"  # Reads from Delta for update selection (realistic)
    STATELESS = "stateless"  # Stateless targeted scan, no Delta reads, O(batch_size)


class IngestTableConfig(BaseModel):
    """Per-table ingestion configuration."""

    batch_size: int | float | str = 0.1
    insert_fraction: float = 0.6
    update_fraction: float = 0.3
    delete_fraction: float = 0.1
    update_window: int | None = None
    min_life: int = 1
    mutations: MutationSpec = MutationSpec()

    @model_validator(mode="after")
    def validate_fractions(self) -> IngestTableConfig:
        total = self.insert_fraction + self.update_fraction + self.delete_fraction
        if total <= 0:
            raise ValueError("At least one fraction must be positive")
        if abs(total - 1.0) > 0.05:
            raise ValueError(
                f"insert_fraction + update_fraction + delete_fraction must sum " f"to ~1.0, got {total:.3f}"
            )
        return self

    @model_validator(mode="after")
    def resolve_batch_size(self) -> IngestTableConfig:
        if isinstance(self.batch_size, str):
            self.batch_size = parse_human_count(self.batch_size)
        return self


class IngestPlan(BaseModel):
    """Top-level plan for generating an ingestion stream.

    Wraps a ``DataGenPlan`` and adds temporal / change semantics for
    simulating upstream flat-file dumps.
    """

    base_plan: DataGenPlan
    num_batches: int = 5
    mode: IngestMode = IngestMode.INCREMENTAL
    strategy: IngestStrategy = IngestStrategy.SYNTHETIC
    table_configs: dict[str, IngestTableConfig] = {}
    default_config: IngestTableConfig = IngestTableConfig()
    batch_interval_seconds: int = 86400  # 1 day
    start_timestamp: str = "2025-01-01T00:00:00Z"
    ingest_tables: list[str] = []
    include_batch_id: bool = True
    include_load_timestamp: bool = True

    @model_validator(mode="after")
    def validate_table_refs(self) -> IngestPlan:
        valid = {t.name for t in self.base_plan.tables}
        for name in self.table_configs:
            if name not in valid:
                raise ValueError(f"Ingest config references unknown table '{name}'")
        for name in self.ingest_tables:
            if name not in valid:
                raise ValueError(f"ingest_tables references unknown table '{name}'")
        return self

    @model_validator(mode="after")
    def populate_ingest_tables(self) -> IngestPlan:
        if not self.ingest_tables:
            self.ingest_tables = [t.name for t in self.base_plan.tables]
        return self

    def config_for(self, table_name: str) -> IngestTableConfig:
        """Return the ingest config for *table_name*, falling back to defaults."""
        return self.table_configs.get(table_name, self.default_config)
