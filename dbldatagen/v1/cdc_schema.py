"""dbldatagen.v1.cdc_schema -- Pydantic models for CDC data generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import BaseModel, model_validator


if TYPE_CHECKING:
    import numpy as np

from dbldatagen.v1.schema import DataGenPlan, parse_human_count


class CDCFormat(str, Enum):
    """Supported CDC output formats."""

    RAW = "raw"
    SQL_SERVER = "sql_server"
    DEBEZIUM = "debezium"
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

    def config_for(self, table_name: str) -> CDCTableConfig:
        """Return the CDC config for *table_name*, falling back to defaults."""
        return self.table_configs.get(table_name, self.default_config)


def _empty_int64_array() -> np.ndarray:
    import numpy as np

    return np.array([], dtype=np.int64)


@dataclass
class BatchPlan:
    """Pre-computed plan for generating one CDC batch.

    Contains everything the generator needs for a single batch without
    accessing the full ``TableState``.  The ``row_last_write`` dict is
    *sparse* -- it only contains entries for rows in ``update_indices``
    and ``delete_indices``, not for all rows in the table.
    """

    batch_id: int
    table_name: str
    update_indices: np.ndarray = field(default_factory=_empty_int64_array)
    delete_indices: np.ndarray = field(default_factory=_empty_int64_array)
    insert_count: int = 0
    insert_start_index: int = 0
    row_last_write: dict[int, int] = field(default_factory=dict)
    batch_size: int = 0
    timestamp: str = ""
