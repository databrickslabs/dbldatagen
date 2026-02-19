"""Base interface and common types for schema extraction connectors."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from dbldatagen.v1.schema import DataGenPlan, DataType


@dataclass
class InferredColumn:
    """Intermediate representation of an extracted column before strategy assignment."""

    name: str
    native_type: str
    synth_dtype: DataType
    nullable: bool = False
    is_primary_key: bool = False
    is_foreign_key: bool = False
    fk_references: str | None = None  # "table.column" format
    unique: bool = False
    sample_values: list[Any] = field(default_factory=list)
    distinct_count: int | None = None


class SchemaConnector(Protocol):
    """Protocol for schema extraction connectors."""

    def extract(self) -> DataGenPlan:
        """Extract schema and return a DataGenPlan."""

    def to_yaml(self, output_path: str) -> None:
        """Extract the plan and write it as YAML to *output_path*."""
