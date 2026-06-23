"""Public API for the dbldatagen core data generator.

Generates synthetic Spark DataFrames from a declared plan of tables and
columns. Generation is deterministic: the same plan and seed produce the same
output. This module re-exports the plan models (`ColumnSpec`, `TableSpec`,
`DataGenPlan`, `PrimaryKey`, `ForeignKeyRef`, `DataType`, `ResolvedPlan`) and
the generation functions (`generate`, `generate_table`, `resolve_plan`).
"""

from __future__ import annotations


try:
    import pydantic  # noqa: F401  # imported only to trigger the friendly install hint below at load time
except ImportError:
    raise ImportError("dbldatagen.core requires pydantic>=2.0. Install with: pip install 'dbldatagen[core]'") from None

from dbldatagen.core.api import generate
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import ResolvedPlan, resolve_plan
from dbldatagen.core.spec import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
)


__all__ = [
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "ForeignKeyRef",
    "PrimaryKey",
    "ResolvedPlan",
    "TableSpec",
    "generate",
    "generate_table",
    "resolve_plan",
]
