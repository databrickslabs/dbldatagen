"""Declarative Pydantic models for data-generation plans.

Exports the plan models (`ColumnSpec`, `TableSpec`, `DataGenPlan`, `PrimaryKey`,
`ForeignKeyRef`, `ForeignKeyColumn`, `DataType`).

Note:
    DSL helper functions (`integer`, `decimal`, `text`, ...) should be imported
    from `dbldatagen.core.spec.dsl` and are not re-exported here. Import that
    module under an alias, e.g. `from dbldatagen.core.spec import dsl as datagendg`.
"""

from __future__ import annotations

from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyColumn,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
)


__all__ = [
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "ForeignKeyColumn",
    "ForeignKeyRef",
    "PrimaryKey",
    "TableSpec",
]
