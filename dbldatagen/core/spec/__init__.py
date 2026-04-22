"""dbldatagen.core.spec -- Declarative Pydantic models and DSL for data generation plans."""

from __future__ import annotations

from dbldatagen.core.spec.dsl import (
    array,
    decimal,
    expression,
    faker,
    fk,
    integer,
    pattern,
    pk_auto,
    pk_pattern,
    pk_uuid,
    struct,
    text,
    timestamp,
)
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
    "array",
    "decimal",
    "expression",
    "faker",
    "fk",
    "integer",
    "pattern",
    "pk_auto",
    "pk_pattern",
    "pk_uuid",
    "struct",
    "text",
    "timestamp",
]
