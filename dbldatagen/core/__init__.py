"""dbldatagen.core -- Deterministic distributed synthetic data generator for Spark."""

from __future__ import annotations


try:
    import pydantic  # noqa: F401
except ImportError:
    raise ImportError(
        "dbldatagen.core requires pydantic>=2.0. " "Install with: pip install 'dbldatagen[core]'"
    ) from None

from dbldatagen.core.api import generate
from dbldatagen.core.engine.cdc import (
    CDCStream,
    DeltaWriteResult,
    generate_cdc,
    generate_cdc_batch,
    generate_cdc_bulk,
    write_cdc_to_delta,
)
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import ResolvedPlan, resolve_plan
from dbldatagen.core.spec import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    TableSpec,
    array,
    decimal,
    double,
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
from dbldatagen.core.spec.cdc_schema import (
    CDCFormat,
    CDCPlan,
    CDCTableConfig,
    MutationSpec,
    OperationWeights,
)


__all__ = [
    "CDCFormat",
    "CDCPlan",
    "CDCStream",
    "CDCTableConfig",
    "ColumnSpec",
    "DataGenPlan",
    "DataType",
    "DeltaWriteResult",
    "ForeignKeyRef",
    "MutationSpec",
    "OperationWeights",
    "PrimaryKey",
    "ResolvedPlan",
    "TableSpec",
    "array",
    "decimal",
    "double",
    "expression",
    "faker",
    "fk",
    "generate",
    "generate_cdc",
    "generate_cdc_batch",
    "generate_cdc_bulk",
    "generate_table",
    "integer",
    "pattern",
    "pk_auto",
    "pk_pattern",
    "pk_uuid",
    "resolve_plan",
    "struct",
    "text",
    "timestamp",
    "write_cdc_to_delta",
]
