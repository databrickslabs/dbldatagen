from __future__ import annotations

from typing import Any, Literal

from .compat import BaseModel, root_validator


DbldatagenBasicType = Literal[
    "string",
    "int",
    "long",
    "float",
    "double",
    "decimal",
    "boolean",
    "date",
    "timestamp",
    "short",
    "byte",
    "binary",
    "integer",
    "bigint",
    "tinyint",
]
class ColumnDefinition(BaseModel):
    name: str
    type: DbldatagenBasicType | None = None
    primary: bool = False
    options: dict[str, Any] | None = None
    nullable: bool | None = False
    omit: bool | None = False
    baseColumn: str | None = "id"
    baseColumnType: str | None = "auto"

    @root_validator()
    def check_model_constraints(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validates constraints across the entire model after individual fields are processed.
        """
        is_primary = values.get("primary")
        options = values.get("options") or {}  # Handle None case
        name = values.get("name")
        is_nullable = values.get("nullable")
        column_type = values.get("type")

        if is_primary:
            if "min" in options or "max" in options:
                raise ValueError(f"Primary column '{name}' cannot have min/max options.")

            if is_nullable:
                raise ValueError(f"Primary column '{name}' cannot be nullable.")

            if column_type is None:
                raise ValueError(f"Primary column '{name}' must have a type defined.")
        return values
