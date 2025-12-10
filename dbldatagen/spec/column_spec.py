from __future__ import annotations

from typing import Any

from dbldatagen.spec.compat import BaseModel, root_validator
from dbldatagen.types import DbldatagenBasicType


class ColumnDefinition(BaseModel):
    """Defines the specification for a single column in a synthetic data table.

    It supports primary key columns, data columns, and derived columns that reference other columns.

    :param name: Name of the column to be generated (required)
    :param type: Spark SQL data type for the column (e.g., "string", "int", "timestamp").
                 If None, type may be inferred from options or baseColumn. Defaults to None
    :param primary: If True, this column will be treated as a primary key column with unique values.
                    Primary columns cannot have min/max options and cannot be nullable. Defaults to False
    :param options: Dictionary of additional options controlling column generation behavior.
                    Common options include: min, max, step, values, template, distribution, etc.
                    See dbldatagen documentation for full list of available options. Defaults to None
    :param nullable: If True, the column may contain NULL values. Primary columns cannot be nullable.
                     Defaults to False
    :param omit: If True, this column will be generated internally but excluded from the final output.
                 Useful for intermediate columns used in calculations. Defaults to False
    :param baseColumn: Name of another column to use as the basis for generating this column's values.
                       Defaults to "id" which refers to the internal row identifier
    :param baseColumnType: Method for deriving values from the baseColumn. Common values:
                          "auto" (infer behavior), "hash" (hash the base column values),
                          "values" (use base column values directly). Defaults to "auto"

    .. warning::
       Experimental - This API is subject to change in future versions

    .. note::
        Primary columns have special constraints:
        - Must have a type defined
        - Cannot have min/max options
        - Cannot be nullable

    .. note::
        Columns can be chained via baseColumn references, but circular dependencies
        will be caught during validation
    """
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
        """Validates constraints across the entire ColumnDefinition model.

        This validator runs after all individual field validators and checks for cross-field
        constraints that depend on multiple fields being set. It ensures that primary key
        columns meet all necessary requirements and that conflicting options are not specified.

        :param cls: The ColumnDefinition class (automatically provided by Pydantic)
        :param values: Dictionary of all field values for this ColumnDefinition parameters
        :returns: The validated values dictionary, unmodified if all validations pass
        :raises ValueError: If primary column has min/max options, or if primary column is nullable,
                           or if primary column doesn't have a type defined

        .. note::
            This is a Pydantic root validator that runs automatically during model instantiation
        """
        is_primary = values.get("primary")
        options = values.get("options") or {}  # Handle None case
        name = values.get("name")
        is_nullable = values.get("nullable")
        column_type = values.get("type")

        if is_primary:
            if "min" in options or "max" in options:
                raise ValueError(f"Primary key column '{name}' cannot have min/max options.")

            if is_nullable:
                raise ValueError(f"Primary key column '{name}' cannot be nullable.")

            if column_type is None:
                raise ValueError(f"Primary key column '{name}' must have a type defined.")
        return values
