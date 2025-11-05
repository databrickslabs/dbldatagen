from __future__ import annotations

import logging
from typing import Any, Literal, Union

import pandas as pd
from IPython.display import HTML, display

from dbldatagen.spec.column_spec import ColumnDefinition

from .compat import BaseModel, validator


logger = logging.getLogger(__name__)

class UCSchemaTarget(BaseModel):
    catalog: str
    schema_: str
    output_format: str = "delta"  # Default to delta for UC Schema

    @validator("catalog", "schema_")
    def validate_identifiers(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Identifier must be non-empty.")
        if not v.isidentifier():
            logger.warning(
                f"'{v}' is not a basic Python identifier. Ensure validity for Unity Catalog.")
        return v.strip()

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema_} (Format: {self.output_format}, Type: UC Table)"


class FilePathTarget(BaseModel):
    base_path: str
    output_format: Literal["csv", "parquet"]  # No default, must be specified

    @validator("base_path")
    def validate_base_path(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("base_path must be non-empty.")
        return v.strip()

    def __str__(self) -> str:
        return f"{self.base_path} (Format: {self.output_format}, Type: File Path)"


class TableDefinition(BaseModel):
    number_of_rows: int
    partitions: int | None = None
    columns: list[ColumnDefinition]


class ValidationResult:
    """Container for validation results with errors and warnings."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)

    def is_valid(self) -> bool:
        """Returns True if there are no errors."""
        return len(self.errors) == 0

    def __str__(self) -> str:
        """String representation of validation results."""
        lines = []
        if self.is_valid():
            lines.append("✓ Validation passed successfully")
        else:
            lines.append("✗ Validation failed")

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                lines.append(f"  {i}. {error}")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                lines.append(f"  {i}. {warning}")

        return "\n".join(lines)

class DatagenSpec(BaseModel):
    tables: dict[str, TableDefinition]
    output_destination: Union[UCSchemaTarget, FilePathTarget] | None = None # there is a abstraction, may be we can use that? talk to Greg
    generator_options: dict[str, Any] | None = None
    intended_for_databricks: bool | None = None # May be infered.

    def _check_circular_dependencies(
        self,
        table_name: str,
        columns: list[ColumnDefinition]
    ) -> list[str]:
        """
        Check for circular dependencies in baseColumn references.
        Returns a list of error messages if circular dependencies are found.
        """
        errors = []
        column_map = {col.name: col for col in columns}

        for col in columns:
            if col.baseColumn and col.baseColumn != "id":
                # Track the dependency chain
                visited: set[str] = set()
                current = col.name

                while current:
                    if current in visited:
                        # Found a cycle
                        cycle_path = " -> ".join([*list(visited), current])
                        errors.append(
                            f"Table '{table_name}': Circular dependency detected in column '{col.name}': {cycle_path}"
                        )
                        break

                    visited.add(current)
                    current_col = column_map.get(current)

                    if not current_col:
                        break

                    # Move to the next column in the chain
                    if current_col.baseColumn and current_col.baseColumn != "id":
                        if current_col.baseColumn not in column_map:
                            # baseColumn doesn't exist - we'll catch this in another validation
                            break
                        current = current_col.baseColumn
                    else:
                        # Reached a column that doesn't have a baseColumn or uses "id"
                        break

        return errors

    def validate(self, strict: bool = True) -> ValidationResult:  # type: ignore[override]
        """
        Validates the entire DatagenSpec configuration.
        Always runs all validation checks and collects all errors and warnings.

        Args:
            strict: If True, raises ValueError if any errors or warnings are found.
                   If False, only raises ValueError if errors (not warnings) are found.

        Returns:
            ValidationResult object containing all errors and warnings found.

        Raises:
            ValueError: If validation fails based on strict mode setting.
                       The exception message contains all errors and warnings.
        """
        result = ValidationResult()

        # 1. Check that there's at least one table
        if not self.tables:
            result.add_error("Spec must contain at least one table definition")

        # 2. Validate each table (continue checking all tables even if errors found)
        for table_name, table_def in self.tables.items():
            # Check table has at least one column
            if not table_def.columns:
                result.add_error(f"Table '{table_name}' must have at least one column")
                continue  # Skip further checks for this table since it has no columns

            # Check row count is positive
            if table_def.number_of_rows <= 0:
                result.add_error(
                    f"Table '{table_name}' has invalid number_of_rows: {table_def.number_of_rows}. "
                    "Must be a positive integer."
                )

            # Check partitions if specified
            #TODO: though this can be a model field check, we are checking here so that one can correct
            # Can we find a way to use the default way?
            if table_def.partitions is not None and table_def.partitions <= 0:
                result.add_error(
                    f"Table '{table_name}' has invalid partitions: {table_def.partitions}. "
                    "Must be a positive integer or None."
                )

            # Check for duplicate column names
            # TODO: Not something possible if we right model, recheck
            column_names = [col.name for col in table_def.columns]
            duplicates = [name for name in set(column_names) if column_names.count(name) > 1]
            if duplicates:
                result.add_error(
                    f"Table '{table_name}' has duplicate column names: {', '.join(duplicates)}"
                )

            # Build column map for reference checking
            column_map = {col.name: col for col in table_def.columns}

            # TODO: Check baseColumn references, this is tricky? check the dbldefaults
            for col in table_def.columns:
                if col.baseColumn and col.baseColumn != "id":
                    if col.baseColumn not in column_map:
                        result.add_error(
                            f"Table '{table_name}', column '{col.name}': "
                            f"baseColumn '{col.baseColumn}' does not exist in the table"
                        )

            # Check for circular dependencies in baseColumn references
            circular_errors = self._check_circular_dependencies(table_name, table_def.columns)
            for error in circular_errors:
                result.add_error(error)

            # Check primary key constraints
            primary_columns = [col for col in table_def.columns if col.primary]
            if len(primary_columns) > 1:
                primary_names = [col.name for col in primary_columns]
                result.add_warning(
                    f"Table '{table_name}' has multiple primary columns: {', '.join(primary_names)}. "
                    "This may not be the intended behavior."
                )

            # Check for columns with no type and not using baseColumn properly
            for col in table_def.columns:
                if not col.primary and not col.type and not col.options:
                    result.add_warning(
                        f"Table '{table_name}', column '{col.name}': "
                        "No type specified and no options provided. "
                        "Column may not generate data as expected."
                    )

        # 3. Check output destination
        if not self.output_destination:
            result.add_warning(
                "No output_destination specified. Data will be generated but not persisted. "
                "Set output_destination to save generated data."
            )

        # 4. Validate generator options (if any known options)
        if self.generator_options:
            known_options = [
                "random", "randomSeed", "randomSeedMethod", "verbose",
                "debug", "seedColumnName"
            ]
            for key in self.generator_options:
                if key not in known_options:
                    result.add_warning(
                        f"Unknown generator option: '{key}'. "
                        "This may be ignored during generation."
                    )

        # Now that all validations are complete, decide whether to raise
        if (strict and (result.errors or result.warnings)) or (not strict and result.errors):
            raise ValueError(str(result))

        return result


    def display_all_tables(self) -> None:
        for table_name, table_def in self.tables.items():
            print(f"Table: {table_name}")

            if self.output_destination:
                output = f"{self.output_destination}"
                display(HTML(f"<strong>Output destination:</strong> {output}"))
            else:
                message = (
                    "<strong>Output destination:</strong> "
                    "<span style='color: red; font-weight: bold;'>None</span><br>"
                    "<span style='color: gray;'>Set it using the <code>output_destination</code> "
                    "attribute on your <code>DatagenSpec</code> object "
                    "(e.g., <code>my_spec.output_destination = UCSchemaTarget(...)</code>).</span>"
                )
                display(HTML(message))

            df = pd.DataFrame([col.dict() for col in table_def.columns])
            try:
                display(df)
            except NameError:
                print(df.to_string())
