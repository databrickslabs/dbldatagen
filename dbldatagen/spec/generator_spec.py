from __future__ import annotations

import logging
from typing import Any, Literal, Union

import pandas as pd
from IPython.display import HTML, display

from dbldatagen.spec.column_spec import ColumnDefinition

from .compat import BaseModel, validator


logger = logging.getLogger(__name__)


class UCSchemaTarget(BaseModel):
    """Defines a Unity Catalog schema as the output destination for generated data.

    This class represents a Unity Catalog location (catalog.schema) where generated tables
    will be written. Unity Catalog is Databricks' unified governance solution for data and AI.

    :param catalog: Unity Catalog catalog name where tables will be written
    :param schema_: Unity Catalog schema (database) name within the catalog
    :param output_format: Data format for table storage. Defaults to "delta" which is the
                         recommended format for Unity Catalog tables

    .. note::
        The schema parameter is named `schema_` (with underscore) to avoid conflict with
        Python's built-in schema keyword and Pydantic functionality

    .. note::
        Tables will be written to the location: `{catalog}.{schema_}.{table_name}`
    """
    catalog: str
    schema_: str
    output_format: str = "delta"  # Default to delta for UC Schema

    @validator("catalog", "schema_")
    def validate_identifiers(cls, v: str) -> str:
        """Validates that catalog and schema names are valid identifiers.

        Ensures the identifier is non-empty and follows Python identifier conventions.
        Issues a warning if the identifier is not a basic Python identifier, as this may
        cause issues with Unity Catalog.

        :param v: The identifier string to validate (catalog or schema name)
        :returns: The validated and stripped identifier string
        :raises ValueError: If the identifier is empty or contains only whitespace

        .. note::
            This is a Pydantic field validator that runs automatically during model instantiation
        """
        if not v.strip():
            raise ValueError("Identifier must be non-empty.")
        if not v.isidentifier():
            logger.warning(
                f"'{v}' is not a basic Python identifier. Ensure validity for Unity Catalog.")
        return v.strip()

    def __str__(self) -> str:
        """Returns a human-readable string representation of the Unity Catalog target.

        :returns: Formatted string showing catalog, schema, format and type
        """
        return f"{self.catalog}.{self.schema_} (Format: {self.output_format}, Type: UC Table)"


class FilePathTarget(BaseModel):
    """Defines a file system path as the output destination for generated data.

    This class represents a file system location where generated tables will be written
    as files. Each table will be written to a subdirectory within the base path.

    :param base_path: Base file system path where table data files will be written.
                     Each table will be written to {base_path}/{table_name}/
    :param output_format: File format for data storage. Must be either "csv" or "parquet".
                         No default value - must be explicitly specified

    .. note::
        Unlike UCSchemaTarget, this requires an explicit output_format with no default

    .. note::
        The base_path can be a local file system path, DBFS path, or cloud storage path
        (e.g., s3://, gs://, abfs://) depending on your environment
    """
    base_path: str
    output_format: Literal["csv", "parquet"]  # No default, must be specified

    @validator("base_path")
    def validate_base_path(cls, v: str) -> str:
        """Validates that the base path is non-empty.

        :param v: The base path string to validate
        :returns: The validated and stripped base path string
        :raises ValueError: If the base path is empty or contains only whitespace

        .. note::
            This is a Pydantic field validator that runs automatically during model instantiation
        """
        if not v.strip():
            raise ValueError("base_path must be non-empty.")
        return v.strip()

    def __str__(self) -> str:
        """Returns a human-readable string representation of the file path target.

        :returns: Formatted string showing base path, format and type
        """
        return f"{self.base_path} (Format: {self.output_format}, Type: File Path)"


class TableDefinition(BaseModel):
    """Defines the complete specification for a single synthetic data table.

    This class encapsulates all the information needed to generate a table of synthetic data,
    including the number of rows, partitioning, and column specifications.

    :param number_of_rows: Total number of data rows to generate for this table.
                          Must be a positive integer
    :param partitions: Number of Spark partitions to use when generating data.
                      If None, defaults to Spark's default parallelism setting.
                      More partitions can improve generation speed for large datasets
    :param columns: List of ColumnDefinition objects specifying the columns to generate
                   in this table. At least one column must be specified

    .. note::
        Setting an appropriate number of partitions can significantly impact generation performance.
        As a rule of thumb, use 2-4 partitions per CPU core available in your Spark cluster

    .. note::
        Column order in the list determines the order of columns in the generated output
    """
    number_of_rows: int
    partitions: int | None = None
    columns: list[ColumnDefinition]


class ValidationResult:
    """Container for validation results that collects errors and warnings during spec validation.

    This class accumulates validation issues found while checking a DatagenSpec configuration.
    It distinguishes between errors (which prevent data generation) and warnings (which
    indicate potential issues but don't block generation).

    .. note::
        Validation passes if there are no errors, even if warnings are present
    """

    def __init__(self) -> None:
        """Initialize an empty ValidationResult with no errors or warnings."""
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def add_error(self, message: str) -> None:
        """Add an error message to the validation results.

        Errors indicate critical issues that will prevent successful data generation.

        :param message: Descriptive error message explaining the validation failure
        """
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        """Add a warning message to the validation results.

        Warnings indicate potential issues or non-optimal configurations that may affect
        data generation but won't prevent it from completing.

        :param message: Descriptive warning message explaining the potential issue
        """
        self.warnings.append(message)

    def is_valid(self) -> bool:
        """Check if validation passed without errors.

        :returns: True if there are no errors (warnings are allowed), False otherwise
        """
        return len(self.errors) == 0

    def __str__(self) -> str:
        """Generate a formatted string representation of all validation results.

        :returns: Multi-line string containing formatted errors and warnings with counts
        """
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
    """Top-level specification for synthetic data generation across one or more tables.

    This is the main configuration class for the dbldatagen spec-based API. It defines all tables
    to be generated, where the output should be written, and global generation options.

    :param tables: Dictionary mapping table names to their TableDefinition specifications.
                  Keys are the table names that will be used in the output destination
    :param output_destination: Target location for generated data. Can be either a
                              UCSchemaTarget (Unity Catalog) or FilePathTarget (file system).
                              If None, data will be generated but not persisted
    :param generator_options: Dictionary of global options affecting data generation behavior.
                             Common options include:
                             - random: Enable random data generation
                             - randomSeed: Seed for reproducible random generation
                             - randomSeedMethod: Method for computing random seeds
                             - verbose: Enable verbose logging
                             - debug: Enable debug logging
                             - seedColumnName: Name of internal seed column
    :param intended_for_databricks: Flag indicating if this spec is designed for Databricks.
                                   May be automatically inferred based on configuration

    .. note::
        Call the validate() method before using this spec to ensure configuration is correct

    .. note::
        Multiple tables can share the same DatagenSpec and will be generated in the order
        they appear in the tables dictionary
    """
    tables: dict[str, TableDefinition]
    output_destination: Union[UCSchemaTarget, FilePathTarget] | None = None # there is a abstraction, may be we can use that? talk to Greg
    generator_options: dict[str, Any] | None = None
    intended_for_databricks: bool | None = None # May be infered.

    def _check_circular_dependencies(
        self,
        table_name: str,
        columns: list[ColumnDefinition]
    ) -> list[str]:
        """Check for circular dependencies in baseColumn references within a table.

        Analyzes column dependencies to detect cycles where columns reference each other
        in a circular manner (e.g., col A depends on col B, col B depends on col A).
        Such circular dependencies would make data generation impossible.

        :param table_name: Name of the table being validated (used in error messages)
        :param columns: List of ColumnDefinition objects to check for circular dependencies
        :returns: List of error message strings describing any circular dependencies found.
                 Empty list if no circular dependencies exist

        .. note::
            This method performs a graph traversal to detect cycles in the dependency chain
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
        """Validate the entire DatagenSpec configuration comprehensively.

        This method performs extensive validation of the entire spec, including:
        - Ensuring at least one table is defined
        - Validating each table has columns and positive row counts
        - Checking for duplicate column names within tables
        - Verifying baseColumn references point to existing columns
        - Detecting circular dependencies in baseColumn chains
        - Validating primary key constraints
        - Checking output destination configuration
        - Validating generator options

        All validation checks are performed regardless of whether errors are found, allowing
        you to see all issues at once rather than fixing them one at a time.

        :param strict: Controls validation failure behavior:
                      - If True: Raises ValueError for any errors OR warnings found
                      - If False: Only raises ValueError for errors (warnings are tolerated)
        :returns: ValidationResult object containing all collected errors and warnings,
                 even if an exception is raised
        :raises ValueError: If validation fails based on strict mode setting.
                          The exception message contains the formatted ValidationResult

        .. note::
            It's recommended to call validate() before attempting to generate data to catch
            configuration issues early

        .. note::
            Use strict=False during development to see warnings without blocking generation
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
        """Display a formatted view of all table definitions in the spec.

        This method provides a user-friendly visualization of the spec configuration, showing
        each table's structure and the output destination. It's designed for use in Jupyter
        notebooks and will render HTML output when available.

        For each table, displays:
        - Table name
        - Output destination (or warning if not configured)
        - DataFrame showing all columns with their properties

        .. note::
            This method uses IPython.display.HTML when available, falling back to plain text
            output in non-notebook environments

        .. note::
            This is intended for interactive exploration and debugging of spec configurations
        """
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
