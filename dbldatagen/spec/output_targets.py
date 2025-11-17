import logging
from typing import Literal

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
