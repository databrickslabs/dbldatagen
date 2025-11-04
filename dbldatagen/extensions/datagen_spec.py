import os
import logging
from enum import Enum
from typing import List, Dict, Optional, Any, Literal, Union
from pydantic import BaseModel, model_validator, field_validator
import pandas as pd
from IPython.display import display, HTML


logger = logging.getLogger(__name__)


class PathType(Enum):
    UC_TABLE = "Unity Catalog Table"
    UC_VOLUME = "Unity Catalog Volume"
    RELATIVE = "Relative Path"
    ABSOLUTE_NON_UC = "Absolute Non-UC Path"
    UNKNOWN = "Unknown or Invalid Path Structure"


# Allowed (PathType, output_format_lowercase) combinations
_OFF_DB_STRICT_ALLOWED_COMBINATIONS = {
    (PathType.RELATIVE, "delta"),
    (PathType.RELATIVE, "parquet"),
    (PathType.RELATIVE, "csv"),
    # Add more formats like orc, json, text as needed
}

_DATABRICKS_COMPATIBLE_ALLOWED_COMBINATIONS = {
    (PathType.UC_TABLE, "delta"),
    (PathType.UC_VOLUME, "parquet"),
    (PathType.UC_VOLUME, "csv"),
    # Add more formats like orc, json, text as needed
}


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


# Note: There is no way to specify the per table output.
# One will have to create a different config for each schema
class UCSchemaTarget(BaseModel):
    catalog: str
    schema_: str
    output_format: str = "delta"  # Default to delta for UC Schema

    @field_validator("catalog", "schema_", mode="after")
    def validate_identifiers(cls, v):  # noqa: N805, pylint: disable=no-self-argument
        if not v.strip():
            raise ValueError("Identifier must be non-empty.")
        if not v.isidentifier():
            logger.warning(
                f"'{v}' is not a basic Python identifier. Ensure validity for Unity Catalog.")
        return v.strip()

    def __str__(self):
        return f"{self.catalog}.{self.schema_} (Format: {self.output_format}, Type: UC Table)"


class FilePathTarget(BaseModel):
    base_path: str
    output_format: Literal["csv", "parquet"]  # No default, must be specified

    @field_validator("base_path", mode="after")
    def validate_base_path(cls, v):  # noqa: N805, pylint: disable=no-self-argument
        if not v.strip():
            raise ValueError("base_path must be non-empty.")
        return v.strip()

    def __str__(self):
        return f"{self.base_path} (Format: {self.output_format}, Type: File Path)"


class ColumnDefinition(BaseModel):
    name: str
    type: Optional[DbldatagenBasicType] = None
    primary: bool = False
    options: Optional[Dict[str, Any]] = {}
    nullable: Optional[bool] = False
    omit: Optional[bool] = False
    baseColumn: Optional[str] = "id"
    baseColumnType: Optional[str] = "auto"

    @model_validator(mode="after")
    def check_constraints(self):
        if self.primary:
            if "min" in self.options or "max" in self.options:
                raise ValueError(
                    f"Primary column '{self.name}' cannot have min/max options.")
            if self.nullable:
                raise ValueError(
                    f"Primary column '{self.name}' cannot be nullable.")
            if self.primary and self.type is None:
                raise ValueError(
                    f"Primary column '{self.name}' must have a type defined.")
        return self


class TableDefinition(BaseModel):
    number_of_rows: int
    partitions: Optional[int] = None
    columns: List[ColumnDefinition]


class DatagenSpec(BaseModel):
    tables: Dict[str, TableDefinition]
    output_destination: Optional[Union[UCSchemaTarget, FilePathTarget]] = None
    generator_options: Optional[Dict[str, Any]] = {}
    intended_for_databricks: Optional[bool] = None

    @staticmethod
    def _is_running_on_databricks() -> bool:
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    @staticmethod
    def _determine_path_type(path_prefix: str) -> PathType:
        path = path_prefix.strip()
        if len(path.split(".")) == 3:
            return PathType.UC_TABLE
        if path.startswith("/Volumes/"):
            return PathType.UC_VOLUME
        if path.startswith("/") or path.startswith("\\") or (len(path) > 1 and path[1] == ":"):
            return PathType.ABSOLUTE_NON_UC
        return PathType.RELATIVE

    @model_validator(mode="before") 
    def validate_paths_and_formats(cls, values: Dict[str, Any]) -> Dict[str, Any]:  # noqa: N805, pylint: disable=no-self-argument
        output = values.get("output_destination")
        intended = values.get(
            "intended_for_databricks") or cls._is_running_on_databricks()

        if isinstance(output, FilePathTarget):
            path_type = cls._determine_path_type(output.base_path)
            fmt = output.output_format.lower()
            allowed = {PathType.UC_VOLUME,
                       PathType.RELATIVE, PathType.ABSOLUTE_NON_UC}
            if path_type not in allowed:
                raise ValueError(
                    f"Invalid path type '{path_type.value}' for FilePathTarget")
            values["path_type"] = path_type

        elif isinstance(output, UCSchemaTarget):
            path_type = PathType.UC_TABLE
            values["path_type"] = path_type

        return values

    def finalize(self) -> None:
        if self.output_destination is None:
            raise ValueError(
                "output_destination must be specified before finalization.")

    def display_all_tables(self):
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
