from typing import List, Dict, Optional
import json
import logging
from pydantic import ValidationError
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, ColumnInfo, TableConstraint
from dbldatagen.extensions.errors import SpecGenerationError
from dbldatagen.extensions.datagen_spec import (
    ColumnDefinition,
    DatagenSpec,
    TableDefinition,
    DbldatagenBasicType,
)

from dbldatagen.extensions.spec_generators.base import AbstractSpecGenerator
from dbldatagen.extensions.spec_generators.text_templates import guess_template

# Set up logger
logger = logging.getLogger(__name__)


# --- User's custom generator ---
class DatabricksUCSpecGenerator(AbstractSpecGenerator):
    def __init__(self, tables: List[str], workspace_client: Optional[WorkspaceClient] = None):
        try:
            self.workspace_client = workspace_client or WorkspaceClient()
        except Exception as e:
            # Catch potential errors during WorkspaceClient instantiation (e.g., config issues)
            raise SpecGenerationError(
                f"Failed to initialize WorkspaceClient: {e}") from e
        if not tables:
            raise SpecGenerationError("Fully qualified table name required")
        self.tables = tables

    @staticmethod
    def _map_source_type_to_datagen_type(
        source_type_str: Optional[str], column_name: str = "Unknown"
    ) -> DbldatagenBasicType:
        """
        Maps a source data type string to a DbldatagenBasicType.

        Args:
            source_type_str: The source data type string (e.g., from UC schema).
            column_name: The name of the column, for logging purposes.

        Returns:
            The corresponding DbldatagenBasicType.

        Raises:
            SchemaParsingError: If the source type cannot be reliably mapped.
        """
        if not source_type_str:
            logger.warning(
                f"Warning: Source type for column '{column_name}' is None or empty. "
                f"Defaulting to 'string'.")
            return "string"

        s_type = source_type_str.lower().strip()

        # Direct matches with DbldatagenBasicType (assuming DbldatagenBasicType is a Literal or Enum)
        # For Literal, we need to check against its __args__
        if hasattr(DbldatagenBasicType, "__args__") and s_type in DbldatagenBasicType.__args__:  # type: ignore
            return s_type  # type: ignore

        # Handle common variations or alternative names from catalogs
        type_mapping: Dict[str, DbldatagenBasicType] = {
            "int": "integer",
            "integer": "integer",
            "bigint": "long",
            "smallint": "short",
            "tinyint": "byte",
            "varchar": "string",
            "char": "string",
            "text": "string",
            "character varying": "string",
            "numeric": "decimal",
            "number": "decimal",
            "dec": "decimal",
            "double precision": "double",
            "real": "float",
            "bool": "boolean",
            "timestamp_ntz": "timestamp",  # Specific handling for Databricks
            "timestamp with local time zone": "timestamp",
            "timestamp without time zone": "timestamp",
            "date": "date",
            "binary": "binary",
            "varbinary": "binary",
        }

        # Check direct mapping first
        if s_type in type_mapping:
            return type_mapping[s_type]

        # Handle types with parameters like decimal(p,s) or varchar(n)
        if s_type.startswith(("decimal", "numeric")):
            return "decimal"
        if s_type.startswith(("varchar", "char")):
            return "string"

        # Fallback or raise error if mapping is critical
        # Consider making this stricter by raising an error if a type is truly unknown
        logger.warning(
            f"Warning: Unmapped source type '{source_type_str}' for column '{column_name}'. "
            f"Defaulting to 'string'.")
        return "string"

    def _generate_column_definitions(
        self,
        raw_column_infos: List[ColumnInfo],
        primary_key_column_names: List[str],
    ) -> List[ColumnDefinition]:
        """
        Generates a list of Pydantic ColumnDefinition objects from raw column schema information.

        Args:
            raw_column_infos: A list of ColumnInfo objects from Databricks SDK.
            primary_key_column_names: A list of column names that are part of the primary key.

        Returns:
            A list of ColumnDefinition objects.

        Raises:
            SchemaParsingError: If essential column information is missing or invalid.
        """
        pydantic_column_definitions: List[ColumnDefinition] = []

        if not raw_column_infos:
            raise SpecGenerationError(
                "_generate_column_definitions: No column information provided.")

        for raw_col_info in raw_column_infos:
            col_name = getattr(raw_col_info, "name", None)
            if not col_name:
                # Or raise SchemaParsingError
                logger.warning("Warning: Skipping column info object without a 'name' attribute.")
                continue

            source_type_str: Optional[str] = None
            type_json_str = getattr(raw_col_info, "type_json", None)

            if type_json_str:
                try:
                    type_info_dict = json.loads(type_json_str)
                    source_type_str = type_info_dict.get("type")
                except json.JSONDecodeError:
                    logger.warning(
                        f"Warning: Could not parse type_json for column '{col_name}'. "
                        f"Falling back to type_name/type_text."
                    )

            if not source_type_str:  # Fallback to type_name or type_text
                source_type_str = getattr(raw_col_info, "type_name", None)
            if not source_type_str:  # Further fallback
                # Default to string if all else fails
                source_type_str = getattr(raw_col_info, "type_text", None)

            if not source_type_str:
                logger.warning(
                    f"Warning: Could not determine source type for column '{col_name}'. Defaulting to 'string'.")
                source_type_str = "string"

            datagen_col_type = self._map_source_type_to_datagen_type(
                source_type_str, col_name)

            is_primary = col_name in primary_key_column_names

            is_nullable_from_source = getattr(raw_col_info, "nullable", True)
            if isinstance(is_nullable_from_source, str):
                is_nullable_from_source = is_nullable_from_source.lower() == "true"

            # Primary keys cannot be nullable.
            final_nullable = False if is_primary else bool(
                is_nullable_from_source)

            col_data = {
                "name": col_name,
                "type": datagen_col_type,
                "primary": is_primary,
                "nullable": final_nullable,
            }

            if datagen_col_type == "decimal":
                precision = getattr(raw_col_info, "type_precision", None)
                scale = getattr(raw_col_info, "type_scale", None)

                # Try to parse from type_json if not directly available
                if precision is None and type_json_str:
                    try:
                        type_info_dict = json.loads(type_json_str)
                        precision = type_info_dict.get("precision")
                    except (json.JSONDecodeError, TypeError):
                        pass  # Ignore if parsing fails or precision not found

                if scale is None and type_json_str:
                    try:
                        type_info_dict = json.loads(type_json_str)
                        scale = type_info_dict.get("scale")
                    except (json.JSONDecodeError, TypeError):
                        pass  # Ignore if parsing fails or scale not found

                if precision is not None:
                    col_data["precision"] = int(precision)
                if scale is not None:
                    col_data["scale"] = int(scale)

            elif datagen_col_type == "string" and not is_primary:
                template = guess_template(col_name)
                if template:
                    col_data["options"] = {"template": template}

            try:
                column_def = ColumnDefinition(**col_data)  # type: ignore
                pydantic_column_definitions.append(column_def)
            except ValidationError as e:
                raise SpecGenerationError(
                    f"Validation error for column '{col_name}': {e}") from e

        return pydantic_column_definitions

    def _get_table_definition_from_uc(
        self,
        uc_table_full_name: str,
        default_number_of_rows: int = 1000,
        default_partitions: Optional[int] = 1,
    ) -> TableDefinition:
        """
        Retrieves table schema from Unity Catalog and generates a TableDefinition.

        Args:
            uc_table_full_name: The three-part name of the table in Unity Catalog
                                (e.g., "catalog_name.schema_name.table_name").
            default_number_of_rows: Default number of rows for the TableDefinition.
            default_partitions: Default number of partitions.

        Returns:
            A TableDefinition object.

        Raises:
            WorkspaceError: If the table cannot be found or accessed.
            SchemaParsingError: If schema information is incomplete or invalid.
        """
        try:
            table_info: TableInfo = self.workspace_client.tables.get(
                uc_table_full_name)
        except Exception as e:  # Catching generic exception from SDK
            raise SpecGenerationError(
                f"Error fetching table '{uc_table_full_name}' from Unity Catalog: {e}") from e

        if not table_info:
            raise SpecGenerationError(
                f"Table '{uc_table_full_name}' not found in Unity Catalog.")

        if not table_info.columns:
            raise SpecGenerationError(
                f"No columns found for table '{uc_table_full_name}'.")

        primary_key_column_names: List[str] = []
        if table_info.table_constraints:
            for constraint in table_info.table_constraints:
                # Ensure constraint and primary_key_constraint are not None
                if constraint and constraint.primary_key_constraint:
                    # Ensure child_columns is not None and is iterable
                    if constraint.primary_key_constraint.child_columns:
                        primary_key_column_names.extend(
                            constraint.primary_key_constraint.child_columns)

        column_definitions = self._generate_column_definitions(
            raw_column_infos=table_info.columns,
            primary_key_column_names=list(
                set(primary_key_column_names)),  # Ensure uniqueness
        )

        try:
            table_def = TableDefinition(
                number_of_rows=default_number_of_rows,
                partitions=default_partitions,
                columns=column_definitions,
            )
            return table_def
        except ValidationError as e:
            raise SpecGenerationError(
                f"Validation error creating TableDefinition for '{uc_table_full_name}': {e}"
            ) from e

    def generate_spec(self) -> DatagenSpec:
        tables_def = {}
        for table in self.tables:
            table_definition_model: TableDefinition = self._get_table_definition_from_uc(
                uc_table_full_name=table,
                default_number_of_rows=5000,
                default_partitions=4,
            )
            table_name = table.split(".")[-1]
            tables_def[table_name] = table_definition_model

        # Note: The destination is left empty so that it does not confuse the user. Find a way to notify user of that
        config_obj = DatagenSpec(tables=tables_def)
        return config_obj
