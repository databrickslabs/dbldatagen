from collections import defaultdict
import logging
from typing import Optional, Dict, Tuple, Union
import posixpath
from pyspark.sql import SparkSession
import dbldatagen as dg
from .datagen_spec import DatagenSpec, UCSchemaTarget, FilePathTarget, ColumnDefinition


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

INTERNAL_ID_COLUMN_NAME = "id"


class Generator:
    """
    Main data generation orchestrator that handles configuration, preparation, and writing of data.
    """

    def __init__(self, spark: SparkSession, app_name: str = "DataGen_ClassBased"):
        """
        Initialize the Generator with a SparkSession.
        Args:
            spark: An existing SparkSession instance
            app_name: Application name for logging purposes
        Raises:
            RuntimeError: If spark is None
        """
        if not spark:
            logger.error(
                "SparkSession cannot be None during Generator initialization")
            raise RuntimeError("SparkSession cannot be None")
        self.spark = spark
        self._created_spark_session = False
        self.app_name = app_name
        logger.info("Generator initialized with SparkSession")

    def _columnspec_to_datagen_columnspec(self, col_def: ColumnDefinition) -> Dict[str, str]:
        """
        Convert a ColumnDefinition to dbldatagen column specification.
        Args:
            col_def: ColumnDefinition object containing column configuration
        Returns:
            Dictionary containing dbldatagen column specification
        """
        col_name = col_def.name
        col_type = col_def.type
        kwargs = col_def.options.copy()

        if col_def.primary:
            kwargs["colType"] = col_type
            kwargs["baseColumn"] = INTERNAL_ID_COLUMN_NAME

            if col_type == "string":
                kwargs["baseColumnType"] = "hash"
            elif col_type not in ["int", "long", "integer", "bigint", "short"]:
                kwargs["baseColumnType"] = "auto"
                logger.warning(
                    f"Primary key '{col_name}' has non-standard type '{col_type}'")

            # Log conflicting options for primary keys
            conflicting_opts_for_pk = [
                "distribution", "template", "dataRange", "random", "omit",
                "min", "max", "uniqueValues", "values", "expr"
            ]

            for opt_key in conflicting_opts_for_pk:
                if opt_key in kwargs:
                    logger.warning(
                        f"Primary key '{col_name}': Option '{opt_key}' may be ignored")

            if col_def.omit is not None and col_def.omit:
                kwargs["omit"] = True
        else:
            kwargs = col_def.options.copy() if col_def.options is not None else {}

            if col_type:
                kwargs["colType"] = col_type
            if col_def.baseColumn:
                kwargs["baseColumn"] = col_def.baseColumn
            if col_def.baseColumnType:
                kwargs["baseColumnType"] = col_def.baseColumnType
            if col_def.omit is not None:
                kwargs["omit"] = col_def.omit

        return kwargs

    def prepare_data_generators(
        self,
        config: DatagenSpec,
        config_source_name: str = "PydanticConfig"
    ) -> Tuple[Dict[str, Optional[dg.DataGenerator]], bool]:
        """
        Prepare DataGenerator specifications for each table based on the configuration.
        Args:
            config: DatagenSpec Pydantic object containing table configurations
            config_source_name: Name for the configuration source (for logging)
        Returns:
            Tuple containing:
            - Dictionary mapping table names to their configured dbldatagen.DataGenerator objects 
            (or None if prep failed)
            - True if all specs were prepared successfully, False otherwise
        Raises:
            RuntimeError: If SparkSession is not available
            ValueError: If critical primary key configuration errors occur
        """
        logger.info(
            f"Preparing data generators for {len(config.tables)} tables")

        if not self.spark:
            logger.error(
                "SparkSession is not available. Cannot prepare data generators")
            raise RuntimeError(
                "SparkSession is not available. Cannot prepare data generators")

        tables_config = config.tables
        global_gen_options = config.generator_options if config.generator_options else {}

        prepared_generators: Dict[str, Optional[dg.DataGenerator]] = defaultdict(
            lambda: None)
        all_specs_successful = True
        generation_order = list(tables_config.keys())

        for table_name in generation_order:
            table_spec = tables_config[table_name]
            logger.info(f"Preparing table: {table_name}")

            try:
                # Create DataGenerator instance
                data_gen = dg.DataGenerator(
                    sparkSession=self.spark,
                    name=f"{table_name}_spec_from_{config_source_name}",
                    rows=table_spec.number_of_rows,
                    partitions=table_spec.partitions,
                    **global_gen_options,
                )

                # Process each column
                for col_def in table_spec.columns:
                    kwargs = self._columnspec_to_datagen_columnspec(col_def)
                    data_gen = data_gen.withColumn(
                        colName=col_def.name, **kwargs)

                prepared_generators[table_name] = data_gen
                logger.info(f"Successfully prepared table: {table_name}")

            except ValueError as ve:
                # Check if this is a critical primary key error
                error_msg = str(ve).lower()
                if any(phrase in error_msg for phrase in ["pk", "primary key", "primary column"]):
                    logger.error(
                        f"Critical primary key configuration error for table '{table_name}': {ve}")
                    raise ValueError(
                        f"Critical primary key configuration error for table '{table_name}': {ve}") from ve
                else:
                    logger.error(
                        f"Non-critical ValueError during preparation for table '{table_name}': {ve}")
                    prepared_generators[table_name] = None
                    all_specs_successful = False

            except Exception as e:
                logger.error(
                    f"Unexpected error during preparation for table '{table_name}': {e}")
                prepared_generators[table_name] = None
                all_specs_successful = False

        # Log final preparation status
        failed_tables = [name for name,
                         gen in prepared_generators.items() if gen is None]

        if failed_tables:
            logger.warning(f"Failed to prepare tables: {failed_tables}")

        if all_specs_successful:
            logger.info("All data generators prepared successfully")
        else:
            logger.error("Data generator preparation completed with errors")

        return prepared_generators, all_specs_successful

    def write_prepared_data(
        self,
        prepared_generators: Dict[str, Optional[dg.DataGenerator]],
        output_destination: Union[UCSchemaTarget, FilePathTarget, None],
        config_source_name: str = "PydanticConfig",
    ) -> bool:
        """
        Write data from prepared generators to the specified output destination.

        Args:
            prepared_generators: Dictionary of prepared DataGenerator objects
            output_destination: Target destination for data output
            config_source_name: Name for the configuration source (for logging)

        Returns:
            True if all writes were successful, False otherwise
        """
        logger.info("Starting data writing phase")

        if not prepared_generators:
            logger.warning("No prepared data generators to write")
            return True

        all_writes_successful = True
        attempted_any_write = False
        failed_writes = []

        for table_name, data_gen in prepared_generators.items():
            if data_gen is None:
                logger.warning(
                    f"Skipping write for table '{table_name}' - DataGenerator was not prepared")
                continue

            attempted_any_write = True
            logger.info(f"Writing table: {table_name}")

            try:
                df = data_gen.build()
                requested_rows = data_gen.rowCount
                actual_row_count = df.count()
                logger.info(
                    f"Built DataFrame for '{table_name}':{actual_row_count} rows (requested: {requested_rows})")

                if actual_row_count == 0 and requested_rows > 0:
                    logger.warning(
                        f"Table '{table_name}': Requested {requested_rows} rows but built 0")

                # Write data based on destination type
                if isinstance(output_destination, FilePathTarget):
                    output_path = posixpath.join(
                        output_destination.base_path, table_name)
                    df.write.format(
                        output_destination.output_format).save(output_path)
                    logger.info(f"Wrote table '{table_name}' to file path")

                elif isinstance(output_destination, UCSchemaTarget):
                    output_table = f"{output_destination.catalog}.{output_destination.schema_}.{table_name}"
                    df.write.mode("overwrite").saveAsTable(output_table)
                    logger.info(f"Wrote table '{table_name}' to Unity Catalog")
                else:
                    logger.warning(
                        "No output destination specified, skipping data write")
                    return True
            except Exception as e:
                logger.error(
                    f"Error during data build or write for table '{table_name}': {e}")
                failed_writes.append(table_name)
                all_writes_successful = False

        if failed_writes:
            logger.error(f"Failed to write tables: {failed_writes}")

        if not attempted_any_write:
            logger.info("No valid data generators found to write")
            return True

        if all_writes_successful:
            logger.info("All data writes completed successfully")
        else:
            logger.error("Data writing completed with errors")

        return all_writes_successful

    def generate_and_write_data(
        self,
        config: DatagenSpec,
        config_source_name: str = "PydanticConfig"
    ) -> bool:
        """
        Combined method to prepare data generators and write data in one operation.

        This method orchestrates the complete data generation workflow:
        1. Prepare data generators from configuration
        2. Write data to the specified destination

        Args:
            config: DatagenSpec Pydantic object containing table configurations
            config_source_name: Name for the configuration source (for logging)

        Returns:
            True if the entire process completed successfully, False otherwise

        Raises:
            RuntimeError: If SparkSession is not available
            ValueError: If critical errors occur during preparation or writing
        """
        logger.info(
            f"Starting combined data generation and writing for {len(config.tables)} tables")

        try:
            # Phase 1: Prepare data generators
            prepared_generators_map, overall_prep_success = self.prepare_data_generators(
                config, config_source_name)

            if not overall_prep_success:
                failed_tables = [
                    table for table, gen in prepared_generators_map.items() if gen is None]
                logger.error(
                    f"Data generator preparation failed for tables: {failed_tables}")
                raise ValueError(
                    f"Data generator preparation failed for config "
                    f"'{config_source_name}' and tables: {failed_tables}"
                )

            if not prepared_generators_map and list(config.tables.keys()):
                logger.warning(
                    "No data generators were successfully prepared, though tables were defined")
                return True

            # Phase 2: Write data
            write_success = self.write_prepared_data(
                prepared_generators_map,
                config.output_destination,
                config_source_name
            )

            if write_success:
                logger.info(
                    "Combined data generation and writing completed successfully")
                return True
            else:
                logger.error("Data writing phase failed")
                raise ValueError(
                    f"Failed to write prepared data for config: {config_source_name}")

        except ValueError as ve:
            # Re-raise ValueError as-is (these are critical errors)
            logger.error(
                f"Critical error during combined generation and writing: {ve}")
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error during combined data generation and writing: {e}")
            raise ValueError(
                f"Unexpected error during data generation for config: {config_source_name}") from e
