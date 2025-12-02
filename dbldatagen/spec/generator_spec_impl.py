import logging
import posixpath
from typing import Any, Union

from pyspark.sql import SparkSession

import dbldatagen as dg
from dbldatagen.spec.generator_spec import TableDefinition

from .generator_spec import ColumnDefinition, DatagenSpec, FilePathTarget, UCSchemaTarget


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

INTERNAL_ID_COLUMN_NAME = "id"


class Generator:
    """Main orchestrator for generating synthetic data from DatagenSpec configurations.

    This class provides the primary interface for the spec-based data generation API. It handles
    the complete lifecycle of data generation:
    1. Converting spec configurations into dbldatagen DataGenerator objects
    2. Building the actual data as Spark DataFrames
    3. Writing the data to specified output destinations (Unity Catalog or file system)

    The Generator encapsulates all the complexity of translating declarative specs into
    executable data generation plans, allowing users to focus on what data they want rather
    than how to generate it.

    :param spark: Active SparkSession to use for data generation
    :param app_name: Application name used in logging and tracking. Defaults to "DataGen_ClassBased"

    .. warning::
       Experimental - This API is subject to change in future versions

    .. note::
        The Generator requires an active SparkSession. On Databricks, you can use the pre-configured
        `spark` variable. For local development, create a SparkSession first

    .. note::
        The same Generator instance can be reused to generate multiple different specs
    """

    def __init__(self, spark: SparkSession, app_name: str = "DataGen_ClassBased") -> None:
        """Initialize the Generator with a SparkSession.

        :param spark: An active SparkSession instance to use for data generation operations
        :param app_name: Application name for logging and identification purposes
        :raises RuntimeError: If spark is None or not properly initialized
        """
        if not spark:
            logger.error(
                "SparkSession cannot be None during Generator initialization")
            raise RuntimeError("SparkSession cannot be None")
        self.spark = spark
        self._created_spark_session = False
        self.app_name = app_name
        logger.info("Generator initialized with SparkSession")

    def _columnSpecToDatagenColumnSpec(self, col_def: ColumnDefinition) -> dict[str, Any]:
        """Convert a ColumnDefinition spec into dbldatagen DataGenerator column arguments.

        This internal method translates the declarative ColumnDefinition format into the
        keyword arguments expected by dbldatagen's withColumn() method. It handles special
        cases like primary keys, nullable columns, and omitted columns.

        Primary key columns receive special treatment:
        - Automatically use the internal ID column as their base
        - String primary keys use hash-based generation
        - Numeric primary keys maintain sequential values

        :param col_def: ColumnDefinition object from a DatagenSpec
        :returns: Dictionary of keyword arguments suitable for DataGenerator.withColumn()

        .. note::
            This is an internal method not intended for direct use by end users

        .. note::
            Conflicting options for primary keys (like min/max, values, expr) will generate
            warnings but won't prevent generation - the primary key behavior takes precedence
        """
        col_name = col_def.name
        col_type = col_def.type
        kwargs = col_def.options.copy() if col_def.options is not None else {}

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

    def _prepareDataGenerators(
        self,
        config: DatagenSpec,
        config_source_name: str = "PydanticConfig"
    ) -> dict[str, dg.DataGenerator]:
        """Prepare DataGenerator objects for all tables defined in the spec.

        This internal method is the first phase of data generation. It processes the DatagenSpec
        and creates configured dbldatagen.DataGenerator objects for each table, but does not
        yet build the actual data. Each table's definition is converted into a DataGenerator
        with all its columns configured.

        The method:
        1. Iterates through all tables in the spec
        2. Creates a DataGenerator for each table with appropriate row count and partitioning
        3. Adds all columns to each DataGenerator using withColumn()
        4. Applies global generator options
        5. Returns the prepared generators ready for building

        :param config: DatagenSpec containing table definitions and configuration
        :param config_source_name: Descriptive name for the config source, used in logging
                                   and DataGenerator naming
        :returns: Dictionary mapping table names to their prepared DataGenerator instances
        :raises RuntimeError: If SparkSession is not available or if any table preparation fails
        :raises ValueError: If table configuration is invalid (should be caught by validate() first)

        .. note::
            This is an internal method. Use generateAndWriteData() for the complete workflow

        .. note::
            Preparation is separate from building to allow inspection and modification of
            DataGenerators before data generation begins
        """
        logger.info(
            f"Preparing data generators for {len(config.tables)} tables")

        if not self.spark:
            logger.error(
                "SparkSession is not available. Cannot prepare data generators")
            raise RuntimeError(
                "SparkSession is not available. Cannot prepare data generators")

        tables_config: dict[str, TableDefinition] = config.tables
        global_gen_options = config.generator_options if config.generator_options else {}

        prepared_generators: dict[str, dg.DataGenerator] = {}
        generation_order = list(tables_config.keys()) # This becomes impotant when we get into multitable

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
                    kwargs = self._columnSpecToDatagenColumnSpec(col_def)
                    data_gen = data_gen.withColumn(colName=col_def.name, **kwargs)
                    # Has performance implications.

                prepared_generators[table_name] = data_gen
                logger.info(f"Successfully prepared table: {table_name}")

            except Exception as e:
                logger.error(f"Failed to prepare table '{table_name}': {e}")
                raise RuntimeError(
                    f"Failed to prepare table '{table_name}': {e}") from e

        logger.info("All data generators prepared successfully")
        return prepared_generators

    def writePreparedData(
        self,
        prepared_generators: dict[str, dg.DataGenerator],
        output_destination: Union[UCSchemaTarget, FilePathTarget, None],
        config_source_name: str = "PydanticConfig",
    ) -> None:
        """Build and write data from prepared generators to the specified output destination.

        This method handles the second phase of data generation: taking prepared DataGenerator
        objects, building them into actual Spark DataFrames, and writing the results to the
        configured output location.

        The method:
        1. Iterates through all prepared generators
        2. Builds each generator into a DataFrame using build()
        3. Writes the DataFrame to the appropriate destination:
           - For FilePathTarget: Writes to {base_path}/{table_name}/ in specified format
           - For UCSchemaTarget: Writes to {catalog}.{schema}.{table_name} as managed table
        4. Logs row counts and write locations

        :param prepared_generators: Dictionary mapping table names to DataGenerator objects
                                   (typically from _prepareDataGenerators())
        :param output_destination: Target location for output. Can be UCSchemaTarget,
                                  FilePathTarget, or None (no write, data generated only)
        :param config_source_name: Descriptive name for the config source, used in logging
        :raises RuntimeError: If DataFrame building or writing fails for any table
        :raises ValueError: If output destination type is not recognized

        .. note::
            If output_destination is None, data is generated but not persisted anywhere.
            This can be useful for testing or when you want to process the data in-memory

        .. note::
            Writing uses "overwrite" mode, so existing tables/files will be replaced
        """
        logger.info("Starting data writing phase")

        if not prepared_generators:
            logger.warning("No prepared data generators to write")
            return

        for table_name, data_gen in prepared_generators.items():
            logger.info(f"Writing table: {table_name}")

            try:
                df = data_gen.build()
                requested_rows = data_gen.rowCount
                actual_row_count = df.count()
                logger.info(
                    f"Built DataFrame for '{table_name}': {actual_row_count} rows (requested: {requested_rows})")

                if actual_row_count == 0 and requested_rows is not None and requested_rows > 0:
                    logger.warning(f"Table '{table_name}': Requested {requested_rows} rows but built 0")

                # Write data based on destination type
                if isinstance(output_destination, FilePathTarget):
                    output_path = posixpath.join(output_destination.base_path, table_name)
                    df.write.format(output_destination.output_format).mode("overwrite").save(output_path)
                    logger.info(f"Wrote table '{table_name}' to file path: {output_path}")

                elif isinstance(output_destination, UCSchemaTarget):
                    output_table = f"{output_destination.catalog}.{output_destination.schema_}.{table_name}"
                    df.write.mode("overwrite").saveAsTable(output_table)
                    logger.info(f"Wrote table '{table_name}' to Unity Catalog: {output_table}")
                else:
                    logger.warning("No output destination specified, skipping data write")
                    return
            except Exception as e:
                logger.error(f"Failed to write table '{table_name}': {e}")
                raise RuntimeError(f"Failed to write table '{table_name}': {e}") from e
        logger.info("All data writes completed successfully")

    def generateAndWriteData(
        self,
        config: DatagenSpec,
        config_source_name: str = "PydanticConfig"
    ) -> None:
        """Execute the complete data generation workflow from spec to output.

        This is the primary high-level method for generating data from a DatagenSpec. It
        orchestrates the entire process in one call, handling both preparation and writing phases.

        The complete workflow:
        1. Validates that the config is properly structured (you should call config.validate() first)
        2. Converts the spec into DataGenerator objects for each table
        3. Builds the DataFrames by executing the generation logic
        4. Writes the results to the configured output destination
        5. Logs progress and completion status

        This method is the recommended entry point for most use cases. For more control over
        the generation process, use _prepareDataGenerators() and writePreparedData() separately.

        :param config: DatagenSpec object defining tables, columns, and output destination.
                      Should be validated with config.validate() before calling this method
        :param config_source_name: Descriptive name for the config source, used in logging
                                  and naming DataGenerator instances
        :raises RuntimeError: If SparkSession is unavailable, or if preparation or writing fails
        :raises ValueError: If the config is invalid (though config.validate() should catch this first)

        .. note::
            It's strongly recommended to call config.validate() before this method to catch
            configuration errors early with better error messages

        .. note::
            Generation is performed sequentially: table1 is fully generated and written before
            table2 begins. For multi-table generation with dependencies, the order matters

        Example:
            >>> spec = DatagenSpec(
            ...     tables={"users": user_table_def},
            ...     output_destination=UCSchemaTarget(catalog="main", schema_="test")
            ... )
            >>> spec.validate()  # Check for errors first
            >>> generator = Generator(spark)
            >>> generator.generateAndWriteData(spec)
        """
        logger.info(f"Starting combined data generation and writing for {len(config.tables)} tables")

        try:
            # Phase 1: Prepare data generators
            prepared_generators_map = self._prepareDataGenerators(config, config_source_name)

            if not prepared_generators_map and list(config.tables.keys()):
                logger.warning(
                    "No data generators were successfully prepared, though tables were defined")
                return

            # Phase 2: Write data
            self.writePreparedData(
                prepared_generators_map,
                config.output_destination,
                config_source_name
            )

            logger.info(
                "Combined data generation and writing completed successfully")

        except Exception as e:
            logger.error(
                f"Error during combined data generation and writing: {e}")
            raise RuntimeError(
                f"Error during combined data generation and writing: {e}") from e
