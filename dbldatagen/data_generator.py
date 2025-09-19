# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` and `DataGenerator` classes
"""
import contextlib
import copy
import json
import logging
import re
from datetime import date, datetime, timedelta
from functools import partial
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import DataType, IntegerType, LongType, StringType, StructField, StructType

from dbldatagen import datagen_constants
from dbldatagen._version import _get_spark_version
from dbldatagen.column_generation_spec import ColumnGenerationSpec
from dbldatagen.config import OutputDataset
from dbldatagen.constraints import Constraint, SqlExpr
from dbldatagen.datarange import DataRange
from dbldatagen.distributions import DataDistribution
from dbldatagen.html_utils import HtmlUtils
from dbldatagen.schema_parser import SchemaParser
from dbldatagen.serialization import SerializableToDict
from dbldatagen.spark_singleton import SparkSingleton
from dbldatagen.text_generators import TextGenerator
from dbldatagen.utils import (
    DataGenError,
    deprecated,
    ensure,
    split_list_matching_condition,
    topologicalSort,
    write_data_to_output,
)


_OLD_MIN_OPTION: str = "min"
_OLD_MAX_OPTION: str = "max"
_STREAMING_TIMESTAMP_COLUMN: str = "_source_timestamp"
_UNTITLED_NAME_PREFIX: str = "Untitled"
_ALLOWED_KEYS = ["startingId", "rowCount", "output_id"]
_SPARK_DEFAULT_PARALLELISM: int = datagen_constants.SPARK_DEFAULT_PARALLELISM


class DataGenerator(SerializableToDict):
    """
    This class acts as the entry point to all data generation activities.

    :param sparkSession: `SparkSession` object to use
    :param name: Dataset name
    :param randomSeedMethod: Seed method for generating random values (e.g. `None`, `"fixed"`, `"hash_fieldname"`)
    :param rows: Number of rows to generate
    :param startingId: Starting value for generated seed column
    :param randomSeed: Random seed for random number generator
    :param partitions: Number of partitions to generate (default `spark.sparkContext.defaultParallelism`)
    :param verbose: Whether to generate verbose output logs (default `False`)
    :param batchSize: Arrow batch size to use when generating data with Pandas UDFs
    :param debug: Whether to output debug-level logs (default `False`)
    :param seedColumnName: Name of the `seed` or logical `id` column (default `id`)
    :param random: Default randomness for columns which do not explicitly set their own randomness (default `False`)

    By default, the seed column is named `id`. If you need to use this column name in your generated data,
    it is recommended that you use a different name for the seed column (e.g. `_id`).

    This may be specified by setting the `seedColumnName` attribute to `_id`

    .. note::
        When using a shared `SparkSession` (e.g. on Databricks serverless compute), the `SparkContext` is not
        available and the default parallelism is set to 200. We recommend passing an explicit value for `partitions`
        in these scenarios.
    """

    logging.getLogger("py4j").setLevel(logging.WARNING)

    name: str
    logger: logging.Logger
    sparkSession: SparkSession
    _schema: StructType | None
    _randomSeed: int
    _constraints: list[Constraint]
    _columnSpecsByName: dict[str, ColumnGenerationSpec]
    _allColumnSpecs: list[ColumnGenerationSpec]
    _nextNameIndex: int = 0

    def __init__(
        self,
        sparkSession: SparkSession | None = None,
        name: str | None = None,
        *,
        randomSeedMethod: str | None = None,
        rows: int | None = 1000000,
        startingId: int | None = 0,
        randomSeed: int | None = None,
        partitions: int | None = None,
        verbose: bool = False,
        batchSize: int | None = None,
        debug: bool = False,
        seedColumnName: str = datagen_constants.DEFAULT_SEED_COLUMN,
        random: bool = False,
        **kwargs
    ) -> None:
        """ Constructor for data generator object """

        # set up logging
        self.verbose = verbose
        self.debug = debug

        self._setupLogger()
        self._seedColumnName = seedColumnName
        self._outputStreamingFields = False

        self._generationModel = None
        self._constraints = []
        self._randomSeed = randomSeed or datagen_constants.DEFAULT_RANDOM_SEED
        self._nextNameIndex = 0

        if seedColumnName != datagen_constants.DEFAULT_SEED_COLUMN:
            if not self.logger:
                raise ValueError("Attempted to set 'seedColumnName' with no logger instantiated")

            self.logger.info(
                f"Using '{self._seedColumnName}' for seed column in place of '{datagen_constants.DEFAULT_SEED_COLUMN}"
            )

        self.name = name if name else DataGenerator.generateName()
        self._rowCount = rows
        self.starting_id = startingId
        self._schema = None

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self.sparkSession = sparkSession

        # if the active Spark session is stopped, you may end up with a valid SparkSession object but the underlying
        # SparkContext will be invalid
        assert sparkSession is not None, "Spark session not initialized"

        self.partitions = partitions if partitions is not None else self._getDefaultSparkParallelism(sparkSession)

        # check for old versions of args
        if "starting_id" in kwargs:
            self.logger.warning("starting_id is deprecated - use option 'startingId' instead")
            startingId = kwargs["starting_id"]

        if "seed" in kwargs:
            self.logger.warning("seed is deprecated - use option 'randomSeed' instead")
            randomSeed = kwargs["seed"]

        if "seed_method" in kwargs:
            self.logger.warning("seed_method is deprecated - use option 'seedMethod' instead")
            _ = kwargs["seed_method"]

        if "batch_size" in kwargs:
            self.logger.warning("batch_size is deprecated - use option 'batchSize' instead")
            batchSize = kwargs["batch_size"]

        if "use_pandas" in kwargs or "usePandas" in kwargs:
            self.logger.warning("option 'usePandas' is deprecated - Pandas will always be used")

        if "generateWithSelects" in kwargs or "generateWithSelects" in kwargs:
            self.logger.warning("option 'generateWithSelects' switch is deprecated - selects will always be used")

        self._seedMethod = randomSeedMethod

        # set default random setting
        self._defaultRandom = random if random is not None else False

        if randomSeed is None:
            self._instanceRandomSeed = self._randomSeed

            if randomSeedMethod is None:
                self._seedMethod = datagen_constants.RANDOM_SEED_HASH_FIELD_NAME
            else:
                self._seedMethod = randomSeedMethod
        else:
            self._instanceRandomSeed = randomSeed

            # if a valid random seed was supplied but no seed method was applied, make the seed method "fixed"
            if randomSeedMethod is None:
                self._seedMethod = "fixed"

        allowed_seed_methods = [
            None, datagen_constants.RANDOM_SEED_FIXED, datagen_constants.RANDOM_SEED_HASH_FIELD_NAME
        ]
        if self._seedMethod not in allowed_seed_methods:
            msg = f"seedMethod should be None, '{datagen_constants.RANDOM_SEED_FIXED}' or '{datagen_constants.RANDOM_SEED_HASH_FIELD_NAME}' "
            raise DataGenError(msg)

        self._columnSpecsByName = {}
        self._allColumnSpecs = []
        self._buildPlan: list[str] = []
        self.executionHistory: list[str] = []
        self._options: dict[str, Any] = {}
        self._buildOrder: list[list[str]] = []
        self._inferredSchemaFields: list[StructField] = []
        self.buildPlanComputed = False

        # lets add the seed column
        self.withColumn(self._seedColumnName, LongType(), nullable=False, implicit=True, omit=True, noWarn=True)
        self._batchSize = batchSize

        # set up spark session
        self._setupSparkSession(sparkSession)

        # set up use of pandas udfs
        if batchSize:
            self._setupPandas(batchSize)

    def __deepcopy__(self, memo: dict[int, object]) -> "DataGenerator":
        do_not_copy = ["logger", "sparkSession"]
        cls = self.__class__
        clone = cls.__new__(cls)
        memo[id(self)] = clone
        for k, v in self.__dict__.items():
            if k in do_not_copy:
                setattr(clone, k, getattr(self, k))
                continue
            setattr(clone, k, copy.deepcopy(v, memo))
        return clone

    @classmethod
    def _fromInitializationDict(cls, options: dict[str, Any]) -> "DataGenerator":
        """
        Creates a `DataGenerator` instance from a dictionary of class constructor options.

        :param options: Python dictionary of options for the `DataGenerator`, `ColumnGenerationSpecs`, and `Constraints`
        :return: `DataGenerator` instance
        """
        ir = options.copy()
        columns = ir.pop("columns") if "columns" in ir else []
        constraints = ir.pop("constraints") if "constraints" in ir else []
        return (
            DataGenerator(**{k: v for k, v in ir.items() if not isinstance(v, list)})
            ._loadColumnsFromInitializationDicts(columns)
            ._loadConstraintsFromInitializationDicts(constraints)
        )

    @classmethod
    def loadFromInitializationDict(cls, options: dict[str, Any]) -> "DataGenerator":
        """
        Creates a `DataGenerator` instance from a dictionary of class constructor options.

        :param options: Python dictionary of options for the `DataGenerator`, `ColumnGenerationSpecs`, and `Constraints`
        :return: `DataGenerator` instance
        """
        return cls._fromInitializationDict(options)

    def _toInitializationDict(self) -> dict[str, Any]:
        """
        Creates a Python dictionary from a `DataGenerator` instance.

        :return: Python dictionary of options for the `DataGenerator`, `ColumnGenerationSpecs`, and `Constraints`
        """
        _options = {
            "kind": self.__class__.__name__,
            "name": self.name,
            "randomSeedMethod": self._seedMethod,
            "rows": self._rowCount,
            "startingId": self.starting_id,
            "randomSeed": self._randomSeed,
            "partitions": self.partitions,
            "verbose": self.verbose, "batchSize": self._batchSize, "debug": self.debug,
            "seedColumnName": self._seedColumnName,
            "random": self._defaultRandom,
            "columns": [{
                k: v for k, v in column._toInitializationDict().items()
                if k != "kind"}
                for column in self.columnGenerationSpecs],
            "constraints": [constraint._toInitializationDict() for constraint in self.constraints]
        }
        return _options

    def saveToInitializationDict(self) -> dict[str, Any]:
        """
        Creates a Python dictionary from a `DataGenerator` instance.

        :return: Python dictionary of options for the `DataGenerator`, `ColumnGenerationSpecs`, and `Constraints`
        """
        return self._toInitializationDict()

    @property
    def seedColumnName(self) -> str:
        """
        Name of the seed column used for all data generation.

        :return: Seed column name
        """
        return self._seedColumnName

    @classmethod
    def _checkSparkVersion(cls, sparkVersion: str, minSparkVersion: tuple[int, int, int]) -> bool:
        """
        Checks the Spark version to ensure support.

        :param sparkVersion: Spark version string
        :param minSparkVersion: Minimum Spark version as tuple
        :return: True if the version is greater than or equal to the minimum supported version

        Layout of version string must be compatible "xx.xx.xx.patch"
        """
        spark_version_info = _get_spark_version(sparkVersion)

        if spark_version_info < minSparkVersion:
            logging.warning(
                f"*** Minimum version of Python supported is {minSparkVersion} - found version %s ",
                spark_version_info
            )
            return False

        return True

    def _setupSparkSession(self, sparkSession: SparkSession | None = None) -> None:
        """
        Sets up a `SparkSession` for data generation.

        :param sparkSession: Optional `SparkSession`
        """
        if sparkSession is None:
            sparkSession = SparkSingleton.getInstance()

        assert sparkSession is not None, "Spark session not initialized"

        # check if the spark version meets the minimum requirements and warn if not
        self.sparkSession = sparkSession
        sparkVersion = sparkSession.version
        self._checkSparkVersion(sparkVersion, datagen_constants.MIN_SPARK_VERSION)

    def _setupPandas(self, pandasBatchSize: int | None) -> None:
        """
        Sets Spark configurations controlling the batch size for Pandas execution.

        :param pandasBatchSize: Optional batch size for Pandas execution on Spark
        """
        if pandasBatchSize is None:
            raise ValueError("Value 'pandasBatchSize' must be specified")

        if not isinstance(pandasBatchSize, int):
            raise ValueError("Value 'pandasBatchSize' must be specified with type 'int'.")

        self.logger.info("*** using pandas udf for custom functions ***")
        self.logger.info(f"Spark version '{self.sparkSession.version}'")

        with contextlib.suppress(Exception):
            if str(self.sparkSession.version).startswith("3"):
                self.logger.info("Using spark 3.x")
                self.sparkSession.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

            self.sparkSession.conf.set("spark.sql.execution.arrow.enabled", "true")
            if self._batchSize:
                self.sparkSession.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", self._batchSize)

    def _setupLogger(self) -> None:
        """
        Configures a Python logger at warning, info or debug levels based on the parameters used to create the
        `DataGenerator`.
        """
        self.logger = logging.getLogger("DataGenerator")

        if self.debug:
            self.logger.setLevel(logging.DEBUG)

        if self.verbose:
            self.logger.setLevel(logging.INFO)

        self.logger.setLevel(logging.WARNING)

    @staticmethod
    def _getDefaultSparkParallelism(sparkSession: SparkSession | None = None) -> int:
        """
        Gets the default parallelism for a `SparkSession`. Only supported when the `SparkContext` is accessible.

        :param sparkSession: Optional `SparkSession`
        :return: Default parallelism from the associated `SparkContext`
        """
        try:
            if sparkSession and sparkSession.sparkContext:
                return sparkSession.sparkContext.defaultParallelism
            else:
                return _SPARK_DEFAULT_PARALLELISM
        except Exception:  # pylint: disable=broad-exception-caught
            logging.warning(
                f"Error getting default parallelism, using default setting of {datagen_constants.SPARK_DEFAULT_PARALLELISM}"
            )
            return _SPARK_DEFAULT_PARALLELISM

    @classmethod
    def useSeed(cls, seedVal: int) -> None:
        """
        Sets a seed value for random data generation.

        :param seedVal: Seed value
        """
        cls._randomSeed = seedVal

    @deprecated("Use `useSeed` instead")
    @classmethod
    def use_seed(cls, seedVal: int) -> None:
        """
        Sets a seed value for random data generation.

        :param seedVal: Seed value
        """
        cls._randomSeed = seedVal

    @classmethod
    def reset(cls) -> None:
        """
        Resets any state associated with the `DataGenerator`.
        """
        cls._nextNameIndex = 0

    @classmethod
    def generateName(cls) -> str:
        """
        Uses the untitled name prefix and `nextNameIndex` to generate a dummy dataset name.

        :returns: Generated dataset name
        """
        cls._nextNameIndex += 1
        new_name = _UNTITLED_NAME_PREFIX + "_" + str(cls._nextNameIndex)
        return new_name

    def clone(self) -> "DataGenerator":
        """
        Clones the `DataGenerator` via deep copy using the same spark session.

        :returns: Copy of the `DataGenerator`
        """
        new_copy = copy.deepcopy(self)
        new_copy.sparkSession = self.sparkSession
        new_copy.buildPlanComputed = False
        return new_copy

    @property
    def randomSeed(self) -> int:
        """
        Gets the random seed used to generate data.
        """
        return self._instanceRandomSeed

    @property
    def random(self) -> bool:
        """
        Gets the `DataGenerator's` default randomness for columns generated without an explicit `random` argument.
        """
        return self._defaultRandom

    def _markForPlanRegen(self) -> "DataGenerator":
        """
        Marks that the build plan needs to be regenerated.

        :returns: A modified, in-place instance of the `DataGenerator`; Allows for chaining of calls
        """
        self.buildPlanComputed = False
        return self

    def explain(self, suppressOutput: bool = False) -> str:
        """
        Explains the `DataGenerator's` current build plan.

        :param suppressOutput: Whether to suppress display of the build plan
        :returns: String containing the `DataGenerator's` build plan
        """
        if not self.buildPlanComputed:
            self.computeBuildPlan()

        rc = self._rowCount
        tasks = self.partitions
        output = [
            "",
            "Data generation plan",
            "====================",
            f"spec=DateGenerator(name={self.name}, rows={rc}, startingId={self.starting_id}, partitions={tasks})",
            ")",
            "",
            f"seed column: {self._seedColumnName}",
            "",
            f"column build order: {self._buildOrder}",
            "",
            "build plan:"
        ]

        for plan_action in self._buildPlan:
            output.append(" ==> " + plan_action)
        output.extend(["", "execution history:", ""])
        for build_action in self.executionHistory:
            output.append(" ==> " + build_action)
        output.append("")
        output.append("====================")
        output.append("")

        explain_results = "\n".join(output)
        if not suppressOutput:
            print(explain_results)

        return explain_results

    def withRowCount(self, rc: int) -> "DataGenerator":
        """
        Modifies the `DataGenerator's` row count.

        :param rc: New row count
        :returns: A modified version of the current `DataGenerator` with the new row count
        """
        self._rowCount = rc
        return self

    @deprecated("Use `withRowCount` instead")
    def setRowCount(self, rc: int) -> "DataGenerator":
        """
        Modifies the `DataGenerator's` row count.

        :param rc: New row count
        :returns: A modified version of the current `DataGenerator` with the new row count
        """
        self.logger.warning("method `setRowCount` is deprecated, use `withRowCount` instead")
        return self.withRowCount(rc)

    @property
    def rowCount(self) -> int | None:
        """
        Gets the `DataGenerator's` row count.

        This may differ from the original specified row counts, if counts need to be adjusted for purposes of
        keeping the ratio of rows to unique keys correct or other heuristics.

        :returns: Integer row count
        """
        return self._rowCount

    def withIdOutput(self) -> "DataGenerator":
        """
        Gets a `DataGenerator` with an output seed column (defaults to `id`) in the generated dataset.

        If this is not called, the seed column field is omitted from the final generated data set

        :returns: A modified version of the current `DataGenerator` with the seed column
        """
        self._columnSpecsByName[self._seedColumnName].omit = False
        self._markForPlanRegen()

        return self

    def option(self, optionKey: str, optionValue: Any) -> "DataGenerator":  # noqa: ANN401
        """
        Sets a `DataGenerator` option to the specified value.

        :param optionKey: Option key
        :param optionValue: Option value
        :returns: A modified version of the current `DataGenerator` with the new option setting
        """
        ensure(optionKey in _ALLOWED_KEYS)
        self._options[optionKey] = optionValue
        self._markForPlanRegen()
        return self

    def options(self, **kwargs) -> "DataGenerator":
        """
        Sets multiple `DataGenerator` options to their specified values using keyword arguments.

        :returns: A modified version of the current `DataGenerator` with the new option settings
        """
        for key, value in kwargs.items():
            self.option(key, value)
        self._markForPlanRegen()
        return self

    def _processOptions(self) -> "DataGenerator":
        """
        Processes supplied options set by calling `self.option()` or `self.options()`.

        :returns: A modified version of the current `DataGenerator` with the new option settings
        """
        self.logger.info(f"Using options: {self._options!s}")

        for key, value in self._options.items():
            if key == "startingId":
                self.starting_id = value
            elif key in ["rowCount", "row_count"]:
                self._rowCount = value
        return self

    def describe(self) -> dict[str, Any]:
        """
        Gets a dictionary describing the `DataGenerator`. Includes the `DataGenerator's`  name, row count, schema, and
        other options.

        :returns: Dictionary of `DataGenerator` options
        """
        return {
            "name": self.name,
            "rowCount": self._rowCount,
            "schema": self.schema,
            "randomSeed": self._instanceRandomSeed,
            "partitions": self.partitions,
            "columnDefinitions": self._columnSpecsByName,
            "debug": self.debug,
            "verbose": self.verbose
        }

    def __repr__(self) -> str:
        """
        Gets the string representation of a `DataGenerator`.

        :return: String representing the `DataGenerator`
        """
        name = getattr(self, "name", "None")
        rows = getattr(self, "_rowCount", "None")
        partitions = getattr(self, "partitions", "None")
        return f"DataGenerator(name='{name}', rows={rows}, partitions={partitions})"

    def _checkFieldList(self) -> None:
        """
        Checks the field list for common errors and raises exceptions if errors occur.
        """
        ensure(self._inferredSchemaFields is not None, "schemaFields should be non-empty")
        ensure(isinstance(self._inferredSchemaFields, list), "schemaFields should be list")

    @property
    def schemaFields(self) -> list[StructField]:
        """
        Gets a list of schema fields for the `DataGenerator's` output `DataFrame`.

        :returns: A list of StructFields for the `DataGenerator's` output `DataFrame`
        """
        self._checkFieldList()
        return [fd for fd in self._inferredSchemaFields if not self._columnSpecsByName[fd.name].isFieldOmitted]

    @property
    def schema(self) -> StructType:
        """
        Gets the schema for the `DataGenerator's` output `DataFrame`.

        :returns: Spark `StructType` representing the schema=

        .. note::
          If the data generation specification contains columns for which the datatype is inferred, the schema type
          for inferred columns may not be correct until the build command has completed.
        """
        return StructType(self.schemaFields)

    @property
    def inferredSchema(self) -> StructType:
        """
        Gets an inferred, interim schema definition from the `DataGenerator's` field specifications.

        :returns: Spark `StructType` representing the inferred schema

        .. note::
          If the data generation specification contains columns for which the datatype is inferred, the schema type
          for inferred columns may not be correct until the build command has completed.
        """
        self._checkFieldList()
        return StructType(self._inferredSchemaFields)

    def __getitem__(self, key: str) -> ColumnGenerationSpec:
        """ implement the built-in dereference by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self._columnSpecsByName[key]

    def getColumnType(self, colName: str) -> DataType:
        """
        Gets the Spark DataType for the input column.

        :param colName: Column name
        :returns: Spark DataType for the column
        """
        ct = self._columnSpecsByName[colName].datatype
        return ct if ct is not None else IntegerType()

    def isFieldExplicitlyDefined(self, colName: str) -> bool:
        """
        Checks if a column generation spec has been explicitly defined for the input column.

        :param colName: Column name
        :returns: Whether a column generation spec has been explicitly defined for the column (`True` or `False`)

        .. note::
           A column is not considered explicitly defined if it was inferred from a schema or added
           with a wildcard statement. This impacts whether the column can be redefined.
        """
        ensure(colName is not None, "colName should be non-empty")
        col_def = self._columnSpecsByName.get(colName, None)
        return not col_def.implicit if col_def is not None else False

    def getInferredColumnNames(self) -> list[str]:
        """
        Gets a list of the output column names for a `DataGenerator`.

        :returns: List of output column names
        """
        return [fd.name for fd in self._inferredSchemaFields]

    @staticmethod
    def flatten(lst: list[Any]) -> list[Any]:
        """
        Flattens a nested list.

        :param lst: Nested list to flatten
        :returns: Flattened list of items
        """
        return [item for sublist in lst for item in sublist]

    def getColumnSpec(self, name: str) -> ColumnGenerationSpec:
        """
        Gets a `ColumnGenerationSpec` for the specified column.

        :param name: Column name
        :return: `ColumnGenerationSpec` for the column
        """
        assert name is not None and len(name.strip()) > 0, "column name must be non empty string"
        return self._columnSpecsByName[name]

    def getOutputColumnNames(self) -> list[str]:
        """
        Gets a list of output column names in the `DataGenerator's` output `DataFrame`.

        :returns: List of column names in the `DataGenerator's` output `DataFrame`
        """
        return self.flatten([
            self._columnSpecsByName[fd.name].getNames() for fd in
            self._inferredSchemaFields if not self._columnSpecsByName[fd.name].isFieldOmitted
        ])

    def getOutputColumnNamesAndTypes(self) -> list[tuple[str, DataType]]:
        """
        Gets a list of output column names and data types. Flattens nested column names and types. Any
        `ColumnGenerationSpecs` which produce multiple columns will produce multiple list items.

        :returns: A list of tuples of column name and data type
        """
        return self.flatten([self._columnSpecsByName[fd.name].getNamesAndTypes()
                             for fd in
                             self._inferredSchemaFields if not self._columnSpecsByName[fd.name].isFieldOmitted])

    def withSchema(self, sch: StructType) -> "DataGenerator":
        """
        Populates column definitions and specifications for each field in the input schema.

        :param sch: A `StructType` representing the desired schema
        :returns: A modified version of the current `DataGenerator` with `ColumnGenerationSpecs` for the input schema
            fields
        """
        ensure(sch is not None, "schema sch should be non-empty")
        self._schema = sch

        for field in sch.fields:
            self.withColumn(field.name, field.dataType, implicit=True, omit=False, nullable=field.nullable)
        return self

    @staticmethod
    def _computeRange(
        dataRange: DataRange | range | None = None,
        minValue: int | float | complex | date | datetime | None = None,
        maxValue: int | float | complex | date | datetime | None = None,
        step: int | float | complex | timedelta | None = None
    ) -> tuple[Any, Any, Any]:
        """
        Computes a numeric range from the input parameters.

        :param dataRange: Optional DataRange
        :param minValue: Optional minimum value
        :param maxValue: Optional maximum value
        :param step: Optional increment value
        :returns: Tuple of minimum value, maximum value, and increment value
        """
        # TODO: may also need to check for instance of DataRange
        if dataRange and isinstance(dataRange, range):
            if maxValue or minValue != 0 or step != 1:
                raise ValueError("You cant specify both a range and minValue, maxValue or step values")
            return dataRange.start, dataRange.stop, dataRange.step
        return minValue, maxValue, step

    def withColumnSpecs(
        self,
        patterns: str | list[str] | None = None,
        fields: str | list[str] | None = None,
        matchTypes: str | list[str] | DataType | list[DataType] | None = None,
        **kwargs
    ) -> "DataGenerator":
        """
        Adds column specs for columns matching:
           - One or more field names,
           - One or more regex patterns
           - One or more Spark DataTypes

        :param patterns: Single regex pattern or list of regex patterns that match the column names.
        :param fields: Single column name or list of column names to explicitly match.
        :param matchTypes: Single Spark SQL DataType or list of Spark SQL DataTypes to match the column types.
        :returns: A modified version of the current `DataGenerator` with the provided column specs

        .. note::
           matchTypes may also take SQL type strings or a list of SQL type strings such as "array<integer>". However,
           you may not use ``INFER_DATYTYPE`` as part of the matchTypes list.

        You may also add a variety of options to further control the test data generation process.
        For full list of options, see :doc:`/reference/api/dbldatagen.column_spec_options`.
        """
        if fields is not None and isinstance(fields, str):
            fields = [fields]

        if datagen_constants.OPTION_RANDOM not in kwargs:
            kwargs[datagen_constants.OPTION_RANDOM] = self._defaultRandom

        # add support for deprecated legacy names
        if "match_types" in kwargs:
            assert not matchTypes, "Argument 'match_types' is deprecated, use 'matchTypes' instead"
            matchTypes = kwargs["match_types"]
            del kwargs["match_types"]  # remove the legacy option from keyword args as they will be used later

        if patterns and isinstance(patterns, str):
            patterns = ["^" + patterns + "$"]

        if patterns and isinstance(patterns, list):
            patterns = ["^" + pat + "$" for pat in patterns]

        all_fields = self.getInferredColumnNames()
        effective_fields = [x for x in all_fields if
                            (fields is None or x in fields) and x != self._seedColumnName]

        if patterns:
            effective_fields = [x for x in effective_fields for y in patterns if re.search(y, x) is not None]

        if matchTypes:
            effective_types = []
            match_types = [matchTypes] if not isinstance(matchTypes, list) else matchTypes

            for match_type in match_types:
                if isinstance(match_type, str):
                    if match_type == datagen_constants.INFER_DATATYPE:
                        raise ValueError("You cannot use INFER_DATATYPE with the method `withColumnSpecs`")

                    effective_types.append(SchemaParser.columnTypeFromString(match_type))
                else:
                    effective_types.append(match_type)

            effective_fields = [
                x for x in effective_fields for y in effective_types if self.getColumnType(x) == y
            ]

        for f in effective_fields:
            self.withColumnSpec(f, implicit=True, **kwargs)

        return self

    def _checkColumnOrColumnList(self, columns: str | list[str], allowId: bool = False) -> bool:
        """
        Checks if the input column or columns exist in the current DataGenerator specification.

        :param columns: Single column name or list of column names
        :param allowId: Whether to allow the DataGenerator's seed column (e.g. `id`) when checking column names
        :returns: Whether the column or list of columns exist in the current DataGenerator specification
        """
        inferred_columns = self.getInferredColumnNames()
        if allowId and columns == self._seedColumnName:
            return True

        if isinstance(columns, list):
            for column in columns:
                ensure(column in inferred_columns, f" column `{column}` must refer to defined column")
        else:
            ensure(columns in inferred_columns,f" column `{columns}` must refer to defined column")
        return True

    def withColumnSpec(
        self,
        colName: str,
        *,
        minValue: int | float | complex | date | datetime | None = None,
        maxValue: int | float | complex | date | datetime | None = None,
        step: int | float | complex | timedelta | None = 1,
        prefix: str | None = None,
        random: bool | None = None,
        distribution: DataDistribution | None = None,
        implicit: bool = False,
        dataRange: DataRange | None = None,
        omit: bool = False,
        baseColumn: str | None = None,
        **kwargs
    ) -> "DataGenerator":
        """
        Adds a `ColumnGenerationSpec` for an existing column.

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern

        You may also add a variety of options to further control the test data generation process.
        For full list of options, see :doc:`/reference/api/dbldatagen.column_spec_options`.
        """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colName in self.getInferredColumnNames(), f" column `{colName}` must refer to defined column")

        if baseColumn is not None:
            self._checkColumnOrColumnList(baseColumn)

        ensure(not self.isFieldExplicitlyDefined(colName), f"duplicate column spec for column `{colName}`")
        ensure(
            not isinstance(minValue, DataType),
            """unnecessary `datatype` argument specified for `withColumnSpec` for column `{colName}` -
                    Datatype parameter is only needed for `withColumn` and not permitted for `withColumnSpec`
                """
        )

        if random is None:
            random = self._defaultRandom

        # handle migration of old `min` and `max` options
        if _OLD_MIN_OPTION in kwargs:
            assert minValue is None, \
                "Only one of `minValue` and `minValue` can be specified. Use of `minValue` is preferred"
            minValue = kwargs[_OLD_MIN_OPTION]
            kwargs.pop(_OLD_MIN_OPTION, None)

        if _OLD_MAX_OPTION in kwargs:
            assert maxValue is None, \
                "Only one of `maxValue` and `maxValue` can be specified. Use of `maxValue` is preferred"
            maxValue = kwargs[_OLD_MAX_OPTION]
            kwargs.pop(_OLD_MAX_OPTION, None)

        new_props = {}
        new_props.update(kwargs)

        self.logger.info(
            f"Adding spec for '{colName}' with base column '{baseColumn}', implicit : '{implicit}', omit '{omit}'"
        )

        self._generateColumnDefinition(
            colName,
            self.getColumnType(colName),
            minValue=minValue,
            maxValue=maxValue,
            step=step, prefix=prefix,
            random=random,
            dataRange=dataRange,
            distribution=distribution,
            baseColumn=baseColumn,
            implicit=implicit,
            omit=omit,
            **new_props
        )
        return self

    def hasColumnSpec(self, colName: str) -> bool:
        """
        Checks if there is a `ColumnGenerationSpec` for the input column name in the current `DataGenerator`.

        :param colName: Column name
        :returns: Whether the current `DataGenerator` contains a `ColumnGenerationSpec` for the column
        """
        return colName in self._columnSpecsByName

    def withColumn(
        self,
        colName: str,
        colType: str | DataType = StringType(),
        *,
        minValue: int | float | complex | date | datetime | None = None,
        maxValue: int | float | complex | date | datetime | None = None,
        step: int | float | complex | timedelta | None = 1,
        dataRange: DataRange | None = None, prefix: str | None = None,
        random: bool | None = None,
        distribution: DataDistribution | None = None,
        baseColumn: str | None = None,
        nullable: bool = True,
        omit: bool = False,
        implicit: bool = False,
        noWarn: bool = False,
        **kwargs
    ) -> "DataGenerator":
        """
        Adds a new column generation specification to the `DataGenerator`.

        :param colName: Name of column to add. If this conflicts with the underlying seed column (`id`), it is
                        recommended that the seed column name is customized during the construction of the data
                        generator spec.
        :param colType: Data type for column. This may be specified as either a type from one of the possible
                        pyspark.sql.types (e.g. `StringType`, `DecimalType(10,3)` etc) or as a string containing a Spark
                        SQL type definition (i.e  `String`, `array<Integer>`, `map<String, Float>`)
        :param omit: if True, the column will be omitted from the final set of columns in the generated data.
                     Used to create columns that are used by other columns as intermediate results.
                     Defaults to False

        :param expr: Specifies SQL expression used to create column value. If specified, overrides the default rules
                     for creating column value. Defaults to None

        :param baseColumn: String or list of columns to control order of generation of columns. If not specified,
                           column is dependent on base seed column (which defaults to `id`)

        :returns: A modified version of the current `DataGenerator` with the struct column added

        .. note::
           if the value ``None`` is used for the ``colType`` parameter, the method will try to use the underlying
           datatype derived from the base columns.

           If the value ``INFER_DATATYPE`` is used for the ``colType`` parameter and a SQL expression has been supplied
           via the ``expr`` parameter, the method will try to infer the column datatype from the SQL expression when
           the ``build()`` method is called.

           Inferred data types can only be used if the ``expr`` parameter is specified.

           Note that properties which return a schema based on the specification may not be accurate until the
           ``build()`` method is called. Prior to this, the schema may indicate a default column type for those fields.

        You may also add a variety of additional options to further control the test data generation process.
        For full list of options, see :doc:`/reference/api/dbldatagen.column_spec_options`.
        """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colType is not None, f"Must specify column type for column `{colName}`")
        if baseColumn is not None:
            self._checkColumnOrColumnList(baseColumn, allowId=True)

        if not noWarn and colName == self._seedColumnName:
            self.logger.warning(f"Adding a new column named '{colName}' overrides seed column '{self._seedColumnName}'")
            self.logger.warning("Use `seedColumName` option on DataGenerator construction for different seed column")

        # handle migration of old `min` and `max` options
        if _OLD_MIN_OPTION in kwargs:
            assert minValue is None, \
                "Only one of `minValue` and `minValue` can be specified. Use of `minValue` is preferred"
            minValue = kwargs[_OLD_MIN_OPTION]
            kwargs.pop(_OLD_MIN_OPTION, None)

        if _OLD_MAX_OPTION in kwargs:
            assert maxValue is None, \
                "Only one of `maxValue` and `maxValue` can be specified. Use of `maxValue` is preferred"
            maxValue = kwargs[_OLD_MAX_OPTION]
            kwargs.pop(_OLD_MAX_OPTION, None)

        if random is None:
            random = self._defaultRandom

        new_props = {}
        new_props.update(kwargs)

        self.logger.info(f"effective range: {minValue}, {maxValue}, {step} args: {kwargs}")
        self.logger.info("adding column - `%s` with baseColumn : `%s`, implicit : %s , omit %s",
                         colName, baseColumn, implicit, omit)
        newColumn = self._generateColumnDefinition(
            colName,
            colType,
            minValue=minValue,
            maxValue=maxValue,
            step=step,
            prefix=prefix,
            random=random,
            distribution=distribution,
            baseColumn=baseColumn,
            dataRange=dataRange,
            implicit=implicit,
            omit=omit,
            **new_props
        )

        # note for inferred columns, the column type is initially sey to a StringType but may be superceded later
        self._inferredSchemaFields.append(StructField(colName, newColumn.datatype, nullable))
        return self

    def _loadColumnsFromInitializationDicts(self, columns: list[dict[str, Any]]) -> "DataGenerator":
        """
        Adds a set of columns to the synthetic generation specification.

        :param columns: A list of column generation specifications as dictionaries
        :returns:       A modified in-place instance of a data generator allowing for chaining of calls
                        following a builder pattern
        """
        for column in columns:
            _column = column.copy()
            for k, v in _column.items():
                if not isinstance(v, dict):
                    continue
                value_superclass = (
                    DataRange if k == "dataRange"
                    else DataDistribution if k == "distribution"
                    else TextGenerator
                )
                value_subclasses = value_superclass.__subclasses__()
                if v["kind"] not in [s.__name__ for s in value_subclasses]:
                    raise ValueError(f"{v['kind']} is not a valid object type for property {k}")
                value_class = next((s for s in value_subclasses if s.__name__ == v["kind"]), type(None))
                if not issubclass(value_class, SerializableToDict):
                    raise NotImplementedError(f"Object of class {value_class} is not serializable.")
                _column[k] = value_class._fromInitializationDict(v)  # type: ignore
            self.withColumn(**_column)
        return self

    def _mkSqlStructFromList(self, fields: list[str | tuple[str, str]]) -> str:
        """
        Create a SQL struct expression from a list of fields

        :param fields: a list of elements that make up the SQL struct expression (each being a string or tuple)
        :returns: SQL expression to generate the struct

        .. note::
          This method is used internally when creating struct columns. It is not intended for general use.

          Each element of the list may be a simple string, or a tuple.
          When the element is specified as a simple string, it must be the name of a previously defined column which
          will be used as both the field name within the struct and the SQL expression to generate the field value.

          When the element is specified as a tuple, it must be a tuple of two elements. The first element must be the
          name of the field within the struct. The second element must be a SQL expression that will be used to generate
          the field value, and may reference previously defined columns.
        """
        assert fields is not None and isinstance(fields, list), \
            "Fields must be a non-empty list of fields that make up the struct elements"
        assert len(fields) >= 1, "Fields must be a non-empty list of fields that make up the struct elements"

        struct_expressions = []

        for fieldSpec in fields:
            if isinstance(fieldSpec, str):
                struct_expressions.append(f"'{fieldSpec}'")
                struct_expressions.append(fieldSpec)
            elif isinstance(fieldSpec, tuple):
                assert len(fieldSpec) == 2, "tuple must be field name and SQL expression strings"
                assert isinstance(fieldSpec[0], str), "First element must be field name string"
                assert isinstance(fieldSpec[1], str), "Second element must be field value SQL string"
                struct_expressions.append(f"'{fieldSpec[0]}'")
                struct_expressions.append(fieldSpec[1])

        struct_expression = f"named_struct({','.join(struct_expressions)})"
        return struct_expression

    def _mkStructFromDict(self, fields: dict[str, Any]) -> str:
        assert fields is not None and isinstance(fields, dict), \
            "Fields must be a non-empty dict of fields that make up the struct elements"
        struct_expressions = []

        for key, value in fields.items():
            struct_expressions.append(f"'{key}'")
            if isinstance(value, str):
                struct_expressions.append(str(value))
            elif isinstance(value, dict):
                struct_expressions.append(self._mkStructFromDict(value))
            elif isinstance(value, list):
                array_expressions = ",".join([str(x) for x in value])
                struct_expressions.append(f"array({array_expressions})")
            else:
                raise ValueError(f"Invalid field element for field `{key}`")

        struct_expression = f"named_struct({','.join(struct_expressions)})"
        return struct_expression

    def withStructColumn(
        self,
        colName: str,
        fields: list[str | tuple[str, str]] | dict[str, Any] | None = None,
        asJson: bool = False,
        **kwargs
    ) -> "DataGenerator":
        """
        Adds a struct column to the synthetic data generation specification. This will add a new column composed of
        a struct of the specified fields.

        :param colName: Column name
        :param fields: A list of elements to compose as a struct valued column (each being a string or tuple), or a
            dictionary outlining the structure of the struct valued column
        :param asJson: Whether to generate the struct as a JSON string (default `False`)
        :param kwargs: Optional keyword arguments to pass to the underlying column generators (see `withColumn`)
        :return: A modified version of the current `DataGenerator` with the struct column added

        .. note::
            Additional options for the field specification may be specified as keyword arguments.

            The fields specification specified by the `fields` argument may be :

            - A list of field references (`strings`) which will be used as both the field name and the SQL expression
            - A list of tuples of the form **(field_name, field_expression)** where `field_name` is the name of the
              field. In that case, the `field_expression` string should be a SQL expression to generate the field value
            - A Python dict outlining the structure of the struct column. The keys of the dict are the field names

            When using the `dict` form of the field specifications, a field whose value is a list will be treated
            as creating a SQL array literal.
        """
        assert fields is not None and isinstance(fields, list | dict), \
            "Fields argument must be a list of field specifications or dict outlining the target structure "
        assert isinstance(colName, str) and len(colName) > 0, "Must specify a column name"

        if isinstance(fields, list):
            assert len(fields) > 0, \
                "Must specify at least one field for struct column"
            struct_expr = self._mkSqlStructFromList(fields)
        elif isinstance(fields, dict):
            struct_expr = self._mkStructFromDict(fields)
        else:
            raise ValueError(f"Invalid field specification for struct column `{colName}`")

        if asJson:
            output_expr = f"to_json({struct_expr})"
            newDf = self.withColumn(colName, StringType(), expr=output_expr, **kwargs)
        else:
            newDf = self.withColumn(colName, datagen_constants.INFER_DATATYPE, expr=struct_expr, **kwargs)

        return newDf

    def _generateColumnDefinition(
        self,
        colName: str,
        colType: DataType | str | None = None,
        baseColumn: str | None = None,
        *,
        implicit: bool = False,
        omit: bool = False,
        nullable: bool = True,
        **kwargs
    ) -> ColumnGenerationSpec:
        """ generate field definition and column spec

        .. note::
            Any time that a new column definition is added, we'll mark that the build plan needs to be regenerated.
            For our purposes, the build plan determines the order of column generation etc.

        :returns: Newly added column_spec
        """
        if colType is None:
            if baseColumn is None:
                raise ValueError("No value provided for 'baseColumn' when generating column without 'colType'")
            colType = self.getColumnType(baseColumn)

            if colType == datagen_constants.INFER_DATATYPE:
                raise ValueError("When base column(s) have inferred datatype, you must specify the column type")

        new_props = {}
        new_props.update(kwargs)

        # if the column  has the option `random` set to true
        # then use the instance level random seed
        # otherwise use the default random seed for the class
        if datagen_constants.OPTION_RANDOM_SEED in new_props:
            effective_random_seed = new_props[datagen_constants.OPTION_RANDOM_SEED]
            new_props.pop(datagen_constants.OPTION_RANDOM_SEED)
            new_props[datagen_constants.OPTION_RANDOM] = True

            # if random seed has override but randomSeedMethod does not
            # set it to fixed
            if datagen_constants.OPTION_RANDOM_SEED_METHOD not in new_props:
                new_props[datagen_constants.OPTION_RANDOM_SEED_METHOD] = datagen_constants.RANDOM_SEED_FIXED

        elif new_props.get(datagen_constants.OPTION_RANDOM):
            effective_random_seed = self._instanceRandomSeed
        else:
            effective_random_seed = datagen_constants.DEFAULT_RANDOM_SEED

        # handle column level override
        if datagen_constants.OPTION_RANDOM_SEED_METHOD in new_props:
            effective_random_seed_method = new_props[datagen_constants.OPTION_RANDOM_SEED_METHOD]
            new_props.pop(datagen_constants.OPTION_RANDOM_SEED_METHOD)
        else:
            effective_random_seed_method = self._seedMethod

        column_spec = ColumnGenerationSpec(
            colName,
            colType,
            baseColumn=baseColumn,
            implicit=implicit,
            omit=omit,
            randomSeed=effective_random_seed,
            randomSeedMethod=effective_random_seed_method,
            nullable=nullable,
            verbose=self.verbose,
            debug=self.debug,
            seedColumnName=self._seedColumnName,
            **new_props
        )

        self._columnSpecsByName[colName] = column_spec

        # if column spec for column already exists - remove it
        items_to_remove = [x for x in self._allColumnSpecs if x.name == colName]
        for x in items_to_remove:
            self._allColumnSpecs.remove(x)

        self._allColumnSpecs.append(column_spec)

        # mark that the build plan needs to be regenerated
        self._markForPlanRegen()

        return column_spec

    def _getBaseDataFrame(
            self, startId: int | None = None, streaming: bool = False, options: dict[str, Any] | None = None
    ) -> DataFrame:
        """ generate the base data frame and seed column (which defaults to `id`) , partitioning the data if necessary

        This is used when generating the test data.

        A base data frame is created and then each of the additional columns are generated, according to
        base column dependency order, and added to the base data frame using expressions or withColumn statements.

        :returns: Spark data frame for base data that drives the data generation
        """

        start_id = startId or 0
        row_count = self._rowCount or 0
        end_id = row_count + start_id
        id_partitions = self.partitions if self.partitions is not None else 4

        if not streaming:
            self.logger.info(
                f"Generating data frame with ids from {startId} to {end_id} with {id_partitions} partitions"
            )
            self.executionHistory.append(
                f"Generating data frame with ids from {startId} to {end_id} with {id_partitions} partitions"
            )

            df = self.sparkSession.range(
                start=start_id,
                end=end_id,
                numPartitions=id_partitions
            )

            # spark.range generates a dataframe with the column `id` so rename it if its not our seed column
            if self._seedColumnName != datagen_constants.SPARK_RANGE_COLUMN:
                df = df.withColumnRenamed(datagen_constants.SPARK_RANGE_COLUMN, self._seedColumnName)

            return df

        status = (
            f"Generating streaming data frame with ids from {startId} to {end_id} with {id_partitions} partitions"
        )
        self.logger.info(status)
        self.executionHistory.append(status)

        reader = self.sparkSession.readStream.format("rate")
        if options is not None:
            if "rowsPerSecond" not in options:
                options["rowsPerSecond"] = 1
            if "numPartitions" not in options:
                options["numPartitions"] = id_partitions

            for k, v in options.items():
                reader = reader.option(k, v)
            return reader.load().withColumnRenamed("value", self._seedColumnName)

        else:
            return (
                reader
                .option("rowsPerSecond", 1)
                .option("numPartitions", id_partitions)
                .load()
                .withColumnRenamed("value", self._seedColumnName)
            )

    def _computeColumnBuildOrder(self) -> list[list[str]]:
        """ compute the build ordering using a topological sort on dependencies

        In order to avoid references to columns that have not yet been generated, the test data generation process
        sorts the columns according to the order they need to be built.

        This determines which columns are built first.

        The test generation process will select the columns in the correct order at the end so that the columns
        appear in the correct order in the final output.

        :returns: the build ordering
        """
        dependency_ordering = [
            (x.name, set(x.dependencies)) if x.name != self._seedColumnName
            else (self._seedColumnName, set()) for x in self._allColumnSpecs
        ]

        self.logger.info("dependency list: %s", str(dependency_ordering))

        build_order = topologicalSort(dependency_ordering, flatten=False, initial_columns=[self._seedColumnName])

        self._buildOrder = build_order  # type: ignore

        self.logger.info("columnBuildOrder: %s", str(self._buildOrder))

        self._buildOrder = self._adjustBuildOrderForSqlDependencies(self._buildOrder, self._columnSpecsByName)

        return self._buildOrder

    def _adjustBuildOrderForSqlDependencies(self, buildOrder: list[list[str]], columnSpecsByName: dict[str, ColumnGenerationSpec]) -> list[list[str]]:
        """ Adjust column build order according to the following heuristics

        1: if the column being built in a specific build order phase has a SQL expression and it references
           other columns in the same build phase (or potentially references them as the expression parsing is
           primitive), separate that phase into multiple phases.

        It will also issue a warning if the SQL expression appears to reference a column built later

        :param buildOrder: list of lists of ids - each sublist represents phase of build
        :param columnSpecsByName: dictionary to map column names to column specs
        :returns: Spark SQL dataframe of generated test data
        """
        new_build_order = []

        all_columns = {item for sublist in buildOrder for item in sublist}
        built_columns: list[str] = []
        prior_phase_built_columns: list[str] = []

        # for each phase, evaluate it to see if it needs to be split
        for current_phase in buildOrder:
            separate_phase_columns = []

            for columnBeingBuilt in current_phase:

                if columnBeingBuilt in columnSpecsByName:
                    cs = columnSpecsByName[columnBeingBuilt]

                    if cs.expr is not None:
                        sql_references = SchemaParser.columnsReferencesFromSQLString(cs.expr, filterItems=all_columns)

                        # determine references to columns not yet built
                        forward_references = set(sql_references) - set(built_columns)
                        if len(forward_references) > 0:
                            msg = f"Column '{columnBeingBuilt} may have forward references to {forward_references}."
                            self.logger.warning(msg)
                            self.logger.warning("Use `baseColumn` attribute to correct build ordering if necessary")

                        references_not_yet_built = set(sql_references) - set(prior_phase_built_columns)

                        if len(references_not_yet_built.intersection(set(current_phase))) > 0:
                            separate_phase_columns.append(columnBeingBuilt)

                # for each column, get the set of sql references and filter against column names
                built_columns.append(columnBeingBuilt)

            if len(separate_phase_columns) > 0:
                # split phase based on columns in separate_phase_column_list set
                separate_cols_set = set(separate_phase_columns)
                is_in_separate_cols = partial(lambda col_set, el: el in col_set, separate_cols_set)
                revised_phase = split_list_matching_condition(current_phase, is_in_separate_cols)
                new_build_order.extend(revised_phase)
            else:
                # no change to phase
                new_build_order.append(current_phase)

            prior_phase_built_columns.extend(current_phase)

        return new_build_order

    @property
    def build_order(self) -> list[list[str]]:
        """
        Gets the `DataGenerator's` build order. Excludes the seed column (`id` by default).

        :returns: A list of lists of columns which can be built at the same time (e.g. do not have dependencies)
        """
        if not self.buildPlanComputed:
            self.computeBuildPlan()

        return [x for x in self._buildOrder if x != [self._seedColumnName]]

    def _getColumnDataTypes(self, columns: list[str]) -> list[DataType]:
        """
        Gets the data types for all `ColumnGenerationSpecs`.

        :param columns: list of Spark `DataTypes`
        """
        return [self._columnSpecsByName[colspec].datatype for colspec in columns]

    @property
    def columnGenerationSpecs(self) -> list[ColumnGenerationSpec]:
        """
        Gets a list of all `ColumnGenerationSpecs`.

        :returns: List of `ColumnGenerationSpecs`
        """
        return self._allColumnSpecs

    @property
    def constraints(self) -> list[Constraint]:
        """
        Gets a list of all `Constraints`.

        :returns: List of `Constraints`
        """
        return self._constraints

    def withConstraint(self, constraint: Constraint) -> "DataGenerator":
        """
        Adds a constraint to the `DataGenerator`. Constraints control data generation (e.g. by imposing value limits).

        :param constraint: A `Constraint` object
        :returns: A modified version of the current DataGenerator with the constraint applied

        .. note::
            Constraints are applied at the end of the data generation. Depending on the type of the constraint, the
            constraint may also affect other aspects of the data generation.
        """
        assert constraint is not None, "Constraint cannot be empty"
        assert isinstance(constraint, Constraint),  \
            "Value for 'constraint' must be an instance or subclass of the Constraint class."
        self._constraints.append(constraint)
        return self

    def withConstraints(self, constraints: list[Constraint]) -> "DataGenerator":
        """
        Adds multiple constraints to the `DataGenerator`. Constraints control data generation (e.g. by imposing value
        limits).

        :param constraints: A list of `Constraint` objects
        :returns: A modified version of the current `DataGenerator` with the constraints applied

        .. note::
            Constraints are applied at the end of the data generation. Depending on the type of the constraint, the
            constraint may also affect other aspects of the data generation.
        """
        assert constraints is not None, "Constraints list cannot be empty"

        for constraint in constraints:
            assert constraint is not None, "Constraint cannot be empty"
            assert isinstance(constraint, Constraint),  \
                "Constraint must be an instance of, or an instance of a subclass of the Constraint class"

        self._constraints.extend(constraints)
        return self

    def withSqlConstraint(self, sqlExpression: str) -> "DataGenerator":
        """
        Adds a SQL expression constraint to the current `DataGenerator`.

        :param sqlExpression: SQL expression for constraint. Rows will be returned where SQL expression evaluates true
        :returns: A modified version of the current `DataGenerator` with the SQL expression constraint applied

        .. note::
            Note in the current implementation, this may be equivalent to adding where clauses to the generated dataframe
            but in future releases, this may be optimized to affect the underlying data generation so that constraints
            are satisfied more efficiently.
        """
        self.withConstraint(SqlExpr(sqlExpression))
        return self

    def _loadConstraintsFromInitializationDicts(self, constraints: list[dict[str, Any]]) -> "DataGenerator":
        """ Adds a set of constraints to the synthetic generation specification.
            :param constraints: A list of constraints as dictionaries
            :returns:       A modified in-place instance of a data generator allowing for chaining of calls
                            following a builder pattern
        """
        for c in constraints:
            t = next((s for s in Constraint.__subclasses__() if s.__name__ == c["kind"]), Constraint)
            self.withConstraint(t._fromInitializationDict(c))
        return self

    def computeBuildPlan(self) -> "DataGenerator":
        """
        Computes a pseudo build plan to prepare the `DataGenerator` for building.

        This build plan is not a true build plan - it is only used for debugging purposes, but does not actually
        drive the column generation order.

        :returns: A modified version of the current `DataGenerator` with a computed pseudo build plan
        """
        self._buildPlan = []
        self.executionHistory = []
        self._processOptions()
        self._buildPlan.append(f"Build Spark data frame with seed column: '{self._seedColumnName}'")

        # add temporary columns
        for cs in self._allColumnSpecs:
            # extend overall build plan with build plan of each column spec
            self._buildPlan.extend(cs._initialBuildPlan)

            # handle generation of any temporary columns
            for tmp_col in cs.temporaryColumns:
                # create column spec for temporary column if its not already present
                if not self.hasColumnSpec(tmp_col[0]):
                    self._buildPlan.append(f"materializing temporary column {tmp_col[0]}")
                    self.withColumn(tmp_col[0], tmp_col[1], **tmp_col[2])

        # TODO: set up the base column data type information
        for cs in self._allColumnSpecs:
            base_column_datatypes = self._getColumnDataTypes(cs.baseColumns)
            cs.setBaseColumnDatatypes(base_column_datatypes)

        self._computeColumnBuildOrder()

        for x1 in self._buildOrder:
            for x in x1:
                cs = self._columnSpecsByName[x]
                self._buildPlan.append(cs.getPlanEntry())

        self.buildPlanComputed = True
        return self

    def _applyPreGenerationConstraints(self, withStreaming: bool = False) -> None:
        """ Apply pre data generation constraints """
        if self._constraints is not None and len(self._constraints) > 0:
            for constraint in self._constraints:
                assert isinstance(constraint, Constraint), "Value for 'constraint' should be of type 'Constraint'"
                if withStreaming and not constraint.supportsStreaming:
                    raise RuntimeError(f"Constraint `{constraint}` does not support streaming data generation")
                constraint.prepareDataGenerator(self)

    def _applyPostGenerationConstraints(self, df: DataFrame) -> DataFrame:
        """ Build and apply the constraints using two mechanisms
            - Apply transformations to dataframe
            - Apply expressions as SQL filters using where clauses"""
        if self._constraints is not None and len(self._constraints) > 0:

            for constraint in self._constraints:
                df = constraint.transformDataframe(self, df)

            # get set of constraint expressions
            constraint_expressions = [constraint.filterExpression for constraint in self._constraints]
            combined_constraint_expression = Constraint.mkCombinedConstraintExpression(constraint_expressions)

            # apply the filter
            if combined_constraint_expression is not None:
                self.executionHistory.append(f"Applying constraint expression: {combined_constraint_expression}")
                df = df.where(combined_constraint_expression)

        return df

    def build(
        self,
        withTempView: bool = False,
        withView: bool = False,
        withStreaming: bool = False,
        options: dict[str, Any] | None = None
    ) -> DataFrame:
        """
        Builds a Spark `DataFrame` from the current `DataGenerator`.

        If `withStreaming` is True, this will generate a streaming `DataFrame`. Use options
        (e.g. `{'rowsPerSecond': 5000}`) to control the rate of streaming data generation.

        For example:

        `dfTestData = testDataSpec.build(withStreaming=True,options={'rowsPerSecond': 5000})`

        :param withTempView: Whether to create a temporary view for the generated data (default `False`)
        :param withView: Whether to create a global temporary view for the generated data (default `False`)
        :param withStreaming: Whether to create the generated data using a streaming source (default `False`)
            - If True, generates streaming data using Spark Structured Streaming's rate source
            - If False, generates batch data
        :param options: A dictionary of options controlling data generation
        :returns: Spark `DataFrame` with generated data
        """
        self.logger.debug("starting build ... withStreaming [%s]", withStreaming)
        self.executionHistory = []

        self._applyPreGenerationConstraints(withStreaming=withStreaming)
        self.computeBuildPlan()

        output_columns = self.getOutputColumnNames()
        ensure(output_columns is not None and len(output_columns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base seed column
               """)

        df1 = self._getBaseDataFrame(self.starting_id, streaming=withStreaming, options=options)

        self.executionHistory.append("Using Pandas Optimizations {True}")

        # build columns
        df1 = self._buildColumnExpressionsWithSelects(df1)

        # apply post generation constraints
        df1 = self._applyPostGenerationConstraints(df1)

        df1 = df1.select(*self.getOutputColumnNames())
        self.executionHistory.append(f"selecting columns: {self.getOutputColumnNames()}")

        # register temporary or global views if necessary
        if withView:
            self.executionHistory.append("registering view")
            self.logger.info("Registered global view [%s]", self.name)
            df1.createGlobalTempView(self.name)
            self.logger.info("Registered!")
        elif withTempView:
            self.executionHistory.append("registering temp view")
            self.logger.info("Registering temporary view [%s]", self.name)
            df1.createOrReplaceTempView(self.name)
            self.logger.info("Registered!")

        return df1

    # noinspection PyProtectedMember
    def _buildColumnExpressionsWithSelects(self, df1: DataFrame) -> DataFrame:
        """
        Build column generation expressions with selects
        :param df1: dataframe for base data generator
        :return: new dataframe

        The data generator build plan is separated into `rounds` of expressions. Each round consists of
        expressions that are generated using a single `select` operation
        """
        self.executionHistory.append("Generating data with selects")
        # generation with selects may be more efficient as less intermediate data frames
        # are generated resulting in shorter lineage
        for colNames in self.build_order:
            build_round = ["*"]
            column_specs_applied = []
            inx_col = 0
            self.executionHistory.append(f"building stage for columns: {colNames}")
            for colName in colNames:
                col1 = self._columnSpecsByName[colName]
                column_generators = col1.makeGenerationExpressions()
                self.executionHistory.extend(col1.executionHistory)
                if isinstance(column_generators, list) and len(column_generators) == 1:
                    build_round.append(column_generators[0].alias(colName))
                elif isinstance(column_generators, list) and len(column_generators) > 1:
                    for i, cg in enumerate(column_generators):
                        build_round.append(cg.alias(f"{colName}_{i}"))
                else:
                    build_round.append(column_generators.alias(colName))
                column_specs_applied.append(col1)
                inx_col = inx_col + 1

            df1 = df1.select(*build_round)

            # apply any post select processing
            for cs in column_specs_applied:
                cs._onSelect(df1)
        return df1

    @staticmethod
    def _sqlTypeFromSparkType(dt: DataType) -> str:
        """
        Gets the string representation of a Spark DataType.

        :param dt: Spark DataType
        :returns: String representation of the Spark DataType
        """
        return dt.simpleString()

    @staticmethod
    def _mkInsertOrUpdateStatement(columns: list[str], srcAlias: str, substitutions: list[str] | None, isUpdate: bool = True) -> str:
        if substitutions is None:
            substitutions = []
        results = []
        subs = {}
        for x in columns:
            subs[x] = f"{srcAlias}.{x}"
        for x in substitutions:
            subs[x[0]] = x[1]

        for substitution_col in columns:
            new_val = subs[substitution_col]
            if isUpdate:
                results.append(f"{substitution_col}={new_val}")
            else:
                results.append(f"{new_val}")

        return ", ".join(results)

    def scriptTable(
        self,
        name: str | None = None,
        location: str | None = None,
        tableFormat: str = "delta",
        asHtml: bool = False
    ) -> str:
        """
        Gets a Spark SQL `CREATE TABLE AS SELECT` statement suitable for the format of test data set.

        :param name: Delta table name to use in generated script
        :param location: Optional table location (default `None`). If specified, will generate an external table in
            this path.
        :param tableFormat: Table format (default `"delta"`)
        :param asHtml: Whether to generate output suitable for use with the `displayHTML` method (default `False`)
        :returns: A Spark SQL expression string representing the `CREATE TABLE AS SELECT` statement
        """
        assert name is not None, "`name` must be specified"

        self.computeBuildPlan()

        output_columns = self.getOutputColumnNamesAndTypes()

        results = [f"CREATE TABLE IF NOT EXISTS {name} ("]
        ensure(output_columns is not None and len(output_columns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base seed column
               """)

        col_expressions = []
        for col_to_output in output_columns:
            col_expressions.append(f"    {col_to_output[0]} {DataGenerator._sqlTypeFromSparkType(col_to_output[1])}")
        results.append(",\n".join(col_expressions))
        results.append(")")
        results.append(f"using {tableFormat}")

        if location is not None:
            results.append(f"location '{location}'")

        statement = "\n".join(results)

        if asHtml:
            statement = HtmlUtils.formatCodeAsHtml(statement)

        return statement

    def scriptMerge(
        self,
        tgtName: str | None = None,
        srcName: str | None = None,
        *,
        updateExpr: str | None = None,
        delExpr: str | None = None,
        joinExpr: str | None = None,
        timeExpr: str | None = None,
        insertExpr: str | None = None,
        useExplicitNames: bool = True,
        updateColumns: list[str] | None = None,
        updateColumnExprs: list[str] | None = None,
        insertColumns: list[str] | None = None,
        insertColumnExprs: list[str] | None = None,
        srcAlias: str = "src",
        tgtAlias: str = "tgt",
        asHtml: bool = False
    ) -> str:
        """
        Gets a Spark SQL `MERGE` statement suitable for the format of test dataset.

        :param tgtName: Name of target table to use in generated script
        :param srcName: Name of source table to use in generated script
        :param updateExpr: Optional string representing the UPDATE WHEN condition (default `None`). If not present, any
            row which matches the target is considered an update.
        :param delExpr: Optional string representing the DELETE WHEN condition (default `None`). If not present, no
            delete clause is generated.
        :param joinExpr: String representing the MERGE ON condition (e.g. `tgt.id=src.id`)
        :param timeExpr: Optional time travel expression (e.g. `TIMESTAMP AS OF timestamp_expression` or `
            VERSION AS OF version`)
        :param insertExpr: Optional string representing the INSERT WHEN condition. If not present, any row which does
            not match the target is considered an insert.
        :param useExplicitNames: If True, generates explicit column names in insert and update statements.
        :param updateColumns: Optional list of strings designating columns to update. If not supplied, uses all columns
            defined in the DataGenerator.
        :param updateColumnExprs: Optional list of strings designating designating column expressions for update.
            By default, will use src column as update value for the target table. This should have the form
            [ ("update_column_name", "update column expr"), ...]
        :param insertColumns: Optional list of strings designating columns to insert. If not supplied, uses all columns
            defined in the DataGenerator.
        :param insertColumnExprs: Optional list of strings designating designating column expressions for insert.
            By default, will use src column as insert value into the target table. This should have the form
            [ ("insert_column_name", "insert column expr"), ...]
        :param tgtAlias: Optional alias for target table (default `tgt`)
        :param srcAlias: Optional alias for source table (defaults `src`)
        :param asHtml: Whether to generate output suitable for use with the `displayHTML` method (default `False`)
        :returns: A Spark SQL expression string representing the `MERGE` statement
        """
        assert tgtName is not None, "you must specify a target table"
        assert srcName is not None, "you must specify a source table"
        assert joinExpr is not None, "you must specify a join expression"

        self.computeBuildPlan()

        # get list of column names
        output_columns = [x[0] for x in self.getOutputColumnNamesAndTypes()]

        ensure(output_columns is not None and len(output_columns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base seed column
               """)

        # use list of column names if not supplied
        if insertColumns is None:
            insertColumns = output_columns

        if updateColumns is None:
            updateColumns = output_columns

        # build merge statement
        results = [f"MERGE INTO `{tgtName}` as {tgtAlias}"]

        # use time expression if supplied
        if timeExpr is None:
            results.append(f"USING `{srcName}` as {srcAlias}")
        else:
            results.append(f"USING `{srcName}` {timeExpr} as {srcAlias}")

        # add join condition
        results.append(f"ON {joinExpr}")

        # generate update clause
        update_clause = None
        if updateExpr is not None:
            update_clause = f"WHEN MATCHED and {updateExpr} THEN UPDATE "
        else:
            update_clause = "WHEN MATCHED THEN UPDATE "

        if not useExplicitNames:
            update_clause = update_clause + " SET *"
        else:
            update_clause = (
                update_clause + " SET " +
                DataGenerator._mkInsertOrUpdateStatement(updateColumns, srcAlias, updateColumnExprs)
            )

        results.append(update_clause)

        # generate delete clause
        if delExpr is not None:
            results.append(f"WHEN MATCHED and {delExpr} THEN DELETE")

        if insertExpr is not None:
            ins_clause = f"WHEN NOT MATCHED and {insertExpr} THEN INSERT "
        else:
            ins_clause = "WHEN NOT MATCHED THEN INSERT "

        if not useExplicitNames:
            ins_clause = ins_clause + " *"
        else:
            ins_clause = (
                ins_clause + "(" + ",".join(insertColumns) + ") VALUES (" +
                DataGenerator._mkInsertOrUpdateStatement(insertColumns, srcAlias, insertColumnExprs, False) + ")"
            )

        results.append(ins_clause)
        result = "\n".join(results)

        if asHtml:
            result = HtmlUtils.formatCodeAsHtml(results)

        return result

    def buildOutputDataset(
            self, output_dataset: OutputDataset,
            with_streaming: bool | None = None,
            generator_options: dict[str, Any] | None = None
    ) -> StreamingQuery | None:
        """
        Builds a `DataFrame` from the `DataGenerator` and writes the data to a target table.

        :param output_dataset: Output configuration for writing generated data
        :param with_streaming: Whether to generate data using streaming. If None, auto-detects based on trigger
        :param generator_options: Options for building the generator (e.g. `{"rowsPerSecond": 100}`)
        :returns: A Spark `StreamingQuery` if data is written in streaming, otherwise `None`
        """
        # Auto-detect streaming mode if not explicitly specified
        if with_streaming is None:
            with_streaming = output_dataset.trigger is not None and len(output_dataset.trigger) > 0

        df = self.build(withStreaming=with_streaming, options=generator_options)
        return write_data_to_output(df, config=output_dataset)

    @staticmethod
    def loadFromJson(options: str) -> "DataGenerator":
        """
        Creates a `DataGenerator` from a JSON string.

        :param options: A JSON string containing data generation options
        :return: A `DataGenerator` with the specified options
        """
        options_dict = json.loads(options)
        return DataGenerator.loadFromInitializationDict(options_dict)

    def saveToJson(self) -> str:
        """
        Gets the JSON string representation of a `DataGenerator`.

        :return: A JSON string representation of the `DataGenerator`
        """
        return json.dumps(self.saveToInitializationDict())
