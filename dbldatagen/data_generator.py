# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` and `DataGenerator` classes
"""
import copy
import logging
import re

from pyspark.sql.types import LongType, IntegerType, StringType, StructType, StructField, DataType

from ._version import _get_spark_version
from .column_generation_spec import ColumnGenerationSpec
from .datagen_constants import DEFAULT_RANDOM_SEED, RANDOM_SEED_FIXED, RANDOM_SEED_HASH_FIELD_NAME, \
    DEFAULT_SEED_COLUMN, SPARK_RANGE_COLUMN, MIN_SPARK_VERSION, \
    OPTION_RANDOM, OPTION_RANDOM_SEED, OPTION_RANDOM_SEED_METHOD, \
    INFER_DATATYPE
from .html_utils import HtmlUtils
from .schema_parser import SchemaParser
from .spark_singleton import SparkSingleton
from .utils import ensure, topologicalSort, DataGenError, deprecated, split_list_matching_condition

_OLD_MIN_OPTION = 'min'
_OLD_MAX_OPTION = 'max'

_STREAMING_TIMESTAMP_COLUMN = "_source_timestamp"


class DataGenerator:
    """ Main Class for test data set generation

    This class acts as the entry point to all test data generation activities.

    :param sparkSession: spark Session object to use
    :param name: is name of data set
    :param randomSeedMethod: = seed method for random numbers - either None, 'fixed', 'hash_fieldname'
    :param rows: = amount of rows to generate
    :param startingId: = starting value for generated seed column
    :param randomSeed: = seed for random number generator
    :param partitions: = number of partitions to generate, if not provided, uses `spark.sparkContext.defaultParallelism`
    :param verbose: = if `True`, generate verbose output
    :param batchSize: = UDF batch number of rows to pass via Apache Arrow to Pandas UDFs
    :param debug: = if set to True, output debug level of information
    :param seedColumnName: = if set, this should be the name of the `seed` or logical `id` column. Defaults to `id`
    :param random: = if set, specifies default value of `random` attribute for all columns where not set

    By default the seed column is named `id`. If you need to use this column name in your generated data,
    it is recommended that you use a different name for the seed column - for example `_id`.

    This may be specified by setting the `seedColumnName` attribute to `_id`
    """

    # class vars
    _nextNameIndex = 0
    _untitledNamePrefix = "Untitled"
    _randomSeed = DEFAULT_RANDOM_SEED

    _allowed_keys = ["startingId", "rowCount", "output_id"]

    # set up logging

    # restrict spurious messages from java gateway
    logging.getLogger("py4j").setLevel(logging.WARNING)

    # logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.NOTSET)

    def __init__(self, sparkSession=None, name=None, randomSeedMethod=None,
                 rows=1000000, startingId=0, randomSeed=None, partitions=None, verbose=False,
                 batchSize=None, debug=False, seedColumnName=DEFAULT_SEED_COLUMN,
                 random=False,
                 **kwargs):
        """ Constructor for data generator object """

        # set up logging
        self.verbose = verbose
        self.debug = debug

        self._setupLogger()
        self._seedColumnName = seedColumnName
        self._outputStreamingFields = False

        if seedColumnName != DEFAULT_SEED_COLUMN:
            self.logger.info(f"Using '{self._seedColumnName}' for seed column in place of '{DEFAULT_SEED_COLUMN}")

        self.name = name if name is not None else self.generateName()
        self._rowCount = rows
        self.starting_id = startingId
        self.__schema__ = None

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self.sparkSession = sparkSession

        # if the active Spark session is stopped, you may end up with a valid SparkSession object but the underlying
        # SparkContext will be invalid
        assert sparkSession is not None, "Spark session not initialized"
        assert sparkSession.sparkContext is not None, "Expecting spark session to have valid sparkContext"

        self.partitions = partitions if partitions is not None else sparkSession.sparkContext.defaultParallelism

        # check for old versions of args
        if "starting_id" in kwargs:
            self.logger.warning("starting_id is deprecated - use option 'startingId' instead")
            startingId = kwargs["starting_id"]

        if "seed" in kwargs:
            self.logger.warning("seed is deprecated - use option 'randomSeed' instead")
            randomSeed = kwargs["seed"]

        if "seed_method" in kwargs:
            self.logger.warning("seed_method is deprecated - use option 'seedMethod' instead")
            seedMethod = kwargs["seed_method"]

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
                self._seedMethod = RANDOM_SEED_HASH_FIELD_NAME
            else:
                self._seedMethod = randomSeedMethod
        else:
            self._instanceRandomSeed = randomSeed

            # if a valid random seed was supplied but no seed method was applied, make the seed method "fixed"
            if randomSeedMethod is None:
                self._seedMethod = "fixed"

        if self._seedMethod not in [None, RANDOM_SEED_FIXED, RANDOM_SEED_HASH_FIELD_NAME]:
            msg = f"seedMethod should be None, '{RANDOM_SEED_FIXED}' or '{RANDOM_SEED_HASH_FIELD_NAME}' "
            raise DataGenError(msg)

        self._columnSpecsByName = {}
        self._allColumnSpecs = []
        self._buildPlan = []
        self.executionHistory = []
        self._options = {}
        self._buildOrder = []
        self._inferredSchemaFields = []
        self.buildPlanComputed = False

        # lets add the seed column
        self.withColumn(self._seedColumnName, LongType(), nullable=False, implicit=True, omit=True, noWarn=True)
        self._batchSize = batchSize

        # set up spark session
        self._setupSparkSession(sparkSession)

        # set up use of pandas udfs
        self._setupPandas(batchSize)

    @property
    def seedColumnName(self):
        """ return the name of data generation seed column"""
        return self._seedColumnName

    @classmethod
    def _checkSparkVersion(cls, sparkVersion, minSparkVersion):
        """
        check spark version
        :param sparkVersion: spark version string
        :param minSparkVersion: min spark version as tuple
        :return: True if version passes minVersion

        Layout of version string must be compatible "xx.xx.xx.patch"
        """
        sparkVersionInfo = _get_spark_version(sparkVersion)

        if sparkVersionInfo < minSparkVersion:
            logging.warning(f"*** Minimum version of Python supported is {minSparkVersion} - found version %s ",
                            sparkVersionInfo)
            return False

        return True

    def _setupSparkSession(self, sparkSession):
        """
        Set up spark session
        :param sparkSession: spark session to use
        :return: nothing
        """
        if sparkSession is None:
            sparkSession = SparkSingleton.getInstance()

        assert sparkSession is not None, "Spark session not initialized"

        self.sparkSession = sparkSession

        # check if the spark version meets the minimum requirements and warn if not
        sparkVersion = sparkSession.version
        self._checkSparkVersion(sparkVersion, MIN_SPARK_VERSION)

    def _setupPandas(self, pandasBatchSize):
        """
        Set up pandas
        :param pandasBatchSize: batch size for pandas, may be None
        :return: nothing
        """
        assert pandasBatchSize is None or type(pandasBatchSize) is int, \
            "If pandas_batch_size is specified, it must be an integer"
        self.logger.info("*** using pandas udf for custom functions ***")
        self.logger.info("Spark version: %s", self.sparkSession.version)
        if str(self.sparkSession.version).startswith("3"):
            self.logger.info("Using spark 3.x")
            self.sparkSession.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        else:
            self.sparkSession.conf.set("spark.sql.execution.arrow.enabled", "true")

        if self._batchSize is not None:
            self.sparkSession.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", self._batchSize)

    def _setupLogger(self):
        """Set up logging

        This will set the logger at warning, info or debug levels depending on the instance construction parameters
        """
        self.logger = logging.getLogger("DataGenerator")
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        elif self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARNING)

    @classmethod
    def useSeed(cls, seedVal):
        """ set seed for random number generation

            Arguments:
            :param seedVal: - new value for the random number seed
        """
        cls._randomSeed = seedVal

    @deprecated("Use `useSeed` instead")
    @classmethod
    def use_seed(cls, seedVal):
        """ set seed for random number generation

            Arguments:
            :param seedVal: - new value for the random number seed

        """
        cls._randomSeed = seedVal

    @classmethod
    def reset(cls):
        """ reset any state associated with the data """
        cls._nextNameIndex = 0

    @classmethod
    def generateName(cls):
        """ get a name for the data set

            Uses the untitled name prefix and nextNameIndex to generate a dummy dataset name

            :returns: string containing generated name
        """
        cls._nextNameIndex += 1
        new_name = cls._untitledNamePrefix + '_' + str(cls._nextNameIndex)
        return new_name

    # noinspection PyAttributeOutsideInit
    def clone(self):
        """Make a clone of the data spec via deep copy preserving same spark session

        :returns: deep copy of test data generator definition
        """
        old_spark_session = self.sparkSession
        old_logger = self.logger
        new_copy = None
        try:
            # temporarily set the spark session to null
            self.sparkSession = None

            # set logger to None before copy, disable pylint warning to ensure not triggered for this statement
            self.logger = None  # pylint: disable=attribute-defined-outside-init
            new_copy = copy.deepcopy(self)
            new_copy.sparkSession = old_spark_session
            new_copy.buildPlanComputed = False
            new_copy._setupLogger()
        finally:
            # now set it back
            self.sparkSession = old_spark_session
            # set logger to old value, disable pylint warning to ensure not triggered for this statement
            self.logger = old_logger  # pylint: disable=attribute-defined-outside-init
        return new_copy

    @property
    def randomSeed(self):
        """ return the data generation spec random seed"""
        return self._instanceRandomSeed

    @property
    def random(self):
        """ return the data generation spec default random setting for columns to be used
            when an explicit `random` attribute setting is not supplied
        """
        return self._defaultRandom

    def _markForPlanRegen(self):
        """Mark that build plan needs to be regenerated

        :returns: modified in-place instance of test data generator allowing for chaining of calls following
                  Builder pattern
        """
        self.buildPlanComputed = False
        return self

    def explain(self, suppressOutput=False):
        """Explain the test data generation process

        :param suppressOutput: If True, suppress display of build plan
        :returns: String containing explanation of test data generation for this specification
        """
        if not self.buildPlanComputed:
            self.computeBuildPlan()

        rc = self._rowCount
        tasks = self.partitions
        output = ["",
                  "Data generation plan", "====================",
                  f"spec=DateGenerator(name={self.name}, rows={rc}, startingId={self.starting_id}, partitions={tasks})",
                  ")",
                  "",
                  f"seed column: {self._seedColumnName}", "",
                  f"column build order: {self._buildOrder}", "", "build plan:"]

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

    def withRowCount(self, rc):
        """Modify the row count - useful when starting a new spec from a clone

        :param rc: The count of rows to generate
        :returns: modified in-place instance of test data generator allowing for chaining of calls following
                  Builder pattern
        """
        self._rowCount = rc
        return self

    @deprecated('Use `withRowCount` instead')
    def setRowCount(self, rc):
        """Modify the row count - useful when starting a new spec from a clone

        .. warning::
           Method is deprecated - use `withRowCount` instead

        :param rc: The count of rows to generate
        :returns: modified in-place instance of test data generator allowing for chaining of calls following
                  Builder pattern

        """
        self.logger.warning("method `setRowCount` is deprecated, use `withRowCount` instead")
        return self.withRowCount(rc)

    @property
    def rowCount(self):
        """ Return the row count

        This may differ from the original specified row counts, if counts need to be adjusted for purposes of
        keeping the ratio of rows to unique keys correct or other heuristics
        """
        return self._rowCount

    def withIdOutput(self):
        """ output seed column field (defaults to `id`) as a column in the generated data set if specified

        If this is not called, the seed column field is omitted from the final generated data set

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        self._columnSpecsByName[self._seedColumnName].omit = False
        self._markForPlanRegen()

        return self

    def option(self, optionKey, optionValue):
        """ set option to option value for later processing

        :param optionKey: key for option
        :param optionValue: value for option
        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        ensure(optionKey in self._allowed_keys)
        self._options[optionKey] = optionValue
        self._markForPlanRegen()
        return self

    def options(self, **kwargs):
        """ set options in bulk

        Allows for multiple options with option=optionValue style of option passing

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        for key, value in kwargs.items():
            self.option(key, value)
        self._markForPlanRegen()
        return self

    def _processOptions(self):
        """ process options to give effect to the options supplied earlier

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        self.logger.info("options: %s", str(self._options))

        for key, value in self._options.items():
            if key == "startingId":
                self.starting_id = value
            elif key in ["rowCount", "row_count"]:
                self._rowCount = value
        return self

    def describe(self):
        """ return description of the dataset generation spec

        :returns: Dict object containing key attributes of test data generator instance
        """

        return {
            'name': self.name,
            'rowCount': self._rowCount,
            'schema': self.schema,
            'randomSeed': self._instanceRandomSeed,
            'partitions': self.partitions,
            'columnDefinitions': self._columnSpecsByName,
            'debug': self.debug,
            'verbose': self.verbose
        }

    def __repr__(self):
        """ return the repr string for the class"""
        name = getattr(self, "name", "None")
        rows = getattr(self, "_rowCount", "None")
        partitions = getattr(self, "partitions", "None")
        return f"DataGenerator(name='{name}', rows={rows}, partitions={partitions})"

    def _checkFieldList(self):
        """Check field list for common errors

        Does not return anything but will assert / raise exceptions if errors occur
        """
        ensure(self._inferredSchemaFields is not None, "schemaFields should be non-empty")
        ensure(type(self._inferredSchemaFields) is list, "schemaFields should be list")

    @property
    def schemaFields(self):
        """ get list of schema fields for final output schema

        :returns: list of fields in schema
        """
        self._checkFieldList()
        return [fd for fd in self._inferredSchemaFields if not self._columnSpecsByName[fd.name].isFieldOmitted]

    @property
    def schema(self):
        """ infer spark output schema definition from the field specifications

        :returns: Spark SQL `StructType` for schema

        ..note::
          If the data generation specification contains columns for which the datatype is inferred, the schema type
          for inferred columns may not be correct until the build command has completed.

        """
        return StructType(self.schemaFields)

    @property
    def inferredSchema(self):
        """ infer spark interim schema definition from the field specifications

        ..note::
          If the data generation specification contains columns for which the datatype is inferred, the schema type
          for inferred columns may not be correct until the build command has completed.

        """
        self._checkFieldList()
        return StructType(self._inferredSchemaFields)

    def __getitem__(self, key):
        """ implement the built in derefernce by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self._columnSpecsByName[key]

    def getColumnType(self, colName):
        """ Get column Spark SQL datatype for specified column

        :param colName: name of column as string
        :returns: Spark SQL datatype for named column
        """
        ct = self._columnSpecsByName[colName].datatype
        return ct if ct is not None else IntegerType()

    def isFieldExplicitlyDefined(self, colName):
        """ return True if column generation spec has been explicitly defined for column, else false

        .. note::
           A column is not considered explicitly defined if it was inferred from a schema or added
           with a wildcard statement. This impacts whether the column can be redefined.
        """
        ensure(colName is not None, "colName should be non-empty")
        col_def = self._columnSpecsByName.get(colName, None)
        return not col_def.implicit if col_def is not None else False

    def getInferredColumnNames(self):
        """ get list of output columns """
        return [fd.name for fd in self._inferredSchemaFields]

    @staticmethod
    def flatten(lst):
        """ flatten list

        :param lst: list to flatten
        """
        return [item for sublist in lst for item in sublist]

    def getColumnSpec(self, name):
        """ get column spec for column having name supplied

        :param name: name of column to find spec for
        :return: column spec for named column if any
        """
        assert name is not None and len(name.strip()) > 0, "column name must be non empty string"
        return self._columnSpecsByName[name]

    def getOutputColumnNames(self):
        """ get list of output columns by flattening list of lists of column names
            normal columns will have a single column name but column definitions that result in
            multiple columns will produce a list of multiple names

            :returns: list of column names to be output in generated data set
        """
        return self.flatten([self._columnSpecsByName[fd.name].getNames()
                             for fd in
                             self._inferredSchemaFields if not self._columnSpecsByName[fd.name].isFieldOmitted])

    def getOutputColumnNamesAndTypes(self):
        """ get list of output columns by flattening list of lists of column names and types
            normal columns will have a single column name but column definitions that result in
            multiple columns will produce a list of multiple names
        """
        return self.flatten([self._columnSpecsByName[fd.name].getNamesAndTypes()
                             for fd in
                             self._inferredSchemaFields if not self._columnSpecsByName[fd.name].isFieldOmitted])

    def withSchema(self, sch):
        """ populate column definitions and specifications for each of the columns in the schema

        :param sch: Spark SQL schema, from which fields are added
        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        ensure(sch is not None, "schema sch should be non-empty")
        self.__schema__ = sch

        for fs in sch.fields:
            self.withColumn(fs.name, fs.dataType, implicit=True, omit=False, nullable=fs.nullable)
        return self

    def _computeRange(self, dataRange, minValue, maxValue, step):
        """ Compute merged range based on parameters

        :returns: effective minValue, maxValue, step as tuple
        """
        # TODO: may also need to check for instance of DataRange
        if dataRange is not None and isinstance(dataRange, range):
            if maxValue is not None or minValue != 0 or step != 1:
                raise ValueError("You cant specify both a range and minValue, maxValue or step values")

            return dataRange.start, dataRange.stop, dataRange.step
        else:
            return minValue, maxValue, step

    def withColumnSpecs(self, patterns=None, fields=None, matchTypes=None, **kwargs):
        """Add column specs for columns matching
           a) list of field names,
           b) one or more regex patterns
           c) type (as in pyspark.sql.types)

        :param patterns: patterns may specified a single pattern as a string or a list of patterns
                         that match the column names. May be omitted.
        :param fields: a string specifying an explicit field to match , or a list of strings specifying
                       explicit fields to match. May be omitted.
        :param matchTypes: a single Spark SQL datatype or list of Spark SQL data types to match. May be omitted.
        :returns: modified in-place instance of test data generator allowing for chaining of calls following
                  Builder pattern

        .. note::
           matchTypes may also take SQL type strings or a list of SQL type strings such as "array<integer>". However,
           you may not use ``INFER_DATYTYPE`` as part of the matchTypes list.

        You may also add a variety of options to further control the test data generation process.
        For full list of options, see :doc:`/reference/api/dbldatagen.column_spec_options`.

        """
        if fields is not None and type(fields) is str:
            fields = [fields]

        if OPTION_RANDOM not in kwargs:
            kwargs[OPTION_RANDOM] = self._defaultRandom

        # add support for deprecated legacy names
        if "match_types" in kwargs:
            assert matchTypes is None, "Argument 'match_types' is deprecated, use 'matchTypes' instead"
            matchTypes = kwargs["match_types"]
            del kwargs["match_types"]  # remove the legacy option from keyword args as they will be used later

        if matchTypes is not None and type(matchTypes) is not list:
            matchTypes = [matchTypes]  # if only one match type, make a list of that match type

        if patterns is not None and type(patterns) is str:
            patterns = ["^" + patterns + "$"]
        elif type(patterns) is list:
            patterns = ["^" + pat + "$" for pat in patterns]

        all_fields = self.getInferredColumnNames()
        effective_fields = [x for x in all_fields if
                            (fields is None or x in fields) and x != self._seedColumnName]

        if patterns is not None:
            effective_fields = [x for x in effective_fields for y in patterns if re.search(y, x) is not None]

        if matchTypes is not None:
            effective_types = []

            for typ in matchTypes:
                if isinstance(typ, str):
                    if typ == INFER_DATATYPE:
                        raise ValueError("You cannot use INFER_DATATYPE with the method `withColumnSpecs`")

                    effective_types.append(SchemaParser.columnTypeFromString(typ))
                else:
                    effective_types.append(typ)

            effective_fields = [x for x in effective_fields for y in effective_types
                                if self.getColumnType(x) == y]

        for f in effective_fields:
            self.withColumnSpec(f, implicit=True, **kwargs)
        return self

    def _checkColumnOrColumnList(self, columns, allowId=False):
        """ Check if column or columns refer to existing columns

        :param columns: a single column or list of columns as strings
        :param allowId: If True, allows the specialized seed column (which defaults to `id`) to be present in columns
        :returns: True if test passes
        """
        inferred_columns = self.getInferredColumnNames()
        if allowId and columns == self._seedColumnName:
            return True

        if type(columns) is list:
            for column in columns:
                ensure(column in inferred_columns,
                       f" column `{column}` must refer to defined column")
        else:
            ensure(columns in inferred_columns,
                   f" column `{columns}` must refer to defined column")
        return True

    def withColumnSpec(self, colName, minValue=None, maxValue=None, step=1, prefix=None,
                       random=None, distribution=None,
                       implicit=False, dataRange=None, omit=False, baseColumn=None, **kwargs):
        """ add a column specification for an existing column

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

        ensure(not isinstance(minValue, DataType),
               f"""unnecessary `datatype` argument specified for `withColumnSpec` for column `{colName}` -
                    Datatype parameter is only needed for `withColumn` and not permitted for `withColumnSpec`
               """)

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

        self.logger.info("adding column spec - `%s` with baseColumn : `%s`, implicit : %s , omit %s",
                         colName, baseColumn, implicit, omit)

        self._generateColumnDefinition(colName, self.getColumnType(colName), minValue=minValue, maxValue=maxValue,
                                       step=step, prefix=prefix,
                                       random=random, dataRange=dataRange,
                                       distribution=distribution, baseColumn=baseColumn,
                                       implicit=implicit, omit=omit, **new_props)
        return self

    def hasColumnSpec(self, colName):
        """returns true if there is a column spec for the column

        :param colName: name of column to check for
        :returns: True if column has spec, False otherwise
        """
        return colName in self._columnSpecsByName

    def withColumn(self, colName, colType=StringType(), minValue=None, maxValue=None, step=1,
                   dataRange=None, prefix=None, random=None, distribution=None,
                   baseColumn=None, nullable=True,
                   omit=False, implicit=False, noWarn=False,
                   **kwargs):
        """ add a new column to the synthetic data generation specification

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

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern

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

        self.logger.info("effective range: %s, %s, %s args: %s", minValue, maxValue, step, kwargs)
        self.logger.info("adding column - `%s` with baseColumn : `%s`, implicit : %s , omit %s",
                         colName, baseColumn, implicit, omit)
        newColumn = self._generateColumnDefinition(colName, colType, minValue=minValue, maxValue=maxValue,
                                                   step=step, prefix=prefix, random=random,
                                                   distribution=distribution, baseColumn=baseColumn,
                                                   dataRange=dataRange,
                                                   implicit=implicit, omit=omit, **new_props)

        # note for inferred columns, the column type is initially sey to a StringType but may be superceded later
        self._inferredSchemaFields.append(StructField(colName, newColumn.datatype, nullable))
        return self

    def _mkSqlStructFromList(self, fields):
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

    def _mkStructFromDict(self, fields):
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

    def withStructColumn(self, colName, fields=None, asJson=False, **kwargs):
        """
        Add a struct column to the synthetic data generation specification. This will add a new column composed of
        a struct of the specified fields.

        :param colName: name of column
        :param fields: list of elements to compose as a struct valued column (each being a string or tuple), or a dict
                          outlining the structure of the struct column
        :param asJson: If False, generate a struct valued column. If True, generate a JSON string column
        :param kwargs: keyword arguments to pass to the underlying column generators as per `withColumn`
        :return: A modified in-place instance of data generator allowing for chaining of calls
                  following the Builder pattern

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
        assert fields is not None and isinstance(fields, (list, dict)), \
            "Fields argument must be a list of field specifications or dict outlining the target structure "
        assert type(colName) is str and len(colName) > 0, "Must specify a column name"

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
            newDf = self.withColumn(colName, StringType(), expr=output_expr,  **kwargs)
        else:
            newDf = self.withColumn(colName, INFER_DATATYPE, expr=struct_expr, **kwargs)

        return newDf

    def _generateColumnDefinition(self, colName, colType=None, baseColumn=None,
                                  implicit=False, omit=False, nullable=True, **kwargs):
        """ generate field definition and column spec

        .. note:: Any time that a new column definition is added,
                  we'll mark that the build plan needs to be regenerated.
           For our purposes, the build plan determines the order of column generation etc.

        :returns: Newly added column_spec
        """
        if colType is None:
            colType = self.getColumnType(baseColumn)

            if colType == INFER_DATATYPE:
                raise ValueError("When base column(s) have inferred datatype, you must specify the column type")

        new_props = {}
        new_props.update(kwargs)

        # if the column  has the option `random` set to true
        # then use the instance level random seed
        # otherwise use the default random seed for the class
        if OPTION_RANDOM_SEED in new_props:
            effective_random_seed = new_props[OPTION_RANDOM_SEED]
            new_props.pop(OPTION_RANDOM_SEED)
            new_props[OPTION_RANDOM] = True

            # if random seed has override but randomSeedMethod does not
            # set it to fixed
            if OPTION_RANDOM_SEED_METHOD not in new_props:
                new_props[OPTION_RANDOM_SEED_METHOD] = RANDOM_SEED_FIXED

        elif OPTION_RANDOM in new_props and new_props[OPTION_RANDOM]:
            effective_random_seed = self._instanceRandomSeed
        else:
            effective_random_seed = self._randomSeed

        # handle column level override
        if OPTION_RANDOM_SEED_METHOD in new_props:
            effective_random_seed_method = new_props[OPTION_RANDOM_SEED_METHOD]
            new_props.pop(OPTION_RANDOM_SEED_METHOD)
        else:
            effective_random_seed_method = self._seedMethod

        column_spec = ColumnGenerationSpec(colName, colType,
                                           baseColumn=baseColumn,
                                           implicit=implicit,
                                           omit=omit,
                                           randomSeed=effective_random_seed,
                                           randomSeedMethod=effective_random_seed_method,
                                           nullable=nullable,
                                           verbose=self.verbose,
                                           debug=self.debug,
                                           seedColumnName=self._seedColumnName,
                                           **new_props)

        self._columnSpecsByName[colName] = column_spec

        # if column spec for column already exists - remove it
        items_to_remove = [x for x in self._allColumnSpecs if x.name == colName]
        for x in items_to_remove:
            self._allColumnSpecs.remove(x)

        self._allColumnSpecs.append(column_spec)

        # mark that the build plan needs to be regenerated
        self._markForPlanRegen()

        return column_spec

    def _getBaseDataFrame(self, startId=0, streaming=False, options=None):
        """ generate the base data frame and seed column (which defaults to `id`) , partitioning the data if necessary

        This is used when generating the test data.

        A base data frame is created and then each of the additional columns are generated, according to
        base column dependency order, and added to the base data frame using expressions or withColumn statements.

        :returns: Spark data frame for base data that drives the data generation
        """

        end_id = self._rowCount + startId
        id_partitions = self.partitions if self.partitions is not None else 4

        if not streaming:
            status = f"Generating data frame with ids from {startId} to {end_id} with {id_partitions} partitions"
            self.logger.info(status)
            self.executionHistory.append(status)
            df1 = self.sparkSession.range(start=startId,
                                          end=end_id,
                                          numPartitions=id_partitions)

            # spark.range generates a dataframe with the column `id` so rename it if its not our seed column
            if SPARK_RANGE_COLUMN != self._seedColumnName:
                df1 = df1.withColumnRenamed(SPARK_RANGE_COLUMN, self._seedColumnName)

        else:
            status = (
                f"Generating streaming data frame with ids from {startId} to {end_id} with {id_partitions} partitions")
            self.logger.info(status)
            self.executionHistory.append(status)

            df1 = (self.sparkSession.readStream
                   .format("rate"))
            if options is not None:
                if "rowsPerSecond" not in options:
                    options['rowsPerSecond'] = 1
                if "numPartitions" not in options:
                    options['numPartitions'] = id_partitions

                for k, v in options.items():
                    df1 = df1.option(k, v)
                df1 = (df1.load()
                       .withColumnRenamed("value", self._seedColumnName)
                       )

            else:
                df1 = (df1.option("rowsPerSecond", 1)
                       .option("numPartitions", id_partitions)
                       .load()
                       .withColumnRenamed("value", self._seedColumnName)
                       )

        return df1

    def _computeColumnBuildOrder(self):
        """ compute the build ordering using a topological sort on dependencies

        In order to avoid references to columns that have not yet been generated, the test data generation process
        sorts the columns according to the order they need to be built.

        This determines which columns are built first.

        The test generation process will select the columns in the correct order at the end so that the columns
        appear in the correct order in the final output.

        :returns: the build ordering
        """
        dependency_ordering = [(x.name, set(x.dependencies)) if x.name != self._seedColumnName else (
            self._seedColumnName, set())
                               for x in self._allColumnSpecs]

        self.logger.info("dependency list: %s", str(dependency_ordering))

        self._buildOrder = list(
            topologicalSort(dependency_ordering, flatten=False, initial_columns=[self._seedColumnName]))

        self.logger.info("columnBuildOrder: %s", str(self._buildOrder))

        self._buildOrder = self._adjustBuildOrderForSqlDependencies(self._buildOrder, self._columnSpecsByName)

        return self._buildOrder

    def _adjustBuildOrderForSqlDependencies(self, buildOrder, columnSpecsByName):
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

        all_columns = set([item for sublist in buildOrder for item in sublist])
        built_columns = []
        prior_phase_built_columns = []

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
                revised_phase = split_list_matching_condition(current_phase,
                                                              lambda el, cols=separate_phase_columns: el in cols)
                new_build_order.extend(revised_phase)
            else:
                # no change to phase
                new_build_order.append(current_phase)

            prior_phase_built_columns.extend(current_phase)

        return new_build_order

    @property
    def build_order(self):
        """ return the build order minus the seed column (which defaults to `id`)

        The build order will be a list of lists - each list specifying columns that can be built at the same time
        """
        if not self.buildPlanComputed:
            self.computeBuildPlan()

        return [x for x in self._buildOrder if x != [self._seedColumnName]]

    def _getColumnDataTypes(self, columns):
        """ Get data types for columns

        :param columns: list of columns to retrieve data types for
        """
        return [self._columnSpecsByName[colspec].datatype for colspec in columns]

    def computeBuildPlan(self):
        """ prepare for building by computing a pseudo build plan

        The build plan is not a true build plan - it is only used for debugging purposes, but does not actually
        drive the column generation order.

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
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

    def build(self, withTempView=False, withView=False, withStreaming=False, options=None):
        """ build the test data set from the column definitions and return a dataframe for it

        if `withStreaming` is True, generates a streaming data set.
        Use options to control the rate of generation of test data if streaming is used.

        For example:

        `dfTestData = testDataSpec.build(withStreaming=True,options={ 'rowsPerSecond': 5000})`

        :param withTempView: if True, automatically creates temporary view for generated data set
        :param withView: If True, automatically creates global view for data set
        :param withStreaming: If True, generates data using Spark Structured Streaming Rate source suitable
                              for writing with `writeStream`
        :param options: optional Dict of options to control generating of streaming data
        :returns: Spark SQL dataframe of generated test data
        """
        self.logger.debug("starting build ... withStreaming [%s]", withStreaming)
        self.executionHistory = []
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
    def _buildColumnExpressionsWithSelects(self, df1):
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
                if type(column_generators) is list and len(column_generators) == 1:
                    build_round.append(column_generators[0].alias(colName))
                elif type(column_generators) is list and len(column_generators) > 1:
                    i = 0
                    for cg in column_generators:
                        build_round.append(cg.alias(f'{colName}_{i}'))
                        i += 1
                else:
                    build_round.append(column_generators.alias(colName))
                column_specs_applied.append(col1)
                inx_col = inx_col + 1

            df1 = df1.select(*build_round)

            # apply any post select processing
            for cs in column_specs_applied:
                cs._onSelect(df1)
        return df1

    def _sqlTypeFromSparkType(self, dt):
        """Get sql type for spark type
           :param dt: instance of Spark SQL type such as IntegerType()
        """
        return dt.simpleString()

    def _mkInsertOrUpdateStatement(self, columns, srcAlias, substitutions, isUpdate=True):
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

    def scriptTable(self, name=None, location=None, tableFormat="delta", asHtml=False):
        """ generate create table script suitable for format of test data set

        :param name: name of table to use in generated script
        :param location: path to location of data. If specified (default is None), will generate
                         an external table definition.
        :param tableFormat: table format for table
        :param asHtml: if true, generate output suitable for use with `displayHTML` method in notebook environment
        :returns: SQL string for scripted table
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
            col_expressions.append(f"    {col_to_output[0]} {self._sqlTypeFromSparkType(col_to_output[1])}")
        results.append(",\n".join(col_expressions))
        results.append(")")
        results.append(f"using {tableFormat}")

        if location is not None:
            results.append(f"location '{location}'")

        results = "\n".join(results)

        if asHtml:
            results = HtmlUtils.formatCodeAsHtml(results)

        return results

    def scriptMerge(self, tgtName=None, srcName=None, updateExpr=None, delExpr=None, joinExpr=None, timeExpr=None,
                    insertExpr=None,
                    useExplicitNames=True,
                    updateColumns=None, updateColumnExprs=None,
                    insertColumns=None, insertColumnExprs=None,
                    srcAlias="src", tgtAlias="tgt",
                    asHtml=False
                    ):
        """ generate merge table script suitable for format of test data set

        :param tgtName: name of target table to use in generated script
        :param tgtAlias: alias for target table - defaults to `tgt`
        :param srcName: name of source table to use in generated script
        :param srcAlias: alias for source table - defaults to `src`
        :param updateExpr: optional string representing updated condition. If not present, then
                            any row that does not match join condition is considered an update
        :param delExpr: optional string representing delete condition - For example `src.action='DEL'`.
                         If not present, no delete clause is generated
        :param insertExpr: optional string representing insert condition - If not present,
                        there is no condition on insert other than no match
        :param joinExpr: string representing join condition. For example, `tgt.id=src.id`
        :param timeExpr: optional time travel expression - for example : `TIMESTAMP AS OF timestamp_expression`
                        or `VERSION AS OF version`
        :param insertColumns: Optional list of strings designating columns to insert.
                               If not supplied, uses all columns defined in spec
        :param insertColumnExprs: Optional list of strings designating designating column expressions for insert.
            By default, will use src column as insert value into
            target table. This should have the form [ ("insert_column_name", "insert column expr"), ...]
        :param updateColumns: List of strings designating columns to update.
                               If not supplied, uses all columns defined in spec
        :param updateColumnExprs: Optional list of strings designating designating column expressions for update.
            By default, will use src column as update value for
            target table. This should have the form [ ("update_column_name", "update column expr"), ...]
        :param useExplicitNames: If True, generate explicit column names in insert and update statements
        :param asHtml: if true, generate output suitable for use with `displayHTML` method in notebook environment
        :returns: SQL string for scripted merge statement
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
            update_clause = (update_clause
                             + " SET "
                             + self._mkInsertOrUpdateStatement(columns=updateColumns, srcAlias=srcAlias,
                                                               substitutions=updateColumnExprs))

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
            ins_clause = (ins_clause + "(" + ",".join(insertColumns)
                          + ") VALUES (" +
                          self._mkInsertOrUpdateStatement(columns=insertColumns, srcAlias=srcAlias,
                                                          substitutions=insertColumnExprs, isUpdate=False)
                          + ")"
                          )

        results.append(ins_clause)

        result = "\n".join(results)

        if asHtml:
            result = HtmlUtils.formatCodeAsHtml(results)

        return result
