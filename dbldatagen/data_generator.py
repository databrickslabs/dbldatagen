# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` and `DataGenerator` classes
"""
import math
from datetime import date, datetime, timedelta
import re
import copy
import logging

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round as sql_round, array, expr
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType

from .column_generation_spec import ColumnGenerationSpec
from .utils import ensure, topologicalSort, DataGenError, deprecated
from .daterange import DateRange
from .spark_singleton import SparkSingleton

_OLD_MIN_OPTION = 'min'
_OLD_MAX_OPTION = 'max'


class DataGenerator:
    """ Main Class for test data set generation

    This class acts as the entry point to all test data generation activities.

    :param sparkSession: spark Session object to use
    :param name: is name of data set
    :param seed_method: = seed method for random numbers - either None, 'fixed', 'hash_fieldname'
    :param generateWithSelects: = if `True`, optimize data generation with selects, otherwise use `withColumn`
    :param rows: = amount of rows to generate
    :param startingId: = starting value for generated seed column
    :param seed: = seed for random number generator
    :param partitions: = number of partitions to generate
    :param verbose: = if `True`, generate verbose output
    :param usePandas: = if `True`, use Pandas UDFs during data generation
    :param pandasUdfBatchSize: = UDF batch number of rows to pass via Apache Arrow to Pandas UDFs
    :param debug: = if set to True, output debug level of information
    """

    # class vars
    _nextNameIndex = 0
    _untitledNamePrefix = "Untitled"
    _randomSeed = 42

    _allowed_keys = ["startingId", "row_count", "output_id"]

    # set up logging

    # restrict spurious messages from java gateway
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.NOTSET)

    def __init__(self, sparkSession=None, name=None, seed_method=None, generateWithSelects=True,
                 rows=1000000, startingId=0, seed=None, partitions=None, verbose=False,
                 usePandas=True, pandasUdfBatchSize=None, debug=False):
        """ Constructor for data generator object """

        # set up logging
        self.verbose = verbose
        self.debug = debug

        self._setupLogger()

        self.name = name if name is not None else self.generateName()
        self._rowCount = rows
        self.startingId = startingId
        self.__schema__ = None

        self.seedMethod = seed_method
        self.seed = seed if seed is not None else self._randomSeed

        # if a seed was supplied but no seed method was applied, make the seed method "fixed"
        if seed is not None and seed_method is None:
            self.seedMethod = "fixed"

        self.columnSpecsByName = {}
        self.allColumnSpecs = []
        self.build_plan = []
        self.execution_history = []
        self._options = {}
        self._buildOrder = []
        self.inferredSchemaFields = []
        self.partitions = partitions if partitions is not None else 10
        self.build_plan_computed = False
        self.withColumn(ColumnGenerationSpec.SEED_COLUMN, LongType(), nullable=False, implicit=True, omit=True)
        self.generateWithSelects = generateWithSelects
        self._usePandas = usePandas
        self.pandas_udf_batch_size = pandasUdfBatchSize

        if sparkSession is None:
            sparkSession = SparkSingleton.getInstance()

        assert sparkSession is not None, "The spark session attribute must be initialized"

        self.sparkSession = sparkSession
        if sparkSession is None:
            raise DataGenError("""Spark session not initialized

            The spark session attribute must be initialized in the DataGenerator initialization

            i.e DataGenerator(sparkSession=spark, name="test", ...)
            """)

        # set up use of pandas udfs if necessary
        self._setupPandasIfNeeded(pandasUdfBatchSize)

        if seed_method is not None and seed_method != "fixed" and seed_method != "hash_fieldname":
            raise DataGenError("""seed_method should be None, 'fixed' or 'hash_fieldname' """)

    def _setupPandasIfNeeded(self, batchSize):
        """
        Set up pandas if needed
        :param batchSize: batch size for pandas, may be None
        :return: nothing
        """
        assert batchSize is None or type(batchSize) is int, \
            "If pandas_batch_size is specified, it must be an integer"
        if self._usePandas:
            self.logger.info("*** using pandas udf for custom functions ***")
            self.logger.info("Spark version: %s", self.sparkSession.version)
            if str(self.sparkSession.version).startswith("3"):
                self.logger.info("Using spark 3.x")
                self.sparkSession.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            else:
                self.sparkSession.conf.set("spark.sql.execution.arrow.enabled", "true")

            if self.pandas_udf_batch_size is not None:
                self.sparkSession.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", self.pandas_udf_batch_size)

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
        new_name = (cls._untitledNamePrefix + '_' + str(cls._nextNameIndex))
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
            new_copy.build_plan_computed = False
            new_copy._setupLogger()
        finally:
            # now set it back
            self.sparkSession = old_spark_session
            # set logger to old value, disable pylint warning to ensure not triggered for this statement
            self.logger = old_logger  # pylint: disable=attribute-defined-outside-init
        return new_copy

    def _markForPlanRegen(self):
        """Mark that build plan needs to be regenerated

        :returns: modified in-place instance of test data generator allowing for chaining of calls following
                  Builder pattern
        """
        self.build_plan_computed = False
        return self

    def explain(self, suppressOutput=False):
        """Explain the test data generation process

        :param suppressOutput: If True, suppress display of build plan
        :returns: String containing explanation of test data generation for this specification
        """
        if not self.build_plan_computed:
            self.computeBuildPlan()

        output = ["", "Data generation plan", "====================",
                  """spec=DateGenerator(name={}, rows={}, startingId={}, partitions={})"""
                      .format(self.name, self._rowCount, self.startingId, self.partitions), ")", "",
                  "column build order: {}".format(self._buildOrder), "", "build plan:"]

        for plan_action in self.build_plan:
            output.append(" ==> " + plan_action)
        output.extend(["", "execution history:", ""])
        for build_action in self.execution_history:
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
        """ output seed column field (defaults to `id`) as a column in the test data set if specified

        If this is not called, the seed column field is omitted from the final test data set

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        self.columnSpecsByName[ColumnGenerationSpec.SEED_COLUMN].omit = False
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
        """ set suppliedOptions in bulk

        Allows for multiple suppliedOptions with option=optionValue style of option passing

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        for key, value in kwargs.items():
            self.option(key, value)
        self._markForPlanRegen()
        return self

    def _processOptions(self):
        """ process suppliedOptions to give effect to the suppliedOptions supplied earlier

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        self.logger.info("suppliedOptions: %s", str(self._options))

        for key, value in self._options.items():
            if key == "startingId":
                self.startingId = value
            elif key == "row_count":
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
            'seed': self.seed,
            'partitions': self.partitions,
            'columnDefinitions': self.columnSpecsByName,
            'debug': self.debug,
            'verbose': self.verbose
        }

    def __repr__(self):
        """ return the repr string for the class"""
        return "{}(name='{}', rows={}, partitions={})".format(__class__.__name__,
                                                              self.name,
                                                              self._rowCount,
                                                              self.partitions)

    def _checkFieldList(self):
        """Check field list for common errors

        Does not return anything but will assert / raise exceptions if errors occur
        """
        ensure(self.inferredSchemaFields is not None, "schemaFields should be non-empty")
        ensure(type(self.inferredSchemaFields) is list, "schemaFields should be list")

    @property
    def schemaFields(self):
        """ get list of schema fields for final output schema

        :returns: list of fields in schema
        """
        self._checkFieldList()
        return [fd for fd in self.inferredSchemaFields if not self.columnSpecsByName[fd.name].isFieldOmitted]

    @property
    def schema(self):
        """ infer spark output schema definition from the field specifications

        :returns: Spark SQL `StructType` for schema
        """
        return StructType(self.schemaFields)

    @property
    def inferredSchema(self):
        """ infer spark interim schema definition from the field specifications"""
        self._checkFieldList()
        return StructType(self.inferredSchemaFields)

    def __getitem__(self, key):
        """ implement the built in derefernce by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self.columnSpecsByName[key]

    def getColumnType(self, colName):
        """ Get column Spark SQL datatype for specified column

        :param colName: name of column as string
        :returns: Spark SQL datatype for named column
        """
        ct = self.columnSpecsByName[colName].datatype
        return ct if ct is not None else IntegerType()

    def isFieldExplicitlyDefined(self, colName):
        """ return True if column generation spec has been explicitly defined for column, else false

        .. note::
           A column is not considered explicitly defined if it was inferred from a schema or added
           with a wildcard statement. This impacts whether the column can be redefined.
        """
        ensure(colName is not None, "colName should be non-empty")
        col_def = self.columnSpecsByName.get(colName, None)
        return not col_def.implicit if col_def is not None else False

    def getInferredColumnNames(self):
        """ get list of output columns """
        return [fd.name for fd in self.inferredSchemaFields]

    @staticmethod
    def flatten(lst):
        """ flatten list

        :param lst: list to flatten
        """
        return [item for sublist in lst for item in sublist]

    def getOutputColumnNames(self):
        """ get list of output columns by flattening list of lists of column names
            normal columns will have a single column name but column definitions that result in
            multiple columns will produce a list of multiple names

            :returns: list of column names to be output in generated data set
        """
        return self.flatten([self.columnSpecsByName[fd.name].getNames()
                             for fd in
                             self.inferredSchemaFields if not self.columnSpecsByName[fd.name].isFieldOmitted])

    def getOutputColumnNamesAndTypes(self):
        """ get list of output columns by flattening list of lists of column names and types
            normal columns will have a single column name but column definitions that result in
            multiple columns will produce a list of multiple names
        """
        return self.flatten([self.columnSpecsByName[fd.name].getNamesAndTypes()
                             for fd in
                             self.inferredSchemaFields if not self.columnSpecsByName[fd.name].isFieldOmitted])

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

        You may also add a variety of suppliedOptions to further control the test data generation process.
        For full list of suppliedOptions, see :doc:`/reference/api/dbldatagen.column_spec_options`.

        """
        if fields is not None and type(fields) is str:
            fields = [fields]
        if matchTypes is not None and type(matchTypes) is not list:
            matchTypes = [matchTypes]
        if patterns is not None and type(patterns) is str:
            patterns = ["^" + patterns + "$"]
        elif type(patterns) is list:
            patterns = ["^" + pat + "$" for pat in patterns]

        all_fields = self.getInferredColumnNames()
        effective_fields = [x for x in all_fields if
                            (fields is None or x in fields) and x != ColumnGenerationSpec.SEED_COLUMN]

        if patterns is not None:
            effective_fields = [x for x in effective_fields for y in patterns if re.search(y, x) is not None]

        if matchTypes is not None:
            effective_fields = [x for x in effective_fields for y in matchTypes
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
        if allowId and columns == ColumnGenerationSpec.SEED_COLUMN:
            return True

        if type(columns) is list:
            for column in columns:
                ensure(column in inferred_columns,
                       " column `{0}` must refer to defined column".format(column))
        else:
            ensure(columns in inferred_columns,
                   " column `{0}` must refer to defined column".format(columns))
        return True

    def withColumnSpec(self, colName, minValue=None, maxValue=None, step=1, prefix=None,
                       random=False, distribution=None,
                       implicit=False, dataRange=None, omit=False, baseColumn=None, **kwargs):
        """ add a column specification for an existing column

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern

        You may also add a variety of suppliedOptions to further control the test data generation process.
        For full list of suppliedOptions, see :doc:`/reference/api/dbldatagen.column_spec_options`.

        """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colName in self.getInferredColumnNames(), " column `{0}` must refer to defined column".format(colName))
        if baseColumn is not None:
            self._checkColumnOrColumnList(baseColumn)
        ensure(not self.isFieldExplicitlyDefined(colName), "duplicate column spec for column `{0}`".format(colName))

        # handle migration of old `min` and `max` suppliedOptions
        if _OLD_MIN_OPTION in kwargs.keys():
            assert minValue is None, \
                "Only one of `minValue` and `minValue` can be specified. Use of `minValue` is preferred"
            minValue = kwargs[_OLD_MIN_OPTION]
            kwargs.pop(_OLD_MIN_OPTION, None)

        if _OLD_MAX_OPTION in kwargs.keys():
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
                                       random=random, data_range=dataRange,
                                       distribution=distribution, baseColumn=baseColumn,
                                       implicit=implicit, omit=omit, **new_props)
        return self

    def hasColumnSpec(self, colName):
        """returns true if there is a column spec for the column

        :param colName: name of column to check for
        :returns: True if column has spec, False otherwise
        """
        return colName in self.columnSpecsByName.keys()

    def withColumn(self, colName, colType=StringType(), minValue=None, maxValue=None, step=1,
                   dataRange=None, prefix=None, random=False, distribution=None,
                   baseColumn=None, nullable=True,
                   omit=False, implicit=False,
                   **kwargs):
        """ add a new column for specification

        :param colName: name of column to add
        :param colType: type of column to add - should be either string of simple type name or type from 
        
        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern

        You may also add a variety of suppliedOptions to further control the test data generation process.
        For full list of suppliedOptions, see :doc:`/reference/api/dbldatagen.column_spec_options`.

        """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colType is not None, "Must specify column type for column `{0}`".format(colName))
        if baseColumn is not None:
            self._checkColumnOrColumnList(baseColumn, allowId=True)

        # handle migration of old `min` and `max` suppliedOptions
        if _OLD_MIN_OPTION in kwargs.keys():
            assert minValue is None, \
                "Only one of `minValue` and `minValue` can be specified. Use of `minValue` is preferred"
            minValue = kwargs[_OLD_MIN_OPTION]
            kwargs.pop(_OLD_MIN_OPTION, None)

        if _OLD_MAX_OPTION in kwargs.keys():
            assert maxValue is None, \
                "Only one of `maxValue` and `maxValue` can be specified. Use of `maxValue` is preferred"
            maxValue = kwargs[_OLD_MAX_OPTION]
            kwargs.pop(_OLD_MAX_OPTION, None)

        new_props = {}
        new_props.update(kwargs)

        from .schema_parser import SchemaParser
        if type(colType) == str:
            colType = SchemaParser.columnTypeFromString(colType)

        self.logger.info("effective range: %s, %s, %s args: %s", minValue, maxValue, step, kwargs)
        self.logger.info("adding column - `%s` with baseColumn : `%s`, implicit : %s , omit %s",
                         colName, baseColumn, implicit, omit)
        self._generateColumnDefinition(colName, colType, minValue=minValue, maxValue=maxValue,
                                       step=step, prefix=prefix, random=random,
                                       distribution=distribution, baseColumn=baseColumn, data_range=dataRange,
                                       implicit=implicit, omit=omit, **new_props)
        self.inferredSchemaFields.append(StructField(colName, colType, nullable))
        return self

    def _generateColumnDefinition(self, colName, colType=None, baseColumn=None,
                                  implicit=False, omit=False, nullable=True, **kwargs):
        """ generate field definition and column spec

        .. note:: Any time that a new column definition is added,
                  we'll mark that the build plan needs to be regenerated.
           For our purposes, the build plan determines the order of column generation etc.

        :returns: modified in-place instance of data generator allowing for chaining of calls
                  following Builder pattern
        """
        if colType is None:
            colType = self.getColumnType(baseColumn)

        column_spec = ColumnGenerationSpec(colName, colType,
                                           baseColumn=baseColumn,
                                           implicit=implicit,
                                           omit=omit,
                                           randomSeed=self._randomSeed,
                                           randomSeedMethod=self.seedMethod,
                                           nullable=nullable,
                                           verbose=self.verbose,
                                           debug=self.debug,
                                           **kwargs)

        self.columnSpecsByName[colName] = column_spec

        # if column spec for column already exists - remove it
        items_to_remove = [x for x in self.allColumnSpecs if x.name == colName]
        for x in items_to_remove:
            self.allColumnSpecs.remove(x)

        self.allColumnSpecs.append(column_spec)

        # mark that the build plan needs to be regenerated
        self._markForPlanRegen()

        return self

    def _getBaseDataFrame(self, start_id=0, streaming=False, options=None):
        """ generate the base data frame and seed column (which defaults to `id`) , partitioning the data if necessary

        This is used when generating the test data.

        A base data frame is created and then each of the additional columns are generated, according to
        base column dependency order, and added to the base data frame using expressions or withColumn statements.

        :returns: Spark data frame for base data that drives the data generation
        """

        end_id = self._rowCount + start_id
        id_partitions = self.partitions if self.partitions is not None else 4

        if not streaming:
            status = ("Generating data frame with ids from {} to {} with {} partitions"
                      .format(start_id, end_id, id_partitions))
            self.logger.info(status)
            self.execution_history.append(status)
            df1 = self.sparkSession.range(start=start_id,
                                          end=end_id,
                                          numPartitions=id_partitions)

            if ColumnGenerationSpec.SEED_COLUMN != "id":
                df1 = df1.withColumnRenamed("id", ColumnGenerationSpec.SEED_COLUMN)

        else:
            status = ("Generating streaming data frame with ids from {} to {} with {} partitions"
                      .format(start_id, end_id, id_partitions))
            self.logger.info(status)
            self.execution_history.append(status)

            df1 = (self.sparkSession.readStream
                   .format("rate"))
            if options is not None:
                if "rowsPerSecond" not in options:
                    options['rowsPerSecond'] = 1
                if "numPartitions" not in options:
                    options['numPartitions'] = id_partitions

                for k, v in options.items():
                    df1 = df1.option(k, v)
                df1 = df1.load().withColumnRenamed("value", ColumnGenerationSpec.SEED_COLUMN)
            else:
                df1 = (df1.option("rowsPerSecond", 1)
                       .option("numPartitions", id_partitions)
                       .load()
                       .withColumnRenamed("value", ColumnGenerationSpec.SEED_COLUMN)
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
        dependency_ordering = [(x.name, set(x.dependencies)) if x.name != ColumnGenerationSpec.SEED_COLUMN else (
            ColumnGenerationSpec.SEED_COLUMN, set())
                               for x in self.allColumnSpecs]

        # self.pp_list(dependency_ordering, msg="dependencies")

        self.logger.info("dependency list: %s", str(dependency_ordering))

        self._buildOrder = list(
            topologicalSort(dependency_ordering, flatten=False, initial_columns=[ColumnGenerationSpec.SEED_COLUMN]))

        self.logger.info("columnBuildOrder: %s", str(self._buildOrder))

        # self.pp_list(self._buildOrder, "build order")
        return self._buildOrder

    @property
    def buildOrder(self):
        """ return the build order minus the seed column (which defaults to `id`)

        The build order will be a list of lists - each list specifying columns that can be built at the same time
        """
        return [x for x in self._buildOrder if x != [ColumnGenerationSpec.SEED_COLUMN]]

    def _getColumnDataTypes(self, columns):
        """ Get data types for columns

        :param columns: list of columns to retrieve data types for
        """
        return [self.columnSpecsByName[colspec].datatype for colspec in columns]

    def computeBuildPlan(self):
        """ prepare for building by computing a pseudo build plan

        The build plan is not a true build plan - it is only used for debugging purposes, but does not actually
        drive the column generation order.

        :returns: modified in-place instance of test data generator allowing for chaining of calls
                  following Builder pattern
        """
        self.build_plan = []
        self.execution_history = []
        self._processOptions()
        self.build_plan.append("Build Spark data frame with seed column: {}".format(ColumnGenerationSpec.SEED_COLUMN))

        # add temporary columns
        for cs in self.allColumnSpecs:
            # extend overall build plan with build plan of each column spec
            self.build_plan.extend(cs.initialBuildPlan)

            # handle generation of any temporary columns
            for tmp_col in cs.temporaryColumns:
                # create column spec for temporary column if its not already present
                if not self.hasColumnSpec(tmp_col[0]):
                    self.build_plan.append("materializing temporary column {}".format(tmp_col[0]))
                    self.withColumn(tmp_col[0], tmp_col[1], **tmp_col[2])

        # TODO: set up the base column data type information
        for cs in self.allColumnSpecs:
            base_column_datatypes = self._getColumnDataTypes(cs.baseColumns)
            cs.setBaseColumnDatatypes(base_column_datatypes)

        self._computeColumnBuildOrder()

        for x1 in self._buildOrder:
            for x in x1:
                cs = self.columnSpecsByName[x]
                self.build_plan.append(cs.getPlanEntry())

        self.build_plan_computed = True
        return self

    def build(self, withTempView=False, withView=False, withStreaming=False, options=None):
        """ build the test data set from the column definitions and return a dataframe for it

        if `withStreaming` is True, generates a streaming data set.
        Use suppliedOptions to control the rate of generation of test data if streaming is used.

        For example:

        `dfTestData = testDataSpec.build(withStreaming=True,suppliedOptions={ 'rowsPerSecond': 5000})`

        :param withTempView: if True, automatically creates temporary view for generated data set
        :param withView: If True, automatically creates global view for data set
        :param withStreaming: If True, generates data using Spark Structured Streaming Rate source suitable
                              for writing with `writeStream`
        :param options: optional Dict of suppliedOptions to control generating of streaming data
        :returns: Spark SQL dataframe of generated test data
        """
        self.logger.debug("starting build ... withStreaming [%s]", withStreaming)
        self.execution_history = []
        self.computeBuildPlan()

        output_columns = self.getOutputColumnNames()
        ensure(output_columns is not None and len(output_columns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base seed column
               """)

        df1 = self._getBaseDataFrame(self.startingId, streaming=withStreaming, options=options)

        if self._usePandas:
            self.execution_history.append("Using Pandas Optimizations {}".format(self._usePandas))

        # build columns
        if self.generateWithSelects:
            df1 = self._buildColumnExpressionsWithSelects(df1)
        else:
            df1 = self._buildColumnExpressionsUsingWithColumn(df1)

        df1 = df1.select(*self.getOutputColumnNames())
        self.execution_history.append("selecting columns: {}".format(self.getOutputColumnNames()))

        # register temporary or global views if necessary
        if withView:
            self.execution_history.append("registering view")
            self.logger.info("Registered global view [%s]", self.name)
            df1.createGlobalTempView(self.name)
            self.logger.info("Registered!")
        elif withTempView:
            self.execution_history.append("registering temp view")
            self.logger.info("Registering temporary view [%s]", self.name)
            df1.createOrReplaceTempView(self.name)
            self.logger.info("Registered!")

        return df1

    def _buildColumnExpressionsUsingWithColumn(self, df1):
        """
        Build column expressions using withColumn
        :param df1: dataframe for base data generator
        :return: new dataframe
        """
        # build columns
        self.execution_history.append("Generating data with withColumn statements")
        column_build_order = [item for sublist in self.buildOrder for item in sublist]
        for colName in column_build_order:
            col1 = self.columnSpecsByName[colName]
            column_generators = col1.makeGenerationExpressions()
            self.execution_history.extend(col1.execution_history)

            if type(column_generators) is list and len(column_generators) == 1:
                df1 = df1.withColumn(colName, column_generators[0])
            elif type(column_generators) is list and len(column_generators) > 1:
                i = 0
                for cg in column_generators:
                    df1 = df1.withColumn('{0}_{1}'.format(colName, i), cg)
                    i += 1
            else:
                df1 = df1.withColumn(colName, column_generators)
        return df1

    def _buildColumnExpressionsWithSelects(self, df1):
        """
        Build column generation expressions with selects
        :param df1: dataframe for base data generator
        :return: new dataframe
        """
        self.execution_history.append("Generating data with selects")
        # generation with selects may be more efficient as less intermediate data frames
        # are generated resulting in shorter lineage
        for colNames in self.buildOrder:
            build_round = ["*"]
            inx_col = 0
            self.execution_history.append("building rounding for : {}".format(colNames))
            for colName in colNames:
                col1 = self.columnSpecsByName[colName]
                column_generators = col1.makeGenerationExpressions(self._usePandas)
                self.execution_history.extend(col1.execution_history)
                if type(column_generators) is list and len(column_generators) == 1:
                    build_round.append(column_generators[0].alias(colName))
                elif type(column_generators) is list and len(column_generators) > 1:
                    i = 0
                    for cg in column_generators:
                        build_round.append(cg.alias('{0}_{1}'.format(colName, i)))
                        i += 1
                else:
                    build_round.append(column_generators.alias(colName))
                inx_col = inx_col + 1

            df1 = df1.select(*build_round)
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
            subs[x] = "{}.{}".format(srcAlias, x)
        for x in substitutions:
            subs[x[0]] = x[1]

        for substitution_col in columns:
            new_val = subs[substitution_col]
            if isUpdate:
                results.append("{}={}".format(substitution_col, new_val))
            else:
                results.append("{}".format(new_val))

        return ", ".join(results)

    def scriptTable(self, name=None, location=None, table_format="delta"):
        """ generate create table script suitable for format of test data set

        :param name: name of table to use in generated script
        :param location: path to location of data. If specified (default is None), will generate
                         an external table definition.
        :param table_format: table format for table
        :returns: SQL string for scripted table
        """
        assert name is not None, "`name` must be specified"

        self.computeBuildPlan()

        output_columns = self.getOutputColumnNamesAndTypes()

        results = ["CREATE TABLE IF NOT EXISTS {} (".format(name)]
        ensure(output_columns is not None and len(output_columns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base seed column
               """)

        col_expressions = []
        for col_to_output in output_columns:
            col_expressions.append("    {} {}".format(col_to_output[0], self._sqlTypeFromSparkType(col_to_output[1])))
        results.append(",\n".join(col_expressions))
        results.append(")")
        results.append("using {}".format(table_format))

        if location is not None:
            results.append("location '{}'".format(location))

        return "\n".join(results)

    def scriptMerge(self, tgtName=None, srcName=None, updateExpr=None, delExpr=None, joinExpr=None, timeExpr=None,
                    insExpr=None,
                    useExplicitNames=True,
                    updateColumns=None, updateColumnExprs=None,
                    insertColumns=None, insertColumnExprs=None,
                    srcAlias="src", tgtAlias="tgt"):
        """ generate merge table script suitable for format of test data set

        :param tgtName: name of target table to use in generated script
        :param tgtAlias: alias for target table - defaults to `tgt`
        :param srcName: name of source table to use in generated script
        :param srcAlias: alias for source table - defaults to `src`
        :param updateExpr: optional string representing updated condition. If not present, then
                            any row that does not match join condition is considered an update
        :param delExpr: optional string representing delete condition - For example `src.action='DEL'`.
                         If not present, no delete clause is generated
        :param insExpr: optional string representing insert condition - If not present,
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
        results = ["MERGE INTO `{}` as {}".format(tgtName, tgtAlias)]

        # use time expression if supplied
        if timeExpr is None:
            results.append("USING `{}` as {}".format(srcName, srcAlias))
        else:
            results.append("USING `{}` {} as {}".format(srcName, timeExpr, srcAlias))

        # add join condition
        results.append("ON {}".format(joinExpr))

        # generate update clause
        update_clause = None
        if updateExpr is not None:
            update_clause = """WHEN MATCHED and {} THEN UPDATE """.format(updateExpr)
        else:
            update_clause = """WHEN MATCHED THEN UPDATE """

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
            results.append("WHEN MATCHED and {} THEN DELETE".format(delExpr))

        if insExpr is not None:
            ins_clause = "WHEN NOT MATCHED and {} THEN INSERT ".format(insExpr)
        else:
            ins_clause = "WHEN NOT MATCHED THEN INSERT "

        if not useExplicitNames:
            ins_clause = ins_clause + " *"
        else:
            ins_clause = (ins_clause
                          + "({})".format(",".join(insertColumns))
                          + " VALUES ({})".format(self._mkInsertOrUpdateStatement(columns=insertColumns,
                                                                                  srcAlias=srcAlias,
                                                                                  substitutions=insertColumnExprs,
                                                                                  isUpdate=False))
                          )

        results.append(ins_clause)

        return "\n".join(results)
