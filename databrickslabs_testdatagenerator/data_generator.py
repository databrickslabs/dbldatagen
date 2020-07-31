# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` and `DataGenerator` classes
"""

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, expr
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType
import math
from datetime import date, datetime, timedelta
import re

from .column_generation_spec import ColumnGenerationSpec
from .utils import ensure, topologicalSort, DataGenError
from .dataranges import DateRange
from .spark_singleton import SparkSingleton
import copy
import logging


class DataGenerator:
    """ Main Class for test data set generation

    This class acts as the entry point to all test data generation activities.

    :param sparkSession: spark Session object to use
    :param name: is name of data set
    :param seed_method: = seed method for random numbers - either None, 'fixed', 'hash_fieldname'
    :param generate_with_selects: = if `True`, optimize datae generation with selects, otherwise use `withColumn`
    :param rows: = amount of rows to generate
    :param starting_id: = starting id for generated id column
    :param seed: = seed for random number generator
    :param partitions: = number of partitions to generate
    :param verbose: = if `True`, generate verbose output
    :param use_pandas: = if `True`, use Pandas UDFs during test data generation
    :param pandas_udf_batch_size: = UDF batch number of rows to pass via Apache Arrow to Pandas UDFs
    :param debug: = if set to True, output debug level of information
    """

    # class vars
    _nextNameIndex = 0
    _untitledNamePrefix = "Untitled"
    _randomSeed = 42

    _allowed_keys = ["starting_id", "row_count", "output_id"]

    # set up logging

    # restrict spurious messages from java gateway
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.NOTSET)

    def __init__(self, sparkSession=None, name=None, seed_method=None, generate_with_selects=True,
                 rows=1000000, starting_id=0, seed=None, partitions=None, verbose=False,
                 use_pandas=True, pandas_udf_batch_size=None, debug=False):
        """ Constructor for data generator object """

        # set up logging
        self.verbose = verbose
        self.debug = debug

        self._setup_logger()

        self.name = name if name is not None else self.generateName()
        self.rowCount = rows
        self.starting_id = starting_id
        self.__schema__ = None

        self.seed_method = seed_method
        self.seed = seed if seed is not None else self._randomSeed

        # if a seed was supplied but no seed method was applied, make the seed method "fixed"
        if seed is not None and seed_method is None:
            self.seed_method = "fixed"

        self.columnSpecsByName = {}
        self.allColumnSpecs = []
        self.build_plan = []
        self.execution_history = []
        self._options = {}
        self._build_order = []
        self.inferredSchemaFields = []
        self.partitions = partitions if partitions is not None else 10
        self.build_plan_computed = False
        self.withColumn("id", LongType(), nullable=False, implicit=True, omit=True)
        self.generateWithSelects = generate_with_selects
        self.use_pandas = use_pandas
        self.pandas_udf_batch_size = pandas_udf_batch_size

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
        assert pandas_udf_batch_size is None or type(pandas_udf_batch_size) is int, \
            "If pandas_batch_size is specified, it must be an integer"
        if self.use_pandas:
            self.logger.info("*** using pandas udf for custom functions ***")
            self.logger.info("Spark version: %s", self.sparkSession.version)
            if str(self.sparkSession.version).startswith("3"):
                self.logger.info("Using spark 3.x")
                self.sparkSession.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            else:
                self.sparkSession.conf.set("spark.sql.execution.arrow.enabled", "true")

            if self.pandas_udf_batch_size is not None:
                self.sparkSession.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", self.pandas_udf_batch_size)

        if seed_method is not None and seed_method != "fixed" and seed_method != "hash_fieldname":
            raise DataGenError("""seed_method should be None, 'fixed' or 'hash_fieldname' """)

    def _setup_logger(self):
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
    def seed(cls, seedVal):
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
        newName = (cls._untitledNamePrefix + '_' + str(cls._nextNameIndex))
        return newName

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
            self.logger = None
            new_copy = copy.deepcopy(self)
            new_copy.sparkSession = old_spark_session
            new_copy.build_plan_computed = False
            new_copy._setup_logger()
        finally:
            # now set it back
            self.sparkSession = old_spark_session
            self.logger = old_logger
        return new_copy

    def markForPlanRegen(self):
        """Mark that build plan needs to be regenerated

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        self.build_plan_computed = False
        return self


    def explain(self):
        """Explain the test data generation process

        :returns: String containing explanation of test data generation for this specification
        """
        if not self.build_plan_computed:
            self.computeBuildPlan()

        output = ["", "Data generation plan", "====================",
                  """spec=DateGenerator(name={}, rows={}, starting_id={}, partitions={})"""
                      .format(self.name, self.rowCount, self.starting_id, self.partitions), ")", "",
                  "column build order: {}".format(self._build_order), "", "build plan:"]

        for plan_action in self.build_plan:
            output.append(" ==> " + plan_action)
        output.extend(["", "execution history:", ""])
        for build_action in self.execution_history:
            output.append(" ==> " + build_action)
        output.append("")
        output.append("====================")
        output.append("")

        print("\n".join(output))

    def setRowCount(self, rc):
        """Modify the row count - useful when starting a new spec from a clone

        :param rc: The count of rows to generate
        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        self.rowCount=rc
        return self

    def withIdOutput(self):
        """ output id field as a column in the test data set if specified

        If this is not called, the id field is omitted from the final test data set

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        self.columnSpecsByName["id"].omit = False
        self.markForPlanRegen()

        return self

    def option(self, option_key, option_value):
        """ set option to option value for later processing

        :param option_key: key for option
        :param option_value: value for option
        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        ensure(option_key in self._allowed_keys)
        self._options[option_key] = option_value
        self.markForPlanRegen()
        return self

    def options(self, **kwargs):
        """ set options in bulk

        Allows for multiple options with option=option_value style of option passing

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        for key, value in kwargs.items():
            self.option(key, value)
        self.markForPlanRegen()
        return self

    def _processOptions(self):
        """ process options to give effect to the options supplied earlier

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        self.logger.info("options: %s", str(self._options))

        for key, value in self._options.items():
            if key == "starting_id":
                self.starting_id = value
            elif key == "row_count":
                self.rowCount = value
        return self

    def describe(self):
        """ return description of the dataset generation spec

        :returns: Dict object containing key attributes of test data generator instance
        """

        return {
            'name': self.name,
            'rowCount': self.rowCount,
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
                                                              self.rowCount,
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
        """ return True if column generation spec has been explicitly defined for column, else false """
        ensure(colName is not None, "colName should be non-empty")
        colDef = self.columnSpecsByName.get(colName, None)
        return not colDef.implicit if colDef is not None else False

    def getInferredColumnNames(self):
        """ get list of output columns """
        return [fd.name for fd in self.inferredSchemaFields]

    @staticmethod
    def flatten(l):
        """ flatten list

        :param l: list to flatten
        """
        return [item for sublist in l for item in sublist]

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
        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        ensure(sch is not None, "schema sch should be non-empty")
        self.__schema__ = sch

        for fs in sch.fields:
            self.withColumn(fs.name, fs.dataType, implicit=True, omit=False, nullable=fs.nullable)
        return self

    def _computeRange(self, data_range, min, max, step):
        """ Compute merged range based on parameters

        :returns: effective min, max, step as tuple
        """
        if data_range is not None and isinstance(data_range, range):
            if max is not None or min != 0 or step != 1:
                raise ValueError("You cant specify both a range and min, max or step values")

            return data_range.start, data_range.stop, data_range.step
        else:
            return min, max, step

    def withColumnSpecs(self, patterns=None, fields=None, match_types=None, **kwargs):
        """Add column specs for columns matching
           a) list of field names,
           b) one or more regex patterns
           c) type (as in pyspark.sql.types)

        :param patterns: patterns may specified a single pattern as a string or a list of patterns that match the column names. May be omitted.
        :param fields: a string specifying an explicit field to match , or a list of strings specifying explicit fields to match. May be omitted.
        :param match_types: a single Spark SQL datatype or list of Spark SQL data types to match. May be omitted.
        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        if fields is not None and type(fields) is str:
            fields = [fields]
        if match_types is not None and type(match_types) is not list:
            match_types = [match_types]
        if patterns is not None and type(patterns) is str:
            patterns = ["^" + patterns + "$"]
        elif type(patterns) is list:
            patterns = ["^" + pat + "$" for pat in patterns]

        all_fields=self.getInferredColumnNames()
        effective_fields = [x for x in all_fields if (fields is None or x in fields) and x != "id"]

        if patterns is not None:
            effective_fields = [x for x in effective_fields for y in patterns if re.search(y, x) is not None]

        if match_types is not None:
            effective_fields = [x for x in effective_fields for y in match_types
                                if self.getColumnType(x) == y ]

        for f in effective_fields:
            self.withColumnSpec(f, implicit=True, **kwargs)
        return self

    def _checkColumnOrColumnList(self, columns, allow_id=False):
        """ Check if column or columns refer to existing columns

        :param columns: a single column or list of columns as strings
        :param allow_id: If True, allows the specialized column `id` to be present in columns
        :returns: True if test passes
        """
        inferredColumns = self.getInferredColumnNames()
        if allow_id and columns == "id":
            return True

        if type(columns) is list:
            for column in columns:
                ensure(column in inferredColumns,
                       " column `{0}` must refer to defined column".format(column))
        else:
            ensure(columns in inferredColumns,
                   " column `{0}` must refer to defined column".format(columns))
        return True

    def withColumnSpec(self, colName, min=None, max=None, step=1, prefix=None, random=False, distribution=None,
                       implicit=False, data_range=None, omit=False, base_column="id", **kwargs):
        """ add a column specification for an existing column

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colName in self.getInferredColumnNames(), " column `{0}` must refer to defined column".format(colName))
        self._checkColumnOrColumnList(base_column)
        ensure(not self.isFieldExplicitlyDefined(colName), "duplicate column spec for column `{0}`".format(colName))

        newProps = {}
        newProps.update(kwargs)

        self.logger.info(
            "adding column spec - `{0}` with baseColumn : `{1}`, implicit : {2} , omit {3}".format(colName, base_column,
                                                                                                   implicit, omit))
        self.generateColumnDefinition(colName, self.getColumnType(colName), min=min, max=max, step=step, prefix=prefix,
                                      random=random, data_range=data_range,
                                      distribution=distribution, base_column=base_column,
                                      implicit=implicit, omit=omit, **newProps)
        return self

    def hasColumnSpec(self, colName):
        """returns true if there is a column spec for the column

        :param colName: name of column to check for
        :returns: True if column has spec, False otherwise
        """
        return True if colName in self.columnSpecsByName.keys() else False

    def withColumn(self, colName, colType=StringType(), min=None, max=None, step=1,
                   data_range=None, prefix=None, random=False, distribution=None,
                   base_column="id", nullable=True,
                   omit=False, implicit=False,
                   **kwargs):
        """ add a new column for specification

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colType is not None, "Must specify column type for column `{0}`".format(colName))
        self._checkColumnOrColumnList(base_column, allow_id=True)
        newProps = {}
        newProps.update(kwargs)

        from .schema_parser import SchemaParser
        if type(colType) == str:
            colType = SchemaParser.columnTypeFromString(colType)

        self.logger.info("effective range: %s, %s, %s args: %s", min, max, step, kwargs)
        self.logger.info(
            "adding column - `{0}` with baseColumn : `{1}`, implicit : {2} , omit {3}".format(colName, base_column,
                                                                                              implicit, omit))
        self.generateColumnDefinition(colName, colType, min=min, max=max, step=step, prefix=prefix, random=random,
                                      distribution=distribution, base_column=base_column, data_range=data_range,
                                      implicit=implicit, omit=omit, **newProps)
        self.inferredSchemaFields.append(StructField(colName, colType, nullable))
        return self

    def generateColumnDefinition(self, colName, colType=None, base_column="id",
                                 implicit=False, omit=False, nullable=True, **kwargs):
        """ generate field definition and column spec

        Note: Any time that a new column definition is added, we'll mark that the build plan needs to be regenerated.
        For our purposes, the build plan determines the order of column generation etc.

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        if colType is None:
            colType = self.getColumnType(base_column)

        column_spec = ColumnGenerationSpec(colName, colType,
                                           base_column=base_column,
                                           implicit=implicit,
                                           omit=omit,
                                           random_seed=self._randomSeed,
                                           random_seed_method=self.seed_method,
                                           nullable=nullable,
                                           verbose=self.verbose,
                                           debug=self.debug,
                                           **kwargs)

        self.columnSpecsByName[colName] = column_spec

        # if column spec for column already exists - remove it
        items_to_remove = [ x  for x in self.allColumnSpecs if x.name == colName ]
        for x in items_to_remove:
            self.allColumnSpecs.remove(x)

        self.allColumnSpecs.append(column_spec)

        # mark that the build plan needs to be regenerated
        self.markForPlanRegen()

        return self

    def getBaseDataFrame(self, start_id=0, streaming=False, options=None):
        """ generate the base data frame and id column , partitioning the data if necessary

        :returns: Spark data frame for base data that drives the data generation
        """

        end_id = self.rowCount + start_id
        id_partitions = self.partitions if self.partitions is not None else 4

        if not streaming:
            status = ("Generating data frame with ids from {} to {} with {} partitions"
                              .format(start_id, end_id, id_partitions))
            self.logger.info(status)
            self.execution_history.append(status)
            df1 = self.sparkSession.range(start=start_id,
                                          end=end_id,
                                          numPartitions=id_partitions)

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

                for k,v in options.items():
                    df1 = df1.option(k,v)
                df1= df1.load().withColumnRenamed("value", "id")
            else:
                df1 = ( df1.option("rowsPerSecond", 1)
                        .option("numPartitions", id_partitions)
                        .load()
                        .withColumnRenamed("value", "id")
                        )

        return df1

    def _ppList(self, alist, msg=""):
        """Pretty printing for lists"""
        print(msg)
        l = len(alist)
        for x in alist:
            print(x)

    def computeColumnBuildOrder(self):
        """ compute the build ordering using a topological sort on dependencies

        :returns: the build ordering
        """
        dependency_ordering = [(x.name, set(x.dependencies)) if x.name != 'id' else ('id', set())
                               for x in self.allColumnSpecs]

        #self.pp_list(dependency_ordering, msg="dependencies")

        self.logger.info("dependency list: %s", str(dependency_ordering))

        self._build_order = list(topologicalSort(dependency_ordering, flatten=False, initial_columns=['id']))

        self.logger.info("columnBuildOrder: %s", str(self._build_order))

        #self.pp_list(self._build_order, "build order")
        return self._build_order

    @property
    def build_order(self):
        """ return the build order minus the `id` column

        The build order will be a list of lists - each list specifying columns that can be built at the same time
        """
        return [x for x in self._build_order if x != ["id"] ]

    def get_column_data_types(self, columns):
        """ Get data types for columns

        :param columns: = list of columns to retrieve data types for
        """
        return [ self.columnSpecsByName[col].datatype for col in columns ]

    def computeBuildPlan(self):
        """ prepare for building

        :returns: modified in-place instance of test data generator allowing for chaining of calls following Builder pattern
        """
        self.build_plan = []
        self.execution_history = []
        self._processOptions()
        self.build_plan.append("Build dataframe with id")

        # add temporary columns
        for cs in self.allColumnSpecs:
            # extend overall build plan with build plan of each column spec
            self.build_plan.extend(cs.initial_build_plan)

            # handle generation of any temporary columns
            for tmp_col in cs.temporary_columns:
                # create column spec for temporary column if its not already present
                if not self.hasColumnSpec(tmp_col[0]):
                    self.build_plan.append("materializing temporary column {}".format(tmp_col[0]))
                    self.withColumn(tmp_col[0], tmp_col[1], **tmp_col[2])

        # TODO: set up the base column data type information
        for cs in self.allColumnSpecs:
            base_column_datatypes = self.get_column_data_types(cs.base_columns)
            cs.set_base_column_datatypes(base_column_datatypes)

        self.computeColumnBuildOrder()

        for x1 in self._build_order:
            for x in x1:
                cs = self.columnSpecsByName[x]
                self.build_plan.append(cs.getPlanEntry())

        self.build_plan_computed=True
        return self

    def build(self, withTempView=False, withView=False, withStreaming=False, options=None):
        """ build the test data set from the column definitions and return a dataframe for it

        if `withStreaming` is True, generates a streaming data set. Use options to control the rate of generation of test data if streaming is used.

        For example:

        `dfTestData = testDataSpec.build(withStreaming=True,options={ 'rowsPerSecond': 5000})`

        :param withTempView: if True, automatically creates temporary view for generated data set
        :param withView: If True, automatically creates global view for data set
        :param withStreaming: If True, generates data using Spark Structured Streaming Rate source suitable for writing with `writeStream`
        :param options: optional Dict of options to control generating of streaming data
        :returns: Spark SQL dataframe of generated test data
        """
        self.logger.debug("starting build ... withStreaming [%s]", withStreaming)
        self.execution_history = []
        self.computeBuildPlan()

        outputColumns = self.getOutputColumnNames()
        ensure(outputColumns is not None and len(outputColumns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base id column
               """)

        df1 = self.getBaseDataFrame(self.starting_id, streaming=withStreaming, options=options)

        if self.use_pandas:
            self.execution_history.append("Using Pandas Optimizations {}".format(self.use_pandas))

        # build columns
        if self.generateWithSelects:
            self.execution_history.append("Generating data with selects")

            # generation with selects may be more efficient as less intermediate data frames
            # are generated resulting in shorter lineage

            for colNames in self.build_order:
                build_round=["*"]
                nCol = 0
                self.execution_history.append("building round for : {}".format(colNames))
                for colName in colNames:
                    col1 = self.columnSpecsByName[colName]
                    columnGenerators = col1.makeGenerationExpressions(self.use_pandas)
                    self.execution_history.extend(col1.execution_history)
                    if type(columnGenerators) is list and len(columnGenerators) == 1:
                        build_round.append( columnGenerators[0].alias(colName))
                    elif type(columnGenerators) is list and len(columnGenerators) > 1:
                        i = 0
                        for cg in columnGenerators:
                            build_round.append(cg.alias('{0}_{1}'.format(colName, i)))
                            i += 1
                    else:
                        build_round.append(columnGenerators.alias(colName))
                    nCol = nCol + 1

                df1 = df1.select(*build_round)
        else:
            # build columns
            self.execution_history.append("Generating data with withColumn statements")

            column_build_order = [ item for sublist in self.build_order for item in sublist ]
            for colName in column_build_order:
                col1 = self.columnSpecsByName[colName]
                columnGenerators = col1.makeGenerationExpressions()
                self.execution_history.extend(col1.execution_history)

                if type(columnGenerators) is list and len(columnGenerators) == 1:
                    df1 = df1.withColumn(colName, columnGenerators[0])
                elif type(columnGenerators) is list and len(columnGenerators) > 1:
                    i = 0
                    for cg in columnGenerators:
                        df1 = df1.withColumn('{0}_{1}'.format(colName, i), cg)
                        i += 1
                else:
                    df1 = df1.withColumn(colName, columnGenerators)

        df1 = df1.select(*self.getOutputColumnNames())
        self.execution_history.append("selecting columns: {}".format(self.getOutputColumnNames()))

        # register temporary or global views if necessary
        if withView:
            self.execution_history.append("registering view")
            self.logger.info("Registered global view [{0}]".format(self.name))
            df1.createGlobalTempView(self.name)
            self.logger.info("Registered!")
        elif withTempView:
            self.execution_history.append("registering temp view")
            self.logger.info("Registering temporary view [{0}]".format(self.name))
            df1.createOrReplaceTempView(self.name)
            self.logger.info("Registered!")

        return df1

    def sqlTypeFromSparkType(self, dt):
        """Get sql type for spark type"""
        return dt.simpleString()

    def scriptTable(self, name=None, location=None,table_format="delta"):
        """ generate create table script suitable for format of test data set

        :returns: SQL string for scripted table
        """
        assert name is not None

        self.computeBuildPlan()

        outputColumns = self.getOutputColumnNamesAndTypes()

        results = [ "CREATE TABLE IF NOT EXISTS {} (".format(name)]
        ensure(outputColumns is not None and len(outputColumns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base id column
               """)

        col_expressions = []
        for col in outputColumns:
            col_expressions.append("    {} {}".format(col[0], self.sqlTypeFromSparkType(col[1])))
        results.append(",\n".join(col_expressions))
        results.append(")")
        results.append("using {}".format(table_format))

        if location is not None:
            results.append("location '{}'".format(location))

        return "\n".join(results)