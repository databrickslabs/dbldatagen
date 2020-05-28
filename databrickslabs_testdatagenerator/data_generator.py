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
from .utils import ensure, topological_sort, DataGenError
from .dataranges import DateRange
from .spark_singleton import SparkSingleton
import copy


class DataGenerator:
    """ Main Class for test data set generation """

    # class vars
    nextNameIndex = 0
    untitledNamePrefix = "Untitled"
    randomSeed = 42

    allowed_keys = ["starting_id", "row_count", "output_id"]

    @classmethod
    def seed(cls, seedVal):
        """ set seed for random number generation

            Arguments:
            :param  seedVal: - new value for the random number seed
        """
        cls.randomSeed = seedVal

    @classmethod
    def reset(cls):
        """ reset any state associated with the data """
        cls.nextNameIndex = 0

    @classmethod
    def generateName(cls):
        """ get a name for the data set
            Uses the untitled name prefix and nextNameIndex to generate a dummy dataset name
        """
        cls.nextNameIndex += 1
        newName = (cls.untitledNamePrefix + '_' + str(cls.nextNameIndex))
        return newName

    def printVerbose(self, *args):
        """ Write out message only if `self.verbose` is set to True"""
        if self.verbose:
            print("data generator:", *args)

    def __init__(self, sparkSession=None, name=None, seed_method=None, generate_with_selects=True,
                 rows=1000000, starting_id=0, seed=None, partitions=None, verbose=False,
                 use_pandas=True, pandas_udf_batch_size=None):
        """ Constructor:
        :param name: is name of data set
        :param rows: = amount of rows to generate
        :param starting_id: = starting id for generated id column
        :param seed: = seed for random number generator
        :param seed_method: = seed method for random numbers - either None, 'fixed', 'hash_fieldname'
        :param partitions: = number of partitions to generate
        :param verbose: = if `True`, generate verbose output
        """
        self.verbose = verbose
        self.name = name if name is not None else self.generateName()
        self.rowCount = rows
        self.starting_id = starting_id
        self.__schema__ = None

        self.seed_method = seed_method
        self.seed = seed if seed is not None else self.randomSeed

        # if a seed was supplied but no seed method was applied, make the seed method "fixed"
        if seed is not None and seed_method is None:
            self.seed_method="fixed"

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
        self.use_pandas=use_pandas
        self.pandas_udf_batch_size = pandas_udf_batch_size

        if sparkSession is None:
            sparkSession = SparkSingleton.get_instance()

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
            print("*** using pandas udf for custom functions ***")
            self.sparkSession.conf.set("spark.sql.execution.arrow.enabled", "true")

            if self.pandas_udf_batch_size is not None:
                self.sparkSession.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", self.pandas_udf_batch_size)

        if seed_method is not None and seed_method != "fixed" and seed_method != "hash_fieldname":
            raise DataGenError("""seed_method should be None, 'fixed' or 'hash_fieldname' """)

    def clone(self):
        """Make a clone of the data spec via deep copy preserving same spark session"""
        old_spark_session = self.sparkSession
        new_copy = None
        try:
            # temporarily set the spark session to null
            self.sparkSession = None
            new_copy = copy.deepcopy(self)
            new_copy.sparkSession = old_spark_session
            new_copy.build_plan_computed = False
        finally:
            # now set it back
            self.sparkSession = old_spark_session
        return new_copy

    def mark_for_replan(self):
        self.build_plan_computed = False


    def explain(self):
        if not self.build_plan_computed:
            self.compute_build_plan()

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
        """Modify the row count - useful when starting a new spec from a clone"""
        self.rowCount=rc
        return self

    def withIdOutput(self):
        """ output id field as a column in the test data set if specified """
        self.columnSpecsByName["id"].omit = False
        self.mark_for_replan()

        return self

    def option(self, option_key, option_value):
        """ set option to option value for later processing"""
        ensure(option_key in self.allowed_keys)
        self._options[option_key] = option_value
        self.mark_for_replan()
        return self

    def options(self, **kwargs):
        """ set options in bulk"""
        for key, value in kwargs.items():
            self.option(key, value)
        self.mark_for_replan()
        return self

    def _process_options(self):
        """ process options to give effect to the options supplied earlier"""
        self.printVerbose("options", self._options)

        for key, value in self._options.items():
            if key == "starting_id":
                self.starting_id = value
            elif key == "row_count":
                self.rowCount = value

    def describe(self):
        """ return description of the dataset generation spec"""
        return {
            'name': self.name,
            'rowCount': self.rowCount,
            'schema': self.schema,
            'seed': self.seed,
            'partitions': self.partitions,
            'columnDefinitions': self.columnSpecsByName}

    def __repr__(self):
        """ return the repr string for the class"""
        return "{}(name='{}', rows={}, partitions={})".format(__class__.__name__,
                                                              self.name,
                                                              self.rowCount,
                                                              self.partitions)

    def checkFieldList(self):
        ensure(self.inferredSchemaFields is not None, "schemaFields should be non-empty")
        ensure(type(self.inferredSchemaFields) is list, "schemaFields should be list")

    @property
    def schemaFields(self):
        """ get list of schema fields for final output schema """
        self.checkFieldList()
        return [fd for fd in self.inferredSchemaFields if not self.columnSpecsByName[fd.name].isFieldOmitted]

    @property
    def schema(self):
        """ infer spark output schema definition from the field specifications"""
        return StructType(self.schemaFields)

    @property
    def inferredSchema(self):
        """ infer spark interim schema definition from the field specifications"""
        self.checkFieldList()
        return StructType(self.inferredSchemaFields)

    def __getitem__(self, key):
        """ implement the built in derefernce by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self.columnSpecsByName[key]


    def getColumnType(self, colName):
        """ Get column type for specified column """
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
        return [item for sublist in l for item in sublist]

    def getOutputColumnNames(self):
        """ get list of output columns by flattening list of lists of column names
            normal columns will have a single column name but column definitions that result in
            multiple columns will produce a list of multiple names
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
        """ populate column definitions and specifications for each of the columns in the schema"""
        ensure(sch is not None, "schema sch should be non-empty")
        self.__schema__ = sch

        for fs in sch.fields:
            self.withColumn(fs.name, fs.dataType, implicit=True, omit=False, nullable=fs.nullable)
        return self

    def _computeRange(self, data_range, min, max, step):
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

    def _check_column_or_column_list(self, columns, allow_id=False):
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

    def withColumnSpec(self, colName, min=None, max=None, step=1, prefix=None, random=False, distribution="normal",
                       implicit=False, data_range=None, omit=False, base_column="id", **kwargs):
        """ add a column specification for an existing column """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colName in self.getInferredColumnNames(), " column `{0}` must refer to defined column".format(colName))
        self._check_column_or_column_list(base_column)
        ensure(not self.isFieldExplicitlyDefined(colName), "duplicate column spec for column `{0}`".format(colName))

        newProps = {}
        newProps.update(kwargs)

        self.printVerbose(
            "adding column spec - `{0}` with baseColumn : `{1}`, implicit : {2} , omit {3}".format(colName, base_column,
                                                                                                   implicit, omit))
        self.generateColumnDefinition(colName, self.getColumnType(colName), min=min, max=max, step=step, prefix=prefix,
                                      random=random, data_range=data_range,
                                      distribution=distribution, base_column=base_column,
                                      implicit=implicit, omit=omit, **newProps)
        return self

    def hasColumnSpec(self, colName):
        """returns true if there is a column spec for the column """
        return True if colName in self.columnSpecsByName.keys() else False

    def withColumn(self, colName, colType=StringType(), min=None, max=None, step=1,
                   data_range=None, prefix=None, random=False, distribution="normal",
                   base_column="id", nullable=True,
                   omit=False, implicit=False,
                   **kwargs):
        """ add a new column for specification """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colType is not None, "Must specify column type for column `{0}`".format(colName))
        self._check_column_or_column_list(base_column, allow_id=True)
        newProps = {}
        newProps.update(kwargs)

        from .schema_parser import SchemaParser
        if type(colType) == str:
            colType = SchemaParser.columnTypeFromString(colType)

        self.printVerbose("effective range:", min, max, step, "args:", kwargs)
        self.printVerbose(
            "adding column - `{0}` with baseColumn : `{1}`, implicit : {2} , omit {3}".format(colName, base_column,
                                                                                              implicit, omit))
        self.generateColumnDefinition(colName, colType, min=min, max=max, step=step, prefix=prefix, random=random,
                                      distribution=distribution, base_column=base_column, data_range=data_range,
                                      implicit=implicit, omit=omit, **newProps)
        self.inferredSchemaFields.append(StructField(colName, colType, nullable))
        return self

    def generateColumnDefinition(self, colName, colType=None, base_column="id",
                                 implicit=False, omit=False, nullable=True, **kwargs):
        """ get field definition and column spec """
        if colType is None:
            colType = self.getColumnType(base_column)

        column_spec = ColumnGenerationSpec(colName, colType,
                                           base_column=base_column,
                                           implicit=implicit,
                                           omit=omit,
                                           random_seed=self.randomSeed,
                                           random_seed_method=self.seed_method,
                                           nullable=nullable, **kwargs)
        self.columnSpecsByName[colName] = column_spec

        # if column spec for column already exists - remove it
        items_to_remove = [ x  for x in self.allColumnSpecs if x.name == colName ]
        for x in items_to_remove:
            self.allColumnSpecs.remove(x)

        self.allColumnSpecs.append(column_spec)
        self.mark_for_replan()

        return self

    def getBaseDataFrame(self, start_id=0, streaming=False, options=None):
        """ generate the base data frame and id column , partitioning the data if necessary """

        end_id = self.rowCount + start_id
        id_partitions = self.partitions if self.partitions is not None else 4

        if not streaming:
            status = ("Generating data frame with ids from {} to {} with {} partitions"
                              .format(start_id, end_id, id_partitions))
            self.printVerbose(status)
            self.execution_history.append(status)
            df1 = self.sparkSession.range(start=start_id,
                                          end=end_id,
                                          numPartitions=id_partitions)

        else:
            status = ("Generating streaming data frame with ids from {} to {} with {} partitions"
                          .format(start_id, end_id, id_partitions))
            self.printVerbose(status)
            self.execution_history.append(status)

            df1 = (self.sparkSession.readStream
                    .format("rate"))
            if options is not None:
                if "rowsPerSecond" not in options:
                    options['rowsPerSecond'] = 1

                for k,v in options.items:
                    df1 = df1.option(k,v)
                df1= df1.load().withColumnRenamed("value", "id")
            else:
                df1 = ( df1.option("rowsPerSecond", 1)
                        .option("numPartitions", id_partitions)
                        .load()
                        )

        return df1

    def pp_list(self, alist, msg=""):
        print(msg)
        l = len(alist)
        for x in alist:
            print(x)

    def compute_column_build_order(self):
        """ compute the build ordering using a topological sort on dependencies"""
        dependency_ordering = [(x.name, set(x.dependencies)) if x.name != 'id' else ('id', set())
                               for x in self.allColumnSpecs]

        #self.pp_list(dependency_ordering, msg="dependencies")

        self.printVerbose("dependency list:", dependency_ordering)

        self._build_order = list(topological_sort(dependency_ordering, flatten=False, initial_columns=['id'] ))

        self.printVerbose("columnBuildOrder:", self._build_order)

        #self.pp_list(self._build_order, "build order")
        return self._build_order

    @property
    def build_order(self):
        """ return the build order minus the `id` column

        The build order will be a list of lists - each list specifying columns that can be built at the same time
        """
        return [x for x in self._build_order if x != ["id"] ]

    def compute_build_plan(self):
        """ prepare for building """
        self.build_plan = []
        self.execution_history = []
        self._process_options()
        self.build_plan.append("Build dataframe with id")


        # add temporary columns
        for cs in self.allColumnSpecs:
            self.build_plan.extend(cs.initial_build_plan)
            for tmp_col in cs.temporary_columns:
                if not self.hasColumnSpec(tmp_col[0]):
                    self.build_plan.append("materializing temporary column {}".format(tmp_col[0]))
                    self.withColumn(tmp_col[0], tmp_col[1], **tmp_col[2])

        self.compute_column_build_order()

        for x1 in self._build_order:
            for x in x1:
                cs = self.columnSpecsByName[x]
                self.build_plan.append(cs.getPlan())

        self.build_plan_computed=True
        return self

    def build(self, withTempView=False, withView=False, withStreaming=False, options=None):
        """ build the test data set from the column definitions and return a dataframe for it"""

        self.execution_history = []
        self.compute_build_plan()

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
                    columnGenerators = col1.make_generation_expressions(self.use_pandas)
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
                columnGenerators = col1.make_generation_expressions()
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
            self.printVerbose("Registered global view [{0}]".format(self.name))
            df1.createGlobalTempView(self.name)
            self.printVerbose("Registered!")
        elif withTempView:
            self.execution_history.append("registering temp view")
            self.printVerbose("Registering temporary view [{0}]".format(self.name))
            df1.createOrReplaceTempView(self.name)
            self.printVerbose("Registered!")

        return df1

    def sqlTypeFromSparkType(self, dt):
        return dt.simpleString()

    def scriptTable(self, name=None, location=None,table_format="delta"):
        """ generate create table script suitable for format of test data set"""
        assert name is not None

        self.compute_build_plan()

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