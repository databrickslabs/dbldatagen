#
# Copyright (C) 2019 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

from .column_generation_spec import ColumnGenerationSpec
from .utils import ensure, topological_sort, DataGenError
from .daterange import DateRange
from .spark_singleton import SparkSingleton



class DataGenerator:
    """ Class for test data set generation """

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

    def __init__(self, sparkSession=None, name=None,
                 rows=1000000, starting_id=0, seed=None, partitions=None, verbose=False):
        """ Constructor:
        :param name: is name of data set
        :param rows: = amount of rows to generate
        :param starting_id: = starting id for generated id column
        :param seed: = seed for random number generator
        :param partitions: = number of partitions to generate
        :param verbose: = if `True`, generate verbose output
        """
        self.verbose = verbose
        self.name = name if name is not None else self.generateName()
        self.rowCount = rows
        self.starting_id = starting_id
        self.__schema__ = None
        self.seed = seed if seed is not None else self.randomSeed
        self.columnSpecsByName = {}
        self.allColumnSpecs = []
        self.build_plan = []
        self._options = {}
        self._build_order = []
        self.inferredSchemaFields = []
        self.partitions = partitions if partitions is not None else 10
        self.withColumn("id", LongType(), nullable=False, implicit=True, omit=True)

        if sparkSession is None:
            sparkSession = SparkSingleton.get_instance()

        assert sparkSession is not None, "The spark session attribute must be initialized"

        self.sparkSession = sparkSession
        if sparkSession is None:
            raise DataGenError("""Spark session not initialized
            
            The spark session attribute must be initialized in the DataGenerator initialization
            
            i.e DataGenerator(sparkSession=spark, name="test", ...)
            """)

    def explain(self):
        output = ["", "Data generation plan", "====================",
                  """spec=DateGenerator(name={}, rows={}, starting_id={}, partitions={})"""
                      .format(self.name, self.rowCount, self.starting_id, self.partitions), ")", "",
                  "column build order: {}".format(self._build_order), "", "build plan:"]

        for plan_action in self.build_plan:
            output.append(" ==> " + plan_action)
        output.append("")
        output.append("====================")
        output.append("")

        print("\n".join(output))

    def withIdOutput(self):
        """ output id field as a column in the test data set if specified """
        self.columnSpecsByName["id"].omit = False
        return self

    def option(self, option_key, option_value):
        """ set option to option value for later processing"""
        ensure(option_key in self.allowed_keys)
        self._options[option_key] = option_value
        return self

    def options(self, **kwargs):
        """ set options in bulk"""
        for key, value in kwargs.items():
            self.option(key, value)
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
                raise ValueError("You cant specufy a range and min, max or step values")

            return data_range.start, data_range.stop, data_range.step
        else:
            return min, max, step

    def withColumnSpec(self, colName, min=0, max=None, step=1, prefix='', random=False, distribution="normal",
                       implicit=False, data_range=None, omit=False, base_column="id", **kwargs):
        """ add a column specification for an existing column """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colName in self.getInferredColumnNames(), " column `{0}` must refer to defined column".format(colName))
        ensure(base_column in self.getInferredColumnNames(),
               "base column `{0}` must refer to defined column".format(base_column))
        ensure(not self.isFieldExplicitlyDefined(colName), "duplicate column spec for column `{0}`".format(colName))

        newProps = {}
        newProps.update(kwargs)

        if isinstance(data_range, range):
            min, max, step = self._computeRange(data_range, min, max, step)
        elif isinstance(data_range, DateRange):
            newProps['begin'] = data_range.begin
            newProps['end'] = data_range.end
            newProps['interval'] = data_range.interval

        self.printVerbose(
            "adding column spec - `{0}` with baseColumn : `{1}`, implicit : {2} , omit {3}".format(colName, base_column,
                                                                                                   implicit, omit))
        self.generateColumnDefinition(colName, self.getColumnType(colName), min=min, max=max, step=step, prefix=prefix,
                                      random=random,
                                      distribution=distribution, base_column=base_column,
                                      implicit=implicit, omit=omit, **newProps)
        return self

    def hasColumnSpec(self, colName):
        """returns true if there is a column spec for the column """
        return True if colName in self.columnSpecsByName.keys() else False

    def withColumn(self, colName, colType=StringType(), min=0, max=None, step=1,
                   data_range=None, prefix='', random=False, distribution="normal",
                   base_column="id", nullable=True,
                   omit=False, implicit=False,
                   **kwargs):
        """ add a new column for specification """
        ensure(colName is not None, "Must specify column name for column")
        ensure(colType is not None, "Must specify column type for column `{0}`".format(colName))
        ensure(base_column == "id" or base_column in self.getInferredColumnNames(),
               "base column `{0}` must refer to defined column (did you set the base column attibute?)".format(
                   base_column))
        newProps = {}
        newProps.update(kwargs)

        if isinstance(data_range, range):
            min, max, step = self._computeRange(data_range, min, max, step)
        elif isinstance(data_range, DateRange):
            newProps['begin'] = data_range.begin
            newProps['end'] = data_range.end
            newProps['interval'] = data_range.interval

        from .schema_parser import SchemaParser
        if type(colType) == str:
            colType = SchemaParser.columnTypeFromString(colType)

        self.printVerbose("effective range:", min, max, step, "args:", kwargs)
        self.printVerbose(
            "adding column - `{0}` with baseColumn : `{1}`, implicit : {2} , omit {3}".format(colName, base_column,
                                                                                              implicit, omit))
        self.generateColumnDefinition(colName, colType, min=min, max=max, step=step, prefix=prefix, random=random,
                                      distribution=distribution, base_column=base_column,
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
                                           nullable=nullable, **kwargs)
        self.columnSpecsByName[colName] = column_spec
        self.allColumnSpecs.append(column_spec)

        return self

    def getBaseDataFrame(self, start_id=0, streaming=False, options=None):
        """ generate the base data frame and id column , partitioning the data if necessary """

        end_id = self.rowCount + start_id
        id_partitions = self.partitions if self.partitions is not None else 8

        if not streaming:
            self.printVerbose("Generating streaming data frame with ids from {} to {} with {} partitions"
                              .format(start_id, end_id, id_partitions))
            df1 = self.sparkSession.range(start=start_id,
                                          end=end_id,
                                          numPartitions=id_partitions)

        else:
            self.printVerbose("Generating data frame with ids from {} to {} with {} partitions"
                          .format(start_id, end_id, id_partitions))


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

    def compute_column_build_order(self):
        """ compute the build ordering using a topological sort on dependencies"""
        dependency_ordering = [(x.name, set(x.dependencies)) if x.name != 'id' else ('id', set())
                               for x in self.allColumnSpecs]

        self.printVerbose("dependency list:", dependency_ordering)

        self._build_order = list(topological_sort(dependency_ordering))

        self.printVerbose("columnBuildOrder:", self._build_order)
        return self._build_order

    @property
    def build_order(self):
        """ return the build order minus the `id` column """
        return [x for x in self._build_order if x != "id"]

    def compute_build_plan(self):
        """ prepare for building """
        self.build_plan = []
        self._process_options()
        self.build_plan.append("Build dataframe with id")

        # add temporary columns
        for cs in self.allColumnSpecs:
            for tmp_col in cs.temporary_columns:
                if not self.hasColumnSpec(tmp_col[0]):
                    self.withColumn(tmp_col[0], tmp_col[1], **tmp_col[2])

        self.compute_column_build_order()

        for x in self._build_order:
            cs = self.columnSpecsByName[x]
            self.build_plan.append(cs.getPlan())

        return self

    def build(self, withTempView=False, withView=False, withStreaming=False, options=None):
        """ build the test data set from the column definitions and return a dataframe for it"""

        self.compute_build_plan()

        outputColumns = self.getOutputColumnNames()
        ensure(outputColumns is not None and len(outputColumns) > 0,
               """
                | You must specify at least one column for output
                | - use withIdOutput() to output base id column
               """)

        df1 = self.getBaseDataFrame(self.starting_id, streaming=withStreaming, options=options)

        # build columns
        for colName in self.build_order:
            col1 = self.columnSpecsByName[colName]
            columnGenerators = col1.make_generation_expressions()

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

        # register temporary or global views if necessary
        if withView:
            self.printVerbose("Registered global view [{0}]".format(self.name))
            df1.createGlobalTempView(self.name)
            self.printVerbose("Registered!")
        elif withTempView:
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