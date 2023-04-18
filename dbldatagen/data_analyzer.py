# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``DataAnalyzer`` class.

This code is experimental and both APIs and code generated is liable to change in future versions.
"""
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, StructType, ArrayType, DataType

import pyspark.sql as ssql
import pyspark.sql.functions as F

from .utils import strip_margins
from .spark_singleton import SparkSingleton


class DataAnalyzer:
    """This class is used to analyze an existing data set to assist in generating a test data set with similar
    characteristics, and to generate code from existing schemas and data

    :param df: Spark dataframe to analyze
    :param sparkSession: Spark session instance to use when performing spark operations

    .. warning::
       Experimental

    """
    _DEFAULT_GENERATED_NAME = "synthetic_data"

    _GENERATED_COMMENT = strip_margins("""
                        |# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
                        |# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
                        |# See the following resources for more details:
                        |#
                        |#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
                        |#   Github project - [https://github.com/databrickslabs/dbldatagen]
                        |#""", '|')

    _GENERATED_FROM_SCHEMA_COMMENT = strip_margins("""
                        |# Column definitions are stubs only - modify to generate correct data  
                        |#""", '|')

    def __init__(self, df=None, sparkSession=None):
        """ Constructor:
        :param df: Dataframe to analyze
        :param sparkSession: Spark session to use
        """
        assert df is not None, "dataframe must be supplied"

        self._df = df

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self._sparkSession = sparkSession
        self._dataSummary = None

    def _displayRow(self, row):
        """Display details for row"""
        results = []
        row_key_pairs = row.asDict()
        for x in row_key_pairs:
            results.append(f"{x}: {row[x]}")

        return ", ".join(results)

    def _addMeasureToSummary(self, measureName, summaryExpr="''", fieldExprs=None, dfData=None, rowLimit=1,
                             dfSummary=None):
        """ Add a measure to the summary dataframe

        :param measureName: Name of measure
        :param summaryExpr: Summary expression
        :param fieldExprs: list of field expressions (or generator)
        :param dfData: Source data df - data being summarized
        :param rowLimit: Number of rows to get for measure
        :param dfSummary: Summary df
        :return: dfSummary with new measure added
        """
        assert dfData is not None, "source data dataframe must be supplied"
        assert measureName is not None and len(measureName) > 0, "invalid measure name"

        # add measure name and measure summary
        exprs = [f"'{measureName}' as measure_", f"string({summaryExpr}) as summary_"]

        # add measures for fields
        exprs.extend(fieldExprs)

        if dfSummary is not None:
            dfResult = dfSummary.union(dfData.selectExpr(*exprs).limit(rowLimit))
        else:
            dfResult = dfData.selectExpr(*exprs).limit(rowLimit)

        return dfResult

    def summarizeToDF(self):
        """ Generate summary analysis of data set as dataframe

        :return: Summary results as dataframe

        The resulting dataframe can be displayed with the ``display`` function in a notebook environment
        or with the ``show`` method.

        The output is also used in code generation  to generate more accurate code.
        """
        self._df.cache().createOrReplaceTempView("data_analysis_summary")

        total_count = self._df.count() * 1.0

        dtypes = self._df.dtypes

        # schema information
        dfDataSummary = self._addMeasureToSummary(
            'schema',
            summaryExpr=f"""to_json(named_struct('column_count', {len(dtypes)}))""",
            fieldExprs=[f"'{dtype[1]}' as {dtype[0]}" for dtype in dtypes],
            dfData=self._df)

        # count
        dfDataSummary = self._addMeasureToSummary(
            'count',
            summaryExpr=f"{total_count}",
            fieldExprs=[f"string(count({dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'null_probability',
            fieldExprs=[f"""string( round( ({total_count} - count({dtype[0]})) /{total_count}, 2)) as {dtype[0]}"""
                        for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        # distinct count
        dfDataSummary = self._addMeasureToSummary(
            'distinct_count',
            summaryExpr="count(distinct *)",
            fieldExprs=[f"string(count(distinct {dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        # min
        dfDataSummary = self._addMeasureToSummary(
            'min',
            fieldExprs=[f"string(min({dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'max',
            fieldExprs=[f"string(max({dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        descriptionDf = self._df.describe().where("summary in ('mean', 'stddev')")
        describeData = descriptionDf.collect()

        for row in describeData:
            measure = row['summary']

            values = {k[0]: '' for k in dtypes}

            row_key_pairs = row.asDict()
            for k1 in row_key_pairs:
                values[k1] = str(row[k1])

            dfDataSummary = self._addMeasureToSummary(
                measure,
                fieldExprs=[f"'{values[dtype[0]]}'" for dtype in dtypes],
                dfData=self._df,
                dfSummary=dfDataSummary)

        # string characteristics for strings and string representation of other values
        dfDataSummary = self._addMeasureToSummary(
            'print_len_min',
            fieldExprs=[f"min(length(string({dtype[0]}))) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'print_len_max',
            fieldExprs=[f"max(length(string({dtype[0]}))) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=dfDataSummary)

        return dfDataSummary

    def summarize(self, suppressOutput=False):
        """ Generate summary analysis of data set and return / print summary results

        :param suppressOutput:  If False, prints results to console also
        :return: Summary results as string
        """
        dfSummary = self.summarizeToDF()

        results = [
            "Data set summary",
            "================"
        ]

        for r in dfSummary.collect():
            results.append(self._displayRow(r))

        summary = "\n".join([str(x) for x in results])

        if not suppressOutput:
            print(summary)

        return summary

    @classmethod
    def _valueFromSummary(cls, dataSummary, colName, measure, defaultValue):
        """ Get value from data summary

        :param dataSummary: Data summary to search, optional
        :param colName: Column name of column to get value for
        :param measure: Measure name of measure to get value for
        :param defaultValue: Default value if any other argument is not specified or value could not be found in
                             data summary
        :return: Value from lookup or `defaultValue` if not found
        """
        if dataSummary is not None and colName is not None and measure is not None:
            if measure in dataSummary:
                measureValues = dataSummary[measure]

                if colName in measureValues:
                    return measureValues[colName]

        # return default value if value could not be looked up or found
        return defaultValue

    @classmethod
    def _generatorDefaultAttributesFromType(cls, sqlType, colName=None, dataSummary=None, sourceDf=None):
        """ Generate default set of attributes for each data type

        :param sqlType: Instance of `pyspark.sql.types.DataType`
        :param colName: Name of column being generated
        :param dataSummary: Map of maps of attributes from data summary, optional
        :param sourceDf: Source dataframe to retrieve attributes of real data, optional
        :return: Attribute string for supplied sqlType

        When generating code from a schema, we have no data heuristics to determine how data should be generated,
        so goal is to just generate code that produces some data.

        Users are expected to modify the generated code to their needs.
        """
        assert isinstance(sqlType, DataType)

        if sqlType == StringType():
            result = """template=r'\\\\w'"""
        elif sqlType in [IntegerType(), LongType()]:
            minValue = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=1000000)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType == ByteType():
            minValue = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=127)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType == ShortType():
            minValue = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=32767)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType == BooleanType():
            result = """expr='id % 2 = 1'"""
        elif sqlType == DateType():
            result = """expr='current_date()'"""
        elif isinstance(sqlType, DecimalType):
            minValue = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=1000)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType in [FloatType(), DoubleType()]:
            minValue = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0.0)
            maxValue = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=1000000.0)
            result = f"""minValue={minValue}, maxValue={maxValue}, step=0.1"""
        elif sqlType == TimestampType():
            result = """begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00", interval="1 minute" """
        elif sqlType == BinaryType():
            result = """expr="cast('dbldatagen generated synthetic data' as binary)" """
        else:
            result = """expr='null'"""

        percentNullsValue = float(cls._valueFromSummary(dataSummary, colName, "null_probability", defaultValue=0.0))

        if percentNullsValue > 0.0:
            result = result + f", percentNulls={percentNullsValue}"

        return result

    @classmethod
    def _scriptDataGeneratorCode(cls, schema, dataSummary=None, sourceDf=None, suppressOutput=False, name=None):
        """
        Generate outline data generator code from an existing dataframe

        This will generate a data generator spec from an existing dataframe. The resulting code
        can be used to generate a data generation specification.

        Note at this point in time, the code generated is stub code only.
        For most uses, it will require further modification - however it provides a starting point
        for generation of the specification for a given data set.

        The dataframe to be analyzed is the dataframe passed to the constructor of the DataAnalyzer object.

        :param schema: Pyspark schema - i.e manually constructed StructType or return value from `dataframe.schema`
        :param dataSummary: Map of maps of attributes from data summary, optional
        :param sourceDf: Source dataframe to retrieve attributes of real data, optional
        :param suppressOutput: Suppress printing of generated code if True
        :param name: Optional name for data generator
        :return: String containing skeleton code

        """
        assert isinstance(schema, StructType), "expecting valid Pyspark Schema"

        stmts = []

        if name is None:
            name = cls._DEFAULT_GENERATED_NAME

        stmts.append(cls._GENERATED_COMMENT)

        stmts.append("import dbldatagen as dg")
        stmts.append("import pyspark.sql.types")

        stmts.append(cls._GENERATED_FROM_SCHEMA_COMMENT)

        stmts.append(strip_margins(
            f"""generation_spec = (
                                    |    dg.DataGenerator(sparkSession=spark, 
                                    |                     name='{name}', 
                                    |                     rows=100000,
                                    |                     random=True,
                                    |                     )""",
            '|'))

        indent = "    "
        for fld in schema.fields:
            col_name = fld.name
            col_type = fld.dataType.simpleString()

            if isinstance(fld.dataType, ArrayType):
                col_type = fld.dataType.elementType.simpleString()
                field_attributes = cls._generatorDefaultAttributesFromType(fld.dataType.elementType)  # no data look up
                array_attributes = """structType='array', numFeatures=(2,6)"""
                name_and_type = f"""'{col_name}', '{col_type}'"""
                stmts.append(indent + f""".withColumn({name_and_type}, {field_attributes}, {array_attributes})""")
            else:
                field_attributes = cls._generatorDefaultAttributesFromType(fld.dataType,
                                                                           colName=col_name,
                                                                           dataSummary=dataSummary,
                                                                           sourceDf=sourceDf)
                stmts.append(indent + f""".withColumn('{col_name}', '{col_type}', {field_attributes})""")
        stmts.append(indent + ")")

        if not suppressOutput:
            for line in stmts:
                print(line)

        return "\n".join(stmts)

    @classmethod
    def scriptDataGeneratorFromSchema(cls, schema, suppressOutput=False, name=None):
        """
        Generate outline data generator code from an existing dataframe

        This will generate a data generator spec from an existing dataframe. The resulting code
        can be used to generate a data generation specification.

        Note at this point in time, the code generated is stub code only.
        For most uses, it will require further modification - however it provides a starting point
        for generation of the specification for a given data set.

        The dataframe to be analyzed is the dataframe passed to the constructor of the DataAnalyzer object.

        :param schema: Pyspark schema - i.e manually constructed StructType or return value from `dataframe.schema`
        :param suppressOutput: Suppress printing of generated code if True
        :param name: Optional name for data generator
        :return: String containing skeleton code

        """
        return cls._scriptDataGeneratorCode(schema,
                                            suppressOutput=suppressOutput,
                                            name=name)

    def scriptDataGeneratorFromData(self, suppressOutput=False, name=None):
        """
        Generate outline data generator code from an existing dataframe

        This will generate a data generator spec from an existing dataframe. The resulting code
        can be used to generate a data generation specification.

        Note at this point in time, the code generated is stub code only.
        For most uses, it will require further modification - however it provides a starting point
        for generation of the specification for a given data set

        The dataframe to be analyzed is the Spark dataframe passed to the constructor of the DataAnalyzer object

        :param suppressOutput: Suppress printing of generated code if True
        :param name: Optional name for data generator
        :return: String containing skeleton code

        """
        assert self._df is not None
        assert type(self._df) is ssql.DataFrame, "sourceDf must be a valid Pyspark dataframe"

        if self._dataSummary is None:
            df_summary = self.summarizeToDF()

            self._dataSummary = {}
            for row in df_summary.collect():
                row_key_pairs = row.asDict()
                self._dataSummary[row['measure_']] = row_key_pairs

        return self._scriptDataGeneratorCode(self._df.schema,
                                             suppressOutput=suppressOutput,
                                             name=name,
                                             dataSummary=self._dataSummary,
                                             sourceDf=self._df)
