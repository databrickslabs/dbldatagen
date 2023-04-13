# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``DataAnalyzer`` class.

This code is experimental and both APIs and code generated is liable to change in future versions.
"""
import logging
from collections import namedtuple
import pprint

import numpy as np

from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, StructType, ArrayType, DataType, MapType

from pyspark import sql
import pyspark.sql.functions as F

from .utils import strip_margins, json_value_from_path
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

    _INT_32_MAX = 2 ** 16 - 1

    _MAX_COLUMN_ELEMENT_LENGTH_THRESHOLD = 40
    _MAX_DISTINCT_THRESHOLD = 20

    _MAX_VALUES_LINE_LENGTH = 60
    _CODE_GENERATION_INDENT = 4
    _MEASURE_ROUNDING = 4

    # tuple for column infor
    ColInfo = namedtuple("ColInfo", ["name", "dt", "isArrayColumn", "isNumeric"])

    # tuple for values info
    ColumnValuesInfo = namedtuple("ColumnValuesInfo", ["name", "statements", "value_refs"])

    def __init__(self, df=None, sparkSession=None, valuesCountThreshold=None):
        """ Constructor:
        :param df: Dataframe to analyze
        :param sparkSession: Spark session to use
        :param valuesCountThreshold: Values will only be computed if less than threshold, If not supplied
               will use default setting (20)
        """
        assert df is not None, "dataframe must be supplied"

        self._df = df.cache()

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self._sparkSession = sparkSession
        self._dataSummary = None
        self._columnsInfo = None
        self._expandedSourceDf = None

        self._valuesCountThreshold = (valuesCountThreshold if valuesCountThreshold is not None
                                      else self._MAX_DISTINCT_THRESHOLD)

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

    @staticmethod
    def _is_numeric_type(dtype):
        """ return true if dtype is numeric, false otherwise"""
        if dtype.lower() in ['smallint', 'tinyint', 'double', 'float', 'bigint', 'int']:
            return True
        elif dtype.lower().startswith("decimal"):
            return True

        return False

    @property
    def columnsInfo(self):
        """ Get extended columns info"""
        if self._columnsInfo is None:
            df_dtypes = self._df.dtypes

            # compile column information [ (name, datatype, isArrayColumn, isNumeric) ]
            columnsInfo = [self.ColInfo(dtype[0],
                                        dtype[1],
                                        1 if dtype[1].lower().startswith('array') else 0,
                                        1 if self._is_numeric_type(dtype[1].lower()) else 0)
                           for dtype in df_dtypes]

            self._columnsInfo = columnsInfo
        return self._columnsInfo

    def summarizeToDF(self):
        """ Generate summary analysis of data set as dataframe

        :return: Summary results as dataframe

        The resulting dataframe can be displayed with the ``display`` function in a notebook environment
        or with the ``show`` method.

        The output is also used in code generation  to generate more accurate code.
        """
        df_under_analysis = self._df

        logger = logging.getLogger(__name__)
        logger.info("Analyzing counts")
        total_count = df_under_analysis.count() * 1.0

        logger.info("Analyzing measures")

        # schema information
        dfDataSummary = self._addMeasureToSummary(
            'schema',
            summaryExpr=f"""to_json(named_struct('column_count', {len(self.columnsInfo)}))""",
            fieldExprs=[f"'{colInfo.dt}' as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=df_under_analysis)

        # count
        dfDataSummary = self._addMeasureToSummary(
            'count',
            summaryExpr=f"{total_count}",
            fieldExprs=[f"string(count({colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'null_probability',
            fieldExprs=[
                f"""string(round(({total_count} - count({colInfo.name})) /{total_count}, 2)) as {colInfo.name}"""
                for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        # distinct count
        dfDataSummary = self._addMeasureToSummary(
            'distinct_count',
            summaryExpr="count(distinct *)",
            fieldExprs=[f"string(count(distinct {colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'item_distinct_count',
            summaryExpr="count(distinct *)",
            fieldExprs=[f"string(count(distinct {colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self._getExpandedSourceDf(),
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'item_count',
            summaryExpr="count(*)",
            fieldExprs=[f"string(count({colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self._getExpandedSourceDf(),
            dfSummary=dfDataSummary)

        # string characteristics for strings and string representation of other values
        dfDataSummary = self._addMeasureToSummary(
            'item_max_printlen',
            fieldExprs=[f"max(length(string({colInfo.name}))) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self._getExpandedSourceDf(),
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'print_len',
            fieldExprs=[f"""to_json(named_struct(
                        'min', min(length(string({colInfo.name}))), 
                        'max', max(length(string({colInfo.name})))))                        
                        as {colInfo.name}"""
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        url_regex = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#()?&//=]*)"

        # string metrics
        dfDataSummary = self._addMeasureToSummary(
            'string_patterns',
            fieldExprs=[f"""to_json(named_struct(
                            'ip_addr', round(count_if({colInfo.name} regexp "^[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}$") 
                                       / count({colInfo.name}), 4), 
                            'alpha_lower', round(count_if({colInfo.name} regexp "^[a-z]+$") 
                                       / count({colInfo.name}), 4), 
                            'alpha_upper', round(count_if({colInfo.name} regexp "^[A-Z]+$") 
                                       / count({colInfo.name}), 4), 
                            'alpha', round(count_if({colInfo.name} regexp "^[A-Za-z]+$") 
                                       / count({colInfo.name}), 4), 
                            'digits', round(count_if({colInfo.name} regexp "^[0-9]+$") 
                                       / count({colInfo.name}), 4),
                            'alphanumeric', round(count_if({colInfo.name} regexp "^[a-zA-Z0-9]+$") 
                                       / count({colInfo.name}), 4), 
                            'url', round(count_if({colInfo.name} regexp "^{url_regex}$") 
                                       / count({colInfo.name}), 4) 
                                       )) 
                            as {colInfo.name}"""
                        if colInfo.dt == "string" else "''"
                        for colInfo in self.columnsInfo],
            dfData=self._getExpandedSourceDf(),
            dfSummary=dfDataSummary)


        # min
        dfDataSummary = self._addMeasureToSummary(
            'min',
            fieldExprs=[f"string(min({colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'max',
            fieldExprs=[f"string(max({colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'cardinality',
            fieldExprs=[f"""to_json(named_struct(
                                'min', min(cardinality({colInfo.name})), 
                                'max', max(cardinality({colInfo.name})))) 
                            as {colInfo.name}"""
                        if colInfo.isArrayColumn else "min(1)"
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'array_value_min',
            fieldExprs=[f"min(array_min({colInfo.name})) as {colInfo.name}" if colInfo.isArrayColumn else "min('')"
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'array_value_max',
            fieldExprs=[f"max(array_max({colInfo.name})) as {colInfo.name}"
                        if colInfo.isArrayColumn else "max('')"
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        rounding = self._MEASURE_ROUNDING

        dfDataSummary = self._addMeasureToSummary(
            'stats',
            fieldExprs=[f"""to_json(named_struct('skewness', round(skewness({colInfo.name}),{rounding}), 
                                                 'kurtosis', round(kurtosis({colInfo.name}),{rounding}),
                                                 'mean', round(mean({colInfo.name}),{rounding}),
                                                 'stddev', round(stddev_pop({colInfo.name}),{rounding})
                                                  )) as {colInfo.name}"""
                        if colInfo.isNumeric else "null"
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
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
    def _valueFromSummary(cls, dataSummary, colName, measure, defaultValue, jsonPath=None):
        """ Get value from data summary

        :param dataSummary: Data summary to search, optional
        :param colName: Column name of column to get value for
        :param measure: Measure name of measure to get value for
        :param defaultValue: Default value if any other argument is not specified or value could not be found in
                             data summary
        :param jsonPath: if jsonPath is supplied, treat initial result as JSON data and perform lookup according to
                         the supplied json path.
        :return: Value from lookup or `defaultValue` if not found
        """
        if dataSummary is not None and colName is not None and measure is not None:
            if measure in dataSummary:
                measureValues = dataSummary[measure]

                if colName in measureValues:
                    result = measureValues[colName]

                    if jsonPath is not None:
                        result = json_value_from_path(jsonPath, result, defaultValue)

                    if result is not None:
                        return result

        # return default value if value could not be looked up or found
        return defaultValue

    @classmethod
    def _generatorDefaultAttributesFromType(cls, sqlType, colName=None, isArrayElement=False, dataSummary=None,
                                            sourceDf=None, valuesInfo=None):
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

        min_attribute = "min" if not isArrayElement else "array_value_min"
        max_attribute = "max" if not isArrayElement else "array_value_max"

        if valuesInfo is not None and \
                colName in valuesInfo and not isinstance(sqlType, (BinaryType, StructType, MapType)):
            result = valuesInfo[colName].value_refs
            assert result is not None and len(result) > 0
        elif sqlType == StringType():
            result = """template=r'\\\\w'"""
        elif sqlType in [IntegerType(), LongType()]:
            minValue = cls._valueFromSummary(dataSummary, colName, min_attribute, defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, max_attribute, defaultValue=1000000)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType == ByteType():
            minValue = cls._valueFromSummary(dataSummary, colName, min_attribute, defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, max_attribute, defaultValue=127)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType == ShortType():
            minValue = cls._valueFromSummary(dataSummary, colName, min_attribute, defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, max_attribute, defaultValue=32767)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType == BooleanType():
            result = """expr='id % 2 = 1'"""
        elif sqlType == DateType():
            result = """expr='current_date()'"""
        elif isinstance(sqlType, DecimalType):
            minValue = cls._valueFromSummary(dataSummary, colName, min_attribute, defaultValue=0)
            maxValue = cls._valueFromSummary(dataSummary, colName, max_attribute, defaultValue=1000)
            result = f"""minValue={minValue}, maxValue={maxValue}"""
        elif sqlType in [FloatType(), DoubleType()]:
            minValue = cls._valueFromSummary(dataSummary, colName, min_attribute, defaultValue=0.0)
            maxValue = cls._valueFromSummary(dataSummary, colName, max_attribute, defaultValue=1000000.0)
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

    def _getExpandedSourceDf(self):
        """ Get dataframe with array values expanded"""

        if self._expandedSourceDf is None:
            df_expandedSummary = self._df

            # expand source dataframe array columns
            columns = df_expandedSummary.columns

            for column in self.columnsInfo:
                if column.isArrayColumn:
                    df_expandedSummary = df_expandedSummary.withColumn(column.name, F.explode_outer(F.col(column.name)))

            df_expandedSummary = df_expandedSummary.select(*columns)
            self._expandedSourceDf = df_expandedSummary

        return self._expandedSourceDf

    def _cleanse_name(self, col_name):
        """cleanse column name for use in code"""
        return col_name.replace(' ', '_')

    def _format_values_list(self, values):
        """ Format values """
        pp = pprint.PrettyPrinter(indent=self._CODE_GENERATION_INDENT,
                                  width=self._MAX_VALUES_LINE_LENGTH,
                                  compact=True)
        values = pp.pformat(values)

        return values

    def _processCategoricalValuesInfo(self, dataSummary=None, sourceDf=None):
        """ Computes values clauses for appropriate columns

        :param dataSummary: Data summary
        :param sourceDf: Source data dataframe
        :return: Map from column name to ColumnValueInfo tuples
                 where ColumnValuesInfo = namedtuple("ColumnValuesInfo", ["name", "statements", "weights", "values"])
        """
        assert dataSummary is not None
        assert sourceDf is not None

        results = {}

        logger = logging.getLogger(__name__)
        logger.info("Performing categorical data analysis")

        for fld in sourceDf.schema.fields:
            col_name = fld.name
            col_base_type = fld.dataType.elementType if isinstance(fld.dataType, ArrayType) else fld.dataType
            col_type = col_base_type.simpleString()

            stmts = []
            value_refs = []

            # we'll compute values set for elements whose max printable length < MAX_COLUMN_ELEMENT_LENGTH_THRESHOLD
            # whose count of distinct elements is < MAX_DISTINCT_THRESHOLD
            # and where the type is numeric or string either by itself or in array variant
            numDistinct = int(self._valueFromSummary(dataSummary, col_name, "item_distinct_count",
                                                     defaultValue=self._INT_32_MAX))
            maxPrintable = int(self._valueFromSummary(dataSummary, col_name, "item_max_printlen",
                                                      defaultValue=self._INT_32_MAX))

            if self._valuesCountThreshold > numDistinct > 1 and \
                    maxPrintable < self._MAX_COLUMN_ELEMENT_LENGTH_THRESHOLD and \
                    col_type in ["float", "double", "int", "smallint", "bigint", "tinyint", "string"]:
                logger.info(f"Retrieving categorical values for column `{col_name}`")

                value_rows = sorted(sourceDf.select(col_name).groupBy(col_name).count().collect(),
                                    key=lambda r1, sk=col_name: r1[sk])
                values = [r[col_name] for r in value_rows]
                weights = [r['count'] for r in value_rows]

                # simplify the weights
                countNonNull = int(self._valueFromSummary(dataSummary, col_name, "item_count",
                                                          defaultValue=sum(weights)))

                weights = ((np.array(weights) / countNonNull) * 100.0).round().astype(np.uint64)
                weights = np.maximum(weights, 1)  # minumum weight must be 1

                # divide by GCD to get simplified weights
                gcd = np.gcd.reduce(weights)

                weights = (weights / gcd).astype(np.uint64)

                # if all of the weights are within 10% of mean, ignore the weights
                avg_weight = np.mean(weights)
                weight_threshold = avg_weight * 0.1
                weight_test = np.abs(weights - avg_weight)
                if np.all(weight_test < weight_threshold):
                    weights = None
                else:
                    weights = list(weights)

                safe_name = self._cleanse_name(col_name)

                if weights is not None:
                    stmts.append(f"{safe_name}_weights = {self._format_values_list(weights)}")
                    value_refs.append(f"""weights = {safe_name}_weights""")

                stmts.append(f"{safe_name}_values = {self._format_values_list(values)}")
                value_refs.append(f"""values={safe_name}_values""")

                results[col_name] = self.ColumnValuesInfo(col_name, stmts, ", ".join(value_refs))

        return results

    @classmethod
    def _scriptDataGeneratorCode(cls, schema, dataSummary=None, sourceDf=None, suppressOutput=False, name=None,
                                 valuesInfo=None):
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
        :param valuesInfo: References and statements for `values` clauses
        :return: String containing skeleton code

        """
        assert isinstance(schema, StructType), "expecting valid Pyspark Schema"

        stmts = []

        if name is None:
            name = cls._DEFAULT_GENERATED_NAME

        stmts.append(cls._GENERATED_COMMENT)

        stmts.append("import dbldatagen as dg")

        stmts.append(cls._GENERATED_FROM_SCHEMA_COMMENT)

        if valuesInfo is not None:
            for k, v in valuesInfo.items():
                stmts.append("")
                stmts.append(f"# values for column `{k}`")
                for line in v.statements:
                    stmts.append(line)

        stmts.append("")
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
                field_attributes = cls._generatorDefaultAttributesFromType(fld.dataType.elementType,
                                                                           colName=col_name,
                                                                           isArrayElement=True,
                                                                           dataSummary=dataSummary,
                                                                           sourceDf=sourceDf,
                                                                           valuesInfo=valuesInfo)

                if dataSummary is not None:
                    minLength = cls._valueFromSummary(dataSummary, col_name, "cardinality", jsonPath="mint",
                                                      defaultValue=2)
                    maxLength = cls._valueFromSummary(dataSummary, col_name, "cardinality", jsonPath="max",
                                                      defaultValue=6)

                    if minLength != maxLength:
                        array_attributes = f"""structType='array', numFeatures=({minLength}, {maxLength})"""
                    else:
                        array_attributes = f"""structType='array', numFeatures={minLength}"""

                else:
                    array_attributes = """structType='array', numFeatures=(2,6)"""
                name_and_type = f"""'{col_name}', '{col_type}'"""
                stmts.append(indent + f""".withColumn({name_and_type}, {field_attributes}, {array_attributes})""")
            else:
                field_attributes = cls._generatorDefaultAttributesFromType(fld.dataType,
                                                                           colName=col_name,
                                                                           dataSummary=dataSummary,
                                                                           sourceDf=sourceDf,
                                                                           valuesInfo=valuesInfo)
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
        assert type(self._df) is sql.DataFrame, "sourceDf must be a valid Pyspark dataframe"

        if self._dataSummary is None:
            logger = logging.getLogger(__name__)
            logger.info("Performing data analysis in preparation for code generation")

            df_summary = self.summarizeToDF()

            self._dataSummary = {}
            logger.info("Performing summary analysis ...")

            analysis_measures = df_summary.collect()

            logger.info("Processing summary analysis results")

            for row in analysis_measures:
                row_key_pairs = row.asDict()
                self._dataSummary[row['measure_']] = row_key_pairs

            values_info = self._processCategoricalValuesInfo(dataSummary=self._dataSummary,
                                                             sourceDf=self._getExpandedSourceDf())

        return self._scriptDataGeneratorCode(self._df.schema,
                                             suppressOutput=suppressOutput,
                                             name=name,
                                             dataSummary=self._dataSummary,
                                             sourceDf=self._df,
                                             valuesInfo=values_info)
