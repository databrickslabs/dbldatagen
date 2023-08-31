# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``DataAnalyzer`` class.

    .. warning::
       Experimental

       This code is experimental and both APIs and code generated is liable to change in future versions.

"""
import logging
import pprint
from collections import namedtuple

import numpy as np
import pyspark.sql.functions as F
from pyspark import sql
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, StructType, ArrayType, DataType, MapType

from .html_utils import HtmlUtils
from .spark_singleton import SparkSingleton
from .utils import strip_margins, json_value_from_path


class DataAnalyzer:
    """This class is used to analyze an existing data set to assist in generating a test data set with similar
    characteristics, and to generate code from existing schemas and data

    :param df: Spark dataframe to analyze
    :param sparkSession: Spark session instance to use when performing spark operations
    :param maxRows: if specified, determines max number of rows to analyze when `analysisLevel` is "sample"
    :param analysisLevel: Determines level of analysis to perform. Options are ["minimal", "sample", "full"].
                              Default is "sample

    You may increase the categorical values threshold to a higher value using the Spark config option
    `dbldatagen.analyzer.categoricalValuesThreshold` in which case, columns with higher numbers of distinct values
    will be evaluated to see if they can be represented as a values list.

    However the sampling may fail if this level is set too high.
    Experimentally, this should be kept below 100 at present.
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
    _CATEGORICAL_VALUE_DEFAULT_THRESHOLD = 50

    _MAX_VALUES_LINE_LENGTH = 60
    _CODE_GENERATION_INDENT = 4
    _MEASURE_ROUNDING = 4

    # tuple for column infor
    ColInfo = namedtuple("ColInfo", ["name", "dt", "isArrayColumn", "isNumeric"])

    # tuple for values info
    ColumnValuesInfo = namedtuple("ColumnValuesInfo", ["name", "statements", "value_refs"])

    # options
    _ANALYSIS_LEVELS = ["minimal", "sample", "analyze_text", "full"]
    _CATEGORICAL_VALUES_THRESHOLD_OPTION = "dbldatagen.analyzer.categoricalValuesThreshold"
    _SAMPLE_ROWS_THRESHOLD_OPTION = "dbldatagen.analyzer.sampleRowsThreshold"
    _CACHE_SOURCE_OPTION = "dbldatagen.analyzer.cacheSource"
    _CACHE_SAMPLE_OPTION = "dbldatagen.analyzer.cacheSample"

    _DEFAULT_SAMPLE_ROWS_THRESHOLD = 10000

    def __init__(self, df=None, sparkSession=None, maxRows=None, analysisLevel="sample"):
        """ Constructor:
        :param df: Dataframe to analyze
        :param sparkSession: Spark session to use
        :param maxRows: if specified, determines max number of rows to analyze.
        :param analysisLevel: Determines level of analysis to perform. Options are ["minimal", "sample", "full"].
                              Default is "sample



        You may increase the categorical values threshold to a higher value, in which case, columns with higher values
        of distinct values will be evaluated to see if they can be represented as a values list.

        However the current implementation will flag an error if the number of categorical causes SQL array sizes to be
        too large. Experimentally, this should be kept below 100 at present.
        """
        assert df is not None, "dataframe must be supplied"

        self._df = df

        assert analysisLevel in self._ANALYSIS_LEVELS, f"analysisLevel must be one of {self._ANALYSIS_LEVELS}"
        assert maxRows is None or maxRows > 0, "maxRows must be greater than 0, if supplied"

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self._sparkSession = sparkSession
        self._dataSummary = None
        self._columnsInfo = None
        self._expandedSampleDf = None

        self._valuesCountThreshold = int(self._sparkSession.conf.get(self._CATEGORICAL_VALUES_THRESHOLD_OPTION,
                                                                     str(self._CATEGORICAL_VALUE_DEFAULT_THRESHOLD)))

        # max rows is supplied parameter or default
        self._maxRows = maxRows or self._DEFAULT_SAMPLE_ROWS_THRESHOLD
        self._df_sampled_data = None
        self._analysisLevel = analysisLevel
        self._cacheSource = self._sparkSession.conf.get(self._CACHE_SOURCE_OPTION, "false").lower() == "true"
        self._cacheSample = self._sparkSession.conf.get(self._CACHE_SAMPLE_OPTION, "true").lower() == "true"

    @classmethod
    def sampleData(cls, df: DataFrame, maxRows: int):
        """
        Sample data from a dataframe specifying the max rows to sample

        :param df: The dataframe to sample
        :param maxRows: The maximum number of rows to samples
        :return: The dataframe with the sampled data
        """
        assert df is not None, "dataframe must be supplied"
        assert maxRows is not None and isinstance(maxRows, int) and maxRows > 0, "maxRows must be a non-zero integer"

        # use count with limit of maxRows + 1 to determine if the dataframe is larger than maxRows
        if df.limit(maxRows + 1).count() <= maxRows:
            return df

        # if the dataframe is larger than maxRows, then sample it
        # and constrain the output to the limit of maxRows
        return df.sample(maxRows / df.count(), seed=42).limit(maxRows)

    @property
    def sourceDf(self):
        """ Get source dataframe"""
        return self._df

    @property
    def sampledSourceDf(self):
        """ Get source dataframe (capped with maxRows if necessary)"""
        if self._df_sampled_data is None:
            # by default, use the full source
            if self._analysisLevel == "full":
                self._df_sampled_data = self._df
            else:
                self._df_sampled_data = self.sampleData(self._df, self._maxRows)

            if self._cacheSample:
                self._df_sampled_data = self._df_sampled_data.cache()

        return self._df_sampled_data

    @property
    def expandedSampleDf(self):
        """ Get dataframe with array values expanded"""

        if self._expandedSampleDf is None:
            df_expandedSample = self.sampledSourceDf

            # expand source dataframe array columns
            columns = df_expandedSample.columns

            for column in self.columnsInfo:
                if column.isArrayColumn:
                    df_expandedSample = df_expandedSample.withColumn(column.name, F.explode_outer(F.col(column.name)))

            df_expandedSample = df_expandedSample.select(*columns)

            if self._cacheSample:
                df_expandedSample = df_expandedSample.cache()

            self._expandedSampleDf = df_expandedSample

        return self._expandedSampleDf

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
        :param summaryExpr: Summary expression - string or sql.Column
        :param fieldExprs: list of field expressions (or generator) - either string or sql.Column instances
        :param dfData: Source data df - data being summarized
        :param rowLimit: Number of rows to get for measure - usually 1
        :param dfSummary: Summary df
        :return: dfSummary with new measure added
        """
        assert dfData is not None, "source data dataframe must be supplied"
        assert measureName is not None and len(measureName) > 0, "invalid measure name"

        # add measure name and measure summary
        exprs = [F.lit(measureName).astype(StringType()).alias("measure_")]

        if isinstance(summaryExpr, str):
            exprs.append(F.expr(summaryExpr).astype(StringType()).alias("summary_"))
        else:
            assert isinstance(summaryExpr, sql.Column), "summaryExpr must be string or sql.Column"
            exprs.append(summaryExpr.astype(StringType()).alias("summary_"))

        # add measures for fields
        for fieldExpr in fieldExprs:
            if isinstance(fieldExpr, str):
                exprs.append(F.expr(fieldExpr).astype(StringType()))
            else:
                assert isinstance(fieldExpr, sql.Column), "fieldExpr must be string or sql.Column"
                exprs.append(fieldExpr)

        dfMeasure = dfData.select(*exprs).limit(rowLimit) if rowLimit is not None else dfData.select(*exprs)

        return dfSummary.union(dfMeasure) if dfSummary is not None else dfMeasure

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
        """ Get extended columns info.

          :return: List of column info tuples (named tuple - ColumnValuesInfo)

        """
        if self._columnsInfo is None:
            df_dtypes = self.sampledSourceDf.dtypes

            # compile column information [ (name, datatype, isArrayColumn, isNumeric) ]
            columnsInfo = [self.ColInfo(dtype[0],
                                        dtype[1],
                                        1 if dtype[1].lower().startswith('array') else 0,
                                        1 if self._is_numeric_type(dtype[1].lower()) else 0)
                           for dtype in df_dtypes]

            self._columnsInfo = columnsInfo
        return self._columnsInfo

    _URL_PREFIX = r"https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}"
    _IMAGE_EXTS = r"(png)|(jpg)|(tif)"

    _regex_patterns = {
        "alpha_upper": r"[A-Z]+",
        "alpha_lower": r"[a-z]+",
        "digits": r"[0-9]+",
        "alphanumeric": r"[a-zA-Z0-9]+",
        "identifier": r"[a-zA-Z0-9_]+",
        "image_url": _URL_PREFIX + r"\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)" + _IMAGE_EXTS,
        "url": _URL_PREFIX + r"\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)",
        "email_common": r"([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6})*",
        "email_uncommon": r"([a-z0-9_\.\+-]+)@([\da-z\.-]+)\.([a-z\.]{2,6})",
        "ip_addr": r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}",
        "free_text": r"(\s*[a-zA-Z0-9]+\s*[\?\.;,]*)*"
    }

    def _compute_pattern_match_clauses(self):
        """Generate string pattern matching expressions to compute probability of matching particular patterns
        """
        stmts = []

        # for each column
        for colInfo in self.columnsInfo:
            clauses = []
            if colInfo.dt == "string":

                # compute named struct of measures matching specific regular expressions
                for k, v in self._regex_patterns.items():
                    clauses.append(F.round(F.expr(f"""count_if(`{colInfo.name}` regexp '^{v}$')"""), 4)
                                   .astype(StringType()).alias(k))

                stmt = F.to_json(F.struct(*clauses)).alias(colInfo.name)
                stmts.append(stmt)
            else:
                stmts.append(F.first(F.lit('')).alias(colInfo.name))
        result = stmts
        return result

    @staticmethod
    def _left4k(name):
        """Return left 4k characters of string"""
        return f"left(string({name}), 4096)"

    _WORD_REGEX = r"\\b\\w+\\b"
    _SPACE_REGEX = r"\\s+"
    _DIGIT_REGEX = r"\\d"
    _PUNCTUATION_REGEX = r"[\\?\\.\\;\\,\\!\\{\\}\\[\\]\\(\\)\\>\\<]"
    _AT_REGEX = r"\\@"
    _PERIOD_REGEX = r"\\."
    _HTTP_REGEX = r"^http[s]?\\:\\/\\/"
    _ALPHA_REGEX = r"[a-zA-Z]"
    _ALPHA_UPPER_REGEX = r"[A-Z]"
    _ALPHA_LOWER_REGEX = r"[a-z]"
    _HEX_REGEX = r"[0-9a-fA-F]"

    _MINMAXAVG = "minmaxavg"
    _BOOLEAN = "boolean"

    _textFeatures = {
        'print_len': ("length(string($name$))", _MINMAXAVG),
        'word_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_WORD_REGEX}', 0))", _MINMAXAVG),
        'space_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_SPACE_REGEX}', 0))", _MINMAXAVG),
        'digit_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_DIGIT_REGEX}', 0))", _MINMAXAVG),
        'punctuation_count': (
            f"size(regexp_extract_all(left(string($name$), 4096), '{_PUNCTUATION_REGEX}', 0))", _MINMAXAVG),
        'at_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_AT_REGEX}', 0))", _MINMAXAVG),
        'period_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_PERIOD_REGEX}', 0))", _MINMAXAVG),
        'http_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_HTTP_REGEX}', 0))", _MINMAXAVG),
        'alpha_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_ALPHA_REGEX}', 0))", _MINMAXAVG),
        'alpha_lower_count': (
            f"size(regexp_extract_all(left(string($name$), 4096), '{_ALPHA_LOWER_REGEX}', 0))", _MINMAXAVG),
        'alpha_upper_count': (
            f"size(regexp_extract_all(left(string($name$), 4096), '{_ALPHA_UPPER_REGEX}', 0))", _MINMAXAVG),
        'hex_digit_count': (f"size(regexp_extract_all(left(string($name$), 4096), '{_HEX_REGEX}', 0))", _MINMAXAVG),
    }

    def generateTextFeatures(self, sourceDf):
        """ Generate text features from source dataframe

        Generates set of text features for each column (analyzing string representation of each column value)

        :param sourceDf: Source datafame
        :return: Dataframe of text features
        """
        # generate named struct of text features for each column

        # we need to double escape backslashes in regular expressions as they will be lost in string expansion

        # for each column, extract text features from string representation of column value (leftmost 4096 characters)

        fieldTextFeatures = []

        # add regular text features
        for colInfo in self.columnsInfo:
            features_clauses = []

            for k, v in self._textFeatures.items():
                feature_expr, strategy = v
                feature_expr = feature_expr.replace("$name$", colInfo.name)  # substitute column name

                if strategy == self._MINMAXAVG:
                    features = F.array(F.min(F.expr(feature_expr)),
                                       F.max(F.expr(feature_expr)),
                                       F.avg(F.expr(feature_expr)))
                    features_clauses.append(features.alias(k))
                elif strategy == self._BOOLEAN:
                    feature = F.when(F.expr(feature_expr), F.lit(1)).otherwise(F.lit(0))
                    features_clauses.append(feature.alias(k))

            column_text_features = F.to_json(F.struct(*features_clauses)).alias(colInfo.name)

            fieldTextFeatures.append(column_text_features)

        dfTextFeatures = self._addMeasureToSummary(
            'text_features',
            fieldExprs=fieldTextFeatures,
            dfData=sourceDf,
            dfSummary=None,
            rowLimit=None)

        return dfTextFeatures

    def summarizeToDF(self):
        """ Generate summary analysis of data set as dataframe

        :return: Summary results as dataframe

        The resulting dataframe can be displayed with the ``display`` function in a notebook environment
        or with the ``show`` method.

        The output is also used in code generation  to generate more accurate code.
        """
        # if self._cacheSource:
        #    self._df.cache().createOrReplaceTempView("data_analysis_summary")

        df_under_analysis = self.sampledSourceDf

        logger = logging.getLogger(__name__)
        logger.info("Analyzing counts")
        total_count = df_under_analysis.count() * 1.0

        logger.info("Analyzing measures")

        # feature : schema information, [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'schema',
            summaryExpr=F.to_json(F.expr(f"""named_struct('column_count', {len(self.columnsInfo)})""")),
            fieldExprs=[f"'{colInfo.dt}' as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self.sourceDf)

        # count
        # feature : count, [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'count',
            summaryExpr="count(*)",
            fieldExprs=[f"string(count({colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self.sourceDf,
            dfSummary=dfDataSummary)

        # feature : probability of nulls, [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'null_probability',
            fieldExprs=[
                f"""string(round((count(*) - count({colInfo.name})) /count(*), 5)) as {colInfo.name}"""
                for colInfo in self.columnsInfo],
            dfData=self.sourceDf,
            dfSummary=dfDataSummary)

        # feature : distinct count, [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'distinct_count',
            summaryExpr="count(distinct *)",
            fieldExprs=[f"string(count(distinct {colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self.sourceDf,
            dfSummary=dfDataSummary)

        # feature : item distinct count (i.e distinct count of array items), [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'item_distinct_count',
            summaryExpr="count(distinct *)",
            fieldExprs=[f"string(count(distinct {colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self.expandedSampleDf,
            dfSummary=dfDataSummary)

        # feature : item  count (i.e count of individual array items), [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'item_count',
            summaryExpr="count(*)",
            fieldExprs=[f"string(count({colInfo.name})) as {colInfo.name}" for colInfo in self.columnsInfo],
            dfData=self.expandedSampleDf,
            dfSummary=dfDataSummary)

        # string characteristics for strings and string representation of other values
        # feature : print len max, [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'print_len',
            fieldExprs=[F.to_json(F.struct(F.expr(f"min(length(string({colInfo.name})))").alias("min"),
                                           F.expr(f"max(length(string({colInfo.name})))").alias("max"),
                                           F.expr(f"avg(length(string({colInfo.name})))").alias("avg")))
                        .alias(colInfo.name)
                        for colInfo in self.columnsInfo],
            dfData=self._df,
            dfSummary=dfDataSummary)

        # string characteristics for strings and string representation of other values
        # feature : item print len max, [minimal, sample, complete]
        dfDataSummary = self._addMeasureToSummary(
            'item_printlen',
            fieldExprs=[F.to_json(F.struct(F.expr(f"min(length(string({colInfo.name})))").alias("min"),
                                           F.expr(f"max(length(string({colInfo.name})))").alias("max"),
                                           F.expr(f"avg(length(string({colInfo.name})))").alias("avg")))
                        .alias(colInfo.name)
                        for colInfo in self.columnsInfo],
            dfData=self.expandedSampleDf,
            dfSummary=dfDataSummary)

        # feature : item print len max, [minimal, sample, complete]
        metrics_clause = self._compute_pattern_match_clauses()

        # string metrics
        # we'll compute probabilities that string values match specific patterns - this can be subsequently
        # used to tailor code generation
        dfDataSummary = self._addMeasureToSummary(
            'string_patterns',
            fieldExprs=metrics_clause,
            dfData=self.expandedSampleDf,
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
            fieldExprs=[f"min(array_min({colInfo.name})) as {colInfo.name}"
                        if colInfo.isArrayColumn else f"first('') as {colInfo.name}"
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        dfDataSummary = self._addMeasureToSummary(
            'array_value_max',
            fieldExprs=[f"max(array_max({colInfo.name})) as {colInfo.name}"
                        if colInfo.isArrayColumn else f"first('') as {colInfo.name}"
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        rounding = self._MEASURE_ROUNDING

        dfDataSummary = self._addMeasureToSummary(
            'stats',
            fieldExprs=[F.to_json(F.struct(
                F.expr(f"round(skewness({colInfo.name}),{rounding})").alias('skewness'),
                F.expr(f"round(kurtosis({colInfo.name}),{rounding})").alias('kurtosis'),
                F.expr(f"round(mean({colInfo.name}),{rounding})").alias('mean'),
                F.expr(f"round(stddev_pop({colInfo.name}),{rounding})").alias('stddev')))
                        .alias(colInfo.name)
                        if colInfo.isNumeric
                        else F.expr("null").alias(colInfo.name)
                        for colInfo in self.columnsInfo],
            dfData=df_under_analysis,
            dfSummary=dfDataSummary)

        if self._analysisLevel in ["analyze_text", "full"]:
            logger.info("Analyzing summary text features")
            dfTextFeaturesSummary = self.generateTextFeatures(self.expandedSampleDf)

            dfDataSummary = dfDataSummary.union(dfTextFeaturesSummary)

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

                value_rows = sorted(sourceDf.select(col_name).where(f"{col_name} is not null")
                                    .groupBy(col_name).count().collect(),
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
    def scriptDataGeneratorFromSchema(cls, schema, suppressOutput=False, name=None, asHtml=False):
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
        :param asHtml: If True, will generate Html suitable for notebook ``displayHtml``. If true, suppresses output
        :param name: Optional name for data generator
        :return: String containing skeleton code (in Html form if `asHtml` is True)

        """
        generated_code = cls._scriptDataGeneratorCode(schema, suppressOutput=asHtml or suppressOutput, name=name)

        if asHtml:
            generated_code = HtmlUtils.formatCodeAsHtml(generated_code)

        return generated_code

    def scriptDataGeneratorFromData(self, suppressOutput=False, name=None, asHtml=False):
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
        :param asHtml: If True, will generate Html suitable for notebook ``displayHtml``. If true, suppresses output
        :return: String containing skeleton code (in Html form if `asHtml` is True)

        """
        assert self.sampledSourceDf is not None
        assert type(self.sampledSourceDf) is sql.DataFrame, "sourceDf must be a valid Pyspark dataframe"

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
                                                             sourceDf=self.expandedSampleDf)

        generated_code = self._scriptDataGeneratorCode(self.sampledSourceDf.schema,
                                                       suppressOutput=asHtml or suppressOutput,
                                                       name=name,
                                                       dataSummary=self._dataSummary,
                                                       sourceDf=self.sampledSourceDf,
                                                       valuesInfo=values_info)

        if asHtml:
            generated_code = HtmlUtils.formatCodeAsHtml(generated_code)

        return generated_code
