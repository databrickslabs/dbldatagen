# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``DataAnalyzer`` class.

This code is experimental and both APIs and code generated is liable to change in future versions.
"""
import logging
from typing import SupportsFloat, SupportsIndex

from pyspark.sql import DataFrame, Row, SparkSession, types

from dbldatagen.spark_singleton import SparkSingleton
from dbldatagen.utils import strip_margins


SUMMARY_FIELD_NAME: str = "summary"
SUMMARY_FIELD_NAME_RENAMED: str = "__summary__"
DATA_SUMMARY_FIELD_NAME: str = "__data_summary__"


class DataAnalyzer:
    """
    This class is used to analyze an existing dataset to assist in generating a test data set with similar data
    characteristics. Analyzer results can be used to generate code from existing schemas and data.

    :param df: Spark ``DataFrame`` to analyze
    :param sparkSession: ``SparkSession`` to use
    :param debug: Whether to log additional debug information (default `False`)
    :param verbose: Whether to log detailed execution information (default `False`)

    .. warning::
       Experimental
    """
    debug: bool
    verbose: bool
    _sparkSession: SparkSession
    _df: DataFrame
    _dataSummary: dict[str, dict[str, object]] | None
    _DEFAULT_GENERATED_NAME: str = "synthetic_data"
    _GENERATED_COMMENT: str = strip_margins(
        """
        |# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
        |# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
        |# See the following resources for more details:
        |#
        |#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
        |#   Github project - [https://github.com/databrickslabs/dbldatagen]
        |#
        """,
        marginChar="|"
    )

    _GENERATED_FROM_SCHEMA_COMMENT: str = strip_margins(
        """
        |# Column definitions are stubs only - modify to generate correct data
        |#
        """,
        marginChar="|"
    )

    def __init__(
        self,
        df: DataFrame | None = None,
        sparkSession: SparkSession | None = None,
        debug: bool = False,
        verbose: bool = False
    ) -> None:
        self.verbose = verbose
        self.debug = debug
        self._setupLogger()

        if df is None:
            raise ValueError("Argument `df` must be supplied when initializing a `DataAnalyzer`")
        self._df = df

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self._sparkSession = sparkSession
        self._dataSummary = None

    def _setupLogger(self) -> None:
        """
        Sets up logging for the ``DataAnalyzer``. Configures the logger at warning, info or debug levels depending on
        the user-requested behavior.
        """
        self.logger = logging.getLogger("DataAnalyzer")
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        elif self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARNING)

    @staticmethod
    def _displayRow(row: Row) -> str:
        """
        Displays details for a row as a string.

        :param row: PySpark ``Row`` object to display
        :returns: String representing row-level details
        """
        row_key_pairs = row.asDict()
        return ",".join([f"{x}: {row[x]}" for x in row_key_pairs])

    @staticmethod
    def _addMeasureToSummary(
        measureName: str,
        *,
        summaryExpr: str = "''",
        fieldExprs: list[str] | None = None,
        dfData: DataFrame | None,
        rowLimit: int = 1,
        dfSummary: DataFrame | None = None
    ) -> DataFrame:
        """
        Adds a new measure to the summary ``DataFrame``.

        :param measureName: Measure name
        :param summaryExpr: Measure expression as a Spark SQL statement
        :param fieldExprs: Optional list of field expressions as Spark SQL Statements
        :param dfData: Source ``DataFrame`` to summarize
        :param rowLimit: Number of rows to use for ``DataFrame`` summarization
        :param dfSummary: Summary metrics ``DataFrame``
        :return: Summary metrics ``DataFrame`` with the added measure
        """
        if dfData is None:
            raise ValueError("Input DataFrame `dfData` must be supplied when adding measures to a summary")

        if measureName is None:
            raise ValueError("Input measure name must be a non-empty string")

        # add measure name and measure summary
        expressions = [f"'{measureName}' as measure_", f"string({summaryExpr}) as summary_"]

        if fieldExprs:
            expressions.extend(fieldExprs)

        if dfSummary is not None:
            return dfSummary.union(dfData.selectExpr(*expressions).limit(rowLimit))

        return dfData.selectExpr(*expressions).limit(rowLimit)

    @staticmethod
    def _get_dataframe_describe_stats(df: DataFrame) -> DataFrame:
        """
        Gets a summary ``DataFrame`` with column-level statistics about the input ``DataFrame``.

        :param df: Input ``DataFrame``
        :returns: Summary ``DataFrame`` with column-level statistics
        """
        src_fields = [fld.name for fld in df.schema.fields]
        renamed_summary = False

        # get summary statistics handling the case where a field named 'summary' exists
        # if the `summary` field name exists, we'll rename it to avoid a conflict
        if SUMMARY_FIELD_NAME in src_fields:
            renamed_summary = True
            df = df.withColumnRenamed(SUMMARY_FIELD_NAME, SUMMARY_FIELD_NAME_RENAMED)

        # The dataframe describe method produces a field named `summary`. We'll rename this to avoid conflict with
        # any natural fields using the same name.
        summary_df = df.describe().withColumnRenamed(SUMMARY_FIELD_NAME, DATA_SUMMARY_FIELD_NAME)

        # if we renamed a field called `summary` in the data, we'll rename it back.
        # The data summary field produced by the describe method has already been renamed so there will be no conflict.
        if renamed_summary:
            summary_df = summary_df.withColumnRenamed(SUMMARY_FIELD_NAME_RENAMED, SUMMARY_FIELD_NAME)

        return summary_df

    def summarizeToDF(self) -> DataFrame:
        """
        Generates a summary analysis of the input ``DataFrame`` of the ``DataAnalyzer``.

        :returns: Summary ``DataFrame`` with analyzer results

        .. note::
        The resulting dataframe can be displayed with the ``display`` function in a notebook environment
        or with the ``show`` method.

        The output is also used in code generation  to generate more accurate code.
        """
        self._df.createOrReplaceTempView("data_analysis_summary")

        total_count = self._df.count() * 1.0
        dtypes = self._df.dtypes
        data_summary_df = self._addMeasureToSummary(
            measureName="schema",
            summaryExpr=f"""to_json(named_struct('column_count', {len(dtypes)}))""",
            fieldExprs=[f"'{dtype[1]}' as {dtype[0]}" for dtype in dtypes],
            dfData=self._df
        )

        data_summary_df = self._addMeasureToSummary(
            measureName="count",
            summaryExpr=f"{total_count}",
            fieldExprs=[f"string(count({dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        data_summary_df = self._addMeasureToSummary(
            measureName="null_probability",
            fieldExprs=[
                f"""string( round( ({total_count} - count({dtype[0]})) /{total_count}, 2)) as {dtype[0]}"""
                        for dtype in dtypes
            ],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        # distinct count
        data_summary_df = self._addMeasureToSummary(
            measureName="distinct_count",
            summaryExpr="count(distinct *)",
            fieldExprs=[f"string(count(distinct {dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        # min
        data_summary_df = self._addMeasureToSummary(
            measureName="min",
            fieldExprs=[f"string(min({dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        data_summary_df = self._addMeasureToSummary(
            measureName="max",
            fieldExprs=[f"string(max({dtype[0]})) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        description_df = (
            self
            ._get_dataframe_describe_stats(self._df)
            .where(f"{DATA_SUMMARY_FIELD_NAME} in ('mean', 'stddev')")
        )
        description_data = description_df.collect()

        for row in description_data:
            measure = row[DATA_SUMMARY_FIELD_NAME]

            values = {k[0]: "" for k in dtypes}

            row_key_pairs = row.asDict()
            for k1 in row_key_pairs:
                values[k1] = str(row[k1])

            data_summary_df = self._addMeasureToSummary(
                measureName=measure,
                fieldExprs=[f"'{values[dtype[0]]}'" for dtype in dtypes],
                dfData=self._df,
                dfSummary=data_summary_df
            )

        # string characteristics for strings and string representation of other values
        data_summary_df = self._addMeasureToSummary(
            measureName="print_len_min",
            fieldExprs=[f"string(min(length(string({dtype[0]})))) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        data_summary_df = self._addMeasureToSummary(
            measureName="print_len_max",
            fieldExprs=[f"string(max(length(string({dtype[0]})))) as {dtype[0]}" for dtype in dtypes],
            dfData=self._df,
            dfSummary=data_summary_df
        )

        return data_summary_df

    def summarize(self, suppressOutput: bool = False) -> str:
        """
        Generates a summary analysis of the input ``DataFrame`` and returns the analysis as a string. Optionally prints
        the summary analysis.

        :param suppressOutput:  Whether to print the summary analysis (default `False`)
        :return: Summary analysis as string
        """
        summary_df = self.summarizeToDF()

        results = [
            "Data set summary",
            "================"
        ]

        for row in summary_df.collect():
            results.append(self._displayRow(row))

        summary = "\n".join([str(x) for x in results])

        if not suppressOutput:
            print(summary)

        return summary

    @classmethod
    def _valueFromSummary(
        cls,
        dataSummary: dict[str, dict[str, object]] | None = None,
        colName: str | None = None,
        measure: str | None = None,
        defaultValue: int | float | str | None = None
    ) -> object:
        """
        Gets a measure value from a data summary given a measure name and column name. Returns a default value when the
        measure value cannot be found.

        :param dataSummary: Optional data summary to search (if ``None``, the default value is returned)
        :param colName: Optional column name
        :param measure: Optional measure name
        :param defaultValue: Default return value
        :return: Measure value or default value
        """
        if dataSummary is None or colName is None or measure is None:
            return defaultValue

        if measure not in dataSummary:
            return defaultValue

        measure_values = dataSummary[measure]
        if colName not in measure_values:
            return defaultValue

        return measure_values[colName]

    @classmethod
    def _generatorDefaultAttributesFromType(
        cls,
        sqlType: types.DataType,
        colName: str | None = None,
        dataSummary: dict | None = None
    ) -> str:
        """
        Generates a Spark SQL expression for the input column and data type. Optionally uses ``DataAnalyzer`` summary
        statistics to create Spark SQL expressions for generating data similar to the input ``DataFrame``.

        :param sqlType: Data type as an instance of ``pyspark.sql.types.DataType``
        :param colName: Column name
        :param dataSummary: Optional map of maps of attributes from the data summary
        :return: Spark SQL expression for supplied column and data type

        .. note::
        When generating expressions from a schema, no data heuristics are available to determine how data should be
        generated. This method will use default values according to Spark's data type limits to generate working
        expressions for data generation.

        Users are expected to modify the generated code to their needs.
        """
        if not isinstance(sqlType, types.DataType):
            raise ValueError(
                f"Argument 'sqlType' with type {type(sqlType)} must be an instance of `pyspark.sql.types.DataType`"
            )

        if sqlType == types.StringType():
            result = """template=r'\\\\w'"""

        elif sqlType in [types.IntegerType(), types.LongType()]:
            min_value = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            max_value = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=1000000)
            result = f"""minValue={min_value}, maxValue={max_value}"""

        elif sqlType == types.ByteType():
            min_value = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            max_value = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=127)
            result = f"""minValue={min_value}, maxValue={max_value}"""

        elif sqlType == types.ShortType():
            min_value = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            max_value = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=32767)
            result = f"""minValue={min_value}, maxValue={max_value}"""

        elif sqlType == types.BooleanType():
            result = """expr='id % 2 = 1'"""

        elif sqlType == types.DateType():
            result = """expr='current_date()'"""

        elif isinstance(sqlType, types.DecimalType):
            max_decimal_value = 10**(sqlType.precision - sqlType.scale) - 10**(-1 * sqlType.scale)
            min_value = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0)
            max_value = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=max_decimal_value)
            result = f"""minValue={min_value}, maxValue={max_value}"""

        elif sqlType in [types.FloatType(), types.DoubleType()]:
            min_value = cls._valueFromSummary(dataSummary, colName, "min", defaultValue=0.0)
            max_value = cls._valueFromSummary(dataSummary, colName, "max", defaultValue=1000000.0)
            result = f"""minValue={min_value}, maxValue={max_value}, step=0.1"""

        elif sqlType == types.TimestampType():
            result = """begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00", interval="1 minute" """

        elif sqlType == types.BinaryType():
            result = """expr="cast('dbldatagen generated synthetic data' as binary)" """

        else:
            result = """expr='null'"""

        summary_value = cls._valueFromSummary(dataSummary, colName, "null_probability", defaultValue=0.0)
        percent_nulls_value = (
            float(summary_value) if isinstance(summary_value, str | SupportsFloat | SupportsIndex) else 0.0
        )

        if percent_nulls_value > 0.0:
            result = result + f", percentNulls={percent_nulls_value}"

        return result

    @classmethod
    def _scriptDataGeneratorCode(
        cls,
        schema: types.StructType,
        *,
        dataSummary: dict | None = None,
        sourceDf: DataFrame | None = None,
        suppressOutput: bool = False,
        name: str | None = None
    ) -> str:
        """
        Generates code to build a ``DataGenerator`` from an existing dataframe. Analyzes the dataframe passed to the
        constructor of the ``DataAnalyzer`` and returns a script for generating similar data.

        :param schema: Pyspark schema as a ``StructType``
        :param dataSummary: Optional map of maps of attributes from the data summary
        :param sourceDf: Optional ``DataFrame`` to retrieve attributes from existing data
        :param suppressOutput: Whether to suppress printing attributes during execution (default `False`)
        :param name: Optional name for data generator
        :return: Data generation code string

        .. note::
        Code generated by this method should be treated as experimental. For most uses, generated code requires further
        modification. Results are intended to provide an initial script for generating data from the input dataset.
        """
        statements = []

        if name is None:
            name = cls._DEFAULT_GENERATED_NAME

        statements.append(cls._GENERATED_COMMENT)
        statements.append("import dbldatagen as dg")
        statements.append("import pyspark.sql.types")
        statements.append(cls._GENERATED_FROM_SCHEMA_COMMENT)
        statements.append(
            strip_margins(
                f"""generation_spec = (
                                    |    dg.DataGenerator(sparkSession=spark,
                                    |                     name='{name}',
                                    |                     rows=100000,
                                    |                     random=True,
                                    |                     )""",
                marginChar="|"
            )
        )

        indent = "    "
        for field in schema.fields:
            column_name = field.name
            column_type = field.dataType.simpleString()

            if isinstance(field.dataType, types.ArrayType):
                column_type = field.dataType.elementType.simpleString()
                field_attributes = cls._generatorDefaultAttributesFromType(field.dataType.elementType)
                array_attributes = "structType='array', numFeatures=(2,6)"
                name_and_type = f"'{column_name}', '{column_type}'"
                statements.append(indent + f".withColumn({name_and_type}, {field_attributes}, {array_attributes})")
            else:
                field_attributes = cls._generatorDefaultAttributesFromType(
                    field.dataType, colName=column_name, dataSummary=dataSummary
                )
                statements.append(indent + f".withColumn('{column_name}', '{column_type}', {field_attributes})")

        statements.append(indent + ")")
        if not suppressOutput:
            for line in statements:
                print(line)

        return "\n".join(statements)

    @classmethod
    def scriptDataGeneratorFromSchema(
        cls, schema: types.StructType, suppressOutput: bool = False, name: str | None = None
    ) -> str:
        """
        Generates code to build a ``DataGenerator`` from an existing dataframe schema. Analyzes the schema of the
        ``DataFrame`` passed to the ``DataAnalyzer`` and returns a script for generating similar data.

        :param schema: Pyspark schema as a ``StructType``
        :param suppressOutput: Whether to suppress printing attributes during execution (default `False`)
        :param name: Optional name for data generator
        :return: Data generation code string

        .. note::
        Code generated by this method should be treated as experimental. For most uses, generated code requires further
        modification. Results are intended to provide an initial script for generating data from the input dataset.
        """
        return cls._scriptDataGeneratorCode(schema, suppressOutput=suppressOutput, name=name)

    def scriptDataGeneratorFromData(self, suppressOutput: bool = False, name: str | None = None) -> str:
        """
        Generates code to build a ``DataGenerator`` from an existing dataframe. Analyzes statistical properties of the
         ``DataFrame`` passed to the ``DataAnalyzer`` and returns a script for generating similar data.

        :param suppressOutput: Whether to suppress printing attributes during execution (default `False`)
        :param name: Optional name for data generator
        :return: Data generation code string

        .. note::
        Code generated by this method should be treated as experimental. For most uses, generated code requires further
        modification. Results are intended to provide an initial script for generating data from the input dataset.
        """
        if not self._df:
            raise ValueError("Missing `DataAnalyzer` property `df` for scripting a data generator from data")

        if self._dataSummary is None:
            df_summary = self.summarizeToDF()
            self._dataSummary = {}

            for row in df_summary.collect():
                row_key_pairs = row.asDict()
                self._dataSummary[row["measure_"]] = row_key_pairs

        return self._scriptDataGeneratorCode(
            self._df.schema, suppressOutput=suppressOutput, name=name, dataSummary=self._dataSummary, sourceDf=self._df
        )
