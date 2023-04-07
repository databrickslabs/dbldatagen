# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the DataAnalyzer class.

This code is experimental and both APIs and code generated is liable to change in future versions.
"""
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, StructField, StructType, MapType, ArrayType, DataType

import pyspark.sql as ssql
import pyspark.sql.functions as F

from .utils import strip_margins
from .spark_singleton import SparkSingleton


class DataAnalyzer:
    """This class is used to analyze an existing data set to assist in generating a test data set with similar
    characteristics, and to generate code from existing schemas and data

    .. warning::
       Experimental

    :param df: Spark data frame to analyze
    :param sparkSession: spark session instance to use when performing spark operations
    """

    def __init__(self, df=None, sparkSession=None):
        """ Constructor:
        :param df: data frame to analyze
        :param sparkSession: spark session to use
        """
        assert df is not None, "dataframe must be supplied"

        self.df = df

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self.sparkSession = sparkSession

    def _lookupFieldType(self, typ):
        """Perform lookup of type name by Spark SQL type name"""
        type_mappings = {
            "LongType": "Long",
            "IntegerType": "Int",
            "TimestampType": "Timestamp",
            "FloatType": "Float",
            "StringType": "String",
        }

        if typ in type_mappings:
            return type_mappings[typ]
        else:
            return typ

    def _summarizeField(self, field):
        """Generate summary for individual field"""
        if isinstance(field, StructField):
            return f"{field.name} {self._lookupFieldType(str(field.dataType))}"
        else:
            return str(field)

    def summarizeFields(self, schema):
        """ Generate summary for all fields in schema"""
        if schema is not None:
            fields = schema.fields
            fields_desc = [self._summarizeField(x) for x in fields]
            return "Record(" + ",".join(fields_desc) + ")"
        else:
            return "N/A"

    def _getFieldNames(self, schema):
        """ get field names from schema"""
        if schema is not None and schema.fields is not None:
            return [x.name for x in schema.fields if isinstance(x, StructField)]
        else:
            return []

    def _displayRow(self, row):
        """Display details for row"""
        results = []
        row_key_pairs = row.asDict()
        for x in row_key_pairs:
            results.append(f"{x}: {row[x]}")

        return ", ".join(results)

    def _prependSummary(self, df, heading):
        """ Prepend summary information"""
        field_names = self._getFieldNames(self.df.schema)
        select_fields = ["summary"]
        select_fields.extend(field_names)

        return (df.withColumn("summary", F.lit(heading))
                .select(*select_fields))

    def summarizeToDF(self):
        """ Generate summary analysis of data set as dataframe

        :param suppressOutput:  if False, prints results to console also
        :return: summary results as dataframe

        The resulting dataframe can be displayed with the `display` function in a notebook environment
        or with the `show` method
        """
        self.df.cache().createOrReplaceTempView("data_analysis_summary")

        dtypes = self.df.dtypes
        print(dtypes)

        # schema information
        exprs = ["'schema' as measure_", f"to_json(named_struct('column_count', {len(dtypes)})) as summary_"]
        exprs.extend([f"'{dtype[1]}' as {dtype[0]}" for dtype in dtypes])

        df = self.df.limit(1).selectExpr(*exprs)

        # count
        exprs = ["'count' as measure_", "string(count(*)) as summary_"]
        exprs.extend([f"string(count({dtype[0]})) as {dtype[0]}" for dtype in dtypes])

        df = df.union(self.df.selectExpr(*exprs))

        # distinct count
        exprs = ["'distinct_count' as measure_", "string(count(distinct *)) as summary_"]
        exprs.extend([f"string(count(distinct {dtype[0]})) as {dtype[0]}" for dtype in dtypes])

        df = df.union(self.df.selectExpr(*exprs))

        # mean
        # exprs = ["'mean' as measure_", "'' as summary_"]
        # exprs.extend([f"string(mean({dtype[0]})) as {dtype[0]}" for dtype in dtypes])

        # df = df.union(df_source_data.selectExpr(*exprs))

        # min
        exprs = ["'min' as measure_", "'' as summary_"]
        exprs.extend([f"string(min({dtype[0]})) as {dtype[0]}" for dtype in dtypes])

        df = df.union(self.df.selectExpr(*exprs))

        # max
        exprs = ["'max' as measure_", "'' as summary_"]
        exprs.extend([f"string(max({dtype[0]})) as {dtype[0]}" for dtype in dtypes])

        df = df.union(self.df.selectExpr(*exprs))

        return df

    def summarize(self, suppressOutput=False):
        """ Generate summary analysis of data set and return / print summary results

        :param suppressOutput:  if False, prints results to console also
        :return: summary results as string
        """
        count = self.df.count()
        distinct_count = self.df.distinct().count()
        partition_count = self.df.rdd.getNumPartitions()

        results = []
        summary = f"""
           count: {count}
           distinct count: {distinct_count}
           partition count: {partition_count} 
        """

        results.append(summary)
        results.append("schema: " + self.summarizeFields(self.df.schema))

        field_names = self._getFieldNames(self.df.schema)
        select_fields = ["summary"]
        select_fields.extend(field_names)

        distinct_expressions = [F.countDistinct(x).alias(x) for x in self._getFieldNames(self.df.schema)]
        results.append(self._displayRow(
            self._prependSummary(self.df.agg(*distinct_expressions),
                                 'distinct_count')
            .select(*select_fields)
            .collect()[0]
        ))

        for r in self.df.describe().collect():
            results.append(self._displayRow(r))

        summary = "\n".join([str(x) for x in results])

        if not suppressOutput:
            print(summary)

        return summary

    DEFAULT_GENERATED_NAME = "synthetic_data"

    GENERATED_COMMENT = strip_margins("""
                        |# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
                        |# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
                        |# See the following resources for more details:
                        |#
                        |#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
                        |#   Github project - [https://github.com/databrickslabs/dbldatagen]
                        |#""", '|')

    GENERATED_FROM_SCHEMA_COMMENT = strip_margins("""
                        |# Column definitions are stubs only - modify to generate correct data  
                        |#""", '|')

    @classmethod
    def _generatorDefaultAttributesFromType(cls, sqlType):
        """ Generate default set of attributes for each data type

        :param sqlType: instance of `pyspark.sql.types.DataType`
        :return: attribute string for supplied sqlType

        When generating code from a schema, we have no data heuristics to determine how data should be generated,
        so goal is to just generate code that produces some data.

        Users are expected to modify the generated code to their needs.
        """
        assert isinstance(sqlType, DataType)

        if sqlType == StringType():
            result = """template=r'\\\\w'"""
        elif sqlType in [IntegerType(), LongType()]:
            result = """minValue=1, maxValue=1000000"""
        elif sqlType in [FloatType(), DoubleType()]:
            result = """minValue=1.0, maxValue=1000000.0, step=0.1"""
        else:
            result = """expr='null'"""
        return result

    @classmethod
    def scriptDataGeneratorFromSchema(cls, schema, suppressOutput=False, name=None):
        """
        generate outline data generator code from an existing data frame

        This will generate a data generator spec from an existing dataframe. The resulting code
        can be used to generate a data generation specification.

        Note at this point in time, the code generated is stub code only.
        For most uses, it will require further modification - however it provides a starting point
        for generation of the specification for a given data set

        The data frame to be analyzed is the data frame passed to the constructor of the DataAnalyzer object

        :param schema: Pyspark schema - i.e manually constructed StructType or return value from `dataframe.schema`
        :param suppressOutput: suppress printing of generated code if True
        :param name: Optional name for data generator
        :return: String containing skeleton code

        """
        assert isinstance(schema, StructType), "expecting valid Pyspark Schema"

        generatedCode = []

        if name is None:
            name = cls.DEFAULT_GENERATED_NAME

        generatedCode.append(cls.GENERATED_COMMENT)

        generatedCode.append("import dbldatagen as dg")
        generatedCode.append("import pyspark.sql.types")

        generatedCode.append(cls.GENERATED_FROM_SCHEMA_COMMENT)

        generatedCode.append(strip_margins(
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

            field_attributes = cls._generatorDefaultAttributesFromType(fld.dataType)

            generatedCode.append(indent + f""".withColumn('{col_name}', '{col_type}', {field_attributes})""")
        generatedCode.append(indent + ")")

        if not suppressOutput:
            for line in generatedCode:
                print(line)

        return "\n".join(generatedCode)

    def scriptDataGeneratorFromData(self, suppressOutput=False, name=None):
        """
        generate outline data generator code from an existing data frame

        This will generate a data generator spec from an existing dataframe. The resulting code
        can be used to generate a data generation specification.

        Note at this point in time, the code generated is stub code only.
        For most uses, it will require further modification - however it provides a starting point
        for generation of the specification for a given data set

        The data frame to be analyzed is the data frame passed to the constructor of the DataAnalyzer object

        :param suppressOutput: suppress printing of generated code if True
        :param name: Optional name for data generator
        :return: String containing skeleton code

        """
        assert self.df is not None
        assert type(self.df) is ssql.DataFrame, "sourceDf must be a valid Pyspark dataframe"
        assert self.df.schema is not None

        return self.scriptDataGeneratorFromSchema(self.df.schema, suppressOutput=suppressOutput, name=name)
