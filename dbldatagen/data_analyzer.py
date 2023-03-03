# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the DataAnalyzer class. This is still a work in progress.
"""
from pyspark.sql.types import StructField
from pyspark.sql.functions import lit
from pyspark.sql import functions as fns
from .utils import strip_margins
from .spark_singleton import SparkSingleton

import pyspark.sql as s

class DataAnalyzer:
    """This class is used to analyze an existing data set to assist in generating a test data set with similar
    characteristics

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

        self.rowCount = 0
        self.schema = None
        self.df = df.cache()

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

    def _getDistinctCounts(self):
        """ Get distinct counts"""
        pass

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

        return (df.withColumn("summary", lit(heading))
                .select(*select_fields))

    def summarize(self):
        """Generate summary of data frame attributes"""
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

        distinct_expressions = [fns.countDistinct(x).alias(x) for x in self._getFieldNames(self.df.schema)]
        results.append(self._displayRow(
            self._prependSummary(self.df.agg(*distinct_expressions),
                                'distinct_count')
                .select(*select_fields)
                .collect()[0]
        ))

        for r in self.df.describe().collect():
            results.append(self._displayRow(r))

        return "\n".join([str(x) for x in results])

    def determineIfDataIsCatagorical(self):
        """
        The following algorithm provides a useful test for identifying categorical data:

            Calculate the number of unique values in the data set.
            Calculate the difference between the number of unique values in the data set and the total number of values
            in the data set.
            Calculate the difference as a percentage of the total number of values in the data set.
            If the percentage difference is 90% or more, then the data set is composed of categorical values.
            When the number of rows is less than around 50, then a lower threshold of 70% works well in practice.

            See: https://jeffreymorgan.io/articles/identifying-categorical-data/
        :return:
        """
        pass

    def determineIfDataIsContinuous(self):
        """

            See: https://www.intellspot.com/discrete-vs-continuous-data/
        :return:
        """
        pass

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
        assert type(self.df) is s.DataFrame, "sourceDf must be a valid Pyspark dataframe"

        self.df.createOrReplaceTempView("evaluation_dataframe")

        sourceDetails = self.sparkSession.sql(f"describe evaluation_dataframe").collect()

        generatedCode = []

        if name is None:
            name = "test_generator"

        generatedCode.append(f"import dbldatagen as dg")

        generatedCode.append(strip_margins(
            f"""generation_spec = (dg.DataGenerator(sparkSession=spark, name='{name}', 
               |                                     rows=100000, 
               |                                     seed_method='hash_fieldname')""", '|'))

        indent = "                   "
        for c in sourceDetails:
            generatedCode.append(indent + f""".withColumn('{c["col_name"]}', '{c["data_type"]}', expr="null")""")
        generatedCode.append(indent + ")")

        if not suppressOutput:
            for line in generatedCode:
                print(line)

        return "\n".join(generatedCode)