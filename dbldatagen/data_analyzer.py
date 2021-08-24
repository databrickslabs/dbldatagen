# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the DataAnalyzer class. This is still a work in progress.
"""
from pyspark.sql.types import StructField
from pyspark.sql.functions import lit
from pyspark.sql import functions as fns


class DataAnalyzer:
    """This class is used to analyze an existing data set to assist in generating a test data set with similar
    characteristics

    .. warning::
       Not fully implemented.

    :param df: Spark data frame to analyze
    :param sparkSession: spark session instance to use when performing spark operations
    """

    def __init__(self, df, sparkSession=None):
        """ Constructor:
        :param df: data frame to analyze
        :param sparkSession: spark session to use
        """
        self.rowCount = 0
        self.schema = None
        self.df = df.cache()
        # assert sparkSession is not None, "The spark session attribute must be initialized"
        # self.sparkSession = sparkSession
        # if sparkSession is None:
        #    raise Exception("""ERROR: spark session not initialized
        #
        #            The spark session attribute must be initialized in the DataGenerator initialization
        #
        #            i.e DataGenerator(sparkSession=spark, name="test", ...)
        #            """)

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
