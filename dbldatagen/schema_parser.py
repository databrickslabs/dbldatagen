# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `SchemaParser` class
"""

import re

from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType


class SchemaParser(object):
    """ SchemaParser class

        Creates pyspark SQL datatype from string
    """
    _match_precision_only = re.compile(r"decimal\s*\(\s*([0-9]+)\s*\)")
    _match_precision_and_scale = re.compile(r"decimal\s*\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)")

    @classmethod
    def _parseDecimal(cls, valueStr):
        """ parse a decimal specifier

        :param valueStr: decimal specifier string such as `decimal(19,4)`, `decimal` or `decimal(10)`
        :returns: DecimalType instance for parsed result
        """
        m = cls._match_precision_only.search(valueStr)
        if m:
            return DecimalType(int(m.group(1)))

        m = cls._match_precision_and_scale.search(valueStr)
        if m:
            return DecimalType(int(m.group(1)), int(m.group(2)))

        return DecimalType()

    @classmethod
    def columnTypeFromString(cls, type_string):
        """ Generate a Spark SQL data type from a string

        Allowable options for `type_string` parameter are:
         * `string`, `varchar`, `char`, `nvarchar`,
         * `int`, `integer`,
         * `bigint`, `long`,
         * `bool`, `boolean`,
         * `timestamp`, `datetime`,
         * `double`, `float`, `date`, `short`, `byte`,
         * `decimal(p, s)` or `number(p, s)`


        :param type_string: String representation of SQL type such as 'integer' etc.
        :returns: Spark SQL type
        """
        assert type_string is not None, "`type_string` must be specified"

        s = type_string.strip().lower()
        if s in ["string", "varchar", "char", "nvarchar"]:
            retval = StringType()
        elif s in ["int", "integer"]:
            retval = IntegerType()
        elif s in ["bigint", "long"]:
            retval = LongType()
        elif s in ["bool", "boolean"]:
            retval = BooleanType()
        elif s in ["timestamp", "datetime"]:
            retval = TimestampType()
        elif s.startswith("decimal") or s.startswith("number"):
            retval = cls._parseDecimal(s)
        elif s == "double":
            retval = DoubleType()
        elif s == "float":
            retval = FloatType()
        elif s == "date":
            retval = DateType()
        elif s == "short":
            retval = ShortType()
        elif s == "byte":
            retval = ByteType()
        else:
            retval = s
        return retval

    @classmethod
    def parseCreateTable(cls, sparkSession, source_schema):
        """ Parse a schema from a schema string

            :param sparkSession: spark session to use
            :param source_schema: should be a table definition minus the create table statement
            :returns: Spark SQL schema instance
        """
        assert (source_schema is not None), "`source_schema` must be specified"
        assert (sparkSession is not None), "`sparkSession` must be specified"
        lines = [x.strip() for x in source_schema.split("\n") if x is not None]
        table_defn = " ".join(lines)

        # get table name from s
        table_name_match = re.search(r"^\s*create\s*(temporary\s*)?table\s*([a-zA-Z0-9_]*)\s*(\(.*)$", table_defn,
                                     flags=re.IGNORECASE)

        if table_name_match:
            table_name = table_name_match.group(2)
            table_body = table_name_match.group(3)
        else:
            raise ValueError("Table name could not be found")

        sparkSession.sql(f"create temporary table {table_name}{table_body}using csv")

        result = sparkSession.sql(f"select * from {table_name}")
        result.printSchema()
        result_schema = result.schema

        sparkSession.sql(f"drop table {table_name}")

        return result_schema
