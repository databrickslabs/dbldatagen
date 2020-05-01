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
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DateType, DecimalType
import re


class SchemaParser(object):
    """" SchemaParser class

        Creates pyspark SQL datatype from string
    """
    match_precision_only = re.compile("decimal\s*\(\s*([0-9]+)\s*\)")
    match_precision_and_scale = re.compile("decimal\s*\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)")

    @classmethod
    def parse_decimal(cls, str):
        """ parse a decimal specifier

        :param str - decimal specifier string such as `decimal(19,4)`, `decimal` or `decimal(10)`
        """
        m = cls.match_precision_only.search( str)
        if m:
            return DecimalType(int(m.group(1)))

        m = cls.match_precision_and_scale.search( str)
        if m:
            return DecimalType(int(m.group(1)), int(m.group(2)))

        return DecimalType()

    @classmethod
    def columnTypeFromString(cls, type_string):
        """ Generate a Spark SQL data type from a string """
        assert type_string is not None

        s = type_string.strip().lower()
        if s == "string" or s == "varchar" or s == "char" or s == "nvarchar":
            return StringType()
        elif s == "int" or s == "integer":
            return IntegerType()
        elif s == "bigint" or s == "long":
            return LongType()
        elif s == "bool" or s == "boolean":
            return BooleanType()
        elif s == "timestamp" or s == "datetime":
            return TimestampType()
        elif s.startswith("decimal") or s.startswith("number"):
            return cls.parse_decimal(s)
        elif s == "double":
            return DoubleType()
        elif s == "float":
            return FloatType()
        elif s == "date":
            return DateType()
        elif s == "short":
            return ShortType()
        else:
            return s

    @classmethod
    def parseCreateTable(cls, sparkSession, s):
        """ Parse a schema from a schema string
            - schema should be a table definition minus the create table statement
        """
        assert (s is not None)
        assert (sparkSession is not None)
        lines = [x.strip() for x in s.split("\n") if x is not None]
        table_defn = " ".join(lines)

        # get table name from s
        table_name_match = re.search(r"^\screate\s*(temporary\s*)?table\s*([a-zA-Z0-9_]*)\s*(\(.*)$", table_defn)

        if table_name_match:
            table_name = table_name_match.group(2)
            table_body = table_name_match.group(3)
        else:
            raise ValueError("Table name could not be found")

        print('table name ', table_name)

        sparkSession.sql("create temporary table {}{}using csv".format(table_name, table_body))

        result = sparkSession.sql("select * from {}".format(table_name))
        result.printSchema()
        result_schema = result.schema

        sparkSession.sql("drop table {}".format(table_name))

        return result_schema
