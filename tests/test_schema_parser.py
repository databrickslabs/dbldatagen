import logging
import pytest

from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, ArrayType, MapType, StructType, StructField

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestSchemaParser:

    @pytest.mark.parametrize("typeDefn, expectedTypeDefn",
                             [("byte", ByteType()),
                              ("tinyint", ByteType()),
                              ("short", ShortType()),
                              ("smallint", ShortType()),
                              ("int", IntegerType()),
                              ("integer", IntegerType()),
                              ("long", LongType()),
                              ("LONG", LongType()),
                              ("bigint", LongType()),
                              ("date", DateType()),
                              ("binary", BinaryType()),
                              ("timestamp", TimestampType()),
                              ("bool", BooleanType()),
                              ("boolean", BooleanType()),
                              ("string", StringType()),
                              ("char(10)", StringType()),
                              ("nvarchar(14)", StringType()),
                              ("nvarchar", StringType()),
                              ("varchar", StringType()),
                              ("varchar(10)", StringType()),
                              ])
    def test_primitive_type_parser(self, typeDefn, expectedTypeDefn, setupLogging):
        output_type = dg.SchemaParser.columnTypeFromString(typeDefn)

        assert output_type == expectedTypeDefn, f"Expect output type {output_type} to match {expectedTypeDefn}"

    @pytest.mark.parametrize("typeDefn, expectedTypeDefn",
                             [("float", FloatType()),
                              ("real", FloatType()),
                              ("double", DoubleType()),
                              ("decimal", DecimalType(10, 0)),
                              ("decimal(11)", DecimalType(11, 0)),
                              ("decimal(15,3)", DecimalType(15, 3)),
                              ])
    def test_numeric_type_parser(self, typeDefn, expectedTypeDefn, setupLogging):
        output_type = dg.SchemaParser.columnTypeFromString(typeDefn)

        assert output_type == expectedTypeDefn, f"Expect output type {output_type} to match {expectedTypeDefn}"

    @pytest.mark.parametrize("typeDefn, expectedTypeDefn",
                             [("array<int>", ArrayType(IntegerType())),
                              ("array<array<string>>", ArrayType(ArrayType(StringType()))),
                              ("map<STRING, INT>", MapType(StringType(), IntegerType())),
                              ("struct<a:binary, b:int, c:float>",
                               StructType([StructField("a", BinaryType()), StructField("b", IntegerType()),
                                           StructField("c", FloatType())]))
                              ])
    def test_complex_type_parser(self, typeDefn, expectedTypeDefn, setupLogging):
        output_type = dg.SchemaParser.columnTypeFromString(typeDefn)

        assert output_type == expectedTypeDefn, f"Expect output type {output_type} to match {expectedTypeDefn}"

    def test_table_definition_parser(self, setupLogging):
        table1 = """CREATE TABLE student (id INT, name STRING, age INT)"""

        table2 = """CREATE TABLE student (
                       id INT, 
                       name STRING, 
                       age INT)
                  """

        table3 = """create table student (
                       id int, 
                       name string, 
                       age int)
                  """

        schema1 = dg.SchemaParser.parseCreateTable(spark, table1)
        assert schema1 is not None, "Schema should not be none"

        assert "id" in schema1.fieldNames()
        assert "name" in schema1.fieldNames()
        assert "age" in schema1.fieldNames()

        schema2 = dg.SchemaParser.parseCreateTable(spark, table2)
        assert schema2 is not None, "schema2 should not be None"

        assert "id" in schema2.fieldNames()
        assert "name" in schema2.fieldNames()
        assert "age" in schema2.fieldNames()

        print("schema3")
        schema3 = dg.SchemaParser.parseCreateTable(spark, table3)
        assert schema3 is not None, "schema3 should not be None"

        assert "id" in schema3.fieldNames()
        assert "name" in schema3.fieldNames()
        assert "age" in schema3.fieldNames()
