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

    @pytest.mark.parametrize("typeDefn",
                             ["decimal(15,3, 3)", "array<string", "map<string, string, int>", "decimal()",
                              "interval", "array<interval>", "map<interval, interval>",
                              "struct<a:interval, b:int>", "binary_float"
                              ])
    def test_parser_exceptions(self, typeDefn, setupLogging):
        with pytest.raises(Exception) as e_info:
            output_type = dg.SchemaParser.columnTypeFromString(typeDefn)

        print("exception:", e_info)


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

    @pytest.mark.parametrize("sqlExpr, expectedText",
                             [("named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                               "named_struct(' ', city_name, ' ', city_id, ' ', city_pop)"),
                              ("cast(10 as decimal(10)",
                               "cast(10 as decimal(10)"),
                              (" ", " "),
                              ("", ""),
                              ])
    def test_sql_expression_cleanser(self, sqlExpr, expectedText):
        newSql = dg.SchemaParser._cleanseSQL(sqlExpr)
        assert sqlExpr == expectedText or sqlExpr != newSql

        assert newSql == expectedText

    @pytest.mark.parametrize("sqlExpr, expectedReferences, filterColumns",
                             [("named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                               ['named_struct', 'city_name',  'city_id', 'city_pop'],
                               None),
                              ("named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                               [ 'city_name', 'city_pop'],
                               ['city_name', 'city_pop']),
                               ("cast(10 as decimal(10)",  ['cast', 'as', 'decimal'], None),
                              ("cast(x as decimal(10)", ['x'], ['x']),
                              (" ", [], None),
                              ("", [], None),
                              ])
    def test_sql_expression_parser(self, sqlExpr, expectedReferences, filterColumns):
        references = dg.SchemaParser.columnsReferencesFromSQLString(sqlExpr, filter=filterColumns)
        assert references is not None

        assert isinstance(references, list), "expected list of potential column references to be returned"

        print(references)

        assert set(references) == set(expectedReferences)


