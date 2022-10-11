import unittest

from pyspark.sql.types import BooleanType, ByteType
from pyspark.sql.types import IntegerType, StringType, DecimalType

import dbldatagen as dg
from dbldatagen import SchemaParser

spark = dg.SparkSingleton.getLocalInstance("unit tests", useAllCores=True)


class TestSchemaParser(unittest.TestCase):

    def test_type_parser(self):
        x_int = SchemaParser.columnTypeFromString("int")

        self.assertIsInstance(x_int, IntegerType().__class__)

        x_int2 = SchemaParser.columnTypeFromString("integer")

        self.assertIsInstance(x_int2, IntegerType().__class__)

        x_str = SchemaParser.columnTypeFromString("string")

        self.assertIsInstance(x_str, StringType().__class__)

    def test_type_parser_decimal(self):
        x_dec = SchemaParser.columnTypeFromString("decimal")

        self.assertIsInstance(x_dec, DecimalType().__class__)

        x_dec2 = SchemaParser.columnTypeFromString("decimal(15)")

        self.assertIsInstance(x_dec2, DecimalType().__class__)
        self.assertEqual(x_dec2.precision, 15)

        x_dec3 = SchemaParser.columnTypeFromString("decimal(19,4)")

        self.assertIsInstance(x_dec3, DecimalType().__class__)
        self.assertEqual(x_dec3.precision, 19)
        self.assertEqual(x_dec3.scale, 4)

    def test_type_parser_byte(self):
        x_byte = SchemaParser.columnTypeFromString("byte")

        self.assertIsInstance(x_byte, ByteType().__class__)

    def test_type_parser_boolean(self):
        x_bool1 = SchemaParser.columnTypeFromString("bool")

        self.assertIsInstance(x_bool1, BooleanType().__class__)

        x_bool2 = SchemaParser.columnTypeFromString("boolean")

        self.assertIsInstance(x_bool2, BooleanType().__class__)

    def test_table_definition_parser(self):
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

        print("schema1")
        schema1 = SchemaParser.parseCreateTable(spark, table1)
        self.assertTrue(schema1 is not None)

        self.assertIn("id", schema1.fieldNames())
        self.assertIn("name", schema1.fieldNames())
        self.assertIn("age", schema1.fieldNames())

        print("schema2")
        schema2 = SchemaParser.parseCreateTable(spark, table2)
        self.assertTrue(schema2 is not None)

        self.assertIn("id", schema2.fieldNames())
        self.assertIn("name", schema2.fieldNames())
        self.assertIn("age", schema2.fieldNames())

        print("schema3")
        schema3 = SchemaParser.parseCreateTable(spark, table3)
        self.assertTrue(schema3 is not None)

        self.assertIn("id", schema3.fieldNames())
        self.assertIn("name", schema3.fieldNames())
        self.assertIn("age", schema3.fieldNames())


# run the tests
# if __name__ == '__main__':
#  print("Trying to run tests")
#  unittest.main(argv=['first-arg-is-ignored'],verbosity=2,exit=False)

# def runTests(suites):
#    suite = unittest.TestSuite()
#    result = unittest.TestResult()
#    for testSuite in suites:
#        suite.addTest(unittest.makeSuite(testSuite))
#    runner = unittest.TextTestRunner()
#    print(runner.run(suite))


# runTests([TestBasicOperation])
