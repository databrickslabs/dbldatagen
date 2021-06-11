from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from pyspark.sql.types import BooleanType, ByteType

from databrickslabs_testdatagenerator import SchemaParser
from pyspark.sql import SparkSession
import unittest
import databrickslabs_testdatagenerator as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


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
        x_byte= SchemaParser.columnTypeFromString("byte")

        self.assertIsInstance(x_byte, ByteType().__class__)

    def test_type_parser_boolean(self):
        x_bool1= SchemaParser.columnTypeFromString("bool")

        self.assertIsInstance(x_bool1, BooleanType().__class__)

        x_bool2= SchemaParser.columnTypeFromString("boolean")

        self.assertIsInstance(x_bool2, BooleanType().__class__)

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
