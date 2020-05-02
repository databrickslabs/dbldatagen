from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from databrickslabs_testdatagenerator import SchemaParser
from pyspark.sql import SparkSession
import unittest



spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()


class TestBasicOperation(unittest.TestCase):

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
        self.assertEquals(x_dec2.precision, 15)

        x_dec3 = SchemaParser.columnTypeFromString("decimal(19,4)")

        self.assertIsInstance(x_dec3, DecimalType().__class__)
        self.assertEquals(x_dec3.precision, 19)
        self.assertEquals(x_dec3.scale, 4)


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
