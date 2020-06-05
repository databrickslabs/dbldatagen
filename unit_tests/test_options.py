from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as dg
from pyspark.sql import SparkSession
import unittest

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()


class TestUseOfOptions(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_basic(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=100000, partitions=20)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code2", "integer", min=1, max=20, step=1, random=True)
            .withColumn("code3", "integer", min=1, max=20, step=1, random=True)
            .withColumn("code4", "integer", min=1, max=20, step=1, random=True)
            # base column specifies dependent column

            .withColumn("site_cd", "string", prefix='site', base_column='code1')
            .withColumn("sector_status_desc", "string", min=1, max=200, step=1, prefix='status', random=True)
            .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
            .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
            )

        df = testdata_generator.build()  # build our dataset

        df.count()

        print("output columns", testdata_generator.getOutputColumnNames())

        df.show()

        df2 = testdata_generator.option("starting_id", 200000).build()  # build our dataset

        df2.count()

        print("output columns", testdata_generator.getOutputColumnNames())

        df2.show()

# run the tests
# if __name__ == '__main__':
#  print("Trying to run tests")
#  unittest.main(argv=['first-arg-is-ignored'],verbosity=2,exit=False)

# def runTests(suites):
#     suite = unittest.TestSuite()
#     result = unittest.TestResult()
#     for testSuite in suites:
#         suite.addTest(unittest.makeSuite(testSuite))
#     runner = unittest.TextTestRunner()
#     print(runner.run(suite))
#
#
# runTests([TestUseOfOptions])
