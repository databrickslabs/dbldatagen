from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as datagen

from pyspark.sql import SparkSession
import unittest

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("sector_technology_desc", StringType(), True),

])

# build spark session

# global spark

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()


class TestSimpleOperation(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_analyzer(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()
        # display(x3_output)

        analyzer = datagen.DataAnalyzer(testDataDF)

        print("Summary;", analyzer.summarize())

    def test_complex_datagen(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                              partitions=4)
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
                        .withColumn("code1", IntegerType(), min=100, max=200)
                        .withColumn("code2", IntegerType(), min=0, max=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        testDataDF2 = testDataSpec.build()

        print("schema", testDataDF2.schema)
        testDataDF2.printSchema()

        testDataSpec.compute_build_plan().explain()

        testDataDF2.show()

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
# runTests([TestSimpleOperation])
