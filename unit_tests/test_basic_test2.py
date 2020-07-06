from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from datetime import datetime, timedelta
from databrickslabs_testdatagenerator import DataGenerator
import databrickslabs_testdatagenerator as dg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

from pyspark.sql import SparkSession
import unittest


schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("state1", StringType(), True),
    StructField("state2", StringType(), True),
    StructField("sector_technology_desc", StringType(), True),

])

interval = timedelta(seconds=10)
start = datetime(2018, 10, 1, 6, 0, 0)
end = datetime.now()

src_interval = timedelta(days=1, hours=1)
src_start = datetime(2017, 10, 1, 0, 0, 0)
src_end = datetime(2018, 10, 1, 6, 0, 0)

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True)

])

spark = dg.SparkSingleton.getLocalInstance("unit tests")


# build spark session

# global spark

spark = dg.SparkSingleton.getLocalInstance("basic tests 2")


class TestBasicOperation2(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_analyzer(self):
        testDataDF = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()
        # display(x3_output)

        analyzer = dg.DataAnalyzer(testDataDF)

        print("Summary;", analyzer.summarize())

    def test_complex_datagen(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
                        .withColumn("code1a", IntegerType(),unique_values=100)
                        .withColumn("code1b", IntegerType(), min=1, max=200)
                        .withColumn("code2", IntegerType(),  max=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        testDataDF2 = testDataSpec.build()

        print("schema", testDataDF2.schema)
        testDataDF2.printSchema()

        testDataSpec.computeBuildPlan().explain()

        #testDataDF2.show()

        testDataDF2.createOrReplaceTempView("testdata")
        df_stats=spark.sql("""select min(code1a) as min1a, 
                              max(code1a) as max1a, 
                              min(code1b) as min1b, 
                              max(code1b) as max1b,
                              min(code2) as min2, 
                              max(code2) as max2
                              from testdata""")
        stats = df_stats.collect()[0]

        print("stats",stats)

        #self.assertEqual(stats.min1, 1)
        #self.assertEqual(stats.min2, 1)
        #self.assertLessEqual(stats.max1, 100)
        #self.assertLessEqual(stats.max1, 200)

    def test_generate_name(self):
        print("test_generate_name")
        n1 = DataGenerator.generateName()
        n2 = DataGenerator.generateName()
        self.assertIsNotNone(n1)
        self.assertIsNotNone(n2)
        self.assertNotEqual(n1, n2, "Names should be different")

    def test_column_specifications(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_column_specifications")
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc", "s"]))
        self.assertEqual(expectedColumns, set(([x.name for x in tgen.allColumnSpecs])))

    def test_inferred_columns(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_inferred_columns")
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc", "s"]))
        print("inferred columns", tgen.getInferredColumnNames())
        self.assertEqual(expectedColumns, set((tgen.getInferredColumnNames())))

    def test_output_columns(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_output_columns")
        expectedColumns = set((["site_id", "site_cd", "c", "c1", "sector_status_desc"]))
        print("output columns", tgen.getOutputColumnNames())
        self.assertEqual(expectedColumns, set((tgen.getOutputColumnNames())))

    def test_with_column_spec_for_missing_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_missing_column")
        with self.assertRaises(Exception):
            t2 = tgen.withColumnSpec("site_dwkey", min=1, max=200, step=1, random=True)

    @unittest.expectedFailure
    def test_with_column_spec_for_missing_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_missing_column")
        # with self.assertRaises(Exception):
        t2 = tgen.withColumnSpec("d", min=1, max=200, step=1, random=True)

    @unittest.expectedFailure
    def test_with_column_spec_for_duplicate_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_duplicate_column")
        # with self.assertRaises(Exception):
        t2 = tgen.withColumnSpec("site_id", min=1, max=200, step=1, random=True)
        t2 = t2.withColumnSpec("site_id", min=1, max=200, step=1, random=True)

    # @unittest.expectedFailure
    def test_with_column_spec_for_duplicate_column2(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_duplicate_column2")
        t2 = tgen.withColumn("site_id", "string", min=1, max=200, step=1, random=True)

    def test_with_column_spec_for_id_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=100)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_id_column")
        t2 = tgen.withIdOutput()
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc"]))
        print("output columns", t2.getOutputColumnNames())
        print("inferred columns", t2.getInferredColumnNames())
        self.assertEqual(expectedColumns, set((t2.getOutputColumnNames())))


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
