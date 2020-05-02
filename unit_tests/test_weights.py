from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand, lit
import unittest

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()

desired_weights = [9, 1, 1, 1]


def weights_as_percentages(w):
    assert w is not None
    total = sum(w)
    percentages = [x / total * 100 for x in w]
    return percentages


class TestWeights(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("setting up")
        cls.rows = 10000

        # will have implied column `id` for ordinal of row
        cls.testdata_generator = (
            datagen.DataGenerator(sparkSession=spark, name="test_dataset1", rows=cls.rows, partitions=4)
            .withIdOutput()  # id column will be emitted in the output
            .withColumn("code1", "integer", min=1, max=20, step=1)
            .withColumn("code4", "integer", min=1, max=40, step=1, random=True)
            .withColumn("sector_status_desc", "string", min=1, max=200, step=1, prefix='status', random=True)
            .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                        weights=desired_weights,
                        random=True)
            )
        cls.testdata_generator.build().cache().createOrReplaceTempView("testdata")

    @classmethod
    def tearDownClass(cls):
        print("tear down")
        spark.stop()

    def test_basic(self):

        df = self.testdata_generator.build()
        row_count = df.count()
        print("row count:", row_count)
        self.assertEqual(row_count, self.rows)

    def test_basic2(self):
        count = spark.sql("select count(*) as rc from testdata").take(1)[0].rc

        print("count ", count)

    def test_generate_values(self):
        df_values = spark.sql(
            "select * from (select tech, count(tech) as rc from testdata group by tech ) a order by tech").collect()
        values = [x.tech for x in df_values]
        print("row values:", values)
        total_count = sum([x.rc for x in df_values])

        percentages = weights_as_percentages([x.rc for x in df_values])
        desired_percentages = weights_as_percentages(desired_weights)

        print("actual percentages", percentages)
        print("desired percentages", desired_percentages)

        self.assertEqual(len(percentages), len(desired_percentages))

        # check that values are close
        for x, y in zip(percentages, desired_percentages):
            self.assertAlmostEqual(x, y, delta=float(x) / 10.0)

    def test_weighted_distribution(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = [x for x in "abcdefghijklmnopqrstuvwxyz"]
        dfAlpha = (datagen.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("alpha", "string", values=alpha_list,
                               weights=alpha_desired_weights,
                               random=True)
                   ).build().cache().createOrReplaceTempView("testdata2")
        df_values = spark.sql("""select * from (select alpha, 
                                                    count(alpha) as rc from testdata2 group by alpha ) a 
                                                order by alpha""").collect()
        values = [x.alpha for x in df_values]
        print("row values:", values)
        total_count = sum([x.rc for x in df_values])

        percentages = weights_as_percentages([x.rc for x in df_values])
        desired_percentages = weights_as_percentages(alpha_desired_weights)

        print("actual percentages", percentages)
        print("desired percentages", desired_percentages)

        self.assertEqual(len(percentages), len(desired_percentages))

        # check that values are close
        for x, y in zip(percentages, desired_percentages):
            self.assertAlmostEqual(x, y, delta=float(x) / 5.0)

    def test_weighted_distribution_int(self):
        num_desired_weights = [9, 4, 1, 10, 5]
        num_list = [1, 2, 3, 4, 5]
        dfAlpha = (datagen.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("code", "integer", values=num_list,
                               weights=num_desired_weights,
                               random=True)
                   ).build().cache().createOrReplaceTempView("testdata3")
        df_values = spark.sql("""select * from (select code, 
                                                    count(code) as rc from testdata3 group by code ) a 
                                                order by code""")

        df_values.printSchema()
        emitted_values = df_values.collect()
        values = [x.code for x in emitted_values]
        print("row values:", values)
        total_count = sum([x.rc for x in emitted_values])

        percentages = weights_as_percentages([x.rc for x in emitted_values])
        desired_percentages = weights_as_percentages(num_desired_weights)

        print("actual percentages", percentages)
        print("desired percentages", desired_percentages)

        self.assertEqual(len(percentages), len(desired_percentages))

        # check that values are close
        for x, y in zip(percentages, desired_percentages):
            self.assertAlmostEqual(x, y, delta=float(x) / 10.0)

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
# runTests([TestWeights])
