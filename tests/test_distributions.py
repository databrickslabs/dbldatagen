from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as dg
import databrickslabs_testdatagenerator.distributions as dist

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand, lit
import unittest
import datetime

spark = dg.SparkSingleton.getLocalInstance("unit tests")

desired_weights = [9, 1, 1, 1]


class TestDistributions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("setting up")
        cls.rows = 10000

        # will have implied column `id` for ordinal of row
        cls.testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=cls.rows, partitions=4)
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

    @classmethod
    def unique_timestamp_seconds(cls):
        return (datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(0)).total_seconds()

    @classmethod
    def weights_as_percentages(cls, w):
        assert w is not None
        total = sum(w)
        percentages = [x / total * 100 for x in w]
        return percentages

    @classmethod
    def get_observed_weights(cls, df, column, values):
        assert df is not None
        assert col is not None
        assert values is not None

        observed_weights = (df.cube(column).count()
                            .withColumnRenamed(column, "value")
                            .withColumnRenamed("count", "rc")
                            .where("value is not null")
                            .collect())

        print(observed_weights)

        counts = {x.value: x.rc for x in observed_weights}
        value_count_pairs = [{'value': x, 'count': counts[x]} for x in values]
        print(value_count_pairs)

        return value_count_pairs

    def assertPercentagesEqual(self, percentages, desired_percentages):
        assert percentages is not None and desired_percentages is not None

        print("actual percentages", percentages)
        print("desired percentages", desired_percentages)

        self.assertEqual(len(percentages), len(desired_percentages))

        # check that values are close
        for x, y in zip(percentages, desired_percentages):
            self.assertAlmostEqual(x, y, delta=float(x) / 5.0)

    # @unittest.skip("not yet debugged")
    def no_basic_distribution(self):
        base_dist = dist.DataDistribution()
        self.assertTrue(base_dist is not None)

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
