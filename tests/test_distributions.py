import datetime
import unittest

import pyspark.sql.functions as F

import dbldatagen as dg
import dbldatagen.distributions as dist

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
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True)
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        cls.testdata_generator.build().cache().createOrReplaceTempView("testdata")

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
        assert column is not None
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

    def no_basic_distribution(self):
        base_dist = dist.DataDistribution()
        self.assertTrue(base_dist is not None)

    def test_basic_normal_distribution(self):
        normal_dist = dist.Normal(mean=0.0, stddev=1.0)
        self.assertIsNotNone(normal_dist)
        print(normal_dist)

        normal_dist2 = normal_dist.withRandomSeed(42)

        self.assertEqual(normal_dist2.randomSeed, 42)
        print(normal_dist2)

        normal_dist3 = normal_dist2.withRounding(True)
        self.assertTrue(normal_dist3.rounding)

    def test_simple_normal_distribution(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True, distribution="normal")
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

    def test_normal_distribution(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True,
                            distribution=dist.Normal(1.0, 1.0))
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

    def test_normal_distribution_seeded1(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4, seed=42)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True,
                            distribution=dist.Normal(1.0, 1.0))
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

    def test_normal_distribution_seeded2(self):
        # will have implied column `id` for ordinal of row
        normal_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4,
                             seed=42, seed_method="hash_fieldname")
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True,
                            distribution=dist.Normal(1.0, 1.0))
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_normal_data = normal_data_generator.build().cache()

        df_summary_general = df_normal_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

    def test_gamma_distribution(self):
        # will have implied column `id` for ordinal of row
        gamma_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True,
                            distribution=dist.Gamma(0.5, 0.5))
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_gamma_data = gamma_data_generator.build().cache()

        df_summary_general = df_gamma_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

    def test_beta_distribution(self):
        # will have implied column `id` for ordinal of row
        beta_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True,
                            distribution=dist.Beta(0.5, 0.5))
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_beta_data = beta_data_generator.build().cache()

        df_summary_general = df_beta_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

    def test_exponential_distribution(self):
        # will have implied column `id` for ordinal of row
        exponential_data_generator = (
            dg.DataGenerator(sparkSession=spark, rows=self.rows, partitions=4)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                .withColumn("code4", "integer", minValue=1, maxValue=40, step=1, random=True,
                            distribution=dist.Exponential(0.5))
                .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                            prefix='status', random=True, distribution="normal")
                .withColumn("tech", "string", values=["GSM", "LTE", "UMTS", "UNKNOWN"],
                            weights=desired_weights,
                            random=True)
        )
        df_exponential_data = exponential_data_generator.build().cache()

        df_summary_general = df_exponential_data.agg(F.min('code4').alias('min_c4'),
                                                F.max('code4').alias('max_c4'),
                                                F.avg('code4').alias('mean_c4'),
                                                F.stddev('code4').alias('stddev_c4'))
        df_summary_general.show()

        summary_data = df_summary_general.collect()[0]

        self.assertEqual(summary_data['min_c4'], 1)
        self.assertEqual(summary_data['max_c4'], 40)

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
