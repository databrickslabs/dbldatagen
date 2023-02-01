import datetime
import unittest

import pyspark.sql.functions as F
import pyspark.sql as psql
import pandas as pd
import numpy as np

import dbldatagen as dg
import dbldatagen.distributions as dist

spark = dg.SparkSingleton.getLocalInstance("unit tests")

desired_weights = [9, 1, 1, 1]


class TestDistributions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
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

        # change to test build process
        print("inside setupClass")

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

    def test_basic_np_rng(self):
        base_dist = dist.DataDistribution()

        # check for random number generator
        rng_random = base_dist.get_np_random_generator(-1)
        self.assertIsNotNone(rng_random)
        random_val = rng_random.random()
        self.assertIsNotNone(random_val)
        self.assertIsInstance(random_val, float)

        rng_fixed = base_dist.get_np_random_generator(42)
        self.assertIsNotNone(rng_random)
        random_val2 = rng_fixed.random()
        self.assertIsNotNone(random_val2)
        self.assertIsInstance(random_val2, float)

    def test_basic_np_basic_normal(self):
        base_dist = dist.DataDistribution()

        # check for random number generator
        rnd_expr = base_dist.generateNormalizedDistributionSample()
        self.assertIsNotNone(rnd_expr)
        self.assertIsInstance(rnd_expr, psql.Column)

        rnd_expr2 = base_dist.withRandomSeed(42).generateNormalizedDistributionSample()
        self.assertIsNotNone(rnd_expr2)
        self.assertIsInstance(rnd_expr2, psql.Column)

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
                             seed=42, seedMethod="hash_fieldname")
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

    def test_normal_generation_func(self):
        dist_instance = dist.Normal(20.0, 1.0)  # instance of normal distribution

        data_size = 10000
        # check the normal function
        means = pd.Series(np.full(data_size, 100.0))
        std_deviations = pd.Series(np.full(data_size, 20.0))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.normal_func(means, std_deviations, seeds)

        self.assertTrue(len(results) == len(means))

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)
        print(m1, s1)

        self.assertAlmostEqual(s1, 0.2, delta=0.1)
        self.assertAlmostEqual(m1, 0.5, delta=0.1)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.normal_func(means, std_deviations, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        self.assertAlmostEqual(s2, 0.2, delta=0.1)
        self.assertAlmostEqual(m2, 0.5, delta=0.1)



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

    def test_gamma_generation_func(self):
        dist_instance = dist.Gamma(0.5, 0.5)  # instance of exponential distribution with sc

        data_size = 10000
        # check the normal function
        shapes = pd.Series(np.full(data_size, dist_instance.shape))
        scales = pd.Series(np.full(data_size, dist_instance.scale))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.gamma_func(shapes, scales, seeds)

        self.assertTrue(len(results) == len(scales))

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)
        print(m1, s1)

        self.assertAlmostEqual(s1, 0.075, delta=0.05)
        self.assertAlmostEqual(m1, 0.055, delta=0.05)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.gamma_func(shapes, scales, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        self.assertAlmostEqual(s2, 0.075, delta=0.05)
        self.assertAlmostEqual(m2, 0.055, delta=0.05)

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

    def test_beta_generation_func(self):
        dist_instance = dist.Beta(0.5, 0.5)  # instance of beta distribution

        data_size = 10000
        # check the normal function
        alphas = pd.Series(np.full(data_size, dist_instance.alpha))
        betas = pd.Series(np.full(data_size, dist_instance.beta))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.beta_func(alphas, betas, seeds)

        self.assertTrue(len(results) == len(alphas))

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)

        self.assertAlmostEqual(s1, 0.35, delta=0.1)
        self.assertAlmostEqual(m1, 0.5, delta=0.1)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.beta_func(alphas, betas, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        self.assertAlmostEqual(s2, 0.35, delta=0.1)
        self.assertAlmostEqual(m2, 0.5, delta=0.1)


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

    def test_exponential_generation_func(self):
        dist_instance = dist.Exponential(0.5)  # instance of exponential distribution with sc

        data_size = 10000
        # check the normal function
        scales = pd.Series(np.full(data_size, dist_instance.scale))
        seeds = pd.Series(np.full(data_size, 42, dtype=np.int32))
        results = dist_instance.exponential_func(scales, seeds)

        self.assertTrue(len(results) == len(scales))

        # get normalized mean and stddev
        s1 = np.std(results)
        m1 = np.mean(results)

        self.assertAlmostEqual(s1, 0.10, delta=0.05)
        self.assertAlmostEqual(m1, 0.10, delta=0.05)

        seeds2 = pd.Series(np.full(data_size, -1, dtype=np.int32))
        results2 = dist_instance.exponential_func(scales, seeds2)
        s2 = np.std(results2)
        m2 = np.mean(results2)

        self.assertAlmostEqual(s2, 0.10, delta=0.05)
        self.assertAlmostEqual(m2, 0.10, delta=0.05)


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
