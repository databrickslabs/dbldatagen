import datetime
import unittest

from pyspark.sql.functions import col

import databrickslabs_testdatagenerator as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")

desired_weights = [9, 1, 1, 1]


class TestWeights(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("setting up")
        cls.rows = 100000

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
    def tearDownClass(cls):
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

    def assertPercentagesEqual(self, percentages, desired_percentages, target_delta=0.2):
        assert percentages is not None and desired_percentages is not None

        print("actual percentages", percentages)
        print("desired percentages", desired_percentages)

        self.assertEqual(len(percentages), len(desired_percentages))

        # check that values are close
        for x, y in zip(percentages, desired_percentages):
            self.assertAlmostEqual(x, y, delta=float(x) * target_delta)

    def test_get_observed_weights(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("pk1", "int", unique_values=100)
                   .withColumn("alpha", "string", values=alpha_list, baseColumn="pk1",
                               weights=alpha_desired_weights, random=True)
                   )
        dfAlpha = dsAlpha.build().cache()

        values = dsAlpha['alpha'].values
        self.assertTrue(values is not None)

        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', values)
        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])

        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    def test_basic(self):
        print(self.unique_timestamp_seconds())

        df = self.testdata_generator.build()
        row_count = df.count()
        print("row count:", row_count)
        self.assertEqual(row_count, self.rows)

    def test_basic2(self):
        count = spark.sql("select count(*) as rc from testdata").take(1)[0].rc
        self.assertEqual(count, self.rows)

    def test_generate_values(self):
        df_values = spark.sql(
            "select * from (select tech, count(tech) as rc from testdata group by tech ) a order by tech").collect()
        values = [x.tech for x in df_values]
        print("row values:", values)
        total_count = sum([x.rc for x in df_values])
        self.assertEqual(total_count, self.rows)

        percentages = self.weights_as_percentages([x.rc for x in df_values])
        desired_percentages = self.weights_as_percentages(desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    def test_weighted_distribution(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("alpha", "string", values=alpha_list,
                               weights=alpha_desired_weights,
                               random=True)
                   )
        dfAlpha = dsAlpha.build().cache()
        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', dsAlpha['alpha'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    def test_weighted_distribution_nr(self):
        """ Test distribution of values with weights for non random values"""
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")

        # dont use seed value as non random fields should be repeatable
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("alpha", "string", values=alpha_list,
                               weights=alpha_desired_weights)
                   )
        dfAlpha = dsAlpha.build().cache()
        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', dsAlpha['alpha'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    # @unittest.skip("not yet finalized")
    def test_weighted_distribution_nr2(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")

        # dont use seed value as non random fields should be repeatable
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("pk1", "int", unique_values=500)
                   .withColumn("pk2", "int", unique_values=500)
                   .withColumn("alpha", "string", values=alpha_list, baseColumn="pk1",
                               weights=alpha_desired_weights)
                   )
        dfAlpha = dsAlpha.build().cache()
        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', dsAlpha['alpha'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

        # for columns with non random values and a single base dependency `pk1`
        # each combination of pk1 and alpha should be the same

        df_counts = (dfAlpha.cube("pk1", "alpha")
                     .count()
                     .where("pk1 is not null and alpha is not null")
                     .orderBy("pk1").withColumnRenamed("count", "rc")
                     )

        # get counts for each primary key from the cube
        # they should be 1 for each primary key
        df_counts_by_key = df_counts.distinct().groupBy("pk1").count().withColumnRenamed("count", "rc")
        self.assertEqual(df_counts_by_key.where("rc > 1").count(), 0)

    def test_weighted_distribution2(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("pk1", "int", unique_values=500)
                   .withColumn("pk2", "int", unique_values=500)
                   .withColumn("alpha", "string", values=alpha_list, baseColumn="pk1",
                               weights=alpha_desired_weights, random=True)
                   )
        dfAlpha = dsAlpha.build().cache()
        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', dsAlpha['alpha'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    def test_weighted_distribution3(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("pk1", "int", unique_values=500)
                   .withColumn("pk2", "int", unique_values=500)
                   .withColumn("alpha", "string", values=alpha_list, baseColumn=["pk1", "pk2"],
                               weights=alpha_desired_weights, random=True)
                   )
        dfAlpha = dsAlpha.build().cache()
        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', dsAlpha['alpha'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    def test_weighted_distribution_nr3(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")

        # dont use seed value as non random fields should be repeatable
        dsAlpha = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4, debug=True)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("pk1", "int", unique_values=500)
                   .withColumn("pk2", "int", unique_values=500)
                   .withColumn("alpha", "string", values=alpha_list, baseColumn=["pk1", "pk2"],
                               weights=alpha_desired_weights)
                   )
        dfAlpha = dsAlpha.build().cache()
        observed_weights = self.get_observed_weights(dfAlpha, 'alpha', dsAlpha['alpha'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(alpha_desired_weights)

        # note for multiple base fields which will use hashing of the field values as the base for the derived fields,
        # there may be substantial deviation from the desired target percentages
        self.assertPercentagesEqual(percentages, desired_percentages, target_delta=1.5)

        # for columns with non random values and base dependency on `pk1` and `pk2`
        # each combination of pk1, pk2 and alpha should be the same

        df_counts = (dfAlpha.cube("pk1", "pk2", "alpha")
                     .count()
                     .where("pk1 is not null and alpha is not null and pk2 is not null")
                     .orderBy("pk1", "pk2").withColumnRenamed("count", "rc")
                     )

        # get counts for each primary key from the cube
        # they should be 1 for each primary key
        df_counts_by_key = df_counts.distinct().groupBy("pk1", "pk2").count().withColumnRenamed("count", "rc")
        self.assertEqual(df_counts_by_key.where("rc > 1").count(), 0)

    def test_weighted_distribution_int(self):
        num_desired_weights = [9, 4, 1, 10, 5]
        num_list = [1, 2, 3, 4, 5]
        dsInt1 = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4)
                  .withIdOutput()  # id column will be emitted in the output
                  .withColumn("code", "integer", values=num_list,
                              weights=num_desired_weights,
                              random=True)
                  )
        dfInt1 = dsInt1.build().cache()

        observed_weights = self.get_observed_weights(dfInt1, 'code', dsInt1['code'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(num_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    def test_weighted_nr_int(self):
        """ Test distribution of non-random values where field is a integer"""
        num_desired_weights = [9, 4, 1, 10, 5]
        num_list = [1, 2, 3, 4, 5]

        # dont use seed value as non random fields should be repeatable
        dsInt1 = (dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=26 * 10000, partitions=4, debug=True)
                  .withIdOutput()  # id column will be emitted in the output
                  .withColumn("code", "integer", values=num_list,
                              weights=num_desired_weights, base_column_type="hash")
                  )
        dfInt1 = dsInt1.build().cache()

        observed_weights = self.get_observed_weights(dfInt1, 'code', dsInt1['code'].values)

        percentages = self.weights_as_percentages([x["count"] for x in observed_weights])
        desired_percentages = self.weights_as_percentages(num_desired_weights)

        self.assertPercentagesEqual(percentages, desired_percentages)

    # @unittest.skip("not yet finalized")
    def test_weighted_repeatable_non_random(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")

        # dont use seed value as non random fields should be repeatable
        dsAlpha = (dg.DataGenerator(sparkSession=spark,
                                    name="test_dataset1",
                                    rows=26 * 1000,
                                    partitions=4)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("alpha", "string", values=alpha_list,
                               weights=alpha_desired_weights)
                   )

        dfAlpha = dsAlpha.build().limit(100).cache()
        values1 = dfAlpha.collect()
        print(values1)

        dfAlpha2 = dsAlpha.clone().build().limit(100).cache()
        values2 = dfAlpha2.collect()

        self.assertEqual(values1, values2)

    def test_weighted_repeatable_random(self):
        alpha_desired_weights = [9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5,
                                 9, 4, 1, 10, 5, 9
                                 ]
        alpha_list = list("abcdefghijklmnopqrstuvwxyz")

        # use seed for random repeatability
        dsAlpha = (dg.DataGenerator(sparkSession=spark,
                                    name="test_dataset1",
                                    rows=26 * 1000,
                                    partitions=4,
                                    seed=43)
                   .withIdOutput()  # id column will be emitted in the output
                   .withColumn("alpha", "string", values=alpha_list,
                               weights=alpha_desired_weights, random=True)
                   )

        dfAlpha = dsAlpha.build().limit(100).cache()
        values1 = dfAlpha.collect()
        print(values1)

        dfAlpha2 = dsAlpha.clone().build().limit(100).cache()
        values2 = dfAlpha2.collect()

        self.assertEqual(values1, values2)

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
