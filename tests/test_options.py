import unittest
import re

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestUseOfOptions(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_basic(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=20000, partitions=4)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=True)
                .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
                .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, random=True)
                # base column specifies dependent column

                .withColumn("site_cd", "string", prefix='site', baseColumn='code1')
                .withColumn("device_status", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
                .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
                .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
        )

        df = testdata_generator.build()  # build our dataset

        numRows = df.count()

        self.assertEqual(numRows, 20000)

        print("output columns", testdata_generator.getOutputColumnNames())

        df.show()

        df2 = testdata_generator.option("startingId", 200000).build()  # build our dataset

        df2.count()

        print("output columns", testdata_generator.getOutputColumnNames())

        df2.show()

        # check `code` values
        code1_values = [r[0] for r in df.select("code1").distinct().collect()]
        self.assertSetEqual(set(code1_values), set(range(1, 21)))

        code2_values = [r[0] for r in df.select("code2").distinct().collect()]
        self.assertSetEqual(set(code2_values), set(range(1, 21)))

        code3_values = [r[0] for r in df.select("code3").distinct().collect()]
        self.assertSetEqual(set(code3_values), set(range(1, 21)))

        code4_values = [r[0] for r in df.select("code3").distinct().collect()]
        self.assertSetEqual(set(code4_values), set(range(1, 21)))

        site_codes = [f"site_{x}" for x in range(1, 21)]
        site_code_values = [r[0] for r in df.select("site_cd").distinct().collect()]
        self.assertSetEqual(set(site_code_values), set(site_codes))

        status_codes = [f"status_{x}" for x in range(1, 201)]
        status_code_values = [r[0] for r in df.select("device_status").distinct().collect()]
        self.assertSetEqual(set(status_code_values), set(status_codes))

        # check `tech` values
        tech_values = [r[0] for r in df.select("tech").distinct().collect()]
        self.assertSetEqual(set(tech_values), set(["GSM", "UMTS", "LTE", "UNKNOWN"]))

        # check test cell values
        test_cell_values = [r[0] for r in df.select("test_cell_flg").distinct().collect()]
        self.assertSetEqual(set(test_cell_values), {0, 1})

    def test_aliased_options(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=10000, partitions=4)
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, distribution="normal")
                .withColumn("code3", "integer", min=1, max=20, step=1, base_column="code1")
                .withColumn("code4", "integer", min=1, max=20, step=1, baseColumn="code1")

                # implicit allows column definition to be overridden - used by system when initializing from schema
                .withColumn("code5", "integer", min=1, max=20, step=1, baseColumn="code1", implicit=True)

                .withColumn("code5", "integer", min=1, max=20, step=1, baseColumn="code4", random_seed=45)
                .withColumn("code6", "integer", minValue=1, maxValue=20, step=1, omit=True)
                .withColumn("code7", "integer", min=1, max=20, step=1, baseColumn="code6")
                .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, distribution="normal")
                .withColumn("site_cd1", "string", prefix='site', baseColumn='code1', text_separator="")
                .withColumn("site_cd2", "string", prefix='site', baseColumn='code1', textSeparator="-")

        )

        colSpec1 = testdata_generator.getColumnSpec("site_cd1")

        print("options", colSpec1.specOptions)

        val1 = colSpec1.getOrElse("textSeparator", "n/a")
        self.assertEqual("", val1, "invalid `textSeparator` option value for ``site_cd1``")

        val2 = colSpec1.getOrElse("text_separator", "n/a")
        self.assertEqual("", val2, "invalid `text_separator` option value for ``site_cd1``")

        colSpec2 = testdata_generator.getColumnSpec("site_cd2")
        val3 = colSpec2.getOrElse("textSeparator", "n/a")
        self.assertEqual("-", val3, "invalid `textSeparator` option value")

        df = testdata_generator.build()  # build our dataset

        match_pattern1 = re.compile(r"\s*site[0-9]+")
        match_pattern2 = re.compile(r"\s*site-[0-9]+")

        df.show()

        output = df.limit(100).collect()

        for row in output:
            site_cd1 = row["site_cd1"]
            print("site code", site_cd1)
            self.assertIsNotNone(site_cd1)
            self.assertTrue(match_pattern1.match(site_cd1))

            site_cd2 = row["site_cd2"]
            self.assertIsNotNone(site_cd2)
            self.assertTrue(match_pattern2.match(site_cd2))

    def test_aliased_options2(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=10000, partitions=4)
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("site_cd1", "string", prefix='site', baseColumn='code1',
                            random_seed_method=dg.RANDOM_SEED_FIXED)
                .withColumn("site_cd2", "string", prefix='site', baseColumn='code1',
                            randomSeedMethod=dg.RANDOM_SEED_HASH_FIELD_NAME)

        )

        colSpec1 = testdata_generator.getColumnSpec("site_cd1")
        assert "randomSeedMethod" in colSpec1.specOptions, "expecting option ``randomSeedMethod`` for `site_cd1`"

        colSpec2 = testdata_generator.getColumnSpec("site_cd2")
        assert "randomSeedMethod" in colSpec2.specOptions, "expecting option ``randomSeedMethod`` for `site_cd2`"







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
