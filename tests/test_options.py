import unittest

import databrickslabs_testdatagenerator as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestUseOfOptions(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_basic(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=100000, partitions=20)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=True)
                .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
                .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, random=True)
                # base column specifies dependent column

                .withColumn("site_cd", "string", prefix='site', base_column='code1')
                .withColumn("device_status", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
                .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
                .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
        )

        df = testdata_generator.build()  # build our dataset

        numRows = df.count()

        self.assertEqual(numRows, 100000)

        print("output columns", testdata_generator.getOutputColumnNames())

        df.show()

        df2 = testdata_generator.option("starting_id", 200000).build()  # build our dataset

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
