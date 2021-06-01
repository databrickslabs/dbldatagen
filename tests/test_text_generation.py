from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from pyspark.sql.types import BooleanType, DateType
import databrickslabs_testdatagenerator as dg
from databrickslabs_testdatagenerator import NRange, ILText
import databrickslabs_testdatagenerator.distributions as dist
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, udf, when, rand

import unittest
import re
import pandas as pd

schema = StructType([
    StructField("PK1", StringType(), True),
    StructField("LAST_MODIFIED_UTC", TimestampType(), True),
    StructField("date", DateType(), True),
    StructField("str1", StringType(), True),
    StructField("nint", IntegerType(), True),
    StructField("nstr1", StringType(), True),
    StructField("nstr2", StringType(), True),
    StructField("nstr3", StringType(), True),
    StructField("nstr4", StringType(), True),
    StructField("nstr5", StringType(), True),
    StructField("nstr6", StringType(), True),
    StructField("email", StringType(), True),
    StructField("ip_addr", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("isDeleted", BooleanType(), True)
])

# add the following if using pandas udfs
#    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \


spark = dg.SparkSingleton.getLocalInstance("unit tests")

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "500")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")


# Test manipulation and generation of test data for a large schema
class TestTextGeneration(unittest.TestCase):
    testDataSpec = None
    row_count = 1000000
    partitions_requested = 24

    def setUp(self):
        print("setting up TestTextDataGenerationTests")
        print("schema", schema)

    @classmethod
    def setUpClass(cls):
        print("setting up class ")
        cls.testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                             partitions=cls.partitions_requested)
                            .withSchema(schema)
                            .withIdOutput()
                            .withColumnSpec("date", percent_nulls=10.0)
                            .withColumnSpec("nint", percent_nulls=10.0, min=1, max=9, step=2)
                            .withColumnSpec("nstr1", percent_nulls=10.0, min=1, max=9, step=2)
                            .withColumnSpec("nstr2", percent_nulls=10.0, min=1.5, max=2.5, step=0.3, format="%04f")
                            .withColumnSpec("nstr3", min=1.0, max=9.0, step=2.0)
                            .withColumnSpec("nstr4", percent_nulls=10.0, min=1, max=9, step=2, format="%04d")
                            .withColumnSpec("nstr5", percent_nulls=10.0, min=1.5, max=2.5, step=0.3, random=True)
                            .withColumnSpec("nstr6", percent_nulls=10.0, min=1.5, max=2.5, step=0.3, random=True,
                                            format="%04f")
                            .withColumnSpec("email", template=r'\w.\w@\w.com|\w@\w.co.u\k')
                            .withColumnSpec("ip_addr", template=r'\n.\n.\n.\n')
                            .withColumnSpec("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                            )

    def test_simple_data(self):
        self.testDataSpec.build().show()

    def test_simple_data2(self):
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("date", percent_nulls=10.0)
                         .withColumnSpecs(patterns="n.*", match_types=StringType(),
                                          percent_nulls=10.0, min=1, max=9, step=2)
                         .withColumnSpecs(patterns="n.*", match_types=IntegerType(),
                                          percent_nulls=10.0, min=1, max=200, step=-2)
                         .withColumnSpec("email", template=r'\w.\w@\w.com|\w@\w.co.u\k')
                         .withColumnSpec("ip_addr", template=r'\n.\n.\n.\n')
                         .withColumnSpec("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                         )
        testDataSpec2.build().show()

    def test_multi_columns(self):
        testDataSpec3 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], base_column_type="values",
                                     template=r"\v-1")
                         )

        testDataSpec3.build().show()

    def test_multi_columns2(self):
        testDataSpec4 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], base_column_type="values",
                                     template=r"\v0-\v1")
                         )
        # in this case we expect values of the form `<first-value> - <second-value>`
        testDataSpec4.build().show()

    def test_multi_columns3(self):
        testDataSpec5 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], base_column_type="values",
                                     template=r"\v\0-\v\1")
                         )

        # in this case we expect values of the form `[ array of values]0 - [array of values]1`
        testDataSpec5.build().show()

    def test_multi_columns4(self):
        testDataSpec6 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], base_column_type="hash",
                                     template=r"\v0-\v1")
                         )
        # here the values for val3 are undefined as the base value for the column is a hash of the base columns
        testDataSpec6.build().show()

    def test_multi_columns5(self):
        testDataSpec7 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], base_column_type="hash",
                                     format="%s")
                         )
        # here the values for val3 are undefined as the base value for the column is a hash of the base columns
        testDataSpec7.build().show()

    def test_multi_columns6(self):
        testDataSpec8 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], base_column_type="values",
                                     format="%s")
                         )
        # here the values for val3 are undefined as the base value for the column is a hash of the base columns
        testDataSpec8.build().show()

    def test_multi_columns7(self):
        testDataSpec9 = (dg.DataGenerator(sparkSession=spark, name="test_data_set3", rows=self.row_count,
                                          partitions=self.partitions_requested, verbose=True)
                         .withIdOutput()
                         .withColumn("val1", IntegerType(), percent_nulls=10.0)
                         .withColumn("val2", IntegerType(), percent_nulls=10.0)
                         .withColumn("val3", StringType(), base_column=["val1", "val2"], format="%s")
                         )
        # here the values for val3 are undefined as the base value for the column is a hash of the base columns
        testDataSpec9.build().show()

    def test_prefix(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=100000, partitions=20)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", min=1, max=20, step=1)
                .withColumn("code3", "integer", min=1, max=20, step=1)
                .withColumn("code4", "integer", min=1, max=20, step=1)
                # base column specifies dependent column

                .withColumn("site_cd", "string", prefix='site', base_column='code1')
                .withColumn("device_status", "string", min=1, max=200, step=1, prefix='status', random=True)

                .withColumn("site_cd2", "string", prefix='site', base_column='code1', text_separator=":")
                .withColumn("device_status2", "string", min=1, max=200, step=1, prefix='status', text_separator=":")

        )

        df = testdata_generator.build()  # build our dataset

        numRows = df.count()

        self.assertEqual(numRows, 100000)

        df2 = testdata_generator.option("starting_id", 200000).build()  # build our dataset

        df2.count()

        print("output columns", testdata_generator.getOutputColumnNames())

        # check `code` values
        code1_values = [ r[0] for r in df.select("code1").distinct().collect() ]
        self.assertSetEqual(set(code1_values), set(range(1,21)))

        code2_values = [ r[0] for r in df.select("code2").distinct().collect() ]
        self.assertSetEqual(set(code2_values), set(range(1,21)))

        code3_values = [ r[0] for r in df.select("code3").distinct().collect() ]
        self.assertSetEqual(set(code3_values), set(range(1,21)))

        code4_values = [ r[0] for r in df.select("code3").distinct().collect() ]
        self.assertSetEqual(set(code4_values), set(range(1,21)))

        site_codes = [ f"site_{x}" for x in range(1,21)]
        site_code_values = [ r[0] for r in df.select("site_cd").distinct().collect() ]
        self.assertSetEqual(set(site_code_values), set(site_codes))

        status_codes = [ f"status_{x}" for x in range(1,201)]
        status_code_values = [ r[0] for r in df.select("device_status").distinct().collect() ]
        self.assertSetEqual(set(status_code_values), set(status_codes))

        site_codes = [ f"site:{x}" for x in range(1,21)]
        site_code_values = [ r[0] for r in df.select("site_cd2").distinct().collect() ]
        self.assertSetEqual(set(site_code_values), set(site_codes))

        status_codes = [ f"status:{x}" for x in range(1,201)]
        status_code_values = [ r[0] for r in df.select("device_status2").distinct().collect() ]
        self.assertSetEqual(set(status_code_values), set(status_codes))

    def test_suffix(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=100000, partitions=20)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", min=1, max=20, step=1)
                .withColumn("code3", "integer", min=1, max=20, step=1)
                .withColumn("code4", "integer", min=1, max=20, step=1)
                # base column specifies dependent column

                .withColumn("site_cd", "string", suffix='site', base_column='code1')
                .withColumn("device_status", "string", min=1, max=200, step=1, suffix='status', random=True)

                .withColumn("site_cd2", "string", suffix='site', base_column='code1', text_separator=":")
                .withColumn("device_status2", "string", min=1, max=200, step=1, suffix='status', text_separator=":")
        )

        df = testdata_generator.build()  # build our dataset

        numRows = df.count()

        self.assertEqual(numRows, 100000)

        site_codes = [ f"{x}_site" for x in range(1,21)]
        site_code_values = [ r[0] for r in df.select("site_cd").distinct().collect() ]
        self.assertSetEqual(set(site_code_values), set(site_codes))

        status_codes = [ f"{x}_status" for x in range(1,201)]
        status_code_values = [ r[0] for r in df.select("device_status").distinct().collect() ]
        self.assertSetEqual(set(status_code_values), set(status_codes))

        site_codes = [ f"{x}:site" for x in range(1,21)]
        site_code_values = [ r[0] for r in df.select("site_cd2").distinct().collect() ]
        self.assertSetEqual(set(site_code_values), set(site_codes))

        status_codes = [ f"{x}:status" for x in range(1,201)]
        status_code_values = [ r[0] for r in df.select("device_status2").distinct().collect() ]
        self.assertSetEqual(set(status_code_values), set(status_codes))

    def test_prefix_and_suffix(self):
        # will have implied column `id` for ordinal of row
        testdata_generator = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=100000, partitions=20)
                .withIdOutput()  # id column will be emitted in the output
                .withColumn("code1", "integer", min=1, max=20, step=1)
                .withColumn("code2", "integer", min=1, max=20, step=1)
                .withColumn("code3", "integer", min=1, max=20, step=1)
                .withColumn("code4", "integer", min=1, max=20, step=1)
                # base column specifies dependent column

                .withColumn("site_cd", "string", suffix='site', base_column='code1', prefix="test")
                .withColumn("device_status", "string", min=1, max=200, step=1, suffix='status',  prefix="test")

                .withColumn("site_cd2", "string", suffix='site', base_column='code1', text_separator=":", prefix="test")
                .withColumn("device_status2", "string", min=1, max=200, step=1, suffix='status', text_separator=":", prefix="test")
        )

        df = testdata_generator.build()  # build our dataset

        numRows = df.count()

        self.assertEqual(numRows, 100000)

        status_codes = [ f"test_{x}_status" for x in range(1,201)]
        status_code_values = [ r[0] for r in df.select("device_status").distinct().collect() ]
        self.assertSetEqual(set(status_code_values), set(status_codes))

        site_codes = [ f"test:{x}:site" for x in range(1,21)]
        site_code_values = [ r[0] for r in df.select("site_cd2").distinct().collect() ]
        self.assertSetEqual(set(site_code_values), set(site_codes))

        status_codes = [ f"test:{x}:status" for x in range(1,201)]
        status_code_values = [ r[0] for r in df.select("device_status2").distinct().collect() ]
        self.assertSetEqual(set(status_code_values), set(status_codes))



# run the tests
# if __name__ == '__main__':
#  print("Trying to run tests")
#  unittest.main(argv=['first-arg-is-ignored'],verbosity=2,exit=False)

# def runTests(suites):
#    suite = unittest.TestSuite()
#    result = unittest.TestResult()
#    for testSuite in suites:
#        suite.addTest(unittest.makeSuite(testSuite))
#    runner = unittest.TextTestRunner()
#    print(runner.run(suite))


# runTests([TestBasicOperation])