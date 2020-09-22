from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from datetime import datetime, timedelta
from databrickslabs_testdatagenerator import DataGenerator
import databrickslabs_testdatagenerator as dg
from databrickslabs_testdatagenerator import NRange, DateRange
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DateType
from datetime import timedelta, datetime

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


class TestQuickTests(unittest.TestCase):
    """These are a set of quick tests to validate some basic behaviors

    The goal for these tests is that they should run fast so focus is on quick execution
    """

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

        results = analyzer.summarize()
        self.assertIsNotNone(results)
        self.assertTrue('min' in results)
        self.assertTrue('max' in results)
        self.assertTrue('count' in results)
        self.assertTrue('stddev' in results)
        print("Summary;", results)


    def test_complex_datagen(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
                        .withColumn("code1a", IntegerType(), unique_values=100)
                        .withColumn("code1b", IntegerType(), min=1, max=200)
                        .withColumn("code2", IntegerType(), max=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        testDataDF2 = testDataSpec.build()

        rowCount = testDataDF2.count()
        self.assertEqual(rowCount, 1000)

        print("schema", testDataDF2.schema)
        testDataDF2.printSchema()

        testDataSpec.computeBuildPlan().explain()

        # testDataDF2.show()

        testDataDF2.createOrReplaceTempView("testdata")
        df_stats = spark.sql("""select min(code1a) as min1a, 
                              max(code1a) as max1a, 
                              min(code1b) as min1b, 
                              max(code1b) as max1b,
                              min(code2) as min2, 
                              max(code2) as max2
                              from testdata""")
        stats = df_stats.collect()[0]

        print("stats", stats)

        # self.assertEqual(stats.min1, 1)
        # self.assertEqual(stats.min2, 1)
        self.assertLessEqual(stats.max1b, 200)
        self.assertGreaterEqual(stats.min1b, 1)


    def test_generate_name(self):
        print("test_generate_name")
        n1 = DataGenerator.generateName()
        n2 = DataGenerator.generateName()
        self.assertIsNotNone(n1)
        self.assertIsNotNone(n2)
        self.assertNotEqual(n1, n2, "Names should be different")

    def test_column_specifications(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_column_specifications")
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc", "s"]))
        self.assertEqual(expectedColumns, set(([x.name for x in tgen.allColumnSpecs])))

    def test_inferred_columns(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_inferred_columns")
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc", "s"]))
        print("inferred columns", tgen.getInferredColumnNames())
        self.assertEqual(expectedColumns, set((tgen.getInferredColumnNames())))

    def test_output_columns(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_output_columns")
        expectedColumns = set((["site_id", "site_cd", "c", "c1", "sector_status_desc"]))
        print("output columns", tgen.getOutputColumnNames())
        self.assertEqual(expectedColumns, set((tgen.getOutputColumnNames())))

    def test_with_column_spec_for_missing_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_missing_column")
        with self.assertRaises(Exception):
            t2 = tgen.withColumnSpec("site_dwkey", min=1, max=200, step=1, random=True)

    @unittest.expectedFailure
    def test_with_column_spec_for_missing_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_missing_column")
        # with self.assertRaises(Exception):
        t2 = tgen.withColumnSpec("d", min=1, max=200, step=1, random=True)

    @unittest.expectedFailure
    def test_with_column_spec_for_duplicate_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_duplicate_column")
        # with self.assertRaises(Exception):
        t2 = tgen.withColumnSpec("site_id", min=1, max=200, step=1, random=True)
        t2 = t2.withColumnSpec("site_id", min=1, max=200, step=1, random=True)

    # @unittest.expectedFailure
    def test_with_column_spec_for_duplicate_column2(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_duplicate_column2")
        t2 = tgen.withColumn("site_id", "string", min=1, max=200, step=1, random=True)

    def test_with_column_spec_for_id_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), min=1, max=200, step=1, prefix='status', random=True)
                .withColumn("s", StringType(), min=1, max=200, step=1, prefix='status', random=True, omit=True))

        print("test_with_column_spec_for_id_column")
        t2 = tgen.withIdOutput()
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc"]))
        print("output columns", t2.getOutputColumnNames())
        print("inferred columns", t2.getInferredColumnNames())
        self.assertEqual(expectedColumns, set((t2.getOutputColumnNames())))

    def test_basic_ranges_with_view(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="ranged_data", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("code1a", IntegerType(), unique_values=100)
                        .withColumn("code1b", IntegerType(), min=1, max=100)
                        .withColumn("code1c", IntegerType(), min=1, max=200, unique_values=100)
                        .withColumn("code1d", IntegerType(), min=1, max=200, step=3, unique_values=50)
                        .withColumn("code2", IntegerType(), max=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        rangedDF = testDataSpec.build(withTempView=True).cache()

        result = spark.sql("""select count(distinct code1a), 
                                     count(distinct code1b), 
                                     count(distinct code1c) 
                                     from ranged_data""").collect()[0]
        self.assertEqual(100, result[0])
        self.assertEqual(100, result[1])
        self.assertEqual(100, result[2])

    def test_basic_formatting(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), min=1, max=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("str5b", StringType(), format="test %s", base_column=["val1", "val2"],
                                    values=["one", "two", "three"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", base_column=["val1", "val2"])
                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        self.assertEqual(rowCount, 100000)

    def test_reversed_ranges(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="ranged_data", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), min=100, max=1, step=-1)
                        .withColumn("val2", IntegerType(), min=100, max=1, step=-3, unique_values=5)
                        .withColumn("val3", IntegerType(), data_range=NRange(100, 1, -1), unique_values=5)
                        .withColumn("val4", IntegerType(), min=1, max=100, step=3, unique_values=5)
                        .withColumn("code1b", IntegerType(), min=1, max=100)
                        .withColumn("code1c", IntegerType(), min=1, max=200, unique_values=100)
                        .withColumn("code1d", IntegerType(), min=1, max=200)
                        .withColumn("code2", IntegerType(), max=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        rangedDF = testDataSpec.build()
        rangedDF.show()

        rowCount = rangedDF.count()
        self.assertEqual(rowCount, 100000)

    def test_date_time_ranges(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="ranged_data", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("last_sync_ts", "timestamp",
                                    data_range=DateRange("2017-10-01 00:00:00",
                                                         "2018-10-06 00:00:00",
                                                         "days=1,hours=1"))
                        .withColumn("last_sync_ts", "timestamp",
                                    data_range=DateRange("2017-10-01 00:00:00",
                                                         "2018-10-06 00:00:00",
                                                         "days=1,hours=1"), unique_values=5)

                        .withColumn("last_sync_ts", "timestamp",
                                    data_range=DateRange("2017-10-01",
                                                         "2018-10-06",
                                                         "days=7",
                                                         datetime_format="%Y-%m-%d"))

                        .withColumn("last_sync_dt1", DateType(),
                                    data_range=DateRange("2017-10-01 00:00:00",
                                                         "2018-10-06 00:00:00",
                                                         "days=1"))
                        .withColumn("last_sync_dt2", DateType(),
                                    data_range=DateRange("2017-10-01 00:00:00",
                                                         "2018-10-06 00:00:00",
                                                         "days=1"), unique_values=5)

                        .withColumn("last_sync_date", DateType(),
                                    data_range=DateRange("2017-10-01",
                                                         "2018-10-06",
                                                         "days=7",
                                                         datetime_format="%Y-%m-%d"))

                        )

        rangedDF = testDataSpec.build()
        rangedDF.show()

        rowCount = rangedDF.count()
        self.assertEqual(rowCount, 100000)

        # TODO: add additional validation statement

    def test_script_table(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), min=1, max=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("str5b", StringType(), format="test %s", base_column=["val1", "val2"],
                                    values=["one", "two", "three"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", base_column=["val1", "val2"])
                        )

        script=testDataSpec.scriptTable(name="Test")
        print(script)

        self.assertIsNotNone(script)

        output_columns = testDataSpec.getOutputColumnNames()
        print(output_columns)
        self.assertSetEqual(set(output_columns), {'id', 'val1', 'val2', 'str1', 'str2', 'str3', 'str4', 'str5',
                                                  'str5a', 'str5b',   'str6'})

        self.assertIsNotNone(script)

        self.assertIn("CREATE TABLE IF NOT EXISTS", script)

        for col in output_columns:
            self.assertTrue(col in script)

    def test_script_merge1(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), min=1, max=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("action", StringType(), format="test %s", base_column=["val1", "val2"],
                                    values=["INS", "DEL", "UPDATE"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", base_column=["val1", "val2"])
                        )

        script = testDataSpec.scriptMerge(tgt_name="Test", src_name="TestInc", join_expr="src.id=tgt.id",
                                       del_expr="src.action='DEL'", update_expr="src.action='UPDATE")
        print(script)

        output_columns = testDataSpec.getOutputColumnNames()
        print(output_columns)
        self.assertSetEqual(set(output_columns), {'id', 'val1', 'val2', 'str1', 'str2', 'str3', 'str4', 'str5', 'str5a', 'action', 'str6'})

        self.assertIsNotNone(script)

        self.assertIn("WHEN MATCHED", script)
        self.assertIn("WHEN NOT MATCHED", script)
        self.assertIn("MERGE INTO", script)

        for col in output_columns:
            self.assertTrue(col in script)

    def test_script_merge_min(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), min=1, max=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", base_column=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", base_column=["val1", "val2"])
                        .withColumn("action", StringType(), format="test %s", base_column=["val1", "val2"],
                                    values=["INS", "DEL", "UPDATE"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", base_column=["val1", "val2"])
                        )

        script=testDataSpec.scriptMerge(tgt_name="Test", src_name="TestInc", join_expr="src.id=tgt.id")
        self.assertIsNotNone(script)

        print(script)

        output_columns = testDataSpec.getOutputColumnNames()
        print(output_columns)
        self.assertSetEqual(set(output_columns), {'id', 'val1', 'val2', 'str1', 'str2', 'str3', 'str4', 'str5', 'str5a', 'action', 'str6'})

        self.assertIsNotNone(script)

        self.assertIn("WHEN MATCHED", script)
        self.assertIn("WHEN NOT MATCHED", script)
        self.assertIn("MERGE INTO", script)

        for col in output_columns:
            self.assertTrue(col in script)


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
