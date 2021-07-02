import unittest

from pyspark.sql.types import ByteType, ShortType, DoubleType, LongType, DecimalType
from pyspark.sql.types import IntegerType, StringType, FloatType
import pyspark.sql.functions as F

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestTypes(unittest.TestCase):
    row_count = 1000
    column_count = 50

    def setUp(self):
        print("setting up")

    @classmethod
    def setUpClass(cls):
        print("setting up class")

    def test_basic_types(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=100000, partitions=id_partitions, verbose=True)
                .withColumn("code1", IntegerType(), minValue=1, maxValue=20, step=1)
                .withColumn("code2", LongType(), maxValue=1000, step=5)
                .withColumn("code3", IntegerType(), minValue=100, maxValue=200, step=1, random=True)
                .withColumn("xcode", StringType(), values=["a", "test", "value"], random=True)
                .withColumn("rating", FloatType(), minValue=1.0, maxValue=5.0, step=0.00001, random=True)
                .withColumn("drating", DoubleType(), minValue=1.0, maxValue=5.0, step=0.00001, random=True))

        df = testdata_defn.build().cache()
        df.printSchema()

        df.show()

        # check column types

        self.assertEqual(IntegerType(), df.schema.fields[0].dataType)
        self.assertEqual(LongType(), df.schema.fields[1].dataType)
        self.assertEqual(IntegerType(), df.schema.fields[2].dataType)
        self.assertEqual(StringType(), df.schema.fields[3].dataType)
        self.assertEqual(FloatType(), df.schema.fields[4].dataType)
        self.assertEqual(DoubleType(), df.schema.fields[5].dataType)

    def test_reduced_range_types(self):
        num_rows = 1000000
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=num_rows, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", ByteType(), minValue=1, maxValue=20, step=1)
                .withColumn("code2", ShortType(), maxValue=1000, step=5))

        testdata_defn.build().createOrReplaceTempView("testdata")
        df = spark.sql("select * from testdata order by basic_short desc, basic_byte desc")

        self.assertEqual(df.count(), num_rows)

        # check that range of code1 and code2 matches expectations
        df_min_max = df.agg(F.min("code1").alias("min_code1"),
                            F.max("code1").alias("max_code1"),
                            F.min("code2").alias("min_code2"),
                            F.max("code2").alias("max_code2"))

        limits = df_min_max.collect()[0]
        self.assertEqual(limits["min_code2"], 0)
        self.assertEqual(limits["min_code1"], 1)
        self.assertEqual(limits["max_code2"], 1000)
        self.assertEqual(limits["max_code1"], 20)

        # check expected types
        types = {x.name: x.dataType for x in df.schema.fields}
        self.assertEqual(type(types["basic_byte"]), type(ByteType()))
        self.assertEqual(type(types["basic_short"]), type(ShortType()))
        self.assertEqual(type(types["code1"]), type(ByteType()))
        self.assertEqual(type(types["code2"]), type(ShortType()))

    @unittest.expectedFailure
    def test_out_of_range_types(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", ByteType(), minValue=1, maxValue=400, step=1))

        testdata_defn.build().createOrReplaceTempView("testdata")
        spark.sql("select * from testdata order by basic_short desc, basic_byte desc").show()

    def test_for_reverse_range(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", ByteType(), minValue=127, maxValue=1, step=-1))

        df = testdata_defn.build().limit(130)
        data_row1 = df.collect()
        print(data_row1[0])
        self.assertEqual(data_row1[0]["code1"], 127, "row0")
        self.assertEqual(data_row1[1]["code1"], 126, "row1")
        self.assertEqual(data_row1[126]["code1"], 1, "row127")
        self.assertEqual(data_row1[127]["code1"], 127, "row128")

    def test_for_reverse_range2(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", ByteType(), minValue=127, maxValue=1, step=-1)
        )

        df = testdata_defn.build().limit(130)
        testdata_defn.explain()
        df.show()

    def test_for_values_with_multi_column_dependencies(self):
        id_partitions = 4
        code_values = ["aa", "bb", "cc", "dd", "ee", "ff"]
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", StringType(),
                            values=code_values,
                            base_column=["basic_byte", "basic_short"])
        )

        df = testdata_defn.build().where("code1 is  null")

        testdata_defn.explain()

        self.assertEqual(df.count(), 0)

        df2 = testdata_defn.build()

        # check unique codes
        unique_code1_count = df2.agg(F.countDistinct("code1").alias("code_count")).collect()[0]["code_count"]
        self.assertEqual(unique_code1_count, 6)

        unique_codes = [x["code1"] for x in df2.select("code1").distinct().collect()]

        self.assertEqual(set(unique_codes), set(code_values))

    def test_for_values_with_single_column_dependencies(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", StringType(),
                            values=["aa", "bb", "cc", "dd", "ee", "ff"],
                            base_column=["basic_byte"])
        )
        df = testdata_defn.build().where("code1 is  null")
        self.assertEqual(df.count(), 0)

    def test_for_values_with_single_column_dependencies2(self):
        id_partitions = 4
        rows_wanted = 1000000
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=rows_wanted, partitions=id_partitions, verbose=True)
                .withIdOutput()
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", StringType(),
                            values=["aa", "bb", "cc", "dd", "ee", "ff"],
                            base_column=["basic_byte"])
        )
        df = testdata_defn.build()
        # df.show()
        testdata_defn.explain()
        self.assertEqual(df.count(), rows_wanted)

    def test_for_values_with_default_column_dependencies(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", StringType(),
                            values=["aa", "bb", "cc", "dd", "ee", "ff"])
        )
        df = testdata_defn.build().where("code1 is  null")
        self.assertEqual(df.count(), 0)
        testdata_defn.explain()

    def test_for_weighted_values_with_default_column_dependencies(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", StringType(),
                            values=["aa", "bb", "cc", "dd", "ee", "ff"],
                            weights=[1, 2, 3, 4, 5, 6])
        )
        df = testdata_defn.build().where("code1 is  null")
        self.assertEqual(df.count(), 0)

    def test_for_weighted_values_with_default_column_dependencies2(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withIdOutput()
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())
                .withColumn("code1", StringType(),
                            values=["aa", "bb", "cc", "dd", "ee", "ff"],
                            weights=[1, 2, 3, 4, 5, 6])
        )
        df = testdata_defn.build().where("code1 is null")
        df.show()

    @unittest.expectedFailure
    def test_out_of_range_types2(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("basic_byte", ByteType())
                .withColumn("basic_short", ShortType())

                .withColumn("code2", ShortType(), maxValue=80000, step=5))

        testdata_defn.build().createOrReplaceTempView("testdata")
        spark.sql("select * from testdata order by basic_short desc, basic_byte desc").show()

    def test_short_types1(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("bb", ByteType(), unique_values=100)
                .withColumn("basic_short", ShortType())

                .withColumn("code2", ShortType(), maxValue=10000, step=5))

        testdata_defn.build().createOrReplaceTempView("testdata")
        data_row = spark.sql("select min(bb) as min_bb, max(bb) as max_bb from testdata ").limit(1).collect()
        self.assertEqual(data_row[0]["min_bb"], 1, "row0")
        self.assertEqual(data_row[0]["max_bb"], 100, "row1")

    def test_short_types1a(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("bb", ByteType(), minValue=35, maxValue=72)
                .withColumn("basic_short", ShortType())

                .withColumn("code2", ShortType(), maxValue=10000, step=5))

        testdata_defn.build().createOrReplaceTempView("testdata")
        data_row = spark.sql("select min(bb) as min_bb, max(bb) as max_bb from testdata ").limit(1).collect()
        self.assertEqual(data_row[0]["min_bb"], 35, "row0")
        self.assertEqual(data_row[0]["max_bb"], 72, "row1")

    def test_short_types1b(self):
        id_partitions = 4

        # result should be the same whether using `minValue` or `min` as options
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("bb", ByteType(), minValue=35, maxValue=72)
                .withColumn("basic_short", ShortType())
                .withColumn("code2", ShortType(), maxValue=10000, step=5))

        testdata_defn.build().createOrReplaceTempView("testdata")
        data_row = spark.sql("select min(bb) as min_bb, max(bb) as max_bb from testdata ").limit(1).collect()
        self.assertEqual(data_row[0]["min_bb"], 35, "row0")
        self.assertEqual(data_row[0]["max_bb"], 72, "row1")

    def test_short_types2(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withColumn("bb", ByteType(), unique_values=100)
                .withColumn("basic_short", ShortType())

                .withColumn("code2", ShortType(), maxValue=4000, step=5))

        testdata_defn.build().show()

    def test_decimal(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withIdOutput()
                .withColumn("code1", DecimalType(10, 3))
                .withColumn("code2", DecimalType(10, 5))
                .withColumn("code3", DecimalType(10, 5), minValue=1.0, maxValue=1000.0)
                .withColumn("code4", DecimalType(10, 5), random=True, continuous=True)
                .withColumn("code5", DecimalType(10, 5), minValue=1.0, maxValue=1000.0, random=True, continuous=True))

        df = testdata_defn.build()
        df.show()

    def test_decimal2(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)
                .withIdOutput()
                .withColumn("code1", DecimalType(10, 3))
                .withColumn("code2", DecimalType(10, 5))
                .withColumn("code3", DecimalType(10, 5), minValue=1.0, maxValue=1000.0)
                .withColumn("code4", DecimalType(10, 5), random=True, continuous=True)
                .withColumn("code5", DecimalType(10, 5), minValue=1.0, maxValue=1000.0, random=True, continuous=True))

        testdata_defn.build().createOrReplaceTempView("testdata")

    def test_decimal_min_and_max_values(self):
        id_partitions = 4
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=1000000, partitions=id_partitions, verbose=True)

                .withIdOutput()
                .withColumn("group1", IntegerType(), expr="1")
                .withColumn("code1", DecimalType(10, 3))
                .withColumn("code2", DecimalType(10, 5))
                .withColumn("code3", DecimalType(10, 5), minValue=1.0, maxValue=1000.0)
                .withColumn("code4", DecimalType(10, 5), random=True, continuous=True)
                .withColumn("code5", DecimalType(10, 5), minValue=2.0, maxValue=2000.0, random=True, continuous=True))

        testdata_defn.build().createOrReplaceTempView("testdata")

        df2 = spark.sql("""select min(code1) as min1, max(code1) as max1, 
                            min(code2) as min2, 
                            max(code2) as max2 ,
                            min(code3) as min3, 
                            max(code3) as max3,
                            min(code4) as min4, 
                            max(code4) as max4,
                            min(code5) as min5, 
                            max(code5) as max5 
                           from testdata group by group1 """)

        results = df2.collect()[0]

        print(results)

        min1, min2, min3, min4, min5 = results['max1'], results['min2'], results['min3'], results['min4'], results[
            'min5']
        max1, max2, max3, max4, max5 = results['max1'], results['max2'], results['max3'], results['max4'], results[
            'max5']

        self.assertGreaterEqual(min1, 0.0)
        self.assertGreaterEqual(min2, 0.0)
        self.assertGreaterEqual(min3, 1.0)
        self.assertGreaterEqual(min4, 0.0)
        self.assertGreaterEqual(min5, 2.0)

        self.assertLessEqual(max1, 9999999.999)
        self.assertLessEqual(max2, 99999.99999)
        self.assertLessEqual(max3, 1000.0)
        self.assertLessEqual(max4, 99999.99999)
        self.assertLessEqual(max5, 2000.0)

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
