from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.types import DoubleType, ShortType, LongType, DecimalType, ByteType, DateType

import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
import unittest
from datetime import timedelta, datetime, date
from databrickslabs_testdatagenerator import DateRange, NRange
from pyspark.sql.functions import expr, lit, udf, when, rand
import pyspark.sql.functions as F

# build spark session

# global spark

spark = datagen.SparkSingleton.getLocalInstance("ranged values")


class TestRangedValuesAndDates(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_date_range_object(self):
        x = DateRange("2017-10-01 00:00:00",
                      "2018-10-06 11:55:00",
                      "days=7")
        print("date range", x)
        print("min", datetime.fromtimestamp(x.min))
        print("max", datetime.fromtimestamp(x.max))

        # TODO: add validation statement

    def test_date_range_object2(self):
        interval = timedelta(days=7, hours=0)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 6, 11, 55, 0)

        x = DateRange(start, end, interval)
        print("date range", x)
        print("min", datetime.fromtimestamp(x.min))
        print("max", datetime.fromtimestamp(x.max))
        print("min gm", datetime.utcfromtimestamp(x.min))
        print("max gm", datetime.utcfromtimestamp(x.max))

        # TODO: add validation statement

    def test_basic_dates(self):
        interval = timedelta(days=7, hours=1)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 1, 6, 0, 0)

        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
                      .build()
                      )

        self.assertIsNotNone(testDataDF.schema)
        self.assertIs(testDataDF.schema.fields[1].dataType, TimestampType())

        # TODO: add validation statement
        df_min_and_max = testDataDF.agg(F.min("last_sync_dt").alias("min_ts"), F.max("last_sync_dt").alias("max_ts"))

        min_and_max = df_min_and_max.collect()[0]
        min_ts = min_and_max['min_ts']
        max_ts = min_and_max['max_ts']
        self.assertGreaterEqual(min_ts, start)
        self.assertLessEqual(max_ts, end)

        count_distinct = testDataDF.select(F.countDistinct("last_sync_dt")).collect()[0][0]
        self.assertLessEqual(10, count_distinct)

    def test_basic_dates_non_random(self):
        interval = timedelta(days=7, hours=1)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 1, 6, 0, 0)

        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval)
                      .build()
                      )

        self.assertIsNotNone(testDataDF.schema)
        self.assertIs(testDataDF.schema.fields[1].dataType, TimestampType())

        # validation statements
        df_min_and_max = testDataDF.agg(F.min("last_sync_dt").alias("min_ts"), F.max("last_sync_dt").alias("max_ts"))

        min_and_max = df_min_and_max.collect()[0]
        min_ts = min_and_max['min_ts']
        max_ts = min_and_max['max_ts']
        self.assertGreaterEqual(min_ts, start)
        self.assertLessEqual(max_ts, end)

        count_distinct = testDataDF.select(F.countDistinct("last_sync_dt")).collect()[0][0]
        self.assertLessEqual(10, count_distinct)



    def test_date_range1(self):
        interval = timedelta(days=1, hours=1)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 1, 6, 0, 0)

        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
                      .withColumn("last_sync_dt1", "timestamp",
                                  data_range=DateRange(start, end, interval), random=True)

                      .build()
                      )

        self.assertIsNotNone(testDataDF.schema)
        self.assertIs(testDataDF.schema.fields[2].dataType, TimestampType())

        # validation statements
        df_min_and_max = testDataDF.agg(F.min("last_sync_dt1").alias("min_ts"), F.max("last_sync_dt1").alias("max_ts"))

        min_and_max = df_min_and_max.collect()[0]
        min_ts = min_and_max['min_ts']
        max_ts = min_and_max['max_ts']
        self.assertGreaterEqual(min_ts, start)
        self.assertLessEqual(max_ts, end)

        count_distinct = testDataDF.select(F.countDistinct("last_sync_dt1")).collect()[0][0]
        self.assertLessEqual(10, count_distinct)



    def test_date_range2(self):
        interval = timedelta(days=1, hours=1)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 6, 0, 0, 0)

        print(DateRange("2017-10-01 00:00:00",
                        "2018-10-06 00:00:00",
                        "days=1,hours=1"))
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_dt1", "timestamp",
                                  data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 00:00:00",
                                                       "days=1,hours=1"), random=True)

                      .build()
                      )

        self.assertIsNotNone(testDataDF.schema)
        self.assertIs(testDataDF.schema.fields[1].dataType, TimestampType())

        # validation statement
        df_min_and_max = testDataDF.agg(F.min("last_sync_dt1").alias("min_ts"), F.max("last_sync_dt1").alias("max_ts"))

        min_and_max = df_min_and_max.collect()[0]
        min_ts = min_and_max['min_ts']
        max_ts = min_and_max['max_ts']
        self.assertGreaterEqual(min_ts, start)
        self.assertLessEqual(max_ts, end)

    def test_date_range3(self):
        start = date(2017, 10, 1)
        end = date(2018, 10, 6)

        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "date",
                                  data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 11:55:00",
                                                       "days=7"), random=True)

                      .build()
                      )

        self.assertIsNotNone(testDataDF.schema)
        self.assertIs(testDataDF.schema.fields[1].dataType, DateType())

        # TODO: add validation statement
        df_min_and_max = testDataDF.agg(F.min("last_sync_date").alias("min_dt"), F.max("last_sync_date").alias("max_dt"))

        min_and_max = df_min_and_max.collect()[0]
        min_dt = min_and_max['min_dt']
        max_dt = min_and_max['max_dt']
        self.assertGreaterEqual(min_dt, start)
        self.assertLessEqual(max_dt, end)

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        self.assertEqual(df_outside2.count(), 0)

    def test_date_range3a(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "date",
                                  data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 00:00:00",
                                                       "days=7"))

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    def test_date_range4(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "date",
                                  data_range=DateRange("2017-10-01",
                                                       "2018-10-06",
                                                       "days=7",
                                                       datetime_format="%Y-%m-%d"), random=True)

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    def test_date_range4a(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "date",
                                  data_range=DateRange("2017-10-01",
                                                       "2018-10-06",
                                                       "days=7",
                                                       datetime_format="%Y-%m-%d"))

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    # @unittest.skip("not yet finalized")
    def test_timestamp_range3(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "timestamp",
                                  data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 00:00:00",
                                                       "days=7"), random=True)

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    def test_timestamp_range3a(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "timestamp",
                                  data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 00:00:00",
                                                       "days=7"))

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    def test_timestamp_range4(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "timestamp",
                                  data_range=DateRange("2017-10-01",
                                                       "2018-10-06",
                                                       "days=7",
                                                       datetime_format="%Y-%m-%d"), random=True)

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    def test_timestamp_range4a(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "timestamp",
                                  data_range=DateRange("2017-10-01",
                                                       "2018-10-06",
                                                       "days=7",
                                                       datetime_format="%Y-%m-%d"))

                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1 = testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEqual(df_outside1.count(), 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEqual(df_outside2.count(), 0)

    def test_unique_values1(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("code1", "int", unique_values=7)
                      .withColumn("code2", "int", unique_values=7, min=20)
                      .build()
                      )

        testDataSummary = testDataDF.selectExpr("min(code1) as min_c1",
                                                "max(code1) as max_c1",
                                                "min(code2) as min_c2",
                                                "max(code2) as max_c2")

        summary = testDataSummary.collect()[0]
        self.assertEqual(summary[0], 1)
        self.assertEqual(summary[1], 7)
        self.assertEqual(summary[2], 20)
        self.assertEqual(summary[3], 26)

    def test_unique_values_ts(self):
        testDataUniqueDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                            .withIdOutput()
                            .withColumn("test_ts", "timestamp", unique_values=51, random=True)
                            .build()
                            )

        testDataUniqueDF.createOrReplaceTempView("testUnique1")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique1")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)

    def test_unique_values_ts2(self):
        testDataUniqueDF2 = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                             .withIdOutput()
                             .withColumn("test_ts", "timestamp", unique_values=51)
                             .build()
                             )

        testDataUniqueDF2.createOrReplaceTempView("testUnique2")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique2")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)

    def test_unique_values_ts3(self):
        testDataUniqueTSDF = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("test_ts", "timestamp", unique_values=51, random=True,
                        data_range=DateRange("2017-10-01 00:00:00",
                                             "2018-10-06 00:00:00",
                                             "minutes=10"))
            .build()
            )

        testDataUniqueTSDF.createOrReplaceTempView("testUniqueTS3")

        dfResults = spark.sql("select count(distinct test_ts) from testUniqueTS3")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)

    def test_unique_values_ts4(self):
        testDataUniqueTSDF2 = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("test_ts", "timestamp", unique_values=51, random=True,
                        begin="2017-10-01", end="2018-10-06", interval="minutes=10")
            .build()
            )

        testDataUniqueTSDF2.createOrReplaceTempView("testUniqueTS4")

        dfResults = spark.sql("select count(distinct test_ts) from testUniqueTS4")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)

    def test_unique_values_date(self):
        testDataUniqueDF3spec = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("test_ts", "date", unique_values=51, interval="1 days")
            )
        testDataUniqueDF3 = testDataUniqueDF3spec.build()

        testDataUniqueDF3.createOrReplaceTempView("testUnique3")

        testDataUniqueDF3spec.explain()

        dfResults = spark.sql("select count(distinct test_ts) from testUnique3")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_date2(self):
        testDataUniqueDF4 = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                             .withIdOutput()
                             .withColumn("test_ts", "date", unique_values=51, random=True)
                             .build()
                             )

        testDataUniqueDF4.createOrReplaceTempView("testUnique4")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique4")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_date3(self):
        testDataUniqueDF4a = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("test_ts", "date", unique_values=51, random=True, begin="2017-10-01", end="2018-10-06",
                        interval="days=2")
            .build()
            )

        testDataUniqueDF4a.createOrReplaceTempView("testUnique4a")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique4a")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_integers(self):
        testDataUniqueIntegersDF = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("val1", "int", unique_values=51, random=True)
            .withColumn("val2", "int", unique_values=57)
            .withColumn("val3", "long", unique_values=93)
            .withColumn("val4", "long", unique_values=87, random=True)
            .withColumn("val5", "short", unique_values=93)
            .withColumn("val6", "short", unique_values=87, random=True)
            .withColumn("val7", "byte", unique_values=93)
            .withColumn("val8", "byte", unique_values=87, random=True)
            .build()
            )

        testDataUniqueIntegersDF.createOrReplaceTempView("testUniqueIntegers")

        dfResults = spark.sql("""
        select count(distinct val1), count(distinct val2), count(distinct val3), 
                  count(distinct val4),
                  count(distinct val5),
                  count(distinct val6),
                  count(distinct val7),
                  count(distinct val8)
          from testUniqueIntegers
        """"")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)
        self.assertEqual(summary[1], 57)
        self.assertEqual(summary[2], 93)
        self.assertEqual(summary[3], 87)
        self.assertEqual(summary[4], 93)
        self.assertEqual(summary[5], 87)
        self.assertEqual(summary[6], 93)
        self.assertEqual(summary[7], 87)
        print("passed")

    def test_unique_values_decimal(self):
        testDataUniqueDecimalsDF = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("val1", "decimal(15,5)", unique_values=51, random=True)
            .withColumn("val2", "decimal(15,5)", unique_values=57)
            .withColumn("val3", "decimal(10,4)", unique_values=93)
            .withColumn("val4", "decimal(10,0)", unique_values=87, random=True)
            .build()
            )

        testDataUniqueDecimalsDF.createOrReplaceTempView("testUniqueDecimal")

        dfResults = spark.sql("""
        select count(distinct val1), 
                  count(distinct val2), 
                  count(distinct val3), 
                  count(distinct val4)
          from testUniqueDecimal
        """"")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)
        self.assertEqual(summary[1], 57)
        self.assertEqual(summary[2], 93)
        self.assertEqual(summary[3], 87)
        print("passed")

    def test_unique_values_float(self):
        testDataUniqueFloatssDF = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
            .withIdOutput()
            .withColumn("val1", "float", unique_values=51, random=True)
            .withColumn("val2", "float", unique_values=57)
            .withColumn("val3", "double", unique_values=93)
            .withColumn("val4", "double", unique_values=87, random=True)
            .build()
            )

        testDataUniqueFloatssDF.createOrReplaceTempView("testUniqueFloats")

        dfResults = spark.sql("""
        select count(distinct val1), 
                  count(distinct val2), 
                  count(distinct val3), 
                  count(distinct val4)
          from testUniqueFloats
        """"")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)
        self.assertEqual(summary[1], 57)
        self.assertEqual(summary[2], 93)
        self.assertEqual(summary[3], 87)
        print("passed")

    def test_unique_values_float2(self):
        testDataUniqueFloatssDF3 = (
            datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4, verbose=True,
                                  debug=True)
            .withIdOutput()
            .withColumn("val1", "float", unique_values=51, random=True, min=1.0)
            .withColumn("val2", "float", unique_values=57, min=-5.0)
            .withColumn("val3", "double", unique_values=93, min=1.0, step=0.24)
            .withColumn("val4", "double", unique_values=87, random=True, min=1.0, step=0.24)
            .build()
            )

        testDataUniqueFloatssDF3.show()

        testDataUniqueFloatssDF3.createOrReplaceTempView("testUniqueFloats2")

        dfResults = spark.sql("""
        select count(distinct val1), 
                  count(distinct val2), 
                  count(distinct val3), 
                  count(distinct val4)
          from testUniqueFloats2
        """"")
        summary = dfResults.collect()[0]
        self.assertEqual(summary[0], 51)
        self.assertEqual(summary[1], 57)
        self.assertEqual(summary[2], 93)
        self.assertEqual(summary[3], 87)
        print("passed")

    def test_ranged_data_int(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("nint", IntegerType(),  min=1, max=9, step=2)
                        .withColumn("nint2", IntegerType(), percent_nulls=10.0, min=1, max=9, step=2)
                        .withColumn("nint3", IntegerType(), percent_nulls=10.0, min=1, max=9, step=2, random=True)
                        .withColumn("sint", ShortType(),  min=1, max=9, step=2)
                        .withColumn("sint2", ShortType(), percent_nulls=10.0, min=1, max=9, step=2)
                        .withColumn("sint3", ShortType(), percent_nulls=10.0, min=1, max=9, step=2, random=True)
                        )

        results = testDataSpec.build()

        # check ranged int data
        nint_values = [ r[0] for r in results.select("nint").distinct().collect() ]
        self.assertSetEqual(set(nint_values), {  1, 3, 5, 7, 9})

        nint2_values = [ r[0] for r in results.select("nint2").distinct().collect() ]
        self.assertSetEqual(set(nint2_values), { None, 1, 3, 5, 7, 9})

        nint3_values = [ r[0] for r in results.select("nint3").distinct().collect() ]
        self.assertSetEqual(set(nint3_values), { None, 1, 3, 5, 7, 9})

        # check ranged short int data
        sint_values = [ r[0] for r in results.select("sint").distinct().collect() ]
        self.assertSetEqual(set(sint_values), {  1, 3, 5, 7, 9})

        sint2_values = [ r[0] for r in results.select("sint2").distinct().collect() ]
        self.assertSetEqual(set(sint2_values), { None, 1, 3, 5, 7, 9})

        sint3_values = [ r[0] for r in results.select("sint3").distinct().collect() ]
        self.assertSetEqual(set(sint3_values), { None, 1, 3, 5, 7, 9})

    def test_ranged_data_long(self):
        # note python 3.6 does not support trailing long literal syntax (i.e 200L) - but all literal ints are long
        long_min = 3147483651
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("lint", LongType(),  min=long_min, max=long_min+8, step=2)
                        .withColumn("lint2", LongType(), percent_nulls=10.0, min=long_min, max=long_min+8, step=2)
                        .withColumn("lint3", LongType(), percent_nulls=10.0, min=long_min, max=long_min+8, step=2, random=True)
                        )

        results = testDataSpec.build()

        # check ranged int data
        nint_values = [ r[0] for r in results.select("lint").distinct().collect() ]
        self.assertSetEqual(set(nint_values), {  long_min, long_min+2, long_min+4, long_min+6, long_min+8})

        nint2_values = [ r[0] for r in results.select("lint2").distinct().collect() ]
        self.assertSetEqual(set(nint2_values), {  None, long_min, long_min+2, long_min+4, long_min+6, long_min+8})

        nint3_values = [ r[0] for r in results.select("lint3").distinct().collect() ]
        self.assertSetEqual(set(nint3_values), {  None, long_min, long_min+2, long_min+4, long_min+6, long_min+8})


    def test_ranged_data_int(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("nint", IntegerType(),  min=1, max=9, step=2)
                        .withColumn("nint2", IntegerType(), percent_nulls=10.0, min=1, max=9, step=2)
                        .withColumn("nint3", IntegerType(), percent_nulls=10.0, min=1, max=9, step=2, random=True)
                        .withColumn("sint", ShortType(),  min=1, max=9, step=2)
                        .withColumn("sint2", ShortType(), percent_nulls=10.0, min=1, max=9, step=2)
                        .withColumn("sint3", ShortType(), percent_nulls=10.0, min=1, max=9, step=2, random=True)
                        .withColumn("byte1", ByteType(),  min=1, max=9, step=2)
                        .withColumn("byte2", ByteType(), percent_nulls=10.0, min=1, max=9, step=2)
                        .withColumn("byte3", ByteType(), percent_nulls=10.0, min=1, max=9, step=2, random=True)
                        )

        results = testDataSpec.build()

        # check ranged int data
        nint_values = [ r[0] for r in results.select("nint").distinct().collect() ]
        self.assertSetEqual(set(nint_values), {  1, 3, 5, 7, 9})

        nint2_values = [ r[0] for r in results.select("nint2").distinct().collect() ]
        self.assertSetEqual(set(nint2_values), { None, 1, 3, 5, 7, 9})

        nint3_values = [ r[0] for r in results.select("nint3").distinct().collect() ]
        self.assertSetEqual(set(nint3_values), { None, 1, 3, 5, 7, 9})

        # check ranged short int data
        sint_values = [ r[0] for r in results.select("sint").distinct().collect() ]
        self.assertSetEqual(set(sint_values), {  1, 3, 5, 7, 9})

        sint2_values = [ r[0] for r in results.select("sint2").distinct().collect() ]
        self.assertSetEqual(set(sint2_values), { None, 1, 3, 5, 7, 9})

        sint3_values = [ r[0] for r in results.select("sint3").distinct().collect() ]
        self.assertSetEqual(set(sint3_values), { None, 1, 3, 5, 7, 9})

    def test_ranged_data_byte(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("byte1", ByteType(),  min=1, max=9, step=2)
                        .withColumn("byte2", ByteType(), percent_nulls=10.0, min=1, max=9, step=2)
                        .withColumn("byte3", ByteType(), percent_nulls=10.0, min=1, max=9, step=2, random=True)
                        .withColumn("byte4", ByteType(), percent_nulls=10.0, min=-5, max=5, step=2, random=True)
                        )

        results = testDataSpec.build()

        # check ranged byte data
        byte_values = [ r[0] for r in results.select("byte1").distinct().collect() ]
        self.assertSetEqual(set(byte_values), {  1, 3, 5, 7, 9})

        byte2_values = [ r[0] for r in results.select("byte2").distinct().collect() ]
        self.assertSetEqual(set(byte2_values), { None, 1, 3, 5, 7, 9})

        byte3_values = [ r[0] for r in results.select("byte3").distinct().collect() ]
        self.assertSetEqual(set(byte3_values), { None, 1, 3, 5, 7, 9})

        byte4_values = [r[0] for r in results.select("byte4").distinct().collect()]
        self.assertSetEqual(set(byte4_values), {None, -5, -3, -1, 1, 3, 5})

    def test_ranged_data_float1(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("fval", FloatType(),  min=1.0, max=9.0, step=2.0)
                        .withColumn("fval2", FloatType(), percent_nulls=10.0, min=1.0, max=9.0, step=2.0)
                        .withColumn("fval3", FloatType(), percent_nulls=10.0, min=1.0, max=9.0, step=2.0, random=True)
                        .withColumn("dval1", DoubleType(),  min=1.0, max=9.0, step=2.0)
                        .withColumn("dval2", DoubleType(), percent_nulls=10.0, min=1.0, max=9.0, step=2.0)
                        .withColumn("dval3", DoubleType(), percent_nulls=10.0, min=1.0, max=9.0, step=2.0, random=True)
                        )

        results = testDataSpec.build()

        # check ranged floating point data
        float_values = [ r[0] for r in results.select("fval").distinct().collect() ]
        self.assertSetEqual(set(float_values), {  1, 3, 5, 7, 9})
        self.assertSetEqual(set(float_values), {  1.0, 3.0, 5.0, 7.0, 9.0})

        float2_values = [ r[0] for r in results.select("fval2").distinct().collect() ]
        self.assertSetEqual(set(float2_values), { None, 1, 3, 5, 7, 9})

        float3_values = [ r[0] for r in results.select("fval3").distinct().collect() ]
        self.assertSetEqual(set(float3_values), { None, 1, 3, 5, 7, 9})

        # check ranged double data
        double_values = [ r[0] for r in results.select("dval1").distinct().collect() ]
        self.assertSetEqual(set(double_values), {  1, 3, 5, 7, 9})

        double2_values = [ r[0] for r in results.select("dval2").distinct().collect() ]
        self.assertSetEqual(set(double2_values), { None, 1, 3, 5, 7, 9})

        double3_values = [ r[0] for r in results.select("dval2").distinct().collect() ]
        self.assertSetEqual(set(double3_values), { None, 1, 3, 5, 7, 9})

    def test_ranged_data_float2(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("fval", FloatType(),  min=1.5, max=3.5, step=0.5)
                        .withColumn("fval2", FloatType(), percent_nulls=10.0, min=1.5, max=3.5, step=0.5)
                        .withColumn("fval3", FloatType(), percent_nulls=10.0, min=1.5, max=3.5, step=0.5, random=True)
                        .withColumn("dval1", DoubleType(),  min=1.5, max=3.5, step=0.5)
                        .withColumn("dval2", DoubleType(), percent_nulls=10.0, min=1.5, max=3.5, step=0.5)
                        .withColumn("dval3", DoubleType(), percent_nulls=10.0, min=1.5, max=3.5, step=0.5, random=True)
                        )

        results = testDataSpec.build()

        # check ranged floating point data
        float_values = [ r[0] for r in results.select("fval").distinct().collect() ]
        self.assertSetEqual(set(float_values), {  1.5, 2.0, 2.5, 3.0, 3.5 })

        float2_values = [ r[0] for r in results.select("fval2").distinct().collect() ]
        self.assertSetEqual(set(float2_values), { None, 1.5, 2.0, 2.5, 3.0, 3.5 })

        float3_values = [ r[0] for r in results.select("fval3").distinct().collect() ]
        self.assertSetEqual(set(float3_values), { None, 1.5, 2.0, 2.5, 3.0, 3.5 })

        # check ranged double data
        double_values = [ r[0] for r in results.select("dval1").distinct().collect() ]
        self.assertSetEqual(set(double_values), {  1.5, 2.0, 2.5, 3.0, 3.5 })

        double2_values = [ r[0] for r in results.select("dval2").distinct().collect() ]
        self.assertSetEqual(set(double2_values), { None, 1.5, 2.0, 2.5, 3.0, 3.5 })

        double3_values = [ r[0] for r in results.select("dval2").distinct().collect() ]
        self.assertSetEqual(set(double3_values), { None, 1.5, 2.0, 2.5, 3.0, 3.5 })

    @staticmethod
    def roundIfNotNull(x, scale):
        if x is None:
            return x
        return round(x, scale)

    def test_ranged_data_float3(self):
        # when modulo arithmetic does not result in even integer such as '
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, verbose=True)
                        .withIdOutput()
                        .withColumn("fval", FloatType(),  min=1.5, max=2.5, step=0.3)
                        .withColumn("fval2", FloatType(), percent_nulls=10.0, min=1.5, max=2.5, step=0.3)
                        .withColumn("fval3", FloatType(), percent_nulls=10.0, min=1.5, max=2.5, step=0.3, random=True)
                        .withColumn("dval1", DoubleType(),  min=1.5, max=2.5, step=0.3)
                        .withColumn("dval2", DoubleType(), percent_nulls=10.0, min=1.5, max=2.5, step=0.3)
                        .withColumn("dval3", DoubleType(), percent_nulls=10.0, min=1.5, max=2.5, step=0.3, random=True)
                        )

        results = testDataSpec.build()

        testDataSpec.explain()

        # check ranged floating point data
        float_values = [ self.roundIfNotNull(r[0],1) for r in results.select("fval").distinct().collect() ]
        self.assertSetEqual(set(float_values), {  1.5, 1.8, 2.1, 2.4 })

        float2_values = [ self.roundIfNotNull(r[0],1) for r in results.select("fval2").distinct().collect() ]
        self.assertSetEqual(set(float2_values), { None, 1.5, 1.8, 2.1, 2.4 })

        float3_values = [ self.roundIfNotNull(r[0],1) for r in results.select("fval3").distinct().collect() ]
        self.assertSetEqual(set(float3_values), { None, 1.5, 1.8, 2.1, 2.4 })

        # check ranged double data
        double_values = [ self.roundIfNotNull(r[0],1) for r in results.select("dval1").distinct().collect() ]
        self.assertSetEqual(set(double_values), {  1.5, 1.8, 2.1, 2.4 })

        double2_values = [ self.roundIfNotNull(r[0],1) for r in results.select("dval2").distinct().collect() ]
        self.assertSetEqual(set(double2_values), { None, 1.5, 1.8, 2.1, 2.4 })

        double3_values = [ self.roundIfNotNull(r[0],1) for r in results.select("dval2").distinct().collect() ]
        self.assertSetEqual(set(double3_values), { None, 1.5, 1.8, 2.1, 2.4 })

    def test_ranged_data_decimal1(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("decimal1", DecimalType(10,4),  min=1.0, max=9.0, step=2.0)
                        .withColumn("decimal2", DecimalType(10,4), percent_nulls=10.0, min=1.0, max=9.0, step=2.0)
                        .withColumn("decimal3", DecimalType(10,4), percent_nulls=10.0, min=1.0, max=9.0, step=2.0, random=True)
                        .withColumn("decimal4", DecimalType(10,4), percent_nulls=10.0, min=-5, max=5, step=2.0, random=True)
                        )

        results = testDataSpec.build()

        # check ranged floating point data
        decimal_values = [ r[0] for r in results.select("decimal1").distinct().collect() ]
        self.assertSetEqual(set(decimal_values), {  1, 3, 5, 7, 9})
        self.assertSetEqual(set(decimal_values), {  1.0, 3.0, 5.0, 7.0, 9.0})

        decimal2_values = [ r[0] for r in results.select("decimal2").distinct().collect() ]
        self.assertSetEqual(set(decimal2_values), { None, 1, 3, 5, 7, 9})

        decimal3_values = [ r[0] for r in results.select("decimal3").distinct().collect() ]
        self.assertSetEqual(set(decimal3_values), { None, 1, 3, 5, 7, 9})

        decimal4_values = [ r[0] for r in results.select("decimal4").distinct().collect() ]
        self.assertSetEqual(set(decimal4_values), { None, -5, -3, -1, 1, 3, 5})

    def test_ranged_data_string1(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("s1", StringType(),  min=1, max=123, step=1, format="testing %05d >>")
                        )

        results = testDataSpec.build()

        # check `s1` values
        s1_expected_values = [ f"testing {x:05} >>" for x in range(1,124) ]
        s1_values = [ r[0] for r in results.select("s1").distinct().collect() ]
        self.assertSetEqual(set(s1_expected_values), set(s1_values))

    def test_ranged_data_string2(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("s1", StringType(),  min=10, max=123, step=1, format="testing %05d >>")
                        )

        results = testDataSpec.build()

        # check `s1` values
        s1_expected_values = [ f"testing {x:05} >>" for x in range(10,124) ]
        s1_values = [ r[0] for r in results.select("s1").distinct().collect() ]
        self.assertSetEqual(set(s1_expected_values), set(s1_values))

    def test_ranged_data_string3(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("s1", StringType(),  min=10, max=123, step=1, format="testing %05d >>", random=True)
                        )

        results = testDataSpec.build()

        # check `s1` values
        s1_expected_values = [ f"testing {x:05} >>" for x in range(10,124) ]
        s1_values = [ r[0] for r in results.select("s1").distinct().collect() ]
        self.assertSetEqual(set(s1_expected_values), set(s1_values))

    def test_ranged_data_string4(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("s1", StringType(),  min=10, max=123, step=2, format="testing %05d >>", random=True)
                        )

        results = testDataSpec.build()

        testDataSpec.explain()

        # check `s1` values
        s1_expected_values = [ f"testing {x:05} >>" for x in range(10,124, 2) ]
        s1_values = [ r[0] for r in results.select("s1").distinct().collect() ]
        self.assertSetEqual(set(s1_expected_values), set(s1_values))

    def test_ranged_data_string5(self):
        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                        .withIdOutput()
                        .withColumn("s1", StringType(),  min=1.5, max=2.5, step=0.3, format="testing %05.1f >>", random=True)
                        )

        results = testDataSpec.build()

        testDataSpec.explain()

        # check `s1` values
        s1_expected_values = [ f"testing {x:05} >>" for x in [1.5,1.8, 2.1, 2.4] ]
        s1_values = [ r[0] for r in results.select("s1").distinct().collect() ]
        self.assertSetEqual(set(s1_expected_values), set(s1_values))



