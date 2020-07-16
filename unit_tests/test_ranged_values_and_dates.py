from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
import unittest
from datetime import timedelta, datetime
from databrickslabs_testdatagenerator import DateRange

# build spark session

# global spark

spark = datagen.SparkSingleton.getLocalInstance("unit tests")


class TestRangedValuesAndDates(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_date_range_object(self):
        x =DateRange("2017-10-01 00:00:00",
                        "2018-10-06 11:55:00",
                        "days=7")
        print("date range", x)
        print("min", datetime.fromtimestamp(x.min))
        print("max", datetime.fromtimestamp(x.max))

    def test_date_range_object2(self):
        interval = timedelta(days=7, hours=0)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 6, 11, 55, 0)

        x =DateRange(start, end, interval)
        print("date range", x)
        print("min", datetime.fromtimestamp(x.min))
        print("max", datetime.fromtimestamp(x.max))
        print("min gm", datetime.utcfromtimestamp(x.min))
        print("max gm", datetime.utcfromtimestamp(x.max))

    def test_basic_dates(self):
        interval = timedelta(days=7, hours=1)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 1, 6, 0, 0)

        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.show()

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

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.show()

    def test_date_range2(self):
        interval = timedelta(days=1, hours=1)
        start = datetime(2017, 10, 1, 0, 0, 0)
        end = datetime(2018, 10, 1, 6, 0, 0)

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

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.show()

    #@unittest.skip("not yet implemented")
    def test_date_range3(self):
        testDataDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .withColumn("last_sync_date", "date",
                                  data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 11:55:00",
                                                       "days=7"), random=True)


                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()

        testDataDF.limit(100).show()

        df_outside1=testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEquals(df_outside1.count() , 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEquals(df_outside2.count() ,0)

    #@unittest.skip("not yet implemented")
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
            self.assertEquals(df_outside1.count(), 0)

            df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
            df_outside2.show()
            self.assertEquals(df_outside2.count(), 0)

    #@unittest.skip("not yet implemented")
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

        df_outside1=testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEquals(df_outside1.count(),  0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEquals(df_outside2.count(),  0)

    #@unittest.skip("not yet finalized")
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

        df_outside1=testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEquals(df_outside1.count(),  0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEquals(df_outside2.count(),  0)

    #@unittest.skip("not yet finalized")
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

        df_outside1=testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEquals(df_outside1.count() , 0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEquals(df_outside2.count() ,0)

    #@unittest.skip("not yet finalized")
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
            self.assertEquals(df_outside1.count(), 0)

            df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
            df_outside2.show()
            self.assertEquals(df_outside2.count(), 0)

    #@unittest.skip("not yet finalized")
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

        df_outside1=testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEquals(df_outside1.count(),  0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEquals(df_outside2.count(),  0)

    #@unittest.skip("not yet finalized")
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

        df_outside1=testDataDF.where("last_sync_date > '2018-10-06' ")
        df_outside1.show()
        self.assertEquals(df_outside1.count(),  0)

        df_outside2 = testDataDF.where("last_sync_date < '2017-10-01' ")
        df_outside2.show()
        self.assertEquals(df_outside2.count(),  0)

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

        summary=testDataSummary.collect()[0]
        self.assertEquals(summary[0], 1)
        self.assertEquals(summary[1], 7)
        self.assertEquals(summary[2], 20)
        self.assertEquals(summary[3], 26)

    def test_unique_values_ts(self):
        testDataUniqueDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                      .withIdOutput()
                      .withColumn("test_ts", "timestamp", unique_values=51, random=True)
                      .build()
                      )


        testDataUniqueDF.createOrReplaceTempView("testUnique1")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique1")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)



    def test_unique_values_ts2(self):
        testDataUniqueDF2 = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                      .withIdOutput()
                      .withColumn("test_ts", "timestamp", unique_values=51)
                      .build()
                      )


        testDataUniqueDF2.createOrReplaceTempView("testUnique2")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique2")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_ts3(self):
        testDataUniqueTSDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                            .withIdOutput()
                            .withColumn("test_ts", "timestamp", unique_values=51, random=True, data_range=DateRange("2017-10-01 00:00:00",
                                                       "2018-10-06 00:00:00",
                                                       "minutes=10"))
                            .build()
                            )

        testDataUniqueTSDF.createOrReplaceTempView("testUniqueTS3")

        dfResults = spark.sql("select count(distinct test_ts) from testUniqueTS3")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_ts4(self):
        testDataUniqueTSDF2 = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                            .withIdOutput()
                            .withColumn("test_ts", "timestamp", unique_values=51, random=True,
                                        begin="2017-10-01", end="2018-10-06",interval="minutes=10")
                            .build()
                            )

        testDataUniqueTSDF2.createOrReplaceTempView("testUniqueTS4")

        dfResults = spark.sql("select count(distinct test_ts) from testUniqueTS4")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_date(self):
        testDataUniqueDF3spec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
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
        testDataUniqueDF4a = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                      .withIdOutput()
                      .withColumn("test_ts", "date", unique_values=51, random=True, begin="2017-10-01", end="2018-10-06",interval="days=2")
                      .build()
                      )


        testDataUniqueDF4a.createOrReplaceTempView("testUnique4a")

        dfResults = spark.sql("select count(distinct test_ts) from testUnique4a")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)

    def test_unique_values_integers(self):
        testDataUniqueIntegersDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
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
        self.assertEquals(summary[0], 51)
        self.assertEquals(summary[1], 57)
        self.assertEquals(summary[2], 93)
        self.assertEquals(summary[3], 87)
        self.assertEquals(summary[4], 93)
        self.assertEquals(summary[5], 87)
        self.assertEquals(summary[6], 93)
        self.assertEquals(summary[7], 87)
        print("passed")

    def test_unique_values_decimal(self):
        testDataUniqueDecimalsDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
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
        self.assertEquals(summary[0], 51)
        self.assertEquals(summary[1], 57)
        self.assertEquals(summary[2], 93)
        self.assertEquals(summary[3], 87)
        print("passed")

    def test_unique_values_float(self):
        testDataUniqueFloatssDF = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
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
        self.assertEquals(summary[0], 51)
        self.assertEquals(summary[1], 57)
        self.assertEquals(summary[2], 93)
        self.assertEquals(summary[3], 87)
        print("passed")

    def test_unique_values_float2(self):
        testDataUniqueFloatssDF2 = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=100000, partitions=4)
                      .withIdOutput()
                      .withColumn("val1", "float", unique_values=51, random=True, min=1.0)
                      .withColumn("val2", "float", unique_values=57, min=-5.0)
                      .withColumn("val3", "double", unique_values=93, min=1.0, step=0.24)
                      .withColumn("val4", "double", unique_values=87, random=True, min=1.0, step=0.24)
                      .build()
                      )


        testDataUniqueFloatssDF2.createOrReplaceTempView("testUniqueFloats2")

        dfResults = spark.sql("""
        select count(distinct val1), 
                  count(distinct val2), 
                  count(distinct val3), 
                  count(distinct val4)
          from testUniqueFloats2
        """"")
        summary = dfResults.collect()[0]
        self.assertEquals(summary[0], 51)
        self.assertEquals(summary[1], 57)
        self.assertEquals(summary[2], 93)
        self.assertEquals(summary[3], 87)
        print("passed")

