from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
import unittest
from datetime import timedelta, datetime
from databrickslabs_testdatagenerator import DateRange

# build spark session

# global spark

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()


class TestDates(unittest.TestCase):
    def setUp(self):
        print("setting up")

    def test_basic_dates(self):
        interval = timedelta(days=1, hours=1)
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
