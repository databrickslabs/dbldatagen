# Databricks notebook source
# MAGIC %md Example 2

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, from_unixtime, date_add, datediff, \
    unix_timestamp, bround
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, StructType, StructField, DateType, \
    TimestampType

from datetime import timedelta, datetime
import math

# build spark session
# This is not needed when running examples inside of a Databricks runtime notebook environment
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Example") \
    .getOrCreate()


# examples of generating date time data

# def computeIntervals(start, end, interval):
#  if type(start) is not datetime:
#    raise TypeError("Expecting start as type datetime.datetime")
#  if type(end) is not datetime:
#    raise TypeError("Expecting start as type datetime.datetime")
#  if type(interval) is not timedelta:
#    raise TypeError("Expecting interval as type datetime.timedelta")
#  i1 = end - start
#  ni1= i1/interval
#  return math.floor(ni1)

# interval = timedelta(days=1, hours=1)
# start = datetime(2017,10,1,0,0,0)
# end = datetime(2018,10,1,6,0,0)
# print("interval", interval)

# print(start, end, computeIntervals(start, end, interval))

# test = (spark.range(100).withColumn("start", from_unixtime(col("id")*lit(interval(1,"hours"))).cast(TimestampType()))
#        .selectExpr("*", "floor(rand() * 350) * (86400 + 3600) as rnd")
#        .withColumn("rnd2", floor(rand() *350) * (86400 + 3600) )
#        .withColumn("computed_end", from_unixtime(unix_timestamp("start") + col("rnd2")))
#        .withColumn("end", date_add("start", -1).cast(TimestampType()))
#        .withColumn("start_of_day", date_add("start", 0).cast(TimestampType()))
#        .withColumn("start_of_day", datediff("start_of_day", "start"))
#         .withColumn("normalized", unix_timestamp("start"))
#       )


