# Databricks notebook source
# MAGIC %md ### Using the Databricks Labs Data Generator ###
# MAGIC 
# MAGIC #### Setup ####
# MAGIC Import the Data Generator library wheel file into your environment and attach to cluster being used to
# MAGIC run notebook

# COMMAND ----------

# MAGIC %md ##Generating a simple data set
# MAGIC 
# MAGIC Lets look at generating a simple data set as follows:
# MAGIC 
# MAGIC - use the `id` field as the key
# MAGIC - generate a predictable value for a theoretical field `code1` that will be the same on every run.
# MAGIC The field will be generated based on modulo arithmetic on the `id` field to produce a code
# MAGIC - generate random fields `code2`, `code3` and `code4` 
# MAGIC - generate a `site code` as a string version of the `site id`
# MAGIC - generate a sector status description
# MAGIC - generate a communications technology field with a discrete set of values

# COMMAND ----------

import dbldatagen as dg

# will have implied column `id` for ordinal of row
testdata_generator = (dg.DataGenerator(spark, name="test_dataset1", rows=100000, partitions=20, randomSeedMethod="hash_fieldname")
                      .withIdOutput()  # id column will be emitted in the output
                      .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                      .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=True)
                      .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
                      .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, random=True)
                      # base column specifies dependent column

                      .withColumn("site_cd", "string", prefix='site', baseColumn='code1')
                      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status',
                                  random=True)
                      .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
                      .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
                      )

df = testdata_generator.build()  # build our dataset

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ### Controlling the starting ID
# MAGIC 
# MAGIC Often when we are generating test data, we want multiple data sets and to control how keys are generated for datasets after the first. We can control the generation of the `id` field by specifing the starting `id` - for example to simulate arrival of new data rows

# COMMAND ----------

df = testdata_generator

df2 = testdata_generator.option("startingId", 200000).build()  # build our dataset

display(df2)

# COMMAND ----------

# MAGIC %md ### Using weights
# MAGIC 
# MAGIC In many cases when we have a series of values for a column, they are not distributed uniformly. By specifying weights, the frequency of generation of specific values can be weighted. 
# MAGIC 
# MAGIC For example:

# COMMAND ----------

import dbldatagen as dg

# will have implied column `id` for ordinal of row
testdata_generator2 = (dg.DataGenerator(spark, name="test_dataset2", rows=100000, partitions=20,
                                       randomSeedMethod="hash_fieldname")
                       .withIdOutput()  # id column will be emitted in the output
                       .withColumn("code1", "integer", minValue=1, maxValue=20, step=1)
                       .withColumn("code2", "integer", minValue=1, maxValue=20, step=1, random=True)
                       .withColumn("code3", "integer", minValue=1, maxValue=20, step=1, random=True)
                       .withColumn("code4", "integer", minValue=1, maxValue=20, step=1, random=True)
                       # base column specifies dependent column

                       .withColumn("site_cd", "string", prefix='site', baseColumn='code1')
                       .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status',
                                   random=True)
                       .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], weights=[5, 1, 1, 1],
                                   random=True)
                       .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
                       )

df3 = testdata_generator2.build()  # build our dataset

display(df3)

# COMMAND ----------

# MAGIC %md ### Generating timestamps
# MAGIC 
# MAGIC In many cases when testing ingest pipelines, our test data needs simulated dates or timestamps to simulate the time, date or timestamp to simulate time of ingest or extraction.

# COMMAND ----------

from datetime import timedelta, datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import dbldatagen as dg

interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("sector_technology_desc", StringType(), True),

])

# build spark session


# will have implied column `id` for ordinal of row
ds = (dg.DataGenerator(spark, name="association_oss_cell_info", rows=100000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', baseColumn='site_id')
      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
      # withColumn adds specification for new column
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
      .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
      .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
      )

df = ds.build()

df.printSchema()


# COMMAND ----------

# MAGIC %md ## Generating text data
# MAGIC You can use the included template, prefix, suffix and format mechanisms to generate text

# COMMAND ----------

import dbldatagen as dg

shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = 10000000

spark.sql("""Create table if not exists test_vehicle_data(
                name string, 
                serial_number string, 
                license_plate string, 
                email string
                ) using Delta""")

table_schema = spark.table("test_vehicle_data").schema

print(table_schema)
  
dataspec = (dg.DataGenerator(spark, rows=10000000, partitions=8, 
                  randomSeedMethod="hash_fieldname")
            .withSchema(table_schema))

dataspec = (dataspec
                .withColumnSpec("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')                                       
                .withColumnSpec("serial_number", minValue=1000000, maxValue=10000000, 
                                 prefix="dr", random=True) 
                .withColumnSpec("email", template=r'\\w.\\w@\\w.com')       
                .withColumnSpec("license_plate", template=r'\\n-\\n')
           )
df1 = dataspec.build()

display(df1)

# COMMAND ----------

# MAGIC %md ### Generating IOT style data

# COMMAND ----------

from pyspark.sql.types import LongType, IntegerType, StringType

import dbldatagen as dg

shuffle_partitions_requested = 8
device_population = 100000
data_rows = 20 * 1000000
partitions_requested = 20

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                   17]

manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

testDataSpec = (dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                                 partitions=partitions_requested, randomSeedMethod='hash_fieldname')
                .withIdOutput()
                # we'll use hash of the base field to generate the ids to 
                # avoid a simple incrementing sequence
                .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                            uniqueValues=device_population, omit=True, baseColumnType="hash")

                # note for format strings, we must use "%lx" not "%x" as the 
                # underlying value is a long
                .withColumn("device_id", StringType(), format="0x%013x",
                            baseColumn="internal_device_id")

                # the device / user attributes will be the same for the same device id 
                # so lets use the internal device id as the base column for these attribute
                .withColumn("country", StringType(), values=country_codes,
                            weights=country_weights,
                            baseColumn="internal_device_id")
                .withColumn("manufacturer", StringType(), values=manufacturers,
                            baseColumn="internal_device_id")

                # use omit = True if you don't want a column to appear in the final output 
                # but just want to use it as part of generation of another column
                .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                            baseColumnType="hash", omit=True)
                .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                            baseColumn="device_id",
                            baseColumnType="hash", omit=True)

                .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                            baseColumn=["line", "model_ser"])
                .withColumn("event_type", StringType(),
                            values=["activation", "deactivation", "plan change",
                                    "telecoms activity", "internet activity", "device error"],
                            random=True)
                .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00", interval="1 minute", random=True)

                )

dfTestData = testDataSpec.build()

display(dfTestData)

# COMMAND ----------

# MAGIC %md ##Generating Streaming Data
# MAGIC 
# MAGIC We can use the specs from the previous exampled to generate streaming data

# COMMAND ----------

# MAGIC %md IOT example

# COMMAND ----------

import time

time_to_run = 180

from pyspark.sql.types import LongType, IntegerType, StringType

import dbldatagen as dg

device_population = 10000
data_rows = 20 * 100000
partitions_requested = 8

country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                   17]

manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

testDataSpec = (dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                                 partitions=partitions_requested, randomSeedMethod='hash_fieldname',
                                 verbose=True)
                .withIdOutput()
                # we'll use hash of the base field to generate the ids to 
                # avoid a simple incrementing sequence
                .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                            uniqueValues=device_population, omit=True, baseColumnType="hash")

                # note for format strings, we must use "%lx" not "%x" as the 
                # underlying value is a long
                .withColumn("device_id", StringType(), format="0x%013x",
                            baseColumn="internal_device_id")

                # the device / user attributes will be the same for the same device id 
                # so lets use the internal device id as the base column for these attribute
                .withColumn("country", StringType(), values=country_codes,
                            weights=country_weights,
                            baseColumn="internal_device_id")
                .withColumn("manufacturer", StringType(), values=manufacturers,
                            baseColumn="internal_device_id")

                # use omit = True if you don't want a column to appear in the final output 
                # but just want to use it as part of generation of another column
                .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                            baseColumnType="hash", omit=True)
                .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                            baseColumn="device_id",
                            baseColumnType="hash", omit=True)

                .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                            baseColumn=["line", "model_ser"])
                .withColumn("event_type", StringType(),
                            values=["activation", "deactivation", "plan change",
                                    "telecoms activity", "internet activity", "device error"],
                            random=True)
                .withColumn("event_ts", "timestamp", expr="now()")

                )

dfTestDataStreaming = testDataSpec.build(withStreaming=True, options={'rowsPerSecond': 500})

display(dfTestDataStreaming)

start_time = time.time()
time.sleep(time_to_run)

# note stopping the stream may produce exceptions - these can be ignored
for x in spark.streams.active:
    x.stop()


# COMMAND ----------

# MAGIC %md Site id and technology example

# COMMAND ----------

from datetime import timedelta, datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import dbldatagen as dg

interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("sector_technology_desc", StringType(), True),

])


# will have implied column `id` for ordinal of row
ds = (dg.DataGenerator(spark, name="association_oss_cell_info", rows=100000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', baseColumn='site_id')
      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
      # withColumn adds specification for new column
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
      .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
      .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
      )

df = ds.build(withStreaming=True, options={'rowsPerSecond': 500})

display(df)
