# Databricks notebook source
# MAGIC %md ### Using the Test Data Generator ###
# MAGIC 
# MAGIC #### Setup ####
# MAGIC Import the Test Data Generator library wheel file into your environment and attach to cluster being used to run notebook

# COMMAND ----------

# MAGIC %md A simple test data set
# MAGIC 
# MAGIC Lets look at generating a simple data set as follows:
# MAGIC 
# MAGIC - use the `id` field as the key
# MAGIC - generate a predictable value for a theoretical field `code1` that will be the same on every run. The field will be generated based on modulo arithmetic on the `id` field to produce a code
# MAGIC - generate random fields `code2`, `code3` and `code4` 

# COMMAND ----------

import databrickslabs_testdatagenerator as datagen

# will have implied column `id` for ordinal of row
testdata_generator = (datagen.DataGenerator(sparkSession=spark, name="test_dataset1", rows=100000, partitions=20)
                      .withIdOutput()  # id column will be emitted in the output
                      .withColumn("code1", "integer", min=1, max=20, step=1)
                      .withColumn("code2", "integer", min=1, max=20, step=1, random=True)
                      .withColumn("code3", "integer", min=1, max=20, step=1, random=True)
                      .withColumn("code4", "integer", min=1, max=20, step=1, random=True)
                      # base column specifies dependent column

                      .withColumn("site_cd", "string", prefix='site', base_column='code1')
                      .withColumn("sector_status_desc", "string", min=1, max=200, step=1, prefix='status', random=True)
                      .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
                      .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
                      )

df = testdata_generator.build()  # build our dataset

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Often when we are generating test data, we want multiple data sets and to control how keys are generated for datasets after the first. We can control the generation of the `id` field by specifing the starting `id` - for example to simulate arrival of new data rows

# COMMAND ----------

df = testdata_generator

df2 = testdata_generator.option("starting_id", 200000).build()  # build our dataset

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In many cases when we have a series of values for a column, they are not distributed uniformly. By specifying weights, the frequency of generation of specific values can be weighted. 
# MAGIC 
# MAGIC For example:

# COMMAND ----------

import databrickslabs_testdatagenerator as datagen

# will have implied column `id` for ordinal of row
testdata_generator2 = (datagen.DataGenerator(sparkSession=spark, name="test_dataset2", rows=100000, partitions=20)
                       .withIdOutput()  # id column will be emitted in the output
                       .withColumn("code1", "integer", min=1, max=20, step=1)
                       .withColumn("code2", "integer", min=1, max=20, step=1, random=True)
                       .withColumn("code3", "integer", min=1, max=20, step=1, random=True)
                       .withColumn("code4", "integer", min=1, max=20, step=1, random=True)
                       # base column specifies dependent column

                       .withColumn("site_cd", "string", prefix='site', base_column='code1')
                       .withColumn("sector_status_desc", "string", min=1, max=200, step=1, prefix='status', random=True)
                       .withColumn("tech", "string", values=["GSM", "UMTS", "LTE", "UNKNOWN"], weights=[5, 1, 1, 1],
                                   random=True)
                       .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
                       )

df3 = testdata_generator2.build()  # build our dataset

display(df3)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC In many cases when testing ingest pipelines, our test data needs simulated dates or timestamps to simulate the time, date or timestamp to simulate time of ingest or extraction.

# COMMAND ----------

from datetime import timedelta, datetime
import math
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
# from databrickslabs_testdatagenerator.data_generator import DataGenerator,ensure
import databrickslabs_testdatagenerator as datagen

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
x3 = (datagen.DataGenerator(sparkSession=spark, name="association_oss_cell_info", rows=100000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", min=1, max=20, step=1)
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', base_column='site_id')
      .withColumn("sector_status_desc", "string", min=1, max=200, step=1, prefix='status', random=True)
      # withColumn adds specification for new column
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
      .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], random=True)
      .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
      )

x3_output = x3.build(withTempView=True)

print(x3.schema)
x3_output.printSchema()
# display(x3_output)

analyzer = datagen.DataAnalyzer(x3_output)

print("Summary;", analyzer.summarize())

from pyspark.sql.functions import count, when, isnan, isnull, col, lit, countDistinct


def extended_summary(df):
    colnames = [c for c in df.columns]
    colnames2 = ["summary"]
    colnames2.extend(colnames)

    summary_df = df.summary()
    summary_colnames = [c for c in summary_df.columns if c != "summary"]
    summary_colnames2 = ["summary"]
    summary_colnames2.extend(summary_colnames)

    print("colnames2", len(colnames2), colnames2)
    print("summary_colnames", len(summary_colnames), summary_colnames)

    summary_null_count = (df.select([count(when(col(c).isNull(), c)).alias(c) for c in summary_colnames])
                          .withColumn("summary", lit('count_isnull'))
                          .select(*summary_colnames2))

    summary_nan_count = (df.select([count(when(isnan(c), c)).alias(c) for c in summary_colnames])
                         .withColumn("summary", lit('count_isnan'))
                         .select(*summary_colnames2))

    summary_distinct_count = (df.select([countDistinct(col(c)).alias(c) for c in summary_colnames])
                              .withColumn("summary", lit('count_distinct'))
                              .select(*summary_colnames2))

    summary_df = summary_df.union(summary_null_count).union(summary_nan_count).union(summary_distinct_count)
    return summary_df


summary_rows = extended_summary(x3_output).collect()
for r in summary_rows:
    print("Summary2", r)

# COMMAND ----------
