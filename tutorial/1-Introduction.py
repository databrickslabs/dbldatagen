# Databricks notebook source
# MAGIC %md ## Introducing the Databricks Labs Data Generator ##

# COMMAND ----------

# MAGIC %md ### First Steps ###
# MAGIC
# MAGIC You will need to import the data generator library in order to use it.
# MAGIC
# MAGIC Within a notebook, you can install the package from PyPi using `pip install` to install the
# MAGIC package with the folling command:
# MAGIC
# MAGIC ```
# MAGIC %pip install dbldatagen
# MAGIC ```
# MAGIC
# MAGIC The [Installation Notes](https://databrickslabs.github.io/dbldatagen/public_docs/installation_notes.html)
# MAGIC provides details on other mechanisms for installation.

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md ### Brief Introduction ###
# MAGIC
# MAGIC You can use the data generator to
# MAGIC
# MAGIC * Generate Pyspark data frames from individual column declarations and schema definitions
# MAGIC * Augment the schema and column definitions with directives as to how data should be generated
# MAGIC     * specify weighting of values
# MAGIC     * specify random or predictable data
# MAGIC     * specify minValue, maxValue and incremental steps
# MAGIC     * generate timestamps falling with specific date ranges
# MAGIC     * specify data types
# MAGIC     * specify arbitrary SQL expressions
# MAGIC     * customize generation of text, timestamps, date and other data
# MAGIC * All of the above can be done within the Databricks notebook environment
# MAGIC
# MAGIC See the help information in the [repository documentation files](https://github.com/databrickslabs/dbldatagen/blob/master/docs/source/APIDOCS.md) and in the [online help Github Pages](https://databrickslabs.github.io/dbldatagen/) for more details.
# MAGIC
# MAGIC The resulting data frames can be saved, used as a source for other operations, converted to view for
# MAGIC consumption from Scala and other languages / environments.
# MAGIC
# MAGIC As the resulting dataframe is a full defined PySpark dataframe, you can supplement resulting data frame with
# MAGIC regular spark code to address scenarios not covered by the library.

# COMMAND ----------

# MAGIC %md ### Using the Data Generator ###
# MAGIC
# MAGIC lets look at several basic scenarios:
# MAGIC * generating a test data set from manually specified columns
# MAGIC * generating a test data set from a schema definition

# COMMAND ----------

# MAGIC %md lets generate a basic data set by manually specifying the columns

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType, FloatType

# will have implied column `id` for ordinal of row
testdata_defn = (
    dg.DataGenerator(spark, name="basic_dataset", rows=100000, partitions=20)
    .withColumn("code1", IntegerType(), minValue=1, maxValue=20, step=1)
    .withColumn("code2", IntegerType(), maxValue=1000, step=5)
    .withColumn("code3", IntegerType(), minValue=100, maxValue=200, step=1, random=True)
    .withColumn("xcode", StringType(), values=["a", "test", "value"], random=True)
    .withColumn("rating", FloatType(), minValue=1.0, maxValue=5.0, step=0.01, random=True)
    .withColumn("non_scaled_rating", FloatType(), minValue=1.0, maxValue=5.0, continuous=True, random=True)
)

df = testdata_defn.build()

display(df)

# COMMAND ----------

# MAGIC %md Lets generate a data set from a schema and augment it.

# COMMAND ----------

from datetime import timedelta, datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import dbldatagen as dg


interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

schema = StructType(
    [
        StructField("site_id", IntegerType(), True),
        StructField("site_cd", StringType(), True),
        StructField("c", StringType(), True),
        StructField("c1", StringType(), True),
        StructField("sector_technology_desc", StringType(), True),
    ]
)

# build spark session


# will have implied column `id` for ordinal of row
# number of partitions will control how many Spark tasks the data generation is distributed over
x3 = (
    dg.DataGenerator(spark, name="my_test_view", rows=1000000, partitions=8)
    .withSchema(schema)
    # withColumnSpec adds specification for existing column
    # here, we speciy data is distributed normally
    .withColumnSpec("site_id", minValue=1, maxValue=20, step=1, distribution="normal", random=True)
    # base column specifies dependent column - here the value of site_cd is dependent on the value of site_id
    .withColumnSpec("site_cd", prefix='site', baseColumn='site_id')
    # withColumn adds specification for new column - even if the basic data set was initialized from a schema
    .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
    .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
    # generate timestamps in over the specified time range
    .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval, random=True)
    # by default all values are populated, but use of percentNulls option introduces nulls randomly
    .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"], percentNulls=0.05, random=True)
    .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
)

# when we specify ``withTempView`` option, the data is available as view in Scala and SQL code
dfOutput = x3.build(withTempView=True)

display(dfOutput)


# COMMAND ----------

# MAGIC %md Using the generated data from other languages

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- we'll generate row counts by site_id
# MAGIC SELECT site_id, count(*) as row_count_by_site from my_test_view
# MAGIC group by site_id
# MAGIC order by site_id asc

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df = spark.sql("""
# MAGIC SELECT site_id, sector_technology_desc, count(*) as row_count_by_site from my_test_view
# MAGIC group by site_id, sector_technology_desc
# MAGIC order by site_id,sector_technology_desc asc
# MAGIC """)
# MAGIC
# MAGIC display(df)
