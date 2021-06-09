# Databricks notebook source
# MAGIC %md ## Introducing the test data generator ##

# COMMAND ----------

# MAGIC %md ### First Steps ###
# MAGIC 
# MAGIC You will need to import the test generator library to workspace in order to use it.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. In your Databricks environment, using the left hand pane to select your workspace, create a library entry for the test data generator library
# MAGIC     1. if theres an older library already installed:
# MAGIC         1. uninstall it from existing clusters that it is installed on
# MAGIC         9. restart the cluster(s) to have the uninstall take effect 
# MAGIC         9. move the library to trash
# MAGIC     9. use the create option for the workspace folder
# MAGIC     9. select library 
# MAGIC     9. select Python Wheel library type (by setting the library type option to `python whl`)
# MAGIC 9. Once the library reference is created, you can install it on your clusters. You may need to restart them to take effect
# MAGIC     
# MAGIC 9. You can now refer to the library in your notebooks. Note the library is a Python 3 wheel library and must be run on a compatible cluster

# COMMAND ----------

# MAGIC %md ### Brief Introduction ###
# MAGIC 
# MAGIC You can use the test data generator to 
# MAGIC 
# MAGIC * Generate Pyspark data frames from individual column declarations and schema definitions
# MAGIC * Augment the schema and column definitions with directive as to how data should be generated
# MAGIC     * specify weighting of values
# MAGIC     * specify random or predictable data
# MAGIC     * specify minValue, maxValue and incremental steps
# MAGIC     * generate timestamps falling with specific date ranges
# MAGIC     * specify data types
# MAGIC     * support for arbitrary SQL expressions
# MAGIC * Analyze an existing data source and generate data similar to the source data
# MAGIC * All of the above can be done within the Databricks notebook environment
# MAGIC 
# MAGIC The resulting data frames can be saved, used as a source for other operations, converted to view for consumption from Scala and other languages / environments. 
# MAGIC 
# MAGIC As the resulting dataframe is a full defined PySpark dataframe, you can supplement resulting data frame with regular spark code to address scenarios not covered by the library.  

# COMMAND ----------

# MAGIC %md ### Using the Test Data Generator ###
# MAGIC 
# MAGIC lets look at several basic scenarios:
# MAGIC * generating a test data set from manually specified columns
# MAGIC * generating a test data set from a schema definition
# MAGIC * generating a test data set to conform with an existing table

# COMMAND ----------

# MAGIC %md lets generate a basic data set by manually specifying the columns

# COMMAND ----------

import databrickslabs_testdatagenerator as datagen
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType

# will have implied column `id` for ordinal of row
testdata_defn = (datagen.DataGenerator(sparkSession=spark, name="basic_dataset", rows=100000000, partitions=20, verbose=True)
      .withColumn("code1", IntegerType(), minValue=1, maxValue=20, step=1)
      .withColumn("code2", IntegerType(), maxValue=1000, step=5)
      .withColumn("code3", IntegerType(), minValue=100, maxValue=200, step=1, random=True)
      .withColumn("xcode", StringType(), values=["a","test", "value"], random=True)
      .withColumn("rating", FloatType(), minValue=1.0, maxValue=5.0, step=0.01, random=True)
      .withColumn("non_scaled_rating", FloatType(), minValue=1.0, maxValue=5.0, continuous=True,  random=True))

df = testdata_defn.build()

display(df)

# COMMAND ----------

from datetime import timedelta, datetime
import math
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
#from databrickslabs_testdatagenerator.data_generator import DataGenerator,ensure
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

#build spark session


# will have implied column `id` for ordinal of row
x3 = (datagen.DataGenerator(sparkSession=spark, name="association_oss_cell_info", rows=100000, partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
      # base column specifies dependent column
      .withIdOutput()
      .withColumnSpec("site_cd", prefix='site', base_column='site_id')
      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1, prefix='status', random=True)
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
    colnames2 = [ "summary" ]
    colnames2.extend(colnames)

    summary_df= df.summary()
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
    print("Summary2", r )


# COMMAND ----------

