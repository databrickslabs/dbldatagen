# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook shows you how to generate and use tabular synthetic data using the Databricks Labs Data Generator
# MAGIC
# MAGIC Further information:
# MAGIC
# MAGIC - Online docs: https://databrickslabs.github.io/dbldatagen/public_docs/index.html
# MAGIC - Project Github home page : https://github.com/databrickslabs/dbldatagen
# MAGIC - Pypi home page: https://pypi.org/project/dbldatagen/
# MAGIC - Other Databricks Labs projects : https://www.databricks.com/learn/labs
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Import Databricks Labs Data Generator using `pip install`
# MAGIC
# MAGIC This is a **Python** notebook so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.
# MAGIC
# MAGIC Generation of the data specification and the dataframe is only supported in Python. However you can create a named view over the data frame or save it to a table and
# MAGIC use it from other languages

# COMMAND ----------

# DBTITLE 1,Install the Data Generator Library (`dbldatagen`) using `pip install`
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Define the data generation specification
# MAGIC
# MAGIC The Databricks Labs Data Generator (`dbldatagen`) uses a data generation specification to control how to generate the data. 
# MAGIC
# MAGIC Once the data generation specification is completed, you can invoke the `build` method to create a dataframe which then then be used or written to tables etc

# COMMAND ----------

# build data generation specification for IOT style device data

from pyspark.sql.types import LongType, IntegerType, StringType

import dbldatagen as dg

device_population = 100000
data_rows = 20 * 1000000
partitions_requested = 20

country_codes = [
    "CN", "US", "FR", "CA", "IN", "JM", "IE", "PK", "GB", "IL", "AU", 
    "SG", "ES", "GE", "MX", "ET", "SA", "LB", "NL",
]
country_weights = [
    1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 
    126, 109, 58, 8, 17,
]

manufacturers = [
    "Delta corp", "Xyzzy Inc.", "Lakehouse Ltd", "Acme Corp", "Embanks Devices",
]

lines = ["delta", "xyzzy", "lakehouse", "gadget", "droid"]

datagenSpec = (
    dg.DataGenerator(spark, name="device_data_set", rows=data_rows, 
                     partitions=partitions_requested)
    .withIdOutput()
    # we'll use hash of the base field to generate the ids to
    # avoid a simple incrementing sequence
    .withColumn("internal_device_id", "long", minValue=0x1000000000000, 
                uniqueValues=device_population, omit=True, baseColumnType="hash",
    )
    # note for format strings, we must use "%lx" not "%x" as the
    # underlying value is a long
    .withColumn(
        "device_id", "string", format="0x%013x", baseColumn="internal_device_id"
    )
    # the device / user attributes will be the same for the same device id
    # so lets use the internal device id as the base column for these attribute
    .withColumn("country", "string", values=country_codes, weights=country_weights, 
                baseColumn="internal_device_id")
    .withColumn("manufacturer", "string", values=manufacturers, 
                baseColumn="internal_device_id", )
    # use omit = True if you don't want a column to appear in the final output
    # but just want to use it as part of generation of another column
    .withColumn("line", "string", values=lines, baseColumn="manufacturer", 
                baseColumnType="hash" )
    .withColumn("event_type", "string", 
                values=["activation", "deactivation", "plan change", "telecoms activity", 
                        "internet activity", "device error", ],
                random=True)
    .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", 
                end="2020-12-31 23:59:00", 
                interval="1 minute", random=True )
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Generate the data
# MAGIC
# MAGIC Invoke `build` to generate a dataframe

# COMMAND ----------

dfGeneratedData = datagenSpec.build(withTempView=True)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: Browse the generated data
# MAGIC

# COMMAND ----------

display(dfGeneratedData)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 5: Querying the data from SQL
# MAGIC
# MAGIC Lets perform a number of quick checks:
# MAGIC
# MAGIC - what is total rows generated and number of distinct device ids
# MAGIC - lets make sure that no device has more than 1 country, manufacturer and line
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) , count(distinct device_id) from device_data_set

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC (select device_id, count(distinct country) as num_countries, 
# MAGIC                   count(distinct manufacturer) as num_manufacturers,
# MAGIC                   count(distinct line) as num_lines
# MAGIC from  device_data_set
# MAGIC group by device_id)
# MAGIC where num_countries > 1 or num_manufacturers > 1 or num_lines > 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 6 (optional): Saving the generated data to the file system
# MAGIC
# MAGIC Lets save the data to the file system
# MAGIC
# MAGIC - what is total rows generated and number of distinct device ids
# MAGIC - lets make sure that no device has more than 1 country, manufacturer and line

# COMMAND ----------

# MAGIC %md ### Step 7 (optional): Saving the data to a table
# MAGIC
# MAGIC Lets save the generated data to a table
