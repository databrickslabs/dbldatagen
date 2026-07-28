# Databricks notebook source
# DBTITLE 1,Install package from PyPi
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md ###Change Data Capture

# COMMAND ----------

# MAGIC %md #### Overview
# MAGIC
# MAGIC We'll generate a customer table, and write out the data.
# MAGIC
# MAGIC Then we generate changes for the table and show merging them in.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table customers1

# COMMAND ----------

BASE_PATH = '/tmp/dbldatagen/cdc/'
dbutils.fs.mkdirs(BASE_PATH)

customers1_location = BASE_PATH + "customers1"

# COMMAND ----------

# MAGIC %md Lets generate 10 million customers
# MAGIC
# MAGIC We'll add a timestamp for when the row was generated and a memo field to mark what operation added it

# COMMAND ----------

import dbldatagen as dg  # lgtm [py/repeated-import]
import pyspark.sql.functions as F

spark.catalog.clearCache()
shuffle_partitions_requested = 8
partitions_requested = 32
data_rows = 10 * 1000 * 1000

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

uniqueCustomers = 10 * 1000000

dataspec = (
    dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
    .withColumn("customer_id", "long", uniqueValues=uniqueCustomers)
    .withColumn("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
    .withColumn("alias", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
    .withColumn(
        "payment_instrument_type",
        values=['paypal', 'Visa', 'Mastercard', 'American Express', 'discover', 'branded visa', 'branded mastercard'],
        random=True,
        distribution="normal",
    )
    .withColumn(
        "int_payment_instrument",
        "int",
        minValue=0000,
        maxValue=9999,
        baseColumn="customer_id",
        baseColumnType="hash",
        omit=True,
    )
    .withColumn(
        "payment_instrument",
        expr="format_number(int_payment_instrument, '**** ****** *####')",
        baseColumn="int_payment_instrument",
    )
    .withColumn("email", template=r'\\w.\\w@\\w.com|\\w-\\w@\\w')
    .withColumn("email2", template=r'\\w.\\w@\\w.com')
    .withColumn("ip_address", template=r'\\n.\\n.\\n.\\n')
    .withColumn(
        "md5_payment_instrument",
        expr="md5(concat(payment_instrument_type, ':', payment_instrument))",
        base_column=['payment_instrument_type', 'payment_instrument'],
    )
    .withColumn("customer_notes", text=dg.ILText(words=(1, 8)))
    .withColumn("created_ts", "timestamp", expr="now()")
    .withColumn("modified_ts", "timestamp", expr="now()")
    .withColumn("memo", expr="'original data'")
)
df1 = dataspec.build()

# write table

df1.write.format("delta").save(customers1_location)

# COMMAND ----------

# MAGIC %md ###lets generate a table definition for it

# COMMAND ----------

customers1_location = BASE_PATH + "customers1"
tableDefn = dataspec.scriptTable(name="customers1", location=customers1_location)

spark.sql(tableDefn)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets check our table
# MAGIC
# MAGIC select * from customers1

# COMMAND ----------

# MAGIC %md ### Changes
# MAGIC
# MAGIC Lets generate some changes

# COMMAND ----------

import pyspark.sql.functions as F

start_of_new_ids = df1.select(F.max('customer_id') + 1).collect()[0][0]

print(start_of_new_ids)

# todo - as sequence for random columns will restart from previous seeds , you will get repeated values on next generation operation
# want to use seed sequences so that new random data is not same as old data from previous runs
df1_inserts = (
    dataspec.clone()
    .option("startingId", start_of_new_ids)
    .withRowCount(10 * 1000)
    .build()
    .withColumn("memo", F.lit("insert"))
    .withColumn("customer_id", F.expr(f"customer_id + {start_of_new_ids}"))
)

# read the written data - if we simply recompute, timestamps of original will be lost
df_original = spark.read.format("delta").load(customers1_location)

df1_updates = (
    df_original.sample(False, 0.1)
    .limit(50 * 1000)
    .withColumn("alias", F.lit('modified alias'))
    .withColumn("modified_ts", F.expr('now()'))
    .withColumn("memo", F.lit("update"))
)

df_changes = df1_inserts.union(df1_updates)

# randomize ordering
df_changes = df_changes.withColumn("order_rand", F.expr("rand()")).orderBy("order_rand").drop("order_rand")

display(df_changes)


# COMMAND ----------

# MAGIC %md ###Now lets merge in the changes
# MAGIC
# MAGIC We can script the merge statement in the data generator

# COMMAND ----------

df_changes.dropDuplicates(["customer_id"]).createOrReplaceTempView("customers1_changes")
sqlStmt = dataspec.scriptMerge(
    tgtName="customers1",
    srcName="customers1_changes",
    joinExpr="src.customer_id=tgt.customer_id",
    updateColumns=["alias", "memo", "modified_ts"],
    updateColumnExprs=[("memo", "'updated on merge'"), ("modified_ts", "now()")],
)

print(sqlStmt)

spark.sql(sqlStmt)

# COMMAND ----------

# MAGIC %md Lets examine our table again

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets check our table for updates
# MAGIC
# MAGIC select * from customers1 where created_ts != modified_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets check our table for inserts
# MAGIC
# MAGIC select * from customers1 where memo = "insert"

# COMMAND ----------

dbutils.fs.rm(BASE_PATH, recurse=True)
