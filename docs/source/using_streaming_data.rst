.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Using Streaming Data
====================

You can make any data spec into a streaming data frame by passing additional options to the ``build`` method.

If the ``withStreaming`` option is used when building the data set, it will use a streaming rate source to generate
the data. You can control the streaming rate with the option ``rowsPerSecond``.

In this case, the row count is ignored.

In most cases, no further changes are needed to run the data generation as a streaming data
generator.

As the generated data frame is a normal spark streaming data frame, all the same caveats and features apply.

Example 1: site code and technology
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datetime import timedelta, datetime
   import math
   from pyspark.sql.types import StructType, StructField, IntegerType, StringType, \
                                 FloatType, TimestampType
   # from dbldatagen.data_generator import DataGenerator,ensure
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
   ds = (
       dg.DataGenerator(spark, name="association_oss_cell_info", rows=100000, partitions=20)
       .withSchema(schema)
       # withColumnSpec adds specification for existing column
       .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
       # base column specifies dependent column
       .withIdOutput()
       .withColumnSpec("site_cd", prefix='site', baseColumn='site_id')
       .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                   prefix='status', random=True)
       # withColumn adds specification for new column
       .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
       .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval,
                   random=True)
       .withColumnSpec("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"],
                   random=True)
       .withColumn("test_cell_flg", "integer", values=[0, 1], random=True)
   )

   df = ds.build(withStreaming=True, options={'rowsPerSecond': 500})

   display(df)



Example 2: IOT style data
^^^^^^^^^^^^^^^^^^^^^^^^^

The following example shows how to control the length of time to run the streaming
data generation for.

.. code-block:: python

   import time
   time_to_run = 180

   from pyspark.sql.types import LongType, IntegerType, StringType

   import dbldatagen as dg

   device_population = 10000
   data_rows = 20 * 100000
   partitions_requested = 8

   country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                    'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
   country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58,
                      8, 17]

   manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

   lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

   testDataSpec = (
       dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                        partitions=partitions_requested,
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
                   weights=country_weights, baseColumn="internal_device_id")
       .withColumn("manufacturer", StringType(), values=manufacturers,
                   baseColumn="internal_device_id")

       # use omit = True if you don't want a column to appear in the final output
       # but just want to use it as part of generation of another column
       .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                   baseColumnType="hash", omit=True)
       .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                   baseColumn="device_id", baseColumnType="hash", omit=True)

       .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                   baseColumn=["line", "model_ser"])
       .withColumn("event_type", StringType(),
                   values=["activation", "deactivation", "plan change",
                           "telecoms activity", "internet activity", "device error"],
                   random=True)
       .withColumn("event_ts", "timestamp", expr="now()")
       )

   dfTestDataStreaming = testDataSpec.build(withStreaming=True, options={'rowsPerSecond': 500})

   # ... do something with your streaming source here
   display(dfTestDataStreaming)

In a separate notebook cell, you can execute the following code to
terminate the streaming after a specified period of time.

.. code-block:: python

   time.sleep(time_to_run)

   # note stopping the stream may produce exceptions - these can be ignored
   for x in spark.streams.active:
       try:
           x.stop()
       except RuntimeError:
           pass

Using streaming data with Delta tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you write the streaming data to a Delta table using a streaming
writer, then the Delta table itself can be used as a streaming source
for downstream consumption.

.. code-block:: python

   from datetime import timedelta, datetime
   import dbldatagen as dg

   interval = timedelta(days=1, hours=1)
   start = datetime(2017, 10, 1, 0, 0, 0)
   end = datetime(2018, 10, 1, 6, 0, 0)

   # row count will be ignored
   ds = (dg.DataGenerator(spark, name="association_oss_cell_info", rows=100000, partitions=20)
         .withColumnSpec("site_id", minValue=1, maxValue=20, step=1)
         .withColumnSpec("site_cd", prefix='site', baseColumn='site_id')
         .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                     prefix='status', random=True)
         .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
         .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval,
                     random=True)
         .withColumn("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"],
                     random=True)
         )

   df = ds.build(withStreaming=True, options={'rowsPerSecond': 500})

   df.writeStream
       .format("delta")
       .outputMode("append")
       .option("checkpointLocation", "/tmp/dbldatagen/streamingDemo/checkpoint")
       .start("/tmp/dbldatagen/streamingDemo/data")

