.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Producing synthetic streaming data
===================================
The Databricks Labs Data Generator can be used to generate synthetic streaming data using a variety of
streaming sources.

When generating streaming data, the number of rows to be generated in the original construction of the data spec is
either ignored, and the number of rows and speed at which they are generated depends on the options passed to
the `build` method.

When generating batch data, the data generation process begins with generating a base dataframe of seed values
using the `spark.range` method.
The rules for additional column generation are then applied to this base data frame.

For generation of streaming data, the base data frame is generated using one of the `spark.readStream` variations.
This is controlled by passing the argument `withStreaming=True` to the `build` method of the DataGenerator instance.
Additional options may be passed to control rate at which synthetic rows are generated.

.. note::
   Prior to this release, only the structured streaming `rate` source was available to generate streaming data. But with
   this release, you can use a `rate-micro-batch` source, a file source (for streaming reads from data in files such as
   delta, parquet or other file formats). Custom formats should also be supported through the new streaming mechanisms

In theory, any of the structured streaming sources are supported but we do not test compatibility for sources other
than the `rate`, `rate-micro-batch` and file based data sources.

.. seealso::
   See the following links for more details:

   * `Spark Structured Streaming data sources  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources>`_

Generating Streaming Data
-------------------------

You can make any data spec into a streaming data frame by passing additional options to the ``build`` method.

If the ``withStreaming`` option is used when building the data set, it will use a streaming source to generate
the data. By default, this will use a structured streaming `rate` data source as the base data frame.

When using the `rate` streaming source, you can control the streaming rate with the option ``rowsPerSecond``.

When using streaming data sources, the row count specified in the call to the DataGenerator instance constructor
is ignored.

In most cases, no further changes are needed to run the data generation as a streaming data
generator.

As the generated data frame is a normal spark streaming data frame, all of the structured streaming caveats
and features apply.

Example 1: site code and technology
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python
   :linenos:
   :emphasize-lines: 35

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


Customizing streaming data generation
-------------------------------------

You can customize how the streaming data is generated using the options to the ``build`` command.

There are two types of options:

 * Options that are interpreted by the streaming data generation process. These options begin with `'dbldatagen.'`

 * Options that are passed through to the underlying streaming data frame. All other options are passed through to the
   `options` method of the underlying dataframe.

.. list-table:: **Data generation options for generating streaming data**
   :header-rows: 1

   * - Option
     - Usage

   * - `dbldatagen.streaming.source`
     - Type of streaming source to generate. Defaults to `rate`

   * - `dbldatagen.streaming.sourcePath`
     - Path for file based data sources.

   * - `dbldatagen.streaming.sourceSchema`
     - Schema for source of streaming file sources

   * - `dbldatagen.streaming.sourceIdField`
     - Name of source id field - defaults to `value`

   * - `dbldatagen.streaming.sourceTimestampField`
     - Name of source timestamp field - defaults to `timestamp`

   * - `dbldatagen.streaming.generateTimestamp`
     - if set to `True`, automatically generates a timestamp field if none present


The type of the streaming source may be the fully qualified name of a custom streaming source, a built in streaming
source such as `rate` or `rate-micro-batch`, or the name of a file format such as `parquet`, `delta`, or `csv`.

File based data source support `csv`, `parquet` and `delta` format files or folders of files. Files or folders of
files in `delta` format do not require specification of a schema as it is inferred from the underlying file.

Files or folders of files in `csv` format require a schema.

Any options that do not begin with the prefix `dbldatagen.` are passed through to the options method of the underlying
based data frame.

When a schema is specified for a file based source, the schema should only specify the schema of the fields in the
underlying source, not for additional fields added by the data generation rules.

.. note::
   Every streaming data source requires a field that can be designated as the seed field or `id` field.
   This takes on the same role of the `id` field when batch data generation is used.

   This field will be renamed to the seed field name `id` (or to the custom seed field name, if it
   has been overriden in the data generator constructor).

   Many streaming operations also require the designation of a timestamp field to represent event time. This may
   be read from the underlying streaming source, or automatically generated. This is also needed if using
   enhanced event time (described in a later section).

What happens if there are other fields in the underlying data source? These are ignored but fields in the generation
spec may refer to them. However, unless a field generation rule replicates the data in the source field, it will not
appear in the generated data.

Example 2: IOT style data
^^^^^^^^^^^^^^^^^^^^^^^^^

The following example shows how to control the length of time to run the streaming
data generation for.

.. code-block:: python
   :emphasize-lines: 60,61,62
   :linenos:

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

   dfTestDataStreaming = testDataSpec.build(withStreaming=True,
                                            options={'rowsPerSecond': 500})

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

Using file based streaming sources
----------------------------------

Spark structured streaming allows for the use of a dataset storage that has been written in `delta`,
`parquet`, `csv` or other file formats as a streaming source. In particular, the Databricks `delta` format includes
additional enhancements such as transactional isolation that make it suitable as a structured streaming source.

The Databricks Labs Data Generator allows for the use of files or datasets in `delta`, `parquet` or `csv` format as a
source for the generation of structured streaming data.

When a file based source is used as a source of structured streaming data, the data generator will use the `seed` or
`id` field from the source and optionally the timestamp field that represents event time.

When the file contains other fields, these fields may be referred to by other data generation rules (for example in the
body of an `expr` attribute). However, these underlying source fields are not propagated to the output data.

Simulating late arriving events
-------------------------------

The data generator also supports simulating the common IOT event time pattern in streaming device data where
most of the events advance in event time as messages are delivered but with a certain percentage of events
arriving late.

By specifying the use of ehanced event time, you can designate that events increase in event time for most events
but that a certain percentage of the events are delayed up to a configurable time interval.

This allows for validation of handling of device data with late arriving data, watermarks, stream/stream joins and many
other typical streaming scenarios.


Using streaming data with Delta tables
--------------------------------------

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

