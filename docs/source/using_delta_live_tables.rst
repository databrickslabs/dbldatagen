.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Using the Data Generator with Delta Live Tables
===============================================

Delta Live Tables is one of the newer Databricks platform features for creating reliable batch and streaming ingest
pipelines using Python and SQL. It allows for creating ingest pipelines using Python code (and SQL) with annotations
to define the tables and views produced.

.. seealso::
   You can find more information on Delta Live Tables at the following links:

  * `Delta Live Tables <https://docs.databricks.com/delta-live-tables/index.html>`_
  * `Delta Live Tables on Azure <https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/>`_
  * `Change Data Capture with Databricks Delta Live Tables <https://www.databricks.com/blog/2022/04/25/simplifying-change-data-capture-with-databricks-delta-live-tables.html>`_

You can use the Databricks Labs Data Generator inside of a Delta Live Tables to generate synthetic data sources for
both batch and streaming sources. You can also use it from outside a Delta Live Tables pipeline to write data to a
file that is subsequently read via `Autoloader` or a `spark.read` operation within a Delta Live Tables pipeline.

Installing the data generator in a Delta Live Tables notebook
-------------------------------------------------------------

You can install the Databricks Labs Data Generator in a DLT notebook using the same command that you would use in a
regular Python notebook

.. code-block::

   %pip install dbldatagen

This must be the first cell in the notebook ahead of any initial markup cells.

You can subsequently define a source based on the use of the data generator to generate a table, view or
live / streaming table.

Defining a DLT live table with synthetic data source
----------------------------------------------------
Once the library has been installed in the Delta Live Tables pipeline, you can use the DLT annotations
combined with the data generator code to create a DLT live or live streaming sources.

For example, the following code will generate a batch of device events indicating their state (`running`, `idle` etc)
along with timestamp of change of state for a series of devices.

.. code-block:: python

   import dlt
   import dbldatagen as dg

   @dlt.table
   def device_data_source():
     DEVICE_STATES = ['RUNNING', 'IDLE', 'DOWN']
     DEVICE_WEIGHTS = [10, 5,1 ]
     SITES = ['alpha', 'beta', 'gamma', 'delta', 'phi', 'mu', 'lambda']
     AREAS = ['area 1', 'area 2', 'area 3', 'area 4', 'area 5']
     LINES = ['line 1', 'line 2', 'line 3', 'line 4', 'line 5', 'line 6']
     TAGS = ['statusCode', 'another notification 1', 'another notification 2',
             'another notification 3']
     NUM_LOCAL_DEVICES = 20

     STARTING_DATETIME="2022-06-01 01:00:00"
     END_DATETIME="2022-09-01 23:59:00"
     EVENT_INTERVAL = "10 seconds"

     testDataSpec = (
       dg.DataGenerator(spark, rows=1000000)
      .withColumn("site", "string", values=SITES,  random=True, omit=True)
      .withColumn("area", "string", values=AREAS, random=True, omit=True)
      .withColumn("line", "string", values=LINES, random=True, omit=True)
      .withColumn("local_device_id", "int", maxValue=NUM_LOCAL_DEVICES-1, omit=True, random=True)

      .withColumn("local_device", "string", prefix="device",  baseColumn="local_device_id")

      .withColumn("deviceKey", "string",
                  expr = "concat('/', site, '/', area, '/', line, '/', local_device)",
                  baseColumn=["local_device", "line", "area", "site"])

      # tag name is name of device signal
      .withColumn("tagName", "string", values=TAGS, random=True)

      # tag value is state
      .withColumn("tagValue", "string",
                  values=DEVICE_STATES, weights=DEVICE_WEIGHTS,
                  random=True)

      .withColumn("tag_ts", "timestamp",
                  begin=STARTING_DATETIME,
                  end=END_DATETIME,
                  interval=EVENT_INTERVAL,
                  random=True)

      )

     dfTestData = testDataSpec.build()

     return dfTestData


To change this to a streaming live table, you only need change the parameters to the call to the `build` method.

For example, replacing the build line with following code will change it to a DLT live streaming table.

.. code-block:: python

     dfTestData = testDataSpec.build(withStreaming=True, options={'rowsPerSecond': 50000}))

You may define many streaming and non-streaming sources in the same Delta Live Tables pipeline notebooks
allowing experimentation, benchmarking and exploration of different design approaches for your pipelines.

Using the synthetic data sources in Delta Live Tables
-----------------------------------------------------
Once one or more synthetic data sources have been defined in a DLT pipeline, they can be used as sources for other
downstreaming computations, views, as sources of joins etc.

The following example illustrates the use of a synthetic data source to experiment with the creation of summary tables.

For this example, we will enhance the source by materializing the ``site`` key and and an `event_date` so that
they can be used for partitioning, and compute a derived summary table indicating the latest state for each device.

First of all, here is the revised source

.. code-block:: python

   import dlt
   import dbldatagen as dg

   @dlt.table(name="device_data_source", partition_cols=["site", "event_date"])
   def device_data_raw():
     DEVICE_STATES = ['RUNNING', 'IDLE', 'DOWN']
     DEVICE_WEIGHTS = [10, 5,1 ]
     SITES = ['alpha', 'beta', 'gamma', 'delta', 'phi', 'mu', 'lambda']
     AREAS = ['area 1', 'area 2', 'area 3', 'area 4', 'area 5']
     LINES = ['line 1', 'line 2', 'line 3', 'line 4', 'line 5', 'line 6']
     TAGS = ['statusCode', 'another notification 1', 'another notification 2',
             'another notification 3']
     NUM_LOCAL_DEVICES = 20

     STARTING_DATETIME="2022-06-01 01:00:00"
     END_DATETIME="2022-09-01 23:59:00"
     EVENT_INTERVAL = "10 seconds"

     rowsRequested = 10 * 1000000

     testDataSpec = (
       dg.DataGenerator(spark, rows=rowsRequested)
      .withColumn("site", "string", values=SITES,  random=True)
      .withColumn("area", "string", values=AREAS, random=True, omit=True)
      .withColumn("line", "string", values=LINES, random=True, omit=True)

      .withColumn("local_device_id", "int", maxValue=NUM_LOCAL_DEVICES-1, omit=True,
                   random=True)
      .withColumn("local_device", "string", prefix="device",
                  baseColumn="local_device_id")

      .withColumn("deviceKey", "string",
                  expr = "concat('/', site, '/', area, '/', line, '/', local_device)",
                  baseColumn=["local_device", "line", "area", "site"])

      # tag name is name of device signal
      .withColumn("tagName", "string", values=TAGS, random=True)

      # tag value is state
      .withColumn("tagValue", "string",
                  values=DEVICE_STATES, weights=DEVICE_WEIGHTS,
                  random=True)

      .withColumn("tag_ts", "timestamp",
                  begin=STARTING_DATETIME,
                  end=END_DATETIME,
                  interval=EVENT_INTERVAL,
                  random=True)

      .withColumn("event_date", "date", expr="to_date(tag_ts)", baseColumn="tag_ts")
      )

     dfTestData = testDataSpec.build()

     return dfTestData.where("tagName = 'statusCode'")

Now lets create a summary table that stores the latest state for each device.

.. code-block:: python

   import dlt
   import dbldatagen as dg

   @dlt.table(name="device_data_summary",
              table_properties={"delta.checkpoint.writeStatsAsStruct" : "true",
                                "delta.checkpoint.writeStatsAsJson" : "false",
                                "delta.autoOptimize.optimizeWrite" : "true"
                               }
             )
   def device_summary():
     return spark.sql("""
       with ranked as (select deviceKey, tagValue, tag_ts,
              row_number() over (partition by deviceKey order by tag_ts desc) as rn
              from LIVE.device_data_source)
       select deviceKey, tagValue as state, tag_ts,rn from ranked
              where rn == 1
     """)

As you can see in the example above, once the synthetic data source is created, it can be referred in the same way
as any other DLT live or streaming table.
