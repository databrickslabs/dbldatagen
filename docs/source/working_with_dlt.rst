.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Working with Delta Live Tables
==============================
The Databricks Labs Data Generator can be used within a Delta Live Tables Python pipeline to generate one or more
synthetic data sources that can be used to test or benchmark the Delta Live Tables pipeline.

To install the library in a Delta Live Tables pipeline, you will need to create a notebook cell as the first cell
of the notebook to install the library via the `%pip` command.

.. code-block:: python

   %pip install dbldatagen

Once the Data Generator has been installed, you can use it to create DLT live tables and live streaming tables.

.. note::
   We recommend using the `rate-micro-batch` streaming source when creating a Delta Live Tables streaming source.

Creating a batch data source
----------------------------

Creating a batch data source in DLT is similar to creating a batch data source in a classic Databricks notebook.

For example the following code snippet creates a dataset of hypothetical device states at different times as typically
reported by device control boards.

Typically in industrial controllers, devices report various status codes known as
`tags` at different times. These status codes can include whether the device is available, active or in some form of
error state. So this simulates a set of devices reporting their status at different times as a batch data set.

.. code-block:: python

   import dbldatagen as dg
   import dlt

   sites = ['alpha', 'beta', 'gamma', 'delta', 'phi', 'mu', 'lambda']
   num_sites = len(sites)
   num_machines = 20
   device_population = num_sites * num_machines

   data_rows = 20 * 1000000
   partitions_requested = 16

   machine_states = ['RUNNING', 'IDLE', 'DOWN']
   machine_weights = [10, 5,1 ]

   tags = ['statusCode', 'another notification 1', 'another notification 2', 'another notification 3']

   starting_datetime="2022-06-01 01:00:00"
   end_datetime="2022-09-01 23:59:00"
   event_interval = "10 seconds"

   @dlt.table(name="device_data_bronze", partition_cols=["site", "event_date"] )
   def machine_data_raw():
     # create our data generation spec
     ds = (dg.DataGenerator(spark, name="mfg_machine_data_set", rows=data_rows,
                                      partitions=partitions_requested,
                                      randomSeedMethod='hash_fieldname')
            # use omit = True if you don't want a column to appear in the final output
            # but just want to use it as part of generation of another column
            .withColumn("internal_site_id", "int", maxValue=num_sites, omit=True,
                        random=True)
            .withColumn("site", "string", values=sites, baseColumn="internal_site_id")

            .withColumn("internal_machine_id", "int", maxValue=num_machines-1, omit=True,
                        random=True)
            .withColumn("machine", "string", prefix="machine",  baseColumn="internal_machine_id")


            # base column entries of machine, site etc means compute these fields first
            # and that generated value depends on these fields
            .withColumn("machineKey", "string", expr = "concat('/', site, '/', machine)",
                        baseColumn=["machine", "site"])

            .withColumn("internal_machine_key", "long", expr = "hash(site, machine)",
                        omit=True, baseColumn=["machine", "site"])

            # value is formatted version of base column
            .withColumn("deviceId", "string", format="0x%013x",
                        baseColumn="internal_machine_key")

            .withColumn("tagName", "string", values=tags, random=True)
            .withColumn("tagValue", "string", values=machine_states, weights=machine_weights,
                        random=True)


            .withColumn("tag_ts", "timestamp",
                        begin=starting_datetime,
                        end=end_datetime,
                        interval=event_interval,
                        random=True)

            .withColumn("event_date", "date", expr="to_date(tag_ts)", baseColumn="tag_ts")
            )

      # now build and return the data frame
     dfTestData = ds.build()
     return dfTestData.where("tag_name = 'status_code')

Creating a streaming data source
--------------------------------

describe creating a streaming data source

Putting it all together
-----------------------
As the synthetic data source defined earlier is a valid DLT table, live table or view, it can be referred to in other
DLT table, live table or view definitions.

The following snippet uses the `lead` function to determine start and end points of machine states

.. code-block:: python

   import dlt

   @dlt.table
   @dlt.expect("valid_end_timestamp", "< add expectation here>")
   def machine_data_silver():

   import pyspark.sql.functions as F
   import pyspark.sql.window as W

   @dlt.table
   @dlt.expect("valid_end_timestamp", "end_ts is not null")
   @dlt.expect("valid_start_timestamp", "start_ts is not null")
   @dlt.expect("site not rolled out", "site <> 'alpha'")
   def machine_data_silver2():
     start_pt = spark.conf.get("report.start")
     end_pt = spark.conf.get("report.end")

     report_start_ts = f"cast('{start_pt}' as timestamp)"
     report_end_ts = f"cast('{end_pt}' as timestamp)"

     # compute start and end periods and supporting data
     df_all = dlt.read("machine_data_silver1")


     # compute start and end timestamps
     windowFunction= (W.Window.partitionBy("site","machine","tagName")
                             .orderBy("site","machine","tagName", "tag_ts"))

     # can use lead / lag in a static dlt table
     df_all = (df_all.withColumn("start_ts",F.col("tag_ts"))
                     .withColumn("_end_ts",F.lead("tag_ts").over(windowFunction))
                     .withColumn("end_ts",F.expr(f"""case when _end_ts is null then {report_end_ts}
                                                          else _end_ts
                                                     end
                                                  """))
                     .drop("_end_ts")
              )

     # Perform further processing and aggregations
     # ,,,

     # return our data frame
     return df_all