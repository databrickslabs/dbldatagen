.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Options and additional features
===============================

Options for column specification
--------------------------------

The following table lists some of the common options that can be applied with the ``withColumn`` and ``withColumnSpec``
methods.

================  ==============================
Parameter         Usage
================  ==============================
minValue          Minimum value for range of generated value. As alternative use ``dataRange``.
maxValue          Minimum value for range of generated value. As alternative use ``dataRange``.
step              Step to use for range of generated value. As an alternative, you may use the `dataRange` parameter
random            If True, will generate random values for column value. Defaults to `False`
randomSeedMethod  Determines how seed will be used. If 'fixed', will use fixed random seed. If set to 'hash_fieldname'
                  will use a hash of the field name as the random seed for a specific column.
baseColumn        Either the string name of the base column, or a list of columns to use to control data generation.
values            List of discrete values for the column. Discrete values can numeric, dates timestamps, strings etc.
weights           List of discrete weights for the column. Controls spread of values
percentNulls      Percentage of nulls to generate for column. Fraction representing percentage between 0.0 and 1.0
uniqueValues      Number of distinct unique values for the column. Use as alternative to data range.
begin             Beginning of range for date and timestamp fields.
end               End of range for date and timestamp fields.
interval          Interval of range for date and timestamp fields.
dataRange         An instance of an `NRange` or `DateRange` object. This can be used in place of ``minValue``, etc.
template          Template controlling text generation
omit              If True, omit column from final output. Use when column is only needed to compute other columns.
expr              SQL expression to control data generation
================  ==============================


.. note::

     If the `dataRange` parameter is specified as well as the `minValue`, `maxValue` or `step`,
     the results are undetermined.

     For more information, see :data:`~dbldatagen.daterange.DateRange`
     or :data:`~dbldatagen.daterange.NRange`.


The full set of options for column specification which may be used with the ``withColumn``, ``withColumnSpec`` and
and ``withColumnSpecs`` method can be found at:

   * :data:`~dbldatagen.column_spec_options.ColumnSpecOptions`

Generating views automatically
------------------------------

Views can be automatically generated when the data set is generated.

The view name will use the ``name`` argument specified when creating the data generator instance.

See the following links for more details:

   * :data:`~dbldatagen.data_generator.DataGenerator.build`

Generating streaming data
-------------------------

By default, the data generator produces data suitable for use in batch data frame processing.

The following code sample illustrates generating a streaming data frame:

.. code-block:: python

   import os
   import time

   from pyspark.sql.types import IntegerType, StringType, FloatType
   import dbldatagen as dg

   # various parameter values
   row_count = 100000
   time_to_run = 15
   rows_per_second = 5000

   time_now = int(round(time.time() * 1000))
   base_dir = "/tmp/datagenerator_{}".format(time_now)
   test_dir = os.path.join(base_dir, "data")
   checkpoint_dir = os.path.join(base_dir, "checkpoint")

   # build our data spec
   dataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                    partitions=4, randomSeedMethod='hash_fieldname')
                   .withIdOutput()
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                   )

   # generate the data using a streaming data frame
   dfData = dataSpec.build(withStreaming=True,
                                   options={'rowsPerSecond': self.rows_per_second})

   (dfData
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", test_dir)
    .option("checkpointLocation", checkpoint_dir)
    .start())

   start_time = time.time()
   time.sleep(self.time_to_run)

   # note stopping the stream may produce exceptions - these can be ignored   recent_progress = []
   for x in spark.streams.active:
       x.stop()

   end_time = time.time()


