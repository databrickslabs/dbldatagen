.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Troubleshooting
===============

To aid in debugging data generation issues, you may use the `explain` method of the
data generator class to produce a synopsis of how the data will be generated.

If run after the `build` method was invoked, the output will include an execution history explaining how the
data was generated.

See:

  * :data:`~dbldatagen.data_generator.DataGenerator.explain`

You may also configure the data generator to produce more verbose output when building the
dataspec and the resulting data set.

To do this, set the ``verbose`` option to ``True`` when creating the dataspec. For example:


.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   partitions_requested = 32
   data_rows = 10 * 1000 * 1000

   uniqueCustomers = 10 * 1000000

   dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested,
                                verbose=True)
               .withColumn("customer_id","long", uniqueValues=uniqueCustomers)
               ...
               )
   df1 = dataspec.build()

See:

  * :data:`~dbldatagen.data_generator.DataGenerator`