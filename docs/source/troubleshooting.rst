.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Troubleshooting
===============

Tools and aids to troubleshooting
---------------------------------

Use of the datagenerator `explain` method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To aid in debugging data generation issues, you may use the `explain` method of the
data generator class to produce a synopsis of how the data will be generated.

If run after the `build` method was invoked, the output will include an execution history explaining how the
data was generated.

See:

  * :data:`~dbldatagen.data_generator.DataGenerator.explain`

You may also configure the data generator to produce more verbose output when building the
dataspec and the resulting data set.

To do this, set the ``verbose`` option to ``True`` when creating the dataspec.

Additionally, setting the ``debug`` option to ``True`` will produce additional debug level output.
These correspond to the ``info`` and ``debug`` log levels of the internal messages.

For example:

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   data_rows = 10 * 1000 * 1000

   uniqueCustomers = 10 * 1000000

   dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=4, verbose=True)
                  .withColumn("customer_id","long", uniqueValues=uniqueCustomers)
                  .withColumn("city", "string", template=r"\w")
                  .withColumn("name", "string", template=r"\w \w|\w \w \w")
                  .withColumn("email", "string", template=r"\w@\w.com|\w@\w.org|\w.\w@\w.com")
                  )
   df = dataspec.build()

   display(df)   df1 = dataspec.build()

See:
  * :data:`~dbldatagen.data_generator.DataGenerator`

Operational message logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default the data generation process output produce error and warning messages via the
Python `logging` module.

In addition, the operation of the data generation process will record messages related to how
the data is being or was generated to an internal explain log during the execution of the `build` method.

So essentially the `explain` method displays the contents of the explain log from the last `build` invocation.
If `build` has not yet been run, it will display the explain logging messages from the build planning process.

Building planning performs pre-build tasks such as computing the order in which columns need to be generated.

Examining log outputs
^^^^^^^^^^^^^^^^^^^^^
Logging outputs will be displayed automatically when using the data generator in a Databricks notebook environment

Common issues and resolution
----------------------------

Attempting to add a column named `id`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the data generator reserves the column named `id` to act as the seed column for other columns in the
data generation spec. However you may need to use the name `id` may be used for a specific column definition in the
generated data which differs from the default seed column in operation.

In this case, you may customize the name of the seed column to an alternative name via the `seedColumnName` parameter
to the construction of the `DataGenerator` instance

The following code shows its use:

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   data_rows = 10 * 1000 * 1000

   uniqueCustomers = 10 * 1000000

   dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=4, seedColumnName='_id')
                  .withColumn("id","long", uniqueValues=uniqueCustomers)
                  .withColumn("city", "string", template=r"\w")
                  .withColumn("name", "string", template=r"\w \w|\w \w \w")
                  .withColumn("email", "string", template=r"\w@\w.com|\w@\w.org|\w.\w@\w.com")
                  )
   df = dataspec.build()

   display(df)

Attempting to compute column before dependent columns are computed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the value for a column is computed based on some transformation of the seed column (named `id` by default).
You can use other columns as the seed column for a given column via the `baseColumn` attribute - which takes either
the name of column as a string or a Python list of column names, if the column is dependent on multiple columns.

Use of the `expr` attribute (which allows for the use of arbitrary SQL expressions) can also create dependencies on
other columns.

If a column depends on other columns through referencing them in the body of the expression specified in the `expr`
attribute, it is necessary to ensure that the columns on which the expression depends are computed first.


.. sidebar:: Column build ordering

   By default, columns will  be built in
   the order they are specified unless there are
   forward references


Use the `baseColumn` attribute to ensure that dependent columns are computed first. The `baseColumn` attribute
may specify either a string that names the column on which the current column depends or a list of column names
specified as a list of strings.


