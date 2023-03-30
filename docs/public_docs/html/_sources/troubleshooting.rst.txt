.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

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


.. sidebar:: Logging

   Warning, error and info messages are available via standard logging capabilities.


By default the data generation process output produce error and warning messages via the
Python `logging` module.

In addition, the operation of the data generation process will record messages related to how
the data is being or was generated to an internal explain log during the execution of the `build` method.

So essentially the `explain` method displays the contents of the explain log from the last `build` invocation.
If `build` has not yet been run, it will display the explain logging messages from the build planning process.

Regular logging messages are generated using the standard logger.

You can display additional logging messages by specifying the `verbose` option during creation of the `DataGenerator`
instance.

.. note:: Building planning performs pre-build tasks such as computing the order in which columns need to be generated.
          Build planning messages are available via the `explain` method

Examining log outputs
^^^^^^^^^^^^^^^^^^^^^
Logging outputs will be displayed automatically when using the data generator in a Databricks notebook environment

Common issues and resolution
----------------------------

Attempting to add a column named `id`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


.. sidebar:: Customizing seed column

   Use the `seedColumnName` attribute when creating `DataGenerator` instance to
   customize the seed column name.


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


.. sidebar:: Column build ordering

   Column build order is optimized for best performance during data generation.
   To ensure columns are computed in correct order, use the `baseColumn` attribute.


Use of the `expr` attribute (which allows for the use of arbitrary SQL expressions) can also create dependencies on
other columns.

If a column depends on other columns through referencing them in the body of the expression specified in the `expr`
attribute, it is necessary to ensure that the columns on which the expression depends are computed first.
Use the `baseColumn` attribute to ensure that dependent columns are computed first. The `baseColumn` attribute
may specify either a string that names the column on which the current column depends or a list of column names
specified as a list of strings.

For example, the following code has dependencies in some of the `expr` SQL expressions on earlier columns.
In these cases, we use the `baseColumn` attribute to ensure the correct column build order.

.. code-block:: python

   import dbldatagen as dg


   country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                    'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
   country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                      17]

   device_population = 100000

   manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

   lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

   testDataSpec = (
       dg.DataGenerator(spark, name="device_data_set", rows=1000000,
                        partitions=8,
                        randomSeedMethod='hash_fieldname')
       # we'll use hash of the base field to generate the ids to
       # avoid a simple incrementing sequence
       .withColumn("internal_device_id", "long", minValue=0x1000000000000,
                   uniqueValues=device_population, omit=True, baseColumnType="hash")

       # note for format strings, we must use "%lx" not "%x" as the
       # underlying value is a long
       .withColumn("device_id", "string", format="0x%013x",
                   baseColumn="internal_device_id")

       # the device / user attributes will be the same for the same device id
       # so lets use the internal device id as the base column for these attribute
       .withColumn("country", "string", values=country_codes,
                   weights=country_weights,
                   baseColumn="internal_device_id")

       .withColumn("manufacturer", "string", values=manufacturers,
                   baseColumn="internal_device_id", omit=True)

       .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                   baseColumnType="hash", omit=True)

       # note use of baseColumn to control column build ordering
       .withColumn("manufacturer_info", "string",
                    expr="to_json(named_struct('line', line, 'manufacturer', manufacturer))",
                   baseColumn=["line", "manufacturer"]
                  )

       .withColumn("event_type", "string",
                   values=["activation", "deactivation", "plan change",
                           "telecoms activity", "internet activity", "device error"],
                   random=True, omit=True)

       .withColumn("event_ts", "timestamp",
                   begin="2020-01-01 01:00:00",
                   end="2020-12-31 23:59:00",
                   interval="1 minute",
                   random=True,
                   omit=True)

       # note use of baseColumn to control column build ordering
       .withColumn("event_info", "string",
                    expr="to_json(named_struct('event_type', event_type, 'event_ts', event_ts))",
                    baseColumn=["event_type", "event_ts"])
       )

   dfTestData = testDataSpec.build()

   display(dfTestData)




