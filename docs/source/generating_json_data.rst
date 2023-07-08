.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Generating JSON and Structured Column Data
==========================================

This section explores generating JSON and structured column data. By structured columns,
we mean columns that are some combination of `struct`, `array` and `map` of other types.

Generating JSON data
--------------------
There are several methods for generating JSON data:

- Generate a dataframe and save it as JSON will generate full data set as JSON
- Generate JSON valued fields using SQL functions such as `named_struct` and `to_json`

Writing dataframe as JSON data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example illustrates the basic technique for generating JSON data from a dataframe.

.. code-block:: python

   from pyspark.sql.types import LongType, IntegerType, StringType

   import dbldatagen as dg


   country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                    'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
   country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                      17]

   manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

   lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

   testDataSpec = (
       dg.DataGenerator(spark, name="device_data_set", rows=1000000,
                        partitions=8, randomSeedMethod='hash_fieldname')
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
                   weights=country_weights,
                   baseColumn="internal_device_id")
       .withColumn("manufacturer", StringType(), values=manufacturers,
                   baseColumn="internal_device_id")

       # use omit = True if you don't want a column to appear in the final output
       # but just want to use it as part of generation of another column
       .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                   baseColumnType="hash")
       .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                   baseColumn="device_id",
                   baseColumnType="hash", omit=True)

       .withColumn("event_type", StringType(),
                   values=["activation", "deactivation", "plan change",
                           "telecoms activity", "internet activity", "device error"],
                   random=True)
       .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                   end="2020-12-31 23:59:00",
                   interval="1 minute", random=True)

       )

   dfTestData = testDataSpec.build()

   dfTestData.write.format("json").mode("overwrite").save("/tmp/jsonData1")

In the most basic form, you can simply save the dataframe to storage in JSON format.

Use of nested structures in data generation specifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When we save a dataframe containing complex column types such as `map`, `struct` and `array`, these will be
converted to equivalent constructs in JSON.

So how do we go about creating these?

We can use a struct valued column to hold the nested structure data and write the results out as JSON

Struct / array and map valued columns can be created by adding a column of the appropriate type and using the `expr`
attribute to assemble the complex column.

Note that in the current release, the `expr` attribute will override other column data generation rules.

.. code-block:: python

   from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                                 DoubleType, BooleanType, ShortType, \
                                 TimestampType, DateType, DecimalType, \
                                 ByteType, BinaryType, ArrayType, MapType, \
                                 StructType, StructField

   import dbldatagen as dg


   country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                    'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
   country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                      17]

   manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

   lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

   testDataSpec = (
       dg.DataGenerator(spark, name="device_data_set", rows=1000000,
                        partitions=8, randomSeedMethod='hash_fieldname')
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
                   weights=country_weights,
                   baseColumn="internal_device_id")

       .withColumn("manufacturer", StringType(), values=manufacturers,
                   baseColumn="internal_device_id", omit=True)
       .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                   baseColumnType="hash", omit=True)
       .withColumn("manufacturer_info", StructType([StructField('line',StringType()),
                                                   StructField('manufacturer', StringType())]),
                   expr="named_struct('line', line, 'manufacturer', manufacturer)",
                   baseColumn=['manufacturer', 'line'])


       .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                   baseColumn="device_id",
                   baseColumnType="hash", omit=True)

       .withColumn("event_type", StringType(),
                   values=["activation", "deactivation", "plan change",
                           "telecoms activity", "internet activity", "device error"],
                   random=True, omit=True)
       .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                   end="2020-12-31 23:59:00",
                   interval="1 minute", random=True, omit=True)

       .withColumn("event_info",
                    StructType([StructField('event_type',StringType()),
                                StructField('event_ts', TimestampType())]),
                   expr="named_struct('event_type', event_type, 'event_ts', event_ts)",
                   baseColumn=['event_type', 'event_ts'])
       )

   dfTestData = testDataSpec.build()
   dfTestData.write.format("json").mode("overwrite").save("/tmp/jsonData2")

As the datatype can also be specified using a string, the ``withColumn`` entry for ``'event_info'`` could also be
written as:

.. code-block:: python

   .withColumn("event_info",
               "struct<event_type:string, event_ts: timestamp>",
               expr="named_struct('event_type', event_type, 'event_ts', event_ts)",
               baseColumn=['event_type', 'event_ts'])

 To simplify the specification of struct valued columns, the defined value of `INFER_DATATYPE` can be used in place of
the datatype when the `expr` attribute is specified. This will cause the datatype to be inferred from the expression.

In this case, the previous code would be written as follows:

.. code-block:: python

   .withColumn("event_info",
               dg.INFER_DATATYPE,
               expr="named_struct('event_type', event_type, 'event_ts', event_ts)")

The helper method ``withStructColumn`` can also be used to simplify the specification of struct valued columns.

Generating JSON valued fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

JSON valued fields can be generated as fields of `string` type and assembled using a combination of Spark SQL
functions such as `named_struct` and `to_json`.

.. code-block:: python

   from pyspark.sql.types import LongType, FloatType, IntegerType, \
                                 StringType, DoubleType, BooleanType, ShortType, \
                                 TimestampType, DateType, DecimalType, ByteType, \
                                 BinaryType, ArrayType, MapType, StructType, StructField

   import dbldatagen as dg


   country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                    'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
   country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                      17]

   manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

   lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

   testDataSpec = (
       dg.DataGenerator(spark, name="device_data_set", rows=1000000,
                        partitions=8,
                        randomSeedMethod='hash_fieldname')
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
                   weights=country_weights,
                   baseColumn="internal_device_id")

       .withColumn("manufacturer", StringType(), values=manufacturers,
                   baseColumn="internal_device_id", omit=True)
       .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                   baseColumnType="hash", omit=True)
       .withColumn("manufacturer_info", "string",
                   expr="to_json(named_struct('line', line, 'manufacturer', manufacturer))",
                   baseColumn=['manufacturer', 'line'])


       .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                   baseColumn="device_id",
                   baseColumnType="hash", omit=True)

       .withColumn("event_type", StringType(),
                   values=["activation", "deactivation", "plan change",
                           "telecoms activity", "internet activity", "device error"],
                   random=True, omit=True)
       .withColumn("event_ts", "timestamp",
                   begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00",
                   interval="1 minute", random=True, omit=True)

       .withColumn("event_info", "string",
                   expr="to_json(named_struct('event_type', event_type, 'event_ts', event_ts))",
                   baseColumn=['event_type', 'event_ts'])
       )

   dfTestData = testDataSpec.build()

   #dfTestData.write.format("json").mode("overwrite").save("/tmp/jsonData2")
   display(dfTestData)

The helper method ``withStructColumn`` in the DataGenerator class can also be used to simplify the specification
of struct valued columns. When the argument ``asJson`` is set to ``True``, the resulting structure
will be transformed to JSON.


Generating complex column data
------------------------------
There are several methods for columns with arrays, structs and maps.

You can define a column has having a value of type array, map or struct. This can
be specified in the datatype parameter to the ``withColumn`` method as a string such as ``"array<string>"`` or as a
composite of datatype object instances.

If the column type is based on a struct, map or array, then the ``expr`` attribute must be specified to provide a
value for the column.

If the ``expr`` attribute is not specified, then the default column value will be ``NULL``.

The following example illustrates some of these techniques:

.. code-block:: python

   import dbldatagen as dg

   ds = (
        dg.DataGenerator(spark, name="test_data_set1", rows=1000, random=True)
       .withColumn("r", "float", minValue=1.0, maxValue=10.0, step=0.1,
                   numColumns=5)
       .withColumn("observations", "array<float>",
                   expr="slice(array(r_0, r_1, r_2, r_3, r_4), 1, abs(hash(id)) % 5 + 1 )",
                   baseColumn="r")
       )

   df = ds.build()
   df.show()


The above example constructs a varying length array valued column ``observations`` using the ``slice`` function to take
variable length subsets of the ``r`` columns.

 .. note::
    Note the use of the `baseColumn` attribute here to ensure correct ordering and separation of phases.

Using inferred datatypes
^^^^^^^^^^^^^^^^^^^^^^^^

When building columns with complex data types such as structs especially with nested structs, it can be repetitive and
error prone to specify the datatypes - especially when the column is based on the resuls of a SQL expression
(as specified by the ``expr`` attribute).

You may use the constant ``INFER_DATATYPE`` in place of the actual datatype when the ``expr`` attribute is used.

When the ``INFER_DATATYPE`` constant is used for the datatype, the actual datatype for the column will be inferred
from the SQL expression passed using the ``expr`` parameter. This is only supported when the ``expr`` parameter is
populated.

The following example illustrates this:


Using multi feature columns to generate arrays
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For array valued columns, where all of the elements of the array are to be generated with the same column
specification, an alternative method is also supported.

You can specify that a column has a specific number of features with ``structType`` attribute value of ``'array'``
to control the generation of the column. In this case, the datatype should be the type of the individual element,
not of the array.

For example, the following code will generate rows with varying numbers of synthetic emails for each customer row:

.. code-block:: python

   import dbldatagen as dg

   ds = (
        dg.DataGenerator(sparkSession=spark, name="customers", rows=1000, partitions=4,
                         random=True)
        .withColumn("name", "string", percentNulls=0.01, template=r'\\w \\w|\\w A. \\w|test')
        .withColumn("emails", "string", template=r'\\w.\\w@\\w.com', numFeatures=(1, 6),
                    structType="array")
   )

   df = ds.build()


