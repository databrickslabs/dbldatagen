.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Options and Additional Features
===============================

Options for column specification
--------------------------------

Data generation for each column begins with generating a base expression for the column, which will be a random value
if `random` is True, or some transformation of the `baseColumns` if not.

The options are then applied - each option successively modifying the generated value. Where possible, the effects of
an options are applied over the effects of other options so the effect is accumulative.

Finally type conversion is applied.

.. note::

   Options such as `minValue`, `maxValue` and `step` can be applied to strings also if their underlying generation
   is from a numeric root or `baseColumn`. The default base column `id` is of type long.

The following table lists some of the common options that can be applied with the ``withColumn`` and ``withColumnSpec``
methods.

.. table:: Column creation options

================  ==============================
Parameter         Usage
================  ==============================
minValue          Minimum value for range of generated value. Alternatively, use ``dataRange``

maxValue          Minimum value for range of generated value. Alternatively, use ``dataRange``

step              Step to use for range of generated value. Alternatively, use ``dataRange``

prefix            Prefix text to apply to expression

random            If `True`, will generate random values for column value. Defaults to `False`

randomSeedMethod  Determines how seed will be used. See `Generating random values`

                  If set to the value 'fixed', will use fixed random seed.

                  If set to 'hash_fieldname', it will use a hash of the field name as the random seed

                  for a specific column.

randomSeed        Random seed for the generation of random numbers. See `Generating random values`


                  If not set, settings will depend on the `randomSeedMethod` and top-level data generator

                  `randomSeed` and `randomSeed` settings.


                  If `randomSeedMethod` is `hashFieldName` for this column or for the data

                  specification as whole, the random seed for each column a hash value based on the

                  field name. This is the default unless these settings are overridden.


                  If `randomSeed` has a value of -1, then the random value will be a random number from

                  the uniform distribution and the data generated will not be the same from run to run.

distribution      Controls the statistical distribution of random values when the column is generated

                  randomly. Accepts the values "normal", or a Distribution object instance

baseColumn        Either the string name of the base column, or a list of columns to use to control

                  data generation

values            List of discrete values for the column.

                  Discrete values can numeric, dates timestamps, strings etc

weights           List of discrete weights for the column. Controls spread of values

percentNulls      Percentage of nulls to generate for column

                  Fraction representing percentage between 0.0 and 1.0

uniqueValues      Number of distinct unique values for the column. Use as alternative to `dataRange`

begin             Beginning of range for date and timestamp fields

end               End of range for date and timestamp fields

interval          Interval of range for date and timestamp fields

dataRange         An instance of an `NRange` or `DateRange` object

                  This can be used in place of ``minValue``, etc

template          Template controlling text generation

omit              If True, omit column from final output.

                  Use when column is only needed to compute other columns

expr              SQL expression to control data generation

numColumns        Number of columns when generating multiple columns with same specification

numFeatures       Synonym for `numColumns`

structType        If set to `array`, generates array value from multiple columns.

================  ==============================


.. note::

     If the `dataRange` parameter is specified as well as the `minValue`, `maxValue` or `step`,
     the results are undetermined.

     For more information, see :data:`~dbldatagen.daterange.DateRange`
     or :data:`~dbldatagen.daterange.NRange`.

Generating multiple columns with same generation spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may generate multiple columns with the same column generation spec by specifying `numFeatures` or `numColumns` with
an integer value to generate a specific number of columns. The generated columns will be suffixed with a number
representing the column - for example "email_0", "email_1" etc.

If you specify the attribute `structType="array"`, the multiple columns will be combined into a single array valued
column.

Generating random values
^^^^^^^^^^^^^^^^^^^^^^^^

By default, each columns' data is generated by applying various transformations to a root value for a column.
The root value is generated from the base column(s) when the random attribute is not true.

The base column value is used directly or indirectly depending on the value of `baseColumnMethod`.

If the attribute, `random` is True, the root column value is generated from a random base column value.

For random columns, the `randomSeedMethod` and the `randomSeed` method determine how the random root value is generated.

When the `randomSeedMethod` attribute value is `fixed`, it will be generated using a random number generator
with a designated `randomSeed` unless the `randomSeed` value is -1. When the `randomSeed` value is -1, then the
generated values will be generated without a fixed random seed, so data will be different from run to run.

If the `randomSeedMethod` value is `hash_fieldname`, the random seed for each column is computed using a hash function
over the field name.

This guarantees that data generation is repeatable unless the `randomSeed` attribute has a value of -1, and the
`randomSeedMethod` value is `fixed`.

The following example illustrates some of these features.

.. code-block:: python

        ds = (
            dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4,
                             random=True)
            .withColumn("name", "string", percentNulls=0.01, template=r'\\w \\w|\\w A. \\w|test')
            .withColumn("emails", "string", template=r'\\w.\\w@\\w.com', random=True,
                        numFeatures=numFeaturesSupplied, structType="array")
        )

        df = ds.build()

The use of `random=True` at the DataGenerator instance level applies `random=True` to all columns.

The combination of `numFeatures=(2,6)` and `structType='array'` will generate array values with varying number of
elements according to the underlying value generation rules - in this case, the use of a template to generate text.

By default random number seeds are derived from field names, and in the case of columns with multiple features,
the seed will be different for each feature element.

Using custom SQL to control data generation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `expr` attribute can be used to specify an arbitrary Spark SQL expression to control how the data is
generated for a column. If the body of the SQL references other columns, you will need to ensure that
those columns are created first.

By default, the columns are created in the order specified.

However, you can control the order of column creation using the `baseColumn` attribute.

More details
^^^^^^^^^^^^

The full set of options for column specification which may be used with the ``withColumn``, ``withColumnSpec`` and
and ``withColumnSpecs`` method can be found at:

   * :data:`~dbldatagen.column_spec_options.ColumnSpecOptions`

Example
^^^^^^^

The following example shows use of these options to generate user records, each having a variable set
of randomly generated emails.

.. code-block:: python

   import dbldatagen as dg
   import logging

   from pyspark.sql.types import ArrayType, StringType

   dataspec = dg.DataGenerator(spark, rows=10 * 1000000)

   logging.info(dataspec.partitions)

   dataspec = (
         dataspec
         .withColumn("name", "string", percentNulls=0.01, template=r'\\w \\w|\\w A. \\w|test')
         .withColumn("serial_number", "string",
                     minValue=1000000, maxValue=10000000,
                     prefix="dr", random=True)

        # generate a fixed length array of email addresses
        .withColumn("email", "string", template=r'\\w.\\w@\\w.com', omit=True,
                    numColumns=5, structType="array",
                    random=True, randomSeed=-1)
        .withColumn("emailCount", "int", expr="abs(hash(id)) % 4)+1)")
        .withColumn("emails", ArrayType(StringType()), expr="slice(email, 1, emailCount",
                        baseColumns=["email"])
         .withColumn("license_plate", "string", template=r'\\n-\\n')
        )
   dfTestData = dataspec.build()

   display(dfTestData)



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
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'],
                               random=True, weights=[9, 1, 1])

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

   # note stopping the stream may produce exceptions
   # - these can be ignored
   for x in spark.streams.active:
       x.stop()

   end_time = time.time()


