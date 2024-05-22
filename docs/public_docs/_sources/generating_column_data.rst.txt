.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Generating Column Data
======================

The data generation specification object controls how the data is to be generated.
This includes:

- The number of rows to be generated
- Whether columns are generated from an existing schema
- Whether implicit columns used in the generation process are output in the final data set
- Whether the generated data set will be a streaming or batch data set
- How the column data should be generated and what the dependencies for each column are
- How random and psuedo-random data is generated
- The structure for structured columns and JSON valued columns

.. seealso::
   See the following links for more details:

  * The DataGenerator object - :data:`~dbldatagen.data_generator.DataGenerator`
  * Adding new columns - :data:`~dbldatagen.data_generator.DataGenerator.withColumn`
  * Controlling how existing columns are generated - :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpec`
  * Adding column generation specs in bulk - :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpecs`
  * Options for column generation - :doc:`options_and_features`
  * Generating JSON and complex data - :doc:`generating_json_data`

Column data is generated for all columns whether imported from a schema or explicitly added
to a data specification. However, column data can be omitted from the final output, allowing columns to be used
for intermediate calculations.

Initializing a data generation spec
-----------------------------------
The DataGenerator object instance defines a specification for how data is to be generated.
It consists of a set of  column generation specifications in addition to various instance level attributes.
These control the data generation process.

The data generation process itself is deferred until the data generation instance ``build`` method is executed.

So until the :data:`~dbldatagen.data_generator.DataGenerator.build` method is invoked, the data generation
specification is in initialization mode.

Once ``build`` has been invoked, the data generation instance holds state about the data set generated.

While ``build`` can be invoked a subsequent time, making further modifications to the definition post build before
calling ``build`` again is not recommended. We recommend the use of the ``clone`` method to make a new data generation
specification similar to an existing one if further modifications are needed.

See the method :data:`~dbldatagen.data_generator.DataGenerator.clone` for further information.

Adding columns to a data generation spec
----------------------------------------

During creation of the data generator object instance (i.e before ``build`` is called)
each invocation of ``withColumnSpec``, ``withColumnSpecs`` or ``withColumn`` adds or modifies a column generation
specification.

When building the data generation spec, the ``withSchema`` method may be used to add columns from an existing schema.
This does _not_ prevent the use of ``withColumn`` to add new columns.

| Use ``withColumn`` to define a new column. This method takes a parameter to specify the data type.
| See the method :data:`~dbldatagen.data_generator.DataGenerator.withColumn` for further details.

Use ``withColumnSpec`` to define how a column previously defined in a schema should be generated. This method does not
take a data type property, but uses the data type information defined in the schema.
See the method :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpec` for further details.

| Use ``withColumnSpecs`` to define how multiple columns imported from a schema should be generated.
  As the pattern matching may inadvertently match an unintended column, it is permitted to override the specification
  added through this method by a subsequent call to ``withColumnSpec`` to change the definition of how a specific column
  should be generated.
| See the method :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpecs` for further details.

Use the method :data:`~dbldatagen.data_generator.DataGenerator.withStructColumn` for simpler creation of struct and
JSON valued columns.

By default all columns are marked as being dependent on an internal ``id`` seed column.
Use the ``baseColumn`` attribute to mark a column as being dependent on another column or set of columns.
Use of the base column attribute has several effects:

* The order in which columns are built will be changed
* The value of the base column (or the hash of the values, depending on the setting for ``baseColumnType``) will
  be used to control the generation of data for the new column. The exception to this is when the ``random`` attribute
  is set - in this case, the root value will be a random number.


.. note::

  Using the same column names for new columns as existing columns may conflict with existing columns.
  This may cause errors during data generation.

  If you need to generate a field with the same name as the seed column (by default `id`), you may override
  the default seed column name in the constructor of the data generation spec through the use of the
  ``seedColumnName`` parameter.


  Note that Spark SQL is case insensitive with respect to column names.
  So a column name that differs only in case to an existing column may cause issues.

Generating complex columns - structs, maps, arrays
--------------------------------------------------

Complex column types are supported - that is a column may have its type specified as an array, map or struct. This can
be specified in the datatype parameter to the `withColumn` method as a string such as "array<string>" or as a
composite of datatype object instances.

If the column type is based on a struct, map or array, then either the `expr` or the `values` attributes must be
specified to provide a value or range of possible values for the column.

If the `values` attribute is being used to specify a range of possible values, each of the values elements must be of
the same type as the column.

If neither the `expr` or `values` attributes are specified, then the default column value will be `NULL`.

For array valued columns, where all of the elements of the array are to be generated with the same column
specification, an alternative method is also supported.

You can specify that a column has a specific number of features with structType of 'array' to control the generation of
the column. In this case, the datatype should be the type of the individual element, not of the array.

For example, the following code will generate rows with varying numbers of synthetic emails for each customer:

.. code-block:: python

   import dbldatagen as dg

   ds = (
        dg.DataGenerator(sparkSession=spark, name="test_dataset1", rows=1000, partitions=4,
                         random=True)
        .withColumn("name", "string", percentNulls=0.01, template=r'\\w \\w|\\w A. \\w|test')
        .withColumn("emails", "string", template=r'\\w.\\w@\\w.com', random=True,
                    numFeatures=(1, 6), structType="array")
   )

   df = ds.build()

| The helper method ``withStructColumn`` of the ``DataGenerator`` class enables simpler definition of structured
  and JSON valued columns.
| See the documentation for the method :data:`~dbldatagen.data_generator.DataGenerator.withStructColumn` for
  further details.


The mechanics of column data generation
---------------------------------------
The data set is generated when the ``build`` method is invoked on the data generation instance.

This performs the following actions:

- A pseudo build plan will be computed for debugging purposes
- The set of columns is reordered to control the order in which column data is generated. The ordering is based on the
  ``baseColumn`` attribute of individual column generation spec.
- Cumulative density functions will be computed for columns where weighted values are specified
- The data set will be computed as a Spark data frame for the data in the order of the computed column ordering
- Percent nulls transformations will be applied to columns where the ``percentNulls`` attribute was specified
- The final set of output fields will be selected (omitting any columns where the ``omit`` attribute was set to
  **True**)

.. note::

  Normally the columns will be built in the order specified in the spec.
  Use of the `baseColumn` attribute may change the column build ordering.


This has several implications:

- If a column is referred to in an expression, the ``baseColumn`` attribute may need to be defined with a dependency
  on that column
- If a column uses a base column with a restricted range of values then it is possible that the column
  will not generate the full range of values in the column generation spec
- If the base column is of type ``boolean`` or some other restricted range type, computations on that base value
  may not produce the expected range of values
- If base column is not specified, you may see errors reporting that the column in an expression does not exist. 
  This may be fixed by specifying a column dependency using the `baseColumn` attribute

.. note::

  The implementation performs primitive scanning of SQL expressions (specified using the `expr` attribute)
  to determine if the sql expression depends on
  earlier columns and if so, will put the building of the column in a separate phase.

  However it does not reorder the building sequence if there is a reference to a column that will be built later in the
  SQL expression.
  To enforce the dependency, you must use the `baseColumn` attribute to indicate the dependency.

