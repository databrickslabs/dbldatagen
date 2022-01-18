Generating column data
======================

The data generation specification object controls how the data is to be generated.
This includes:

- The number of rows to be generated
- Whether columns are generated from an existing schema
- Whether implicit columns used in the generation process are output in the final data set
- Whether the generate data set is a streaming or batch data set
- How the column data should be generated and what the dependencies are
- How random and psuedo-random data is generated

.. seealso::
   See the following links for more details:

  * The DataGenerator object - :data:`~dbldatagen.data_generator.DataGenerator`
  * Adding new columns - :data:`~dbldatagen.data_generator.DataGenerator.withColumn`
  * Controlling how existing columns are generated - :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpec`
  * Adding column generation specs in bulk - :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpecs`
  * Options for column generation - :doc:`options_and_features`

Column data is generated for all columns whether imported from a schema or explicitly added
to a data specification.

Initializing a data generation spec
-----------------------------------
The DataGenerator object instance defines how data is to be generated. It consists of a set of implicit column
generation specifications in addition to various instance level attributes. These control the data generation process.

The data generation process itself is deferred until the data generation instance ``build`` method is executed.

So until the ``build`` command is invoked, the data generation specification is in initialization mode.

Once ``build`` has been invoked, the data generation instance holds state about the data set generated.
While ``build`` can be invoked a subsequent time, making further modifications to the definition post build before
calling ``build`` again is not recommended. We recommend the use of the ``clone`` method to make a new data set similar
to an existing one if furthter modifications are needed.

See :data:`~dbldatagen.data_generator.DataGenerator.clone` for further information.

Adding columns to a data generation spec
----------------------------------------

During creation of the data generator object instance (i.e before ``build`` is called)
each invocation of ``withColumnSpec``, ``withColumnSpecs`` or ``withColumn`` adds or modifies a column generation
specification.

When building the data generation spec, the ``withSchema`` method may be used to add columns from an existing schema.
This does _not_ prevent the use of ``withColumn`` to add new columns.

Use ``withColumn`` to define a new column. This method takes a parameter to specify the data type.
See :data:`~dbldatagen.data_generator.DataGenerator.withColumn`.

Use ``withColumnSpec`` to define how a column previously defined in a schema should be generated. This method does not
take a data type property, but uses the data type information defined in the schema.
See :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpec`.

Use ``withColumnSpecs`` to define how multiple columns imported from a schema should be generated.
As the pattern matching may inadvertently match an unintended column, it is permitted to override the specification
added through this method by a subsequent call to ``withColumnSpec`` to change the definition of how a speific column
should be generated
See :data:`~dbldatagen.data_generator.DataGenerator.withColumnSpecs`.

By default all columns are marked as being dependent on an internal ``id`` column.
Use the ``baseColumn`` attribute to mark a column as being dependent on another column or set of columns.
Use of the base column attribute has several effects:

* The order in which columns are built will be changed
* The value of the base column (or the hash of the values, depending on the setting for ``baseColumnType``) will
  be used to control the generation of data for the new column. The exception to this is when the ``random`` attribute
  is set - in this case, the root value will be a random number.


.. note::

  Using the same column names for new columns as existing columns may conflict with existing columns.
  This may cause errors during data generation.

  Note that Spark SQL is case insensivite with respect to column names.
  So a column name that differs only in case to an existing column may cause issues.

The mechanics of column data generation
---------------------------------------
The data set is generated when the ``build`` method is invoked on the data generation instance.

This performs the following actions:

- A psuedo build plan will be computed for debugging purposes
- The set of columns is reordered to control the order in which column data is generated. The ordering is based on the
  ``baseColumn`` attribute of individual column generation spec.
- Cumulative density functions will be computed for columns where weighted values are specified
- The data set will be computed as a Spark data frame for the data in the order of the computed column ordering
- Percent nulls transformations will be applied to columns where the ``percentNulls`` attribute was specified
- The final set of output fields will be selected (omitting any columns where the ``omit`` attribute was set to
  **True**)

This has several implications:

- If a column is referred to in an expression, the ``baseColumn`` attribute must be defined with a dependency
  on that column
- If a column uses a base column with a restricted range of values then it is possible that the column
  will not generate the full range of values in the column generation spec
- If the base column is of type ``boolean`` or some other restricted range type, computations on that base value
  may not produce the expected range of values
- If base column is not specified, you may see errors reporting that the column in an expression does not exist

