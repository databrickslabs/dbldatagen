# databrickslabs_testdatagenerator.column_generation_spec module

This file defines the ColumnGenerationSpec class

<!-- !! processed by numpydoc !! -->

### class ColumnGenerationSpec(name, colType=None, min=0, max=None, step=1, prefix='', random=False, distribution=None, base_column='id', random_seed=None, random_seed_method=None, implicit=False, omit=False, nullable=True, debug=False, verbose=False, \*\*kwargs)
Bases: `object`

Column generation spec object - specifies how column is to be generated

Each column to be output will have a corresponding ColumnGenerationSpec object.
This is added explicitly using the DataGenerators withColumnSpec or withColumn methods

If none is explicitly added, a default one will be generated.

The full set of arguments to the class is more than the explicitly called out parameters as any
arguments that are not explictly called out can still be passed due to the \*\*kwargs expression.

This class is meant for internal use only.


* **Parameters**

    
    * **name** – Name of column (string).


    * **colType** – Spark SQL datatype instance, representing the type of the column.


    * **min** – minimum value of column


    * **max** – maximum value of the column


    * **step** – numeric step used in column data generation


    * **prefix** – string used as prefix to the column underlying value to produce a string value


    * **random** – Boolean, if True, will generate random values


    * **distribution** – Instance of distribution, that will control the distribution of the generated values


    * **base_column** – String or list of strings representing columns used as basis for generating the column data


    * **random_seed** – random seed value used to generate the random value, if column data is random


    * **random_seed_method** – method for computing random values from the random seed


    * **implicit** – If True, the specification for the column can be replaced by a later definition.
    If not, a later attempt to replace the definition will flag an error.
    Typically used when generating definitions automatically from a schema, or when using wildcards in the specification


    * **omit** – if True, omit from the final output.


    * **nullable** – If True, column may be null - defaults to True.


    * **debug** – If True, output debugging log statements. Defaults to False.


    * **verbose** – If True, output logging statements at the info level. If False (the default), only output warning and error logging statements.


For full list of options, see databrickslabs_testdatagenerator.column_spec_options module.


* **Attributes**

    `baseColumn`

        get the base column used to generate values for this column

    `base_columns`

        Return base columns as list of strings

    `begin`

        get the begin attribute used to generate values for this column

    `datatype`

        get the Spark SQL data type used to generate values for this column

    `end`

        get the end attribute used to generate values for this column

    `expr`

        get the expr attributed used to generate values for this column

    `exprs`

        get the column generation exprs attribute used to generate values for this column.

    `interval`

        get the interval attribute used to generate values for this column

    `isFieldOmitted`

        check if this field should be omitted from the output

    `isWeightedValuesColumn`

        check if column is a weighed values column

    `max`

        get the column generation max value used to generate values for this column

    `min`

        get the column generation min value used to generate values for this column

    `numColumns`

        get the numColumns attribute used to generate values for this column

    `numFeatures`

        get the numFeatures attribute used to generate values for this column

    `prefix`

        get the string prefix used to generate values for this column

    `step`

        get the column generation step value used to generate values for this column

    `suffix`

        get the string suffix used to generate values for this column


### Methods

| `computeAdjustedRangeForColumn`(colType, …)

 | Determine adjusted range for data column

 |
| `compute_basic_dependencies`()

              | get set of basic column dependencies

     |
| `getNames`()

                                | get column names as list of strings

      |
| `getNamesAndTypes`()

                        | get column names as list of tules (name, datatype)

 |
| `getPlanEntry`()

                            | Get execution plan entry for object

                |
| `getScaledIntegerSQLExpression`(col_name, …)

 | Get scaled numeric expression

                      |
| `getSeedExpression`(base_column)

             | Get seed expression for column generation

          |
| `getUniformRandomExpression`(col_name)

       | Get random expression accounting for seed method

   |
| `getUniformRandomSQLExpression`(col_name)

    | Get random SQL expression accounting for seed method

 |
| `keys`()

                                     | Get the keys as list of strings

                      |
| `makeGenerationExpressions`(use_pandas)

      | Generate structured column if multiple columns or features are specified

 |
| `makeSingleGenerationExpression`([index, …])

 | generate column data for a single column value via Spark SQL expression

  |
| `makeWeightedColumnValuesExpression`(values, …)

 | make SQL expression to compute the weighted values expression

            |
| `set_base_column_datatypes`(column_datatypes)

   | Set the data types for the base columns

                                  |
| `structType`()

                                  | get the structType attribute used to generate values for this column

     |
<!-- !! processed by numpydoc !! -->

#### property baseColumn()
get the base column used to generate values for this column

<!-- !! processed by numpydoc !! -->

#### property base_columns()
Return base columns as list of strings

<!-- !! processed by numpydoc !! -->

#### property begin()
get the begin attribute used to generate values for this column

For numeric columns, the range (min, max, step) is used to control data generation.
For date and time columns, the range (begin, end, interval) are used to control data generation

<!-- !! processed by numpydoc !! -->

#### computeAdjustedRangeForColumn(colType, c_min, c_max, c_step, c_begin, c_end, c_interval, c_range, c_unique)
Determine adjusted range for data column

Rules:
- if a datarange is specified , use that
- if begin and end are specified or min and max are specified, use that
- if unique values is specified, compute min and max depending on type

<!-- !! processed by numpydoc !! -->

#### compute_basic_dependencies()
get set of basic column dependencies


* **Returns**

    base columns as list with dependency on ‘id’ added


<!-- !! processed by numpydoc !! -->

#### property datatype()
get the Spark SQL data type used to generate values for this column

<!-- !! processed by numpydoc !! -->

#### property end()
get the end attribute used to generate values for this column

For numeric columns, the range (min, max, step) is used to control data generation.
For date and time columns, the range (begin, end, interval) are used to control data generation

<!-- !! processed by numpydoc !! -->

#### property expr()
get the expr attributed used to generate values for this column

<!-- !! processed by numpydoc !! -->

#### property exprs()
get the column generation exprs attribute used to generate values for this column.

<!-- !! processed by numpydoc !! -->

#### getNames()
get column names as list of strings

<!-- !! processed by numpydoc !! -->

#### getNamesAndTypes()
get column names as list of tules (name, datatype)

<!-- !! processed by numpydoc !! -->

#### getPlanEntry()
Get execution plan entry for object


* **Returns**

    String representation of plan entry


<!-- !! processed by numpydoc !! -->

#### getScaledIntegerSQLExpression(col_name, scale, base_columns, base_datatypes=None, compute_method=None, normalize=False)
Get scaled numeric expression

This will produce a scaled SQL epression from the base columns


* **Parameters**

    
    * **col_name** – = Column name used for error messages and debugging


    * **normalize** – = If True, will normalize to the range 0 .. 1 inclusive


    * **scale** – = Numeric value indicating scaling factor - will scale via modulo arithmetic


    * **base_columns** – = list of base_columns


    * **base_datatypes** – = list of Spark SQL datatypes for columns


    * **compute_method** – = indicates how the value is be derived from base columns - i.e ‘hash’ or ‘values’ - treated as hint only



* **Returns**

    scaled expression as a SQL string


<!-- !! processed by numpydoc !! -->

#### getSeedExpression(base_column)
Get seed expression for column generation

This is used to generate the base value for every column
if using a single base column, then simply use that, otherwise use either
a SQL hash of multiple columns, or an array of the base column values converted to strings


* **Returns**

    Spark SQL col or expr object


<!-- !! processed by numpydoc !! -->

#### getUniformRandomExpression(col_name)
Get random expression accounting for seed method


* **Returns**

    expression of ColDef form - i.e lit, expr etc


<!-- !! processed by numpydoc !! -->

#### getUniformRandomSQLExpression(col_name)
Get random SQL expression accounting for seed method


* **Returns**

    expression as a SQL string


<!-- !! processed by numpydoc !! -->

#### property interval()
get the interval attribute used to generate values for this column

For numeric columns, the range (min, max, step) is used to control data generation.
For date and time columns, the range (begin, end, interval) are used to control data generation

<!-- !! processed by numpydoc !! -->

#### property isFieldOmitted()
check if this field should be omitted from the output

If the field is omitted from the output, the field is available for use in expressions etc.
but dropped from the final set of fields

<!-- !! processed by numpydoc !! -->

#### property isWeightedValuesColumn()
check if column is a weighed values column

<!-- !! processed by numpydoc !! -->

#### keys()
Get the keys as list of strings

<!-- !! processed by numpydoc !! -->

#### makeGenerationExpressions(use_pandas)
Generate structured column if multiple columns or features are specified

if there are multiple columns / features specified using a single definition, it will generate a set of columns conforming to the same definition,
renaming them as appropriate and combine them into a array if necessary (depending on the structure combination instructions)

> 
> * **param self**

>     is ColumnGenerationSpec for column



> * **param use_pandas**

>     indicates that pandas framework should be used



> * **returns**

>     spark sql column or expression that can be used to generate a column


<!-- !! processed by numpydoc !! -->

#### makeSingleGenerationExpression(index=None, use_pandas_optimizations=False)
generate column data for a single column value via Spark SQL expression


* **Returns**

    spark sql column or expression that can be used to generate a column


<!-- !! processed by numpydoc !! -->

#### makeWeightedColumnValuesExpression(values, weights, seed_column_name)
make SQL expression to compute the weighted values expression


* **Returns**

    Spark SQL expr


<!-- !! processed by numpydoc !! -->

#### property max()
get the column generation max value used to generate values for this column

<!-- !! processed by numpydoc !! -->

#### property min()
get the column generation min value used to generate values for this column

<!-- !! processed by numpydoc !! -->

#### property numColumns()
get the numColumns attribute used to generate values for this column

if a column is specified with the numColumns attribute, this is used to create multiple
copies of the column, named colName1 .. colNameN

<!-- !! processed by numpydoc !! -->

#### property numFeatures()
get the numFeatures attribute used to generate values for this column

if a column is specified with the numFeatures attribute, this is used to create multiple
copies of the column, combined into an array or feature vector

<!-- !! processed by numpydoc !! -->

#### property prefix()
get the string prefix used to generate values for this column

When a string field is generated from this spec, the prefix is prepended to the generated string

<!-- !! processed by numpydoc !! -->

#### set_base_column_datatypes(column_datatypes)
Set the data types for the base columns


* **Parameters**

    **column_datatypes** – = list of data types for the base columns


<!-- !! processed by numpydoc !! -->

#### property step()
get the column generation step value used to generate values for this column

<!-- !! processed by numpydoc !! -->

#### structType()
get the structType attribute used to generate values for this column

When a column spec is specified to generate multiple copies of the column, this controls whether
these are combined into an array etc

<!-- !! processed by numpydoc !! -->

#### property suffix()
get the string suffix used to generate values for this column

When a string field is generated from this spec, the suffix is appended to the generated string

<!-- !! processed by numpydoc !! -->
