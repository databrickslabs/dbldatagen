# databrickslabs_testdatagenerator.data_generator module

This file defines the DataGenError and DataGenerator classes

<!-- !! processed by numpydoc !! -->

### class DataGenerator(sparkSession=None, name=None, seed_method=None, generate_with_selects=True, rows=1000000, starting_id=0, seed=None, partitions=None, verbose=False, use_pandas=True, pandas_udf_batch_size=None, debug=False)
Bases: `object`

Main Class for test data set generation

This class acts as the entry point to all test data generation activities.


* **Parameters**

    
    * **sparkSession** – spark Session object to use


    * **name** – is name of data set


    * **seed_method** – = seed method for random numbers - either None, ‘fixed’, ‘hash_fieldname’


    * **generate_with_selects** – = if True, optimize datae generation with selects, otherwise use withColumn


    * **rows** – = amount of rows to generate


    * **starting_id** – = starting id for generated id column


    * **seed** – = seed for random number generator


    * **partitions** – = number of partitions to generate


    * **verbose** – = if True, generate verbose output


    * **use_pandas** – = if True, use Pandas UDFs during test data generation


    * **pandas_udf_batch_size** – = UDF batch number of rows to pass via Apache Arrow to Pandas UDFs


    * **debug** – = if set to True, output debug level of information



* **Attributes**

    `build_order`

        return the build order minus the id column

    `inferredSchema`

        infer spark interim schema definition from the field specifications

    `schema`

        infer spark output schema definition from the field specifications

    `schemaFields`

        get list of schema fields for final output schema


### Methods

| `build`([withTempView, withView, …])

 | build the test data set from the column definitions and return a dataframe for it

 |
| `clone`()

                                       | Make a clone of the data spec via deep copy preserving same spark session

         |
| `computeBuildPlan`()

                            | prepare for building

                                                              |
| `computeColumnBuildOrder`()

                     | compute the build ordering using a topological sort on dependencies

               |
| `describe`()

                                    | return description of the dataset generation spec

                                 |
| `explain`()

                                     | Explain the test data generation process

                                          |
| `flatten`(l)

                                    | flatten list

                                                                      |
| `generateColumnDefinition`(colName[, colType, …])

 | generate field definition and column spec

                                         |
| `generateName`()

                                  | get a name for the data set Uses the untitled name prefix and nextNameIndex to generate a dummy dataset name

 |
| `getBaseDataFrame`([start_id, streaming, options])

 | generate the base data frame and id column , partitioning the data if necessary

                              |
| `getColumnType`(colName)

                           | Get column Spark SQL datatype for specified column

                                                           |
| `getInferredColumnNames`()

                         | get list of output columns

                                                                                   |
| `getOutputColumnNames`()

                           | get list of output columns by flattening list of lists of column names normal columns will have a single column name but column definitions that result in multiple columns will produce a list of multiple names

 |
| `getOutputColumnNamesAndTypes`()

                   | get list of output columns by flattening list of lists of column names and types normal columns will have a single column name but column definitions that result in multiple columns will produce a list of multiple names

 |
| `get_column_data_types`(columns)

                   | Get data types for columns

                                                                                                                                                                                                  |
| `hasColumnSpec`(colName)

                           | returns true if there is a column spec for the column

                                                                                                                                                                       |
| `isFieldExplicitlyDefined`(colName)

                | return True if column generation spec has been explicitly defined for column, else false

                                                                                                                                    |
| `markForPlanRegen`()

                               | Mark that build plan needs to be regenerated

                                                                                                                                                                                |
| `option`(option_key, option_value)

                 | set option to option value for later processing

                                                                                                                                                                             |
| `options`(\*\*kwargs)

                                | set options in bulk

                                                                                                                                                                                                         |
| `reset`()

                                          | reset any state associated with the data

                                                                                                                                                                                    |
| `scriptTable`([name, location, table_format])

      | generate create table script suitable for format of test data set

                                                                                                                                                           |
| `seed`(seedVal)

                                    | set seed for random number generation

                                                                                                                                                                                       |
| `setRowCount`(rc)

                                  | Modify the row count - useful when starting a new spec from a clone

                                                                                                                                                         |
| `sqlTypeFromSparkType`(dt)

                         | Get sql type for spark type :param dt: instance of Spark SQL type such as IntegerType()

                                                                                                                                     |
| `withColumn`(colName[, colType, min, max, …])

      | add a new column for specification

                                                                                                                                                                                          |
| `withColumnSpec`(colName[, min, max, step, …])

     | add a column specification for an existing column

                                                                                                                                                                           |
| `withColumnSpecs`([patterns, fields, match_types])

 | Add column specs for columns matching

                                                                                                                                                                                       |
| `withIdOutput`()

                                   | output id field as a column in the test data set if specified

                                                                                                                                                               |
| `withSchema`(sch)

                                  | populate column definitions and specifications for each of the columns in the schema

                                                                                                                                        |
<!-- !! processed by numpydoc !! -->

#### build(withTempView=False, withView=False, withStreaming=False, options=None)
build the test data set from the column definitions and return a dataframe for it

if withStreaming is True, generates a streaming data set. Use options to control the rate of generation of test data if streaming is used.

For example:

dfTestData = testDataSpec.build(withStreaming=True,options={ ‘rowsPerSecond’: 5000})


* **Parameters**

    
    * **withTempView** – if True, automatically creates temporary view for generated data set


    * **withView** – If True, automatically creates global view for data set


    * **withStreaming** – If True, generates data using Spark Structured Streaming Rate source suitable for writing with writeStream


    * **options** – optional Dict of options to control generating of streaming data



* **Returns**

    Spark SQL dataframe of generated test data


<!-- !! processed by numpydoc !! -->

#### property build_order()
return the build order minus the id column

The build order will be a list of lists - each list specifying columns that can be built at the same time

<!-- !! processed by numpydoc !! -->

#### clone()
Make a clone of the data spec via deep copy preserving same spark session


* **Returns**

    deep copy of test data generator definition


<!-- !! processed by numpydoc !! -->

#### computeBuildPlan()
prepare for building


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### computeColumnBuildOrder()
compute the build ordering using a topological sort on dependencies


* **Returns**

    the build ordering


<!-- !! processed by numpydoc !! -->

#### describe()
return description of the dataset generation spec


* **Returns**

    Dict object containing key attributes of test data generator instance


<!-- !! processed by numpydoc !! -->

#### explain()
Explain the test data generation process


* **Returns**

    String containing explanation of test data generation for this specification


<!-- !! processed by numpydoc !! -->

#### static flatten(l)
flatten list


* **Parameters**

    **l** – list to flatten


<!-- !! processed by numpydoc !! -->

#### generateColumnDefinition(colName, colType=None, base_column='id', implicit=False, omit=False, nullable=True, \*\*kwargs)
generate field definition and column spec

Note: Any time that a new column definition is added, we’ll mark that the build plan needs to be regenerated.
For our purposes, the build plan determines the order of column generation etc.


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### classmethod generateName()
get a name for the data set
Uses the untitled name prefix and nextNameIndex to generate a dummy dataset name


* **Returns**

    string containing generated name


<!-- !! processed by numpydoc !! -->

#### getBaseDataFrame(start_id=0, streaming=False, options=None)
generate the base data frame and id column , partitioning the data if necessary


* **Returns**

    Spark data frame for base data that drives the data generation


<!-- !! processed by numpydoc !! -->

#### getColumnType(colName)
Get column Spark SQL datatype for specified column


* **Parameters**

    **colName** – name of column as string



* **Returns**

    Spark SQL datatype for named column


<!-- !! processed by numpydoc !! -->

#### getInferredColumnNames()
get list of output columns

<!-- !! processed by numpydoc !! -->

#### getOutputColumnNames()
get list of output columns by flattening list of lists of column names
normal columns will have a single column name but column definitions that result in
multiple columns will produce a list of multiple names


* **Returns**

    list of column names to be output in generated data set


<!-- !! processed by numpydoc !! -->

#### getOutputColumnNamesAndTypes()
get list of output columns by flattening list of lists of column names and types
normal columns will have a single column name but column definitions that result in
multiple columns will produce a list of multiple names

<!-- !! processed by numpydoc !! -->

#### get_column_data_types(columns)
Get data types for columns


* **Parameters**

    **columns** – = list of columns to retrieve data types for


<!-- !! processed by numpydoc !! -->

#### hasColumnSpec(colName)
returns true if there is a column spec for the column


* **Parameters**

    **colName** – name of column to check for



* **Returns**

    True if column has spec, False otherwise


<!-- !! processed by numpydoc !! -->

#### property inferredSchema()
infer spark interim schema definition from the field specifications

<!-- !! processed by numpydoc !! -->

#### isFieldExplicitlyDefined(colName)
return True if column generation spec has been explicitly defined for column, else false

<!-- !! processed by numpydoc !! -->

#### markForPlanRegen()
Mark that build plan needs to be regenerated


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### option(option_key, option_value)
set option to option value for later processing


* **Parameters**

    
    * **option_key** – key for option


    * **option_value** – value for option



* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### options(\*\*kwargs)
set options in bulk

Allows for multiple options with option=option_value style of option passing


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### classmethod reset()
reset any state associated with the data

<!-- !! processed by numpydoc !! -->

#### property schema()
infer spark output schema definition from the field specifications


* **Returns**

    Spark SQL StructType for schema


<!-- !! processed by numpydoc !! -->

#### property schemaFields()
get list of schema fields for final output schema


* **Returns**

    list of fields in schema


<!-- !! processed by numpydoc !! -->

#### scriptTable(name=None, location=None, table_format='delta')
generate create table script suitable for format of test data set


* **Parameters**

    
    * **name** – name of table to use in generated script


    * **location** – path to location of data. If specified (default is None), will generate an external table definition.



* **Returns**

    SQL string for scripted table


<!-- !! processed by numpydoc !! -->

#### classmethod seed(seedVal)
set seed for random number generation

Arguments:
:param seedVal: - new value for the random number seed

<!-- !! processed by numpydoc !! -->

#### setRowCount(rc)
Modify the row count - useful when starting a new spec from a clone


* **Parameters**

    **rc** – The count of rows to generate



* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### sqlTypeFromSparkType(dt)
Get sql type for spark type
:param dt: instance of Spark SQL type such as IntegerType()

<!-- !! processed by numpydoc !! -->

#### withColumn(colName, colType=StringType, min=None, max=None, step=1, data_range=None, prefix=None, random=False, distribution=None, base_column='id', nullable=True, omit=False, implicit=False, \*\*kwargs)
add a new column for specification


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


You may also add a variety of options to further control the test data generation process.
For full list of options, see databrickslabs_testdatagenerator.column_spec_options module.

<!-- !! processed by numpydoc !! -->

#### withColumnSpec(colName, min=None, max=None, step=1, prefix=None, random=False, distribution=None, implicit=False, data_range=None, omit=False, base_column='id', \*\*kwargs)
add a column specification for an existing column


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


You may also add a variety of options to further control the test data generation process.
For full list of options, see databrickslabs_testdatagenerator.column_spec_options module.

<!-- !! processed by numpydoc !! -->

#### withColumnSpecs(patterns=None, fields=None, match_types=None, \*\*kwargs)
Add column specs for columns matching

    
    1. list of field names,


    2. one or more regex patterns


    3. type (as in pyspark.sql.types)


* **Parameters**

    
    * **patterns** – patterns may specified a single pattern as a string or a list of patterns that match the column names. May be omitted.


    * **fields** – a string specifying an explicit field to match , or a list of strings specifying explicit fields to match. May be omitted.


    * **match_types** – a single Spark SQL datatype or list of Spark SQL data types to match. May be omitted.



* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


You may also add a variety of options to further control the test data generation process.
For full list of options, see databrickslabs_testdatagenerator.column_spec_options module.

<!-- !! processed by numpydoc !! -->

#### withIdOutput()
output id field as a column in the test data set if specified

If this is not called, the id field is omitted from the final test data set


* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->

#### withSchema(sch)
populate column definitions and specifications for each of the columns in the schema


* **Parameters**

    **sch** – Spark SQL schema, from which fields are added



* **Returns**

    modified in-place instance of test data generator allowing for chaining of calls following Builder pattern


<!-- !! processed by numpydoc !! -->
