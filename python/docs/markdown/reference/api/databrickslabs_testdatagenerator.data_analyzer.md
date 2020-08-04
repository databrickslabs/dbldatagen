# databrickslabs_testdatagenerator.data_analyzer module

This module defines the DataAnalyzer class. This is still a work in progress.

<!-- !! processed by numpydoc !! -->

### class DataAnalyzer(df, sparkSession=None)
Bases: `object`

This class is used to analyze an existing data set to assist in generating a test data set with similar characteristics


* **Parameters**

    
    * **df** – dataframe to analyze


    * **sparkSession** – spark session instance to use when performing spark operations


### Methods

| `displayRow`(row)

 | Display details for row

 |
| `getDistinctCounts`()

                           | Get distinct counts

                                                      |
| `getFieldNames`(schema)

                         | get field names from schema

                                              |
| `lookupFieldType`(typ)

                          | Perform lookup of type name by Spark SQL type name

                       |
| `prependSummary`(df, heading)

                   | Prepend summary information

                                              |
| `summarize`()

                                   | Generate summary

                                                         |
| `summarizeField`(field)

                         | Generate summary for individual field

                                    |
| `summarizeFields`(schema)

                       | Generate summary for all fields in schema

                                |
<!-- !! processed by numpydoc !! -->

#### displayRow(row)
Display details for row

<!-- !! processed by numpydoc !! -->

#### getDistinctCounts()
Get distinct counts

<!-- !! processed by numpydoc !! -->

#### getFieldNames(schema)
get field names from schema

<!-- !! processed by numpydoc !! -->

#### lookupFieldType(typ)
Perform lookup of type name by Spark SQL type name

<!-- !! processed by numpydoc !! -->

#### prependSummary(df, heading)
Prepend summary information

<!-- !! processed by numpydoc !! -->

#### summarize()
Generate summary

<!-- !! processed by numpydoc !! -->

#### summarizeField(field)
Generate summary for individual field

<!-- !! processed by numpydoc !! -->

#### summarizeFields(schema)
Generate summary for all fields in schema

<!-- !! processed by numpydoc !! -->
