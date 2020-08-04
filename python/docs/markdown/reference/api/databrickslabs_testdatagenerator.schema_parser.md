# databrickslabs_testdatagenerator.schema_parser module

<!-- !! processed by numpydoc !! -->

### class SchemaParser()
Bases: `object`

” SchemaParser class

Creates pyspark SQL datatype from string

### Methods

| `columnTypeFromString`(type_string)

 | Generate a Spark SQL data type from a string

 |
| `parseCreateTable`(sparkSession, source_schema)

    | Parse a schema from a schema string

                                                                                                                                                                                         |
| `parseDecimal`(str)

                                | parse a decimal specifier

                                                                                                                                                                                                   |
<!-- !! processed by numpydoc !! -->

#### classmethod columnTypeFromString(type_string)
Generate a Spark SQL data type from a string


* **Parameters**

    **type_string** – String representation of SQL type such as ‘integer’ etc



* **Returns**

    Spark SQL type


<!-- !! processed by numpydoc !! -->

#### classmethod parseCreateTable(sparkSession, source_schema)
Parse a schema from a schema string


* **Parameters**

    **source_schema** – should be a table definition minus the create table statement



* **Returns**

    Spark SQL schema instance


<!-- !! processed by numpydoc !! -->

#### classmethod parseDecimal(str)
parse a decimal specifier


* **Parameters**

    **str** – 
    * decimal specifier string such as decimal(19,4), decimal or decimal(10)




* **Returns**

    DecimalType instance for parsed result


<!-- !! processed by numpydoc !! -->
