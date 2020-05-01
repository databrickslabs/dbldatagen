### Test Data Generator 
Define a data generator

Generates test data data from schema and field specifications


##Implemementation

### Data Generation
* for number of rows specified, generate implicit id ranging from 0 to n - 1
* for each column in schema or interim set of columns , generate column according to colum spec. compute is in order of columns added - columns in schema are considered to be added first
* if no column spec is specified , columns are generated as id column values converted to column type. for columns of date or timestamp - these will be converted to date or timestamp as interpreting first as numeric timestamp. For string values default generation pattern will be to generate string ’item-’+ value of id column
* if column spec is supplied , column generation will proceed according to spec - note column spec does not add column - only describes previously added column ( via schema or addColumn)
* if column spec already exists , later column spec supercedes it
* if column already exists ( due to being in schema or specified via addColumn), adding colum causes error
* column generation proceeds according to order of add column
* output will  drop columns that are implicit ( such as id) or columns where column spec specifies omit=True. If the id column is to be retained a column spec can be specified for id with omit=False
* a schema is not required - the resulting schema will be result of all addColumn statements
* final output layout will be derived from schema + add columns

### Column value generation
- Values for each column will be in the order of columns added. Optionally, we will reorder column generation for more efficient column generation but that will not affect layout of final dataset ( we will do select if necessary to reorder)
- Each column will be generated based on base column value or a random value in the range 0 to number of rows. By default the base column is the id column - this may be overridden
- If column has limited number of values, the base value will use modulo arithmetic to compute the value 
for array based feature columns , the column spec will determine generation of each value
- Column spec may specify sql expression which will override default column data generation
column spec may specify min , max values and step to control value cycling
- Column spec may specify list of values to restruct values to specific values
- String values are generated from prefix _ base value _ suffix
- Use of omit=True omits column from final output

## Implementation details
* Columns are controlled by the schemaFields which is a collection of StructField definitions, and column definitions which controls how to generate the output fields
* If a column definition is implicit , it may be overwritten
* if a column definition is specified as 'omit', it is omitted from the final set of fields

## Coding style
We will follow the PySpark coding conventions rather than the standard PEP naming conventions
* variables and functions will be named using camel case starting with lower case and uppercasing each word rather than the Python style guide recommendation of underscores to separate each word. I.e. `rowCount` not `row_count`
* Classes and types will be named using camel case starting with upper case - i.e `FieldDefinition`
