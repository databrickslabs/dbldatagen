# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Labs Data Generator ###
# MAGIC The Databricks Labs data generator automates the process of generating test data of a given size, conforming to a specific layout, and allows specification of how data is generated for each field.
# MAGIC 
# MAGIC A schema can be optionally supplied, and new fields can be added to augment the set of fields defined in the schema.
# MAGIC 
# MAGIC The Databricks labs data generator allows specification of options for number of rows to be generated and how the generated test set is partitioned.
# MAGIC 
# MAGIC The data generator produces a spark data frame ( in Python) - which may be subsequently be saved, transformed, cached, persisted or used in any way a PySpark data frame can be used.
# MAGIC 
# MAGIC By declaring a view (temporary or otherwise) over the dataframe, the test data may be exposed to Scala usage also. 

# COMMAND ----------

# MAGIC %md #### General Operation ####
# MAGIC 
# MAGIC The data generator operates by initially generating an `id` column with an integer id in a strictly increasing
# MAGIC sequence. Each additional column can be generated based on a transformation of this id, the transformation of
# MAGIC some other previously defined column, or based on a random number.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Implemementation
# MAGIC 
# MAGIC ### Data Generation
# MAGIC * for number of rows specified, generate implicit id ranging from 0 to n - 1
# MAGIC * for each column in schema or interim set of columns , generate column according to colum spec. compute is in order of columns added - columns in schema are considered to be added first
# MAGIC * if no column spec is specified , columns are generated as id column values converted to column type. for columns of date or timestamp - these will be converted to date or timestamp as interpreting first as numeric timestamp. For string values default generation pattern will be to generate string ’item-’+ value of id column
# MAGIC * if column spec is supplied , column generation will proceed according to spec - note column spec does not add column - only describes previously added column ( via schema or addColumn)
# MAGIC * if column spec already exists , later column spec supercedes it
# MAGIC * if column already exists ( due to being in schema or specified via addColumn), adding colum causes error
# MAGIC * column generation proceeds according to order of add column
# MAGIC * output will  drop columns that are implicit ( such as id) or columns where column spec specifies omit=True. If the id column is to be retained a column spec can be specified for id with omit=False
# MAGIC * a schema is not required - the resulting schema will be result of all addColumn statements
# MAGIC * final output layout will be derived from schema + add columns
# MAGIC 
# MAGIC ### Column value generation
# MAGIC - Values for each column will be in the order of columns added. Optionally, we will reorder column generation for more efficient column generation but that will not affect layout of final dataset ( we will do select if necessary to reorder)
# MAGIC - Each column will be generated based on base column value or a random value in the range 0 to number of rows. By default the base column is the id column - this may be overridden
# MAGIC - If column has limited number of values, the base value will use modulo arithmetic to compute the value 
# MAGIC for array based feature columns , the column spec will determine generation of each value
# MAGIC - Column spec may specify sql expression which will override default column data generation
# MAGIC column spec may specify minValue , maxValue values and step to control value cycling
# MAGIC - Column spec may specify list of values to restruct values to specific values
# MAGIC - String values are generated from prefix _ base value _ suffix
# MAGIC - Use of omit=True omits column from final output
# MAGIC 
# MAGIC ## Implementation details
# MAGIC * Columns are controlled by the schemaFields which is a collection of StructField definitions, and column definitions which controls how to generate the output fields
# MAGIC * If a column definition is implicit , it may be overwritten
# MAGIC * if a column definition is specified as 'omit', it is omitted from the final set of fields

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coding style ##
# MAGIC We will follow the PySpark coding conventions rather than the standard PEP naming conventions
# MAGIC * variables and functions will be named using camel case starting with lower case and uppercasing each word rather than the Python style guide recommendation of underscores to separate each word. I.e. `rowCount` not `row_count`
# MAGIC 
# MAGIC * Classes and types will be named using camel case starting with upper case - i.e `FieldDefinition`