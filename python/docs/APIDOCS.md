# Getting started with the Databricks Labs Test Data Generator

The Databricks labs test data generator is a Spark based solution for generating 
realistic synthetic data. It uses the features of Spark dataframes and Spark SQL 
to generate test data. As the output of the process is a dataframe populated 
with test data , it may be saved to storage in a variety of formats, saved to tables 
or generally manipulated using the existing Spark Dataframe APIs.

> NOTE: This document does not cover all of the classes and methods in the codebase.
>  For further information on classes and methods contained in  these modules, and 
> to explore the python documentation for these modules, build the HTML documentation from 
> the main project directory using `make docs`. Use your browser to explore the documentation by 
> starting with the html file  `./python/docs/build/html/index.html`
>

## General Overview

The Test Data Generator is a Python Library that can be used in several different ways:
1. Generate a test data set for an existing Spark SQL schema. 
2. Generate a test data set adding columns according to specifiers provided
3. Start with an existing schema and add columns along with specifications as to how values are generated

The test data generator includes the following features:

* Specify number of rows to generate
* Specify numeric, time and date ranges for columns
* Generate column data at random or from repeatable seed values
* Generate column data from list of finite column values optionally with weighting of how frequently values occur
* Use template based generation and formatting on string columns
* Use SQL based  expression to control or augment column generation
* Script Spark SQL table creation statement for dataset 


## Tutorials and examples

In the root directory of the project, there are a number of examples and tutorials.

The Python examples in the `examples` folder can be run directly or imported into the Databricks runtime environment as Python files.

The examples in the `tutorials` folder are in notebook export format and are intended to be imported into the Databricks runtime environment.
 
## Basic concepts

The Test Data Generator is a Python framework that uses Spark to generate a dataframe of test data. 

Once the data frame is generated, it can be used with any Spark dataframee compatible API to save or persist data, 
to analyze data, to write it to an external database or stream, or generally used in the same manner as a regular dataframe.

To consume it from Scala, R, SQL or other languages, create a view over the resulting test dataframe and you can use
it from any Databricks Spark runtime compatible language. By use of the appropriate parameters, 
you can instruct the test data generator to automatically register a view as part of generating the test data.

### Generating the test data
The test data generation process is controlled by a test data generation spec which can build a schema implicitly, 
or a schema can be added from an existing table or Spark SQL schema object.

Each column to be generated derives its test data from a set of one or more seed values. 
By default, this is the id field of the base data frame 
(generated with `spark.range` for batch data frames, or using a `Rate` source for streaming data frames).

Each column  can be specified as based on the `id` field or other columns in the test data generation spec. 
Columns may be based on the value of on or more base fields, or on a `hash` of the base values.

Column base values may also be generated at random.

All further data generation on a particular column is controlled by a series of transformations on this base value, 
typically one of :
 
* Mapping the base value to one of a set of discrete values, optionally with the use of weighting
* Arithmetic transformation of the base value 
* Adding string formatting to the base value

There is also support for applying arbitrary SQL expressions, and generation of common data from templates




## Creating a simple test data set

Here is an example of creating a simple test data set without use of a schema. 

```python 
row_count=1000 * 100
testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=row_count,
                                  partitions=4, seed_method='hash_fieldname', 
                                  verbose=True)
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=cls.column_count)
                            .withColumn("code1", IntegerType(), min=100, max=200)
                            .withColumn("code2", IntegerType(), min=0, max=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )

dfTestData = testDataSpec.build()
```

### Building Device IOT Test Data
This example shows generation of IOT device style data:
```python
import databrickslabs_testdatagenerator as dg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DateType, LongType
from databrickslabs_testdatagenerator import DateRange, NRange

shuffle_partitions_requested = 8
device_population=100000
data_rows = 20 * 1000000

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes=['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK','GB','IL',  'AU', 'SG', 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL' ]
country_weights=[1300, 365, 67, 38, 1300, 3, 7, 212,67,9,  25, 6, 47, 83, 126, 109, 58, 8, 17 ]

manufacturers = [ 'Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

lines = [ 'delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']


testDataSpec = (dg.DataGenerator(sparkSession=spark, name="device_data_set", rows=data_rows,
                                             partitions=partitions_requested, seed_method='hash_fieldname', verbose=True, debug=True)
    .withIdOutput()
    # we'll use hash of the base field to generate the ids to avoid a simple incrementing sequence
    .withColumn("internal_device_id", LongType(), min=0x1000000000000, unique_values=device_population)

    # note for format strings, we must use "%lx" not "%x" as the underlying value is a long
    .withColumn("device_id", StringType(), format="0x%013x", base_column="internal_device_id")
    #.withColumn("device_id_2", StringType(), format='0x%013x', base_column="internal_device_id")

    # the device / user attributes will be the same for the same device id - so lets use the internal device id as the base column for these attribute
    .withColumn("country", StringType(), values=country_codes, weights=country_weights,
          base_column="internal_device_id", base_column_type="hash")
    .withColumn("country2a", LongType(), expr="((hash(internal_device_id) % 3847) + 3847) % 3847", 
          base_column="internal_device_id")
    .withColumn("country2", IntegerType(), expr="floor(cast( (((internal_device_id % 3847) + 3847) % 3847) as double) )", 
          base_column="internal_device_id")
    .withColumn("country3", StringType(), values=country_codes, base_column="country2")
    .withColumn("manufacturer", StringType(), values=manufacturers, base_column="internal_device_id")

    # use omit = True if you dont want a column to appear in the final output but just want to use it as part of generation of another column
    .withColumn("line", StringType(), values=lines, base_column="manufacturer", 
           base_column_type="hash", omit=True)
    .withColumn("model_ser", IntegerType(), min=1, max=11,  base_column="device_id", base_column_type="hash", omit=True)

    .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)", base_column=["line", "model_ser"])
    .withColumn("event_type", StringType(), values=["activation", "deactivation", "plan change", "telecoms activity", "internet activity", "device error"], random=True)

    )

dfTestData = testDataSpec.build()

dfTestData.printSchema()

display(dfTestData)
```
