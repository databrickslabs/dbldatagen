# Getting Started with the Databricks Labs Data Generator

The Databricks Labs data generator (aka `dbldatagen`) is a Spark-based solution for generating 
realistic synthetic data. It uses the features of Spark dataframes and Spark SQL 
to create synthetic data at scale. As the process produces a Spark dataframe populated 
with generated data, it may be written to storage in various data formats, saved to tables 
or manipulated using the existing Spark Dataframe APIs. 

The data generator can also be used as a source in a Delta Live Tables pipelines, 
supporting streaming and batch operation.

It has no dependencies on any libraries not already installed in the Databricks 
runtime, and you can use it from Scala, R or other languages by defining
a view over the generated data.

As the data generator is a Spark process, the data generation process can scale to producing synthetic data with 
billions of rows in minutes with reasonable-sized clusters.

For example, at the time of writing, a billion-row version of the IOT data set example listed later in the document
can be generated and written to a Delta table in 
[under 2 minutes using a 12 node x 8 core cluster (using DBR 8.3)](#scaling-it-up)

> NOTE: The markup version of this document does not cover all of the classes and methods in the codebase and some links
> may not work. To see the documentation for the latest release, see the online documentation. 

## General Overview

The Databricks Labs Data Generator is a Python Library that can be used in several different ways:
1. Generate a synthetic data set [without defining a schema in advance](#create-a-data-set-without-pre-existing-schemas)
2. Generate a synthetic data set [for an existing Spark SQL schema.](#creating-data-set-with-pre-existing-schema) 
3. Generate a synthetic data set adding columns according to the specifiers provided
4. Start with an existing schema and add columns along with specifications as to how values are generated

The data generator includes the following features:

* Specify [number of rows to generate](#create-a-data-set-without-pre-existing-schemas)
* Specify [number of Spark partitions to distribute data generation across](#scaling-it-up)
* Specify [numeric, time, and date ranges for columns](./DATARANGES.md)
* Generate column data at [random or from repeatable seed values](#generating-repeatable-data)
* Generate column data from [one or more seed columns](#generating-repeatable-data)  
[values optionally with weighting](#create-a-data-set-without-pre-existing-schemas) of how frequently values occur
* Use [template-based text generation](#creating-data-set-with-pre-existing-schema) 
and [formatting on string columns](textdata)
* Use [SQL based expressions](#using-sql-in-data-generation) to control or augment column generation
* Script Spark SQL table creation statement for dataset 
* Specify a [statistical distribution for random values](./DISTRIBUTIONS.md)
* Support for use within Databricks Delta Live Tables pipelines
* Support for code generation from existing schema or Spark dataframe to synthesize data


## Tutorials and examples

In the 
[Github project directory](https://github.com/databrickslabs/dbldatagen/tree/release/v0.2.1) , 
there are several examples and tutorials.

The Python examples in the `examples` folder can be run directly or imported into the Databricks runtime environment 
as Python notebooks.

The examples in the `tutorials` folder are in Databricks notebook export format and can be imported 
into the Databricks workspace environment.
 
## Basic concepts

The Databricks Labs Data Generator is a Python framework that uses Spark to generate a dataframe of synthetic data. 

Once the data frame is generated, it can be used with any Spark dataframe compatible API to save or persist data, 
to analyze data, to write it to an external database or stream, or used in the same manner as a regular 
PySpark dataframe.

To consume it from Scala, R, SQL or other languages, create a view over the resulting generated dataframe and 
you can use it from any Databricks Spark runtime compatible language. By use of the appropriate parameters, 
you can instruct the data generator to automatically register a view as part of generating the synthetic data.

### Generating the synthetic data
The data generation process is controlled by a data generation spec, defined in code 
which can build a schema implicitly, 
or a schema can be added from an existing table or Spark SQL schema object.

Each column to be generated derives its generated data from a set of one or more seed values. 
By default, this is the `id` field of the base data frame 
(generated with `spark.range` for batch data frames, or using a `Rate` source for streaming data frames).

Each column  can be specified as based on the `id` field or other columns in the synthetic data generation spec. 
Columns may be based on the value of on or more base fields, or on a `hash` of the base values.

Column base values may also be generated at random.

All further data generation on a particular column is controlled by a series of transformations on this base value, 
typically one of :
 
* Mapping the base value to one of a set of discrete values, optionally with the use of weighting
* Arithmetic transformation of the base value 
* Adding string formatting to the base value

There is also support for applying arbitrary SQL expressions, and generation of common data from templates

### Getting started

Before using the data generator, you need to install the package in your environment and import it in your code.
You can install the package from PyPi as a library on your cluster. 

> NOTE: When running in a Databricks notebook environment, you can install directly using 
> the `%pip` command in a notebook cell
>  
> To install as a notebook scoped library, add a cell with the following text and execute it:
>
> `%pip install dbldatagen`
 
The `%pip install` method will work in the Databricks Community Environment and in Delta Live Tables pipelines also.

You can find more details and alternative installation methods at [Installation notes](installation_notes)

The Github based releases are located at 
[Databricks Labs Data Generator releases](https://github.com/databrickslabs/dbldatagen/releases)

Once installed, import the framework in your Python code to use it.

For example:

```python 
import dbldatagen as dg
```

Creating the data set is performed by creating a definition for your dataset via the `DataGenerator` instance, which 
specifies the rules that control data generation. 

Once the `DataGenerator` specification is created, you use the `build` method to generate a Spark dataframe for the 
data

## Creating simple synthetic data sets

You can use the data generator with, or without the use of a pre-existing schema. 
The basic mechanism is as follows:

* Define a `DataGenerator` instance
* Optionally add a schema
* If using a schema, add specifications for how the columns are to be generated with the
`withColumnSpec` method. This does not add new columns, but specifies how columns imported from the 
schema should be generated
* Add new columns with the `withColumn` method

> **_NOTE:_** Column names are case insensitive in Spark SQL. 
> Hence adding a new column that has the same column name but differs in case from an existing column
> name will succeed but potentially cause errors later.
>
> When adding a column spec for an existing column, it is not necessary to define the type.

### Create a data set without pre-existing schemas

Here is an example of creating a simple synthetic data set without use of a schema. 

```python 
import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType

row_count = 1000 * 100
column_count = 10
testDataSpec = (
    dg.DataGenerator(spark, name="test_data_set1", rows=row_count, partitions=4)
    .withIdOutput()
    .withColumn(
        "r",
        FloatType(),
        expr="floor(rand() * 350) * (86400 + 3600)",
        numColumns=column_count,
    )
    .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
    .withColumn("code2", "integer", minValue=0, maxValue=10, random=True)
    .withColumn("code3", StringType(), values=["online", "offline", "unknown"])
    .withColumn(
        "code4", StringType(), values=["a", "b", "c"], random=True, percentNulls=0.05
    )
    .withColumn(
        "code5", "string", values=["a", "b", "c"], random=True, weights=[9, 1, 1]
    )
)

dfTestData = testDataSpec.build()
```
By default, the seed column for each row is the `id` column. Use of the method `withIdOutput()` retains the `id` 
field in the output data. If this is not called, the `id` field is used during data generation, but it is dropped 
from the final data output.

Each of the `withColumn` method calls introduces a new column (or columns).

The example above shows some common uses:

- The `withColumn` method call for the `r` column introduces multiple columns labelled `r1` to `rN` as determined by 
the `numColumns` option. Here, we use the `expr` option to introduce a SQL expression to control the generation of the 
column. Note this expression can refer to any preceding column including the `id` column.

- The `withColumn` method call for the `code1` column specifies the generation of values between 100 and 200 
inclusive. These will be computed using modulo arithmetic on the `id` column. 

- The `withColumn` method call for the `code2` column specifies the generation of values between 0 and 10 
inclusive. These will be computed via a uniformly distributed random value. Note that type strings can be used
in place of "IntegerType()"

> By default all random values are uniformly distributed
> unless either the `weights` option is used or a specific distribution is used. 

- The `withColumn` method call for the `code3` column specifies the generation of string values from 
the allowable values `['online', 'offline', or 'unknown']`
inclusive. These will be computed using modulo arithmetic on the `id` column and the resulting value mapped to the 
set of allowable values

> Specific value lists can be used with any data type fields - but user is responsible for ensuring the values are of 
>a compatible data type.

- The `withColumn` method call for the `code4` column specifies the generation of string values from 
the allowable values `['a', 'b', or 'c']`
inclusive. But the `percentNulls` option gives a 5% chance of a null value being generated. 
These will be computed via a uniformly distributed random value.

> By default null values will not be generated for a field, unless the `percentNulls` option is specified

- The `withColumn` method call for the `code5` column specifies the generation of string values from 
the allowable values `['a', 'b', or 'c']`
inclusive. These will be computed via a uniformly distributed random value but with weighting applied so that
the value `a` occurs 9 times as frequently as the values `b` or `c`

> NOTE: As the seed field named `id` is currently reserved for system use, manually adding a column named `id` can 
> interfere with the data generation. This will be fixed in a forthcoming release

### Creating data set with pre-existing schema
What if we want to generate data conforming to a pre-existing schema? you can specify a schema for your data by either 
taking a schema from an existing table, or computing an explicit schema. 

In this case you would use the `withColumnSpec` method instead of the `withColumn` method.

For fields imported from the schema, the schema controls the field name and data type, but the column specification 
controls how the data is generated.

The following example illustrates use of a table schema to generate data for a table. 
The schema can also be a schema from a view or a manually created schema 

```
import dbldatagen as dg
from pyspark.sql.types import StructType, StructField,  StringType

shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = 10000000

spark.sql("""Create table if not exists test_vehicle_data(
                name string, 
                serial_number string, 
                license_plate string, 
                email string
                ) using Delta""")

table_schema = spark.table("test_vehicle_data").schema

print(table_schema)
  
dataspec = (dg.DataGenerator(spark, rows=10000000, partitions=8)
            .withSchema(table_schema))

dataspec = (
    dataspec.withColumnSpec("name", percentNulls=0.01, template=r"\\w \\w|\\w a. \\w")
    .withColumnSpec(
        "serial_number", minValue=1000000, maxValue=10000000, prefix="dr", random=True
    )
    .withColumnSpec("email", template=r"\\w.\\w@\\w.com")
    .withColumnSpec("license_plate", template=r"\\n-\\n")
)
df1 = dataspec.build()

df1.write.format("delta").mode("overwrite").saveAsTable("test_vehicle_data")
```

Note the `template` parameter allows the generation of arbitrary text such as email addresses, VINs etc.

#### Adding dataspecs to match multiple columns
For large schemas, it can be unwieldy to specify column generation specs for every column in a schema. 

To alleviate this , the framework provides mechanisms to add rules in bulk for multiple columns.

- The `withColumnSpecs` method introduces a column generation specification for all columns matching a specific 
naming pattern or datatype. You can override the column specification for a specific column using 
the `withColumnSpec` method.

For example:

```python
(dg
   .withColumnSpecs(patterns=".*_ID", matchTypes=StringType(), format="%010d", 
                           minValue=10, maxValue=123, step=1)
   .withColumnSpecs(patterns=".*_IDS", matchTypes=StringType(), format="%010d", 
                           minValue=1, maxValue=100, step=1)
)
```
## Generating code from existing an schema or Spark dataframe

You can use the Data Analyzer class to generate code from an existing schema or Spark dataframe. 
The generated code will provide a basic data generator to produce synthetic data that conforms to the 
schema provided or to the schema of the data frame. 

This will allow for quick prototyping of data generation code.

For example, the following code will generate synthetic data generation code from a source dataframe.

```python
import dbldatagen as dg

dfSource = spark.read.format("parquet").load("/tmp/your/source/dataset")

analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

generatedCode = analyzer.scriptDataGeneratorFromData()
```

See the ``DataAnalyzer`` class and section on `Generating From Existing Data` for more details.

## A more complex example - building Device IOT synthetic Data
This example shows generation of IOT device style data consisting of events from devices. 

Here we want to generate a random set of events but ensure that the device properties remain the same for the 
device from event to event.

```python
from pyspark.sql.types import LongType, IntegerType, StringType

import dbldatagen as dg

shuffle_partitions_requested = 8
device_population = 100000
data_rows = 20 * 1000000
partitions_requested = 20

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes = [
    "CN", "US", "FR", "CA", "IN", "JM", "IE", "PK", "GB", "IL", "AU", 
    "SG", "ES", "GE", "MX", "ET", "SA", "LB", "NL",
]
country_weights = [
    1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 
    126, 109, 58, 8, 17,
]

manufacturers = [
    "Delta corp", "Xyzzy Inc.", "Lakehouse Ltd", "Acme Corp", "Embanks Devices",
]

lines = ["delta", "xyzzy", "lakehouse", "gadget", "droid"]

testDataSpec = (
    dg.DataGenerator(spark, name="device_data_set", rows=data_rows, 
                     partitions=partitions_requested)
    .withIdOutput()
    # we'll use hash of the base field to generate the ids to
    # avoid a simple incrementing sequence
    .withColumn("internal_device_id", "long", minValue=0x1000000000000, 
                uniqueValues=device_population, omit=True, baseColumnType="hash",
    )
    # note for format strings, we must use "%lx" not "%x" as the
    # underlying value is a long
    .withColumn(
        "device_id", "string", format="0x%013x", baseColumn="internal_device_id"
    )
    # the device / user attributes will be the same for the same device id
    # so lets use the internal device id as the base column for these attribute
    .withColumn("country", "string", values=country_codes, weights=country_weights, 
                baseColumn="internal_device_id")
    .withColumn("manufacturer", "string", values=manufacturers, 
                baseColumn="internal_device_id", )
    # use omit = True if you don't want a column to appear in the final output
    # but just want to use it as part of generation of another column
    .withColumn("line", "string", values=lines, baseColumn="manufacturer", 
                baseColumnType="hash", omit=True )
    .withColumn("model_ser", "integer", minValue=1, maxValue=11, baseColumn="device_id", 
                baseColumnType="hash", omit=True, )
    .withColumn("model_line", "string", expr="concat(line, '#', model_ser)", 
                baseColumn=["line", "model_ser"] )
    .withColumn("event_type", "string", 
                values=["activation", "deactivation", "plan change", "telecoms activity", 
                        "internet activity", "device error", ],
                random=True)
    .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", 
                end="2020-12-31 23:59:00", 
                interval="1 minute", random=True )
)

dfTestData = testDataSpec.build()

display(dfTestData)
```

- The `withColumn` method call for the `internalDeviceId` column uses the `uniqueValues` option to control the number 
of unique values.
- The `withColumn` method call for the `manufacture` column uses the `baseColumn` option to ensure we get the same 
manufacturer value for each `internalDeviceId`. This allows us to generate IOT style events at random, but still 
constrain properties whenever the same `internalDeviceId` occurs.  
of unique values.

> A column may be based on one or more other columns. This means the value of that column will be used as a seed for 
>generating the new column. The `baseColumnType` option determines if the actual value , or hash of the value is 
>used as the seed value. 

- The `withColumn` method call for the `line` column introduces a temporary column for purposes of 
generating other columns, but through the use of the `omit` option, omits it from the final data set.

> NOTE: Type strings can be used in place of instances of data type objects. Type strings use SQL data type syntax
> and can be used to specify basic types, numeric types such as "decimal(10,3)" as well as complex structured types
> such as "array<string>", "map<string, int>" and "struct<a:binary, b:int, c:float>".
> 
> Type strings are case-insensitive.

### Scaling it up

When generating data, the number of rows to be generated is controlled by the `rows` parameter supplied to the 
creation of the data generator instance.

The number of Spark tasks to be used is controlled by the `partitions` parameter supplied to the 
creation of the data generator instance.

As with any Spark process, the number of Spark tasks controls how many nodes are used to parallelize the activity, with 
each Spark task being assigned to an executor running on an node.

By increasing the number of rows and number of Spark partitions, you can scale up generation to 100s of millions 
or billions of rows.

For example, using the same code as before, but with different rows and partitions settings, you can generate a billion
rows of data and write it to a Delta table in under 2 minutes (with a 12 node x 8 core cluster)

```python
from pyspark.sql.types import LongType, IntegerType, StringType

import dbldatagen as dg

shuffle_partitions_requested = 12 * 4
partitions_requested = 96
device_population = 100000
data_rows = 1000 * 1000000

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG', 'ES',
                 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                   17]

manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

testDataSpec = (
    dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                                 partitions=partitions_requested)
    .withIdOutput()
    # we'll use hash of the base field to generate the ids to 
    # avoid a simple incrementing sequence
    .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                uniqueValues=device_population, omit=True, baseColumnType="hash")

    # note for format strings, we must use "%lx" not "%x" as the 
    # underlying value is a long
    .withColumn("device_id", StringType(), format="0x%013x",
                baseColumn="internal_device_id")

    # the device / user attributes will be the same for the same device id 
    # so lets use the internal device id as the base column for these attribute
    .withColumn("country", StringType(), values=country_codes,
                weights=country_weights,
                baseColumn="internal_device_id")
    .withColumn("manufacturer", StringType(), values=manufacturers,
                baseColumn="internal_device_id")

    # use omit = True if you don't want a column to appear in the final output 
    # but just want to use it as part of generation of another column
    .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                baseColumnType="hash", omit=True)
    .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                baseColumn="device_id",
                baseColumnType="hash", omit=True)

    .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                baseColumn=["line", "model_ser"])
    .withColumn("event_type", StringType(),
                values=["activation", "deactivation", "plan change",
                        "telecoms activity", "internet activity", "device error"],
                random=True)
    .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", 
                end="2020-12-31 23:59:00",
                interval="1 minute", 
                random=True)

    )

dfTestData = testDataSpec.build()

dfTestData.write.format("delta").mode("overwrite").save(
    "/tmp/dbldatagen_examples/a_billion_row_table")
```

## Using SQL in data generation
Any column specification can use arbitrary SQL expressions during data generation via the `expr` parameter.

The following example shows generation of synthetic names, email addresses and 
use of a SQL expression to compute MD5 hashes of hypothetical synthetic credit card :

```python
import dbldatagen as dg

shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = 10000000

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

dataspec = (
    dg.DataGenerator(spark, rows=data_rows, partitions=8)
    .withColumn("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w') 
    .withColumn("payment_instrument_type", values=['paypal', 'visa', 'mastercard', 'amex'], 
                random=True)             
    .withColumn("int_payment_instrument", "int",  minValue=0000, maxValue=9999,  
                baseColumn="name",
                baseColumnType="hash", omit=True)
    .withColumn("payment_instrument", 
                 expr="format_number(int_payment_instrument, '**** ****** *####')",
                 baseColumn="int_payment_instrument")
    .withColumn("email", template=r'\\w.\\w@\\w.com')       
    .withColumn("md5_payment_instrument", 
                expr="md5(concat(payment_instrument_type, ':', payment_instrument))",
                baseColumn=['payment_instrument_type', 'payment_instrument']) 
   )
df1 = dataspec.build()

df1.show()
```

See the section on [text generation](textdata) for more details on these options.

### Generating repeatable data

By default, data is generated by a series of pre-determined transformations on an implicit `id` column in the data set.
Specifying the `baseColumn` argument allows specifying a dependency on one or more other columns. Note that use of the 
parameter `baseColumnType` will allow determining how the base column is used - for example you can specify that the 
actual value, the hash of the value or an array of the values will be used as the starting point.

So generating data sets from the same data specification will result in the same data set every time unless random data 
generation is deliberately chosen for a column. 

If random data is desired, the generated data will be generated using a standard uniform random data generator unless a 
specific [distribution](DISTRIBUTIONS) is used.

However it is often desirable to generate random data that is repeatable - this is where the random seed is used. 
By specifying a random seed value, you can ensure generation of random data but where random sequence repeats 
across runs.

The following example generates repeatable data across runs:

```python
import dbldatagen as dg

shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = 10000000

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

dataspec = (
    dg.DataGenerator(spark, rows=data_rows, partitions=8, randomSeedMethod="hash_fieldname", 
                     randomSeed=42)
    .withColumn("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
    .withColumn("payment_instrument_type", values=['paypal', 'visa', 'mastercard', 'amex'],
                random=True)
    .withColumn("int_payment_instrument", "int",  minValue=0000, maxValue=9999,  
                baseColumn="name",
                baseColumnType="hash", omit=True)
    .withColumn("payment_instrument", 
                expr="format_number(int_payment_instrument, '**** ****** *####')",
                baseColumn="int_payment_instrument")
    .withColumn("email", template=r'\\w.\\w@\\w.com')
    .withColumn("md5_payment_instrument",
                expr="md5(concat(payment_instrument_type, ':', payment_instrument))",
                baseColumn=['payment_instrument_type', 'payment_instrument'])
    )
df1 = dataspec.build()

df1.show()
```

These options allows for generating multiple tables and ensuring referential 
integrity across primary and foreign keys.

## Using generated data from SQL
By defining a view over the generated data, you can use the generated data from SQL.

To simply this process, you can use options to the `build` method to generate temporary or global views over the data.

At the time of writing, the specification for generation of data needs to be authored in Python.
