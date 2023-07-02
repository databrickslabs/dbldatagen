# Databricks Labs Data Generator (`dbldatagen`) 

<!-- Top bar will be removed from PyPi packaged versions -->
<!-- Dont remove: exclude package -->
[Documentation](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) |
[Release Notes](CHANGELOG.md) |
[Examples](examples) |
[Tutorial](tutorial) 
<!-- Dont remove: end exclude package -->

[![build](https://github.com/databrickslabs/dbldatagen/workflows/build/badge.svg?branch=master)](https://github.com/databrickslabs/dbldatagen/actions?query=workflow%3Abuild+branch%3Amaster)
[![PyPi package](https://img.shields.io/pypi/v/dbldatagen?color=green)](https://pypi.org/project/dbldatagen/)
[![codecov](https://codecov.io/gh/databrickslabs/dbldatagen/branch/master/graph/badge.svg)](https://codecov.io/gh/databrickslabs/dbldatagen)
[![PyPi downloads](https://img.shields.io/pypi/dm/dbldatagen?label=PyPi%20Downloads)](https://pypistats.org/packages/dbldatagen)

<!-- 
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/databrickslabs/dbldatagen.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/databrickslabs/dbldatagen/context:python)
[![downloads](https://img.shields.io/github/downloads/databrickslabs/dbldatagen/total.svg)](https://hanadigital.github.io/grev/?user=databrickslabs&repo=dbldatagen)
-->

## Project Description
The `dbldatagen` Databricks Labs project is a Python library for generating synthetic data within the Databricks 
environment using Spark. The generated data may be used for testing, benchmarking, demos, and many 
other uses.

It operates by defining a data generation specification in code that controls 
how the synthetic data is generated.
The specification may incorporate the use of existing schemas or create data in an ad-hoc fashion.

It has no dependencies on any libraries that are not already installed in the Databricks 
runtime, and you can use it from Scala, R or other languages by defining
a view over the generated data.

### Feature Summary
It supports:
* Generating synthetic data at scale up to billions of rows within minutes using appropriately sized clusters 
* Generating repeatable, predictable data supporting the need for producing multiple tables, Change Data Capture, 
merge and join scenarios with consistency between primary and foreign keys
* Generating synthetic data for all of the 
Spark SQL supported primitive types as a Spark data frame which may be persisted, 
saved to external storage or 
used in other computations
* Generating ranges of dates, timestamps, and numeric values
* Generation of discrete values - both numeric and text
* Generation of values at random and based on the values of other fields 
(either based on the `hash` of the underlying values or the values themselves)
* Ability to specify a distribution for random data generation 
* Generating arrays of values for ML-style feature arrays
* Applying weights to the occurrence of values
* Generating values to conform to a schema or independent of an existing schema
* use of SQL expressions in synthetic data generation
* plugin mechanism to allow use of 3rd party libraries such as Faker
* Use within a Databricks Delta Live Tables pipeline as a synthetic data generation source
* Generate synthetic data generation code from existing schema or data (experimental)

Details of these features can be found in the online documentation  -
 [online documentation](https://databrickslabs.github.io/dbldatagen/public_docs/index.html). 

## Documentation

Please refer to the [online documentation](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) for 
details of use and many examples.

Release notes and details of the latest changes for this specific release
can be found in the GitHub repository
[here](https://github.com/databrickslabs/dbldatagen/blob/release/v0.3.5/CHANGELOG.md)

# Installation

Use `pip install dbldatagen` to install the PyPi package.

Within a Databricks notebook, invoke the following in a notebook cell
```commandline
%pip install dbldatagen
```

The Pip install command can be invoked within a Databricks notebook, a Delta Live Tables pipeline 
and even works on the Databricks community edition.

The documentation [installation notes](https://databrickslabs.github.io/dbldatagen/public_docs/installation_notes.html) 
contains details of installation using alternative mechanisms.

## Compatibility 
The Databricks Labs Data Generator framework can be used with Pyspark 3.1.2 and Python 3.8 or later. These are 
compatible with the Databricks runtime 9.1 LTS and later releases.

Older prebuilt releases are tested against Pyspark 3.0.1 (compatible with the Databricks runtime 7.3 LTS 
or later) and built with Python 3.7.5

For full library compatibility for a specific Databricks Spark release, see the Databricks 
release notes for library compatibility

- https://docs.databricks.com/release-notes/runtime/releases.html

When using the Databricks Labs Data Generator on "Unity Catalog" enabled environments, the Data Generator requires
the use of `Single User` or `No Isolation Shared` access modes as some needed features are not available in `Shared` 
mode (for example, use of 3rd party libraries). Depending on settings, the `Custom` access mode may be supported.

See the following documentation for more information:

- https://docs.databricks.com/data-governance/unity-catalog/compute.html

## Using the Data Generator
To use the data generator, install the library using the `%pip install` method or install the Python wheel directly 
in your environment.

Once the library has been installed, you can use it to generate a data frame composed of synthetic data.

For example

```buildoutcfg
import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
column_count = 10
data_rows = 1000 * 1000
df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                                  partitions=4)
           .withIdOutput()
           .withColumn("r", FloatType(), 
                            expr="floor(rand() * 350) * (86400 + 3600)",
                            numColumns=column_count)
           .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
           .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
           .withColumn("code3", StringType(), values=['a', 'b', 'c'])
           .withColumn("code4", StringType(), values=['a', 'b', 'c'], 
                          random=True)
           .withColumn("code5", StringType(), values=['a', 'b', 'c'], 
                          random=True, weights=[9, 1, 1])
 
           )
                            
df = df_spec.build()
num_rows=df.count()                          
```
Refer to the [online documentation](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) for further 
examples. 

The GitHub repository also contains further examples in the examples directory.

## Spark and Databricks Runtime Compatibility
The `dbldatagen` package is intended to be compatible with recent LTS versions of the Databricks runtime, including 
older LTS versions at least from 10.4 LTS and later. It also aims to be compatible with Delta Live Table runtimes, 
including `current` and `preview`. 

While we don't specifically drop support for older runtimes, changes in Pyspark APIs or
APIs from dependent packages such as `numpy`, `pandas`, `pyarrow`, and `pyparsing` make cause issues with older
runtimes. 

By design, installing `dbldatagen` does not install releases of dependent packages in order 
to preserve the curated set of packages pre-installed in any Databricks runtime environment.

When building on local environments, the build process uses the `Pipfile` and requirements files to determine 
the package versions for releases and unit tests. 

## Project Support
Please note that all projects released under [`Databricks Labs`](https://www.databricks.com/learn/labs)
 are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements 
(SLAs).  They are provided AS-IS, and we do not make any guarantees of any kind.  Please do not submit a support ticket 
relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as issues on the GitHub Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.


## Feedback

Issues with the application?  Found a bug?  Have a great idea for an addition?
Feel free to file an [issue](https://github.com/databrickslabs/dbldatagen/issues/new).

