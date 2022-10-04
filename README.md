# Databricks Labs Data Generator (`dbldatagen`)
[Release Notes](CHANGELOG.md) |
[Python Wheel](https://github.com/databrickslabs/dbldatagen/releases/tag/v.0.2.0-rc1-master) |
[Developer Docs](docs/USING_THE_APIS.md) |
[Examples](examples) |
[Tutorial](tutorial) 

[![build](https://github.com/databrickslabs/dbldatagen/workflows/build/badge.svg?branch=master)](https://github.com/databrickslabs/dbldatagen/actions?query=workflow%3Abuild+branch%3Amaster)
[![codecov](https://codecov.io/gh/databrickslabs/dbldatagen/branch/master/graph/badge.svg)](https://codecov.io/gh/databrickslabs/dbldatagen)
[![downloads](https://img.shields.io/github/downloads/databrickslabs/dbldatagen/total.svg)](https://hanadigital.github.io/grev/?user=databrickslabs&repo=dbldatagen)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/databrickslabs/dbldatagen.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/databrickslabs/dbldatagen/context:python)

## Project Description
The `dbldatgen` Databricks Labs project is a Python library for generating synthetic data within the Databricks 
environment using Spark. The generated data may be used for testing, benchmarking, demos and many 
other uses.

It operates by defining a data generation specification in code that controls 
how the synthetic data is to be generated.
The specification may incorporate use of existing schemas, or create data in an adhoc fashion.

It has no dependencies on any libraries that are not already incuded in the Databricks 
runtime, and you can use it from Scala, R or other languages by defining
a view over the generated data.

It supports:
* Generating synthetic data at scale up to billions of rows within minutes using appropriately sized clusters 
* Generating repeatable, predictable data supporting the needs for producing multiple tables, Change Data Capture, 
merge and join scenarios with consistency between primary and foreign keys
* Generating synthetic data for all of the 
Spark SQL supported primitive types as a Spark data frame which may be persisted, 
saved to external storage or 
used in other computations
* Generating ranges of dates, timestamps and numeric values
* Generation of discrete values - both numeric and text
* Generation of values at random and based on the values of other fields 
(either based on the `hash` of the underlying values or the values themselves)
* Ability to specify a distribution for random data generation 
* Generating arrays of values for ML style feature arrays
* Applying weights to the occurrence of values
* Generating values to conform to a schema or independent of an existing schema
* use of SQL expressions in test data generation
* plugin mechanism to allow use of 3rd party libraries such as Faker
* Use within a Databricks Delta Live Tables pipeline as a synthetic data generation source

Details of these features can be found in the [Developer Docs](docs/source/APIDOCS.md) and the online help
(which contains the full documentation including the HTML version of the Developer Docs) -
 [Online Help](https://databrickslabs.github.io/dbldatagen/public_docs/index.html). 



## Project Support
Please note that all projects in the `databrickslabs` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.

## Compatibility 
The Databricks Labs data generator framework can be used with Pyspark 3.1.2 and Python 3.8 or later. These are 
compatible with the Databricks runtime 9.1 LTS and later releases.

Older prebuilt releases are tested against Pyspark 3.0.1 (compatible with the Databricks runtime 7.3 LTS 
or later) and built with Python 3.7.5

For full library compatibility for a specific Databricks Spark release, see the Databricks 
release notes for library compatibility

- https://docs.databricks.com/release-notes/runtime/releases.html

## Using a pre-built release
The release binaries can be accessed at:
- Databricks Labs Github Data Generator releases - https://github.com/databrickslabs/dbldatagen/releases

To use download a wheel file and install using the Databricks install mechanism to install a wheel based
library into your workspace.

Alternatively, you can install the library as a notebook scoped library when working within the Databricks 
notebook environment through the use of a `%pip` cell in your notebook.

To install as a notebook-scoped library, create and execute a notebook cell with the following text:

> `%pip install git+https://github.com/databrickslabs/dbldatagen`

The `%pip install` method will work in the Databricks Community Environment and in Delta Live Tables also.

To install as a notebook-scoped library targetting earlier runtimes, use the following:

> `%pip install git+https://github.com/databrickslabs/dbldatagen@dbr_7_3_LTS_compat`

This will install the DBR 7.3 compatible archival version. 

However for the latest features, use the latest version.

## Using the Project
To use the project, the generated wheel should be installed in your Python notebook as a wheel based library

Once the library has been installed, you can use it to generate a test data frame.

For example

```buildoutcfg
import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
column_count = 10
data_rows = 1000 * 1000
df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                                  partitions=4)
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=column_count)
                            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )
                            
df = df_spec.build()
num_rows=df.count()                          
```

# Getting the Databricks data generator version
You can run the following on the command line to get the library version when building the code

```commandline
python -c "import dbldatagen as dg; print(dg.__version__)"
```

Inside a notebook, you can use the following to get the version:

# Building the code

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed build and testing instructions, including use of alternative 
build environments such as conda.

Dependencies are maintained by [Pipenv](https://pipenv.pypa.io/). In order to start with depelopment, 
you should install `pipenv` and `pyenv`.

Use `make test-with-html-report` to build and run the tests with a coverage report. 

Use `make dist` to make the distributable. The resulting wheel file will be placed in the `dist` subdirectory.
  
## Creating the HTML documentation

Run `make docs` from the main project directory.

The main html document will be in the file (relative to the root of the build directory) `./python/docs/docs/build/html/index.html`

## Running unit tests

If using an environment with multiple Python versions, make sure to use virtual env or similar to pick up correct python versions.

If necessary, set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to point to correct versions of Python.

Run  `make test` from the main project directory to run the unit tests.

## Feedback

Issues with the application?  Found a bug?  Have a great idea for an addition?
Feel free to file an issue.

## Project Support

Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are 
not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not 
make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use 
of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will 
be reviewed as time permits, but there are no formal SLAs for support.