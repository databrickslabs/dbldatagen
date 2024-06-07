# Databricks Labs Synthetic Data Generator Release Notes

## Change History
All notable changes to the Databricks Labs Data Generator will be documented in this file.

### Version 0.4.0

#### Changed
* Updated minimum pyspark version to be 3.2.1, compatible with Databricks runtime 10.4 LTS or later
* Modified data generator to allow specification of constraints to the data generation process
* Updated documentation for generating text data.
* Modified data distribiutions to use abstract base classes
* migrated data distribution tests to use `pytest`
* Additional standard datasets

#### Added
* Added classes for constraints on the data generation via new package `dbldatagen.constraints`
* Added support for standard data sets via the new package `dbldatagen.datasets`


### Version 0.3.6 Post 1

#### Changed
* Updated docs for complex data types / JSON to correct code examples
* Updated license file in public docs

#### Fixed 
* Fixed scenario where `DataAnalyzer` is used on dataframe containing a column named `summary`

### Version 0.3.6

#### Changed
* Updated readme to include details on which versions of Databricks runtime support Unity Catalog `shared` access mode.
* Updated code to use default parallelism of 200 when using a shared Spark session
* Updated code to use Spark's SQL function `element_at` instead of array indexing due to incompatibility

### Notes
* Ths version marks the changing minimum version of Databricks runtime to 10.4 LTS and later releases.
* While there are no known incompatibilities with Databricks 9.1 LTS, we will not test against this release


### Version 0.3.5

#### Changed
* Added formatting of generated code as Html for script methods
* Allow use of inferred types on `withColumn` method when `expr` attribute is used
* Added ``withStructColumn`` method to allow simplified generation of struct and JSON columns
* Modified pipfile to use newer version of package specifications

### Version 0.3.4 Post 3

### Changed
* Build now uses Python 3.8.12. Updated build process to reflect that.

### Version 0.3.4 Post 2

### Fixed
* Fix for use of values in columns of type array, map and struct 
* Fix for generation of arrays via `numFeatures` and `structType` attributes when numFeatures has value of 1


### Version 0.3.4 Post 1

### Fixed
* Fix for use and configuration of root logger 

### Acknowledgements
Thanks to Marvin Schenkel for the contribution

### Version 0.3.4

#### Changed
* Modified option to allow for range when specifying `numFeatures` with `structType='array'` to allow generation
  of varying number of columns
* When generating multi-column or array valued columns, compute random seed with different name for each column
* Additional build ordering enhancements to reduce circumstances where explicit base column must be specified

#### Added
* Scripting of data generation code from schema (Experimental)
* Scripting of data generation code from dataframe (Experimental)
* Added top level `random` attribute to data generator specification constructor


### Version 0.3.3post2

#### Changed
* Fixed use of logger in _version.py and in spark_singleton.py
* Fixed template issues 
* Document reformatting and updates, related code comment changes

### Fixed
* Apply pandas optimizations when generating multiple columns using same `withColumn` or `withColumnSpec`

### Added
* Added use of prospector to build process to validate common code issues


### Version 0.3.2

#### Changed
* Adjusted column build phase separation (i.e which select statement is used to build columns) so that a 
  column with a SQL expression can refer to previously created columns without use of a `baseColumn` attribute
* Changed build labelling to comply with PEP440

#### Fixed 
* Fixed compatibility of build with older versions of runtime that rely on `pyparsing` version 2.4.7

#### Added 
* Parsing of SQL expressions to determine column dependencies

#### Notes
* The enhancements to build ordering does not change actual order of column building -
  but adjusts which phase columns are built in 


### Version 0.3.1

#### Changed
* Refactoring of template text generation for better performance via vectorized implementation
* Additional migration of tests to use of `pytest`

#### Fixed 
* added type parsing support for binary and constructs such as `nvarchar(10)`
* Fixed error occurring when schema contains map, array or struct. 

#### Added 
* Ability to change name of seed column to custom name (defaults to `id`)
* Added type parsing support for structs, maps and arrays and combinations of the above

#### Notes
* column definitions for map, struct or array must use `expr` attribute to initialize field. Defaults to `NULL`

### Version 0.3.0

#### Changes
* Validation for use in Delta Live Tables
* Documentation updates
* Minor bug fixes
* Changes to build and release process to improve performance
* Modified dependencies to base release on package versions used by Databricks Runtime 9.1 LTS
* Updated to Spark 3.2.1 or later
* Unit test updates - migration from `unittest` to `pytest` for many tests

### Version 0.2.1

#### Features
* Uses pipenv for main build process
* Supports Conda based development build process
* Uses `pytest-cov` to track unit test coverage
* Added HTML help - use `make docs` to create it
* Added support for weights with non-random columns
* Added support for generation of streaming data sets
* Added support for multiple dependent base columns
* Added support for scripting of table create statements
* Resolved many PEP8 style issues
* Resolved / triaged prospector / pylint issues
* Changed help to RTD scheme and added additional help content
* moved docs to docs folder
* added support for specific distributions
* renamed packaging to `dbldatagen`
* Releases now available at https://github.com/databrickslabs/dbldatagen/releases
* code tidy up and rename of options
* added text generation plugin support for python functions and 3rd party libraries
* Use of data generator to generate static and streaming data sources in Databricks Delta Live Tables
* added support for install from PyPi


### General Requirements

See the contents of the file `python/require.txt` to see the Python package dependencies

The code for the Databricks Data Generator has the following dependencies

* Requires Databricks runtime 9.1 LTS or later
* Requires Spark 3.1.2 or later 
* Requires Python 3.8.10 or later

While the data generator framework does not require all libraries used by the runtimes, where a library from 
the Databricks runtime is used, it will use the version found in the Databricks runtime for 9.1 LTS or later.
You can use older versions of the Databricks Labs Data Generator by referring to that explicit version.

The recommended method to install the package is to use `pip install` in your notebook to install the package from 
PyPi

For example:

`%pip install dbldatagen`

To use an older DB runtime version in your notebook, you can use the following code in your notebook:

```shell
%pip install git+https://github.com/databrickslabs/dbldatagen@dbr_7_3_LTS_compat
```

See the [Databricks runtime release notes](https://docs.databricks.com/release-notes/runtime/releases.html) 
 for the full list of dependencies used by the Databricks runtime.

This can be found at : https://docs.databricks.com/release-notes/runtime/releases.html

