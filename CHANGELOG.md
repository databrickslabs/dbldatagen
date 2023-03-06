# Databricks Labs Data Generator Release Notes

## Change History
All notable changes to the Databricks Labs Data Generator will be documented in this file.

### Unreleased

#### Changed
* Additional migration of tests to use of `pytest`
* Changed parsing of build options for data generator to support use of custom streaming
* Documentation updates in support of new features such as streaming, complex structures etc
* Adjusted column build phase separation (i.e which select statement is used to build columns) so that a 
  column with a SQL expression can refer to previously created columns without use of a `baseColumn` attribute
* Changed build labelling to comply with PEP440

#### Fixed 

#### Added 
* Added support for additional streaming source types and for use of custom streaming sources
* Added support for use of file reads as a streaming source (for seed and timestamp columns only)
* Added support for complex event time in streaming scenarios. It may also be used in batch scenarios for testing
* Parsing of SQL expressions to determine column dependencies

#### Notes
* This does not change actual order of column building - but adjusts which phase 


#### Fixed 

#### Added 
* Parsing of SQL expressions to determine column dependencies

#### Notes
* This does not change actual order of column building - but adjusts which phase columns are built in 


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

```commandline
%pip install git+https://github.com/databrickslabs/dbldatagen@dbr_7_3_LTS_compat
```

See the [Databricks runtime release notes](https://docs.databricks.com/release-notes/runtime/releases.html) 
 for the full list of dependencies used by the Databricks runtime.

This can be found at : https://docs.databricks.com/release-notes/runtime/releases.html

