## Databricks Labs Data Generator Release Notes

### Requirements

See the contents of the file `python/require.txt` to see the Python package dependencies

### Features
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

### version 0.3.0

The code for the Databricks Data Generator has the following dependencies

* Requires Databricks runtime 9.1 LTS or later
* Requires Spark 3.1.2 or later 
* Requires Python 3.8.10 or later

While the data generator framework does not require all libraries used by the runtimes, where a library from 
the Databricks runtime is used, it will use the version found in the Databricks runtime for 9.1 LTS or later.
You can use older versions of the Databricks Labs Data Generator by referring to that explicit version.

To use an older DB runtime version in your notebook, you can use the following code in your notebook:

```commandline
%pip install git+https://github.com/databrickslabs/dbldatagen@dbr_7_3_LTS_compat
```

See the [Databricks runtime release notes](https://docs.databricks.com/release-notes/runtime/releases.html) 
 for the full list of dependencies.

This can be found at : https://docs.databricks.com/release-notes/runtime/releases.html

## Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project follows the guidelines of [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

[Please read through the Keep a Changelog (~5min)](https://keepachangelog.com/en/1.0.0/).

## [UNRELEASED] - YYYY-MM-DD

### Fixed

- Added ability to override column name used for `id` column 
- Fixes for parsing time strings with seconds, milliseconds 


### Changed
- Added DataGenerator method `withSeedColumnOutput`
- Deprecated DataGenerator method `withIdOutput`
- changed layout of `CHANGELOG.md`
- Updated tests for `utils` class to pytest style instead of unittest style

----
> Unreleased changes must be tracked above this line.
> When releasing, Copy the changelog to below this line, with proper version and date.
> And empty the **[Unreleased]** section above.
----

## Version 0.2.1

### Fixed
- doc updates
- minor bug fixes
- package updates

### Changed
- package dependencies 
- updated Html documentation to reflect Delta Live Tables compatibility


