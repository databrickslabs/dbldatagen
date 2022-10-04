## Databricks Labs Data Generator Release Notes

### Requirements

See the contents of the file `python/require.txt` to see the Python package dependencies

### Version 0.2.0-rc1

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
* moved Github repo to https://github.com/databrickslabs/dbldatagen/releases
* code tidy up and rename of options
* added text generation plugin support for python functions and 3rd party libraries

### version 0.3.0-rc1

The code for the Databricks Data Generator has the following dependencies

* Requires Databricks runtime 9.1 LTS or later
* Requires Spark 3.1.2 or later 
* Requires Python 3.8.10 or later

While the data generator framework does not require all libraries used by the runtimes, where a library from 
the Databricks runtime is used, it will use the version found in the Databricks runtime for 9.1 LTS or later.
You can use older versions of the Databricks Labs Data Generator by referring to that explicit version.

To use an older version in your notebook, you can use the following code in your notebook:

```commandline

```

See the Databricks runtime release notes for the full list of dependencies.

This can be found at : https://docs.databricks.com/release-notes/runtime/releases.html

