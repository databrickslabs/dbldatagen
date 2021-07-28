## Databricks Labs Data Generator Release Notes

### Requirements

See the contents of the file `python/require.txt` to see the Python package dependencies

### Version 0.11.0-dev2

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
