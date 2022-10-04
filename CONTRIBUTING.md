# Contributing to the Databricks Labs Data Generator
We happily welcome contributions to *dbldatagen*. 

We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## License

When you contribute code, you affirm that the contribution is your original work and that you 
license the work to the project under the project's Databricks license. Whether or not you 
state this explicitly, by submitting any copyrighted material via pull request, email, or 
other means you agree to license the material under the project's Databricks license and 
warrant that you have the legal authority to do so.

# Building the code

## Python compatibility

The code has been tested with Python 3.7.5 and 3.8

## Checking your code for common issues

Run `./lint.sh` from the project root directory to run various code style checks. 
These are based on the use of `prospector`, `pylint` and related tools.

## Setting up your build environment
Run `make buildenv` from the root of the project directory to setup a `pipenv` based build environment.

Run `make create-dev-env` from the root of the project directory to 
set up a conda based virtualized Python build environment in the project directory.

You can use alternative build virtualization environments or simply install the requirements
directly in your environment.


## Build steps

Our recommended mechanism for building the code is to use a `conda` or `pipenv` based development process. 

But it can be built with any Python virtualization environment.

### Building with Conda
To build with `conda`, perform the following commands:
  - `make create-dev-env` from the main project directory to create your conda environment, if using
  - activate the conda environment - e.g `conda activate dbl_testdatagenerator`
  - install the necessary dependencies in your conda environment via `make install-dev-dependencies`
  
  - use the following to build and run the tests with a coverage report
    - Run  `make dev-test-with-html-report` from the main project directory.

  - Use the following command to make the distributable:
    - Run `make dev-dist` from the main project directory
  - The resulting wheel file will be placed in the `dist` subdirectory
  
### Building with Pipenv
To build with `pipenv`, perform the following commands:
  - `make buildenv` from the main project directory to create your conda environment, if using
  - install the necessary dependencies in your conda environment via `make install-dev-dependencies`
  
  - use the following to build and run the tests with a coverage report
    - Run  `make test-with-html-report` from the main project directory.

  - Use the following command to make the distributable:
    - Run `make dist` from the main project directory
  - The resulting wheel file will be placed in the `dist` subdirectory

The resulting build has been tested against Spark 3.0.1

## Creating the HTML documentation

Run  `make docs` from the main project directory.

The main html document will be in the file (relative to the root of the build directory)
 `./docs/docs/build/html/index.html`

## Building the Python wheel
Run  `make clean dist` from the main project directory.

# Testing 

## Creating tests
Preferred style is to use pytest rather than unittest but some unittest based code is used in compatibility mode.

Any new tests should be written as pytest compatible test classes.

## Running unit / integration tests

If using an environment with multiple Python versions, make sure to use virtual env or 
similar to pick up correct python versions. The make target `create`

If necessary, set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to point to correct versions of Python.

To run the tests using a `conda` environment:
  - Run `make dev-test` from the main project directory to run the unit tests.

  - Run `make dev-test-with-html-report` to generate test coverage report in `htmlcov/inxdex.html`

To run the tests using a `pipenv` environment:
  - Run `make test` from the main project directory to run the unit tests.

  - Run `make test-with-html-report` to generate test coverage report in `htmlcov/inxdex.html`

# Using the Databricks Labs data generator
To use the project, the generated wheel should be installed in your Python notebook as a wheel based library

Once the library has been installed, you can use it to generate a test data frame.

# Coding Style 

The code follows the Pyspark coding conventions. 

Basically it follows the Python PEP8 coding conventions - but method and argument names used mixed case starting 
with a lower case letter rather than underscores following Pyspark coding conventions.

See https://legacy.python.org/dev/peps/pep-0008/
