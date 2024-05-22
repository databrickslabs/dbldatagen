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

## Package Dependencies
See the contents of the file `python/require.txt` to see the Python package dependencies. 
Dependent packages are not installed automatically by the `dbldatagen` package.

## Python compatibility

The code has been tested with Python 3.8.12 and later.

Older releases were tested with Python 3.7.5 but as of this release, it requires the Databricks 
runtime 9.1 LTS or later. 

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

## Developing new tests
New tests should be created using PyTest with classes combining multiple `Pytest` tests.

Existing test code contains tests based on Python's `unittest` framework but these are 
run on `pytest` rather than `unitest`. 

To get a  `spark` instance for test purposes, use the following code:

```python
import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("<name to flag spark instance>")
```

The name used to flag the spark instance should be the test module or test class name. 

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
The recommended method for installation is to install from the PyPi package

You can install the library as a notebook scoped library when working within the Databricks 
notebook environment through the use of a `%pip` cell in your notebook.

To install as a notebook-scoped library, create and execute a notebook cell with the following text:

> `%pip install dbldatagen`

This installs from the PyPi package

You can also install from release binaries or directly from the Github sources.

The release binaries can be accessed at:
- Databricks Labs Github Data Generator releases - https://github.com/databrickslabs/dbldatagen/releases


The `%pip install` method also works on the Databricks Community Edition.

Alternatively, you use download a wheel file and install using the Databricks install mechanism to install a wheel based
library into your workspace.

The `%pip install` method can also down load a specific binary release.
For example, the following code downloads the release V0.2.1

> '%pip install https://github.com/databrickslabs/dbldatagen/releases/download/v021/dbldatagen-0.2.1-py3-none-any.whl'

# Coding Style 

The code follows the Pyspark coding conventions. 

Basically it follows the Python PEP8 coding conventions - but method and argument names used mixed case starting 
with a lower case letter rather than underscores following Pyspark coding conventions.

See https://legacy.python.org/dev/peps/pep-0008/
