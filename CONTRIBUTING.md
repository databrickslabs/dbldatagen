# Contributing to the Test Data Generator
We happily welcome contributions to *data-generator*. 

We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## License

When you contribute code, you affirm that the contribution is your original work and that you 
license the work to the project under the project's open source license. Whether or not you 
state this explicitly, by submitting any copyrighted material via pull request, email, or 
other means you agree to license the material under the project's open source license and 
warrant that you have the legal authority to do so.

## Building the code

The code has been tested with Python 3.7.6 

# Building the code

Our recommended mechanism for building the code is to use a conda based development process. 

But it can be built with any Python virtualization environment.

To use this, perform the following commands:
  - `make create-dev-env` from the main project directory to create your conda environment, if using
  - activate the conda environment - e.g `conda activate dbl_testdatagenerator`
  - install the necessary dependencies in your conda environment via `make install-dev-dependencies`
  
  use the following to build and run the tests with a coverage report
  - Run  ` make test-with-html-report` from the main project directory.

Use the following command to make the distributable:
  - Run `make dist` from the main project directory
  - The resulting wheel file will be placed in the `dist` subdirectory
  
The resulting build has been tested against Spark 3.1

## Creating the HTML documentation

Run  `make docs` from the main project directory.

The main html document will be in the file (relative to the root of the build directory) `./python/docs/docs/build/html/index.html`

## Running unit tests

If using an environment with multiple Python versions, make sure to use virtual env or 
similar to pick up correct python versions. The make target `create`

If necessary, set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to point to correct versions of Python.

Run  `make tests` from the main project directory to run the unit tests.

Run `make test-with-html-report` to generate test coverage report in `htmlcov/inxdex.html`

### Setting up your build environment
Run `make create-dev-env` from the root of the project directory to 
set up a conda based virtualized Python build environment in the project directory.

You can use alternative build virtualization environments or simply install the requirements
directly in your environment.

### Building the Python wheel
Run  `make clean dist` from the main project directory.

### Creating the HTML documentation

Run  `make docs` from the main project directory.

The main html document will be in the file (relative to the root of the build directory) `./python/docs/docs/build/html/index.html`

### Running unit tests

If using an environment with multiple Python versions, make sure to use virtual env or similar to pick up correct python versions.

If necessary, set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to point to correct versions of Python.

Run  `make install tests` from the main project directory to run the unit tests.

## Using the Project
To use the project, the generated wheel should be installed in your Python notebook as a wheel based library

Once the library has been installed, you can use it to generate a test data frame.

## Coding Style 

The code follows the Pyspark coding conventions. 

Basically it follows the Python PEP8 coding conventions - but method and argument names used mixed case starting with a lower case letter rather than underscores.

See https://legacy.python.org/dev/peps/pep-0008/
