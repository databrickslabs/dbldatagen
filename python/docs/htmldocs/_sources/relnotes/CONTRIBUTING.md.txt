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

It relies on setting up a Python virtual enviroment with the necessary requirements
so it may not build correctly for earlier versions. 

The resulting library has been verified against Databricks runtime 6.5 and Apache Spark 2.4.4

### Setting up your build environment
Run `make clean_buildenv` from the root of the projectg directory to 
set up a virtualized Python build environment in the project directory

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

Basically it follows the Python PEP8 coding conventions - but method names used mixed case starting with a lower case letter rather than underscores.

See https://legacy.python.org/dev/peps/pep-0008/
