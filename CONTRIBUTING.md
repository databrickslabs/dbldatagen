# Contributing to the Databricks Labs Data Generator
We happily welcome contributions to *dbldatagen*. 

We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## License

When you contribute code, you affirm that the contribution is your original work and that you 
license the work to the project under the project's Databricks license. Whether or not you 
state this explicitly, by submitting any copyrighted material via pull request, email, or 
other means you agree to license the material under the project's Databricks license and 
warrant that you have the legal authority to do so.

# Development Setup

## Python Compatibility

The code supports Python 3.9+ and has been tested with Python 3.9.21 and later.

## Quick Start

```bash
# Install development dependencies
make dev

# Format and lint code
make fmt                 # Format with ruff and fix issues
make lint                # Check code quality

# Run tests
make test                # Run tests
make test-cov            # Run tests with coverage report

# Build package
make build               # Build with modern build system
```

## Development Tools

All development tools are configured in `pyproject.toml`.

## Dependencies

All dependencies are defined in `pyproject.toml`:

- `[project.dependencies]` lists dependencies necessary to run the `dbldatagen` library
- `[tool.hatch.envs.default]` lists the default environment necessary to develop, test, and build the `dbldatagen` library

## Spark Dependencies

The builds have been tested against Spark 3.3.0+. This requires OpenJDK 1.8.56 or later version of Java 8.
The Databricks runtimes use the Azul Zulu version of OpenJDK 8.
These are not installed automatically by the build process.

## Creating the HTML documentation

Run  `make docs` from the main project directory.

The main html document will be in the file (relative to the root of the build directory)
 `./docs/docs/build/html/index.html`

## Building the Python wheel

```bash
make build               # Clean and build the package
```

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

## Running Tests

```bash
# Run all tests
make test

# Run tests with coverage report (generates htmlcov/index.html)
make coverage
```

If using an environment with multiple Python versions, make sure to use virtual env or similar to pick up correct python versions.

If necessary, set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to point to correct versions of Python.

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

# Code Quality and Style

## Automated Formatting

Code can be automatically formatted and linted with the following commands:

```bash
# Format code and fix issues automatically
make fmt

# Check code quality without making changes
make lint
```

## Coding Conventions

The code follows PySpark coding conventions:
- Python PEP8 standards with some PySpark-specific adaptations
- Method and argument names use mixed case starting with lowercase (following PySpark conventions)
- Line length limit of 120 characters

See the [Python PEP8 Guide](https://peps.python.org/pep-0008/) for general Python style guidelines.

# Github expectations
When running the unit tests on GitHub, the environment should use the same environment as the latest Databricks
runtime latest LTS release. While compatibility is preserved on LTS releases from Databricks runtime 13.3 onwards, 
unit tests will be run on the environment corresponding to the latest LTS release. 

Libraries will use the same versions as the earliest supported LTS release - currently 13.3 LTS

This means for the current build:

- Use of Ubuntu 22.04 for the test runner
- Use of Java 8
- Use of Python 3.10.12 when testing / building the image

See the following resources for more information
= https://docs.databricks.com/en/release-notes/runtime/15.4lts.html
- https://docs.databricks.com/en/release-notes/runtime/11.3lts.html
- https://github.com/actions/runner-images/issues/10636

