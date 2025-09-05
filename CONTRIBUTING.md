# Contributing to the Databricks Labs Data Generator
While **dbldatagen** cannot accept direct contribution from external contributors, all users can create GitHub Issues to propose new functionality. The dbldatagen team will review and prioritize new features based on user feedback.

## Making a contribution

### Setup
To set up your local environment:

1. Ensure any [Non-Python Dependencies](#other-dependencies) are installed locally.
2. Clone the repository:
   ```bash
   git clone "repository URL"
   ````
   
3. Open the repository in your IDE. Run the following terminal command to create a local development environment:
   ```bash
   make dev
   ```

### Development
When contributing new functionality:

1. Sync changes from the `master` branch:
   ```bash
   git checkout main && git pull
   ```
2. Checkout a new branch from `master`:
   ```bash
   git checkout -b "branch name"
   ```
3. Add your functionality, tests, documentation, and examples.

### Formatting
dbldatagen aims to follow [PEP8 standards](https://peps.python.org/pep-0008/). Code style should be checked for any new commits.

To validate code locally:

1. Run the following terminal command in your IDE:
   ```bash
   make fmt
   ```
2. Fix any issues until no messages remain.

### Testing
dbldatagen aims to have the highest possible test coverage. Code should be tested for any new commits.

To run unit tests locally:

1. Run the following terminal command in your IDE:
   ```bash
   make test-coverage
   ```
2. Verify that all tests pass.
3. Open the coverage report in your browser.
4. Verify that all modified modules have full coverage.

### Submitting a PR
To submit a pull request:

1. Squash all local commits in your branch.
2. Push your changes:
   ```bash
   git push
   ```
3. Navigate to the [Pull Requests](https://github.com/databrickslabs/dbldatagen/pulls) page and click **New pull request**.
4. Complete the template.
5. Submit your PR.

## Building the project locally

### Building the HTML documentation
Documentation can be reviewed locally. To build and open the documentation in your browser, run the following terminal command:
```bash
make docs-serve
```

### Building the Python wheel
dbldatagen can be built locally as a Python wheel. To build the wheel, run the following terminal command:

```bash
make build
```

## Prerequisites

### Python Compatibility
dbldatagen supports Python 3.10+ and is tested with Python 3.10 and later.

### Development Tools
All development tools are configured in `pyproject.toml`.

### Python Dependencies
All Python dependencies are defined in `pyproject.toml`:

1. `[project.dependencies]` lists dependencies installed with the `dbldatagen` library
2. `[tool.hatch.envs.default]` lists the default environment necessary to develop, test, and build the `dbldatagen` library

### Non-Python Dependencies
dbldatagen is tested against Databricks Runtime version 13.3LTS and OpenJDK 17. 

Spark and Java dependencies are not installed automatically by the build process and should be installed manually to develop and run dbldatagen locally.

## Development standards

### Code style
All code should adhere to the following standards:

1. **Formatted and linted** to PEP8 standards.
2. **Type-validated** using [mypy](https://mypy-lang.org/).
3. **Clearly-named** variables, classes, and methods.
4. **Include docstrings** that detail functionality and usage.

### Testing
All tests should use [pytest](https://docs.pytest.org/en/stable/) with fixtures and parameterization where appropriate. This includes:

1. **Unit tests** cover functionality which does not require a Databricks workspace and should always be preferred to integration tests when possible.
2. **Integration tests** cover functionality which requires Databricks compute, Unity Catalog, or other workspace features.

### Branches
All local development should branch from `master` and adhere to the following naming convention:

1. `feat_<feature_name>` for new functionality
2. `fix_<issue_number>_<fix_name>` for bugfixes

### Pull requests
All pull requests should adhere to the following standards:

1. Pull requests should be scoped to 1 repository issue.
2. Local commits should be squashed on your branch before opening a pull request.
3. All pull requests should include functionality, tests, documentation, and examples.

## License
When you contribute code, you affirm that the contribution is your original work and that you 
license the work to the project under the project's Databricks license. Whether or not you 
state this explicitly, by submitting any copyrighted material via pull request, email, or 
other means you agree to license the material under the project's Databricks license and 
warrant that you have the legal authority to do so.
