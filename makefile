all: clean lint fmt test coverage

# Prevent uv from modifying the lock file. UV_FROZEN skips resolution entirely,
# which is required because the lock file uses public PyPI URLs while the actual
# index may be an internal proxy. Use `make lock-dependencies` to update the lock file.
export UV_FROZEN := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt
# macOS: prevent Python worker SEGVs in PySpark UDFs from fork() + native libs.
# No-op on Linux/CI.
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY := YES

UV_RUN := uv run --exact --all-extras --all-groups
UV_TEST := $(UV_RUN) pytest -n 2 --timeout 600 --durations 20

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras --all-groups

lint:
	$(UV_RUN) black --check .
	$(UV_RUN) ruff check .
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 dbldatagen

fmt:
	$(UV_RUN) black .
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 dbldatagen

test:
	$(UV_TEST) --cov=dbldatagen --cov-config=.coveragerc --cov-report= --ignore=tests/core/ tests/
	$(UV_RUN) pytest tests/core/ --cov=dbldatagen/core --cov-config=.coveragerc-core --cov-append --cov-report= --timeout 600 --durations 20 --no-header -q --ignore=tests/core/engine/test_faker_pool.py
	$(UV_RUN) coverage xml --rcfile=.coveragerc-all
	$(UV_RUN) coverage html --rcfile=.coveragerc-all
	$(UV_RUN) coverage report --rcfile=.coveragerc-all --fail-under=80 --skip-covered

coverage:
	$(UV_TEST) --cov=dbldatagen --cov-report=html --ignore=tests/v1/ tests/
	open htmlcov/index.html

test-v0:
	$(UV_TEST) --cov=dbldatagen --cov-config=.coveragerc --cov-report=term-missing:skip-covered --cov-report=xml --cov-report=html --cov-fail-under=80 --ignore=tests/core/ tests/

test-core:
	$(UV_RUN) pytest tests/core/ --timeout 600 --durations 20 --no-header -q --cov=dbldatagen/core --cov-config=.coveragerc-core --cov-report=term-missing:skip-covered --cov-report=xml --cov-report=html:htmlcov-core --ignore=tests/core/engine/test_faker_pool.py

test-ci:
	$(MAKE) test-v0
	$(MAKE) test-core

test-all:
	$(UV_RUN) pytest tests/ --ignore=tests/core/ -n 2 --timeout 600 --durations 20
	$(UV_RUN) pytest tests/core/ --timeout 600 --durations 20 --no-header -q

test-coverage:
	$(UV_RUN) pytest tests/ --ignore=tests/core/ --cov --cov-report=html --timeout 600 --durations 20 && open htmlcov/index.html

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

lock-dependencies: export UV_FROZEN := 0
lock-dependencies:
	uv lock
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	  uv pip compile --generate-hashes --universal --no-header - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' uv.lock

docs-build:
	$(UV_RUN) --group docs sphinx-build -M html docs/source docs/build

docs-clean:
	rm -rf docs/build

docs-serve:
	make docs-build
	open docs/build/html/index.html

.DEFAULT: all
.PHONY: all clean dev lint fmt test coverage build lock-dependencies docs-build docs-clean docs-serve test-v0 test-core test-all test-coverage test-ci
