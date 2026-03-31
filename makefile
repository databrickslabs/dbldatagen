.PHONY: dev test test-cov test-v1 test-v1-cov test-all lint fmt clean build docs

all: clean dev lint fmt test

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

dev:
	uv sync --group dev

lint:
	uv run black --check .
	uv run ruff check .
	uv run mypy .
	uv run pylint --output-format=colorized -j 0 dbldatagen tests

fmt:
	uv run black .
	uv run ruff check . --fix
	uv run mypy .
	uv run pylint --output-format=colorized -j 0 dbldatagen tests

test:
	uv run pytest tests/ -n 10 --cov --cov-report=html --timeout 600 --durations 20

test-v1:
	uv run pytest tests/v1/ --timeout 600 --durations 20 --no-header -q

test-v1-cov:
	uv run pytest tests/v1/ --timeout 600 --durations 20 --no-header -q --cov=dbldatagen/v1 --cov-config=.coveragerc-v1 --cov-report=term-missing:skip-covered --cov-report=xml --cov-report=html:htmlcov-v1 --ignore=tests/v1/test_faker_pool.py

test-all:
	uv run pytest tests/ --ignore=tests/v1/ -n 10 --timeout 600 --durations 20
	uv run pytest tests/v1/ --timeout 600 --durations 20 --no-header -q

test-coverage:
	uv run pytest tests/ --ignore=tests/v1/ --cov --cov-report=html --timeout 600 --durations 20 && open htmlcov/index.html

build:
	uv build

docs-build:
	uv sync --group docs
	uv run sphinx-build -M html docs/source docs/build

docs-clean:
	rm -rf docs/build

docs-serve:
	make docs-build
	open docs/build/html/index.html
