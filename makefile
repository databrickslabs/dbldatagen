.PHONY: dev test lint fmt clean build docs

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

test-coverage:
	make test && open htmlcov/index.html

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