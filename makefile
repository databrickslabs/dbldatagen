.PHONY: install install-dev test test-cov lint fmt clean build docs help

all: clean dev lint fmt test coverage

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	pip install hatch
	hatch env create

dev: .venv/bin/python
	@hatch run which python

lint:
	hatch run verify

fmt:
	hatch run fmt

test:
	hatch run test

coverage:
	hatch run coverage

build:
	hatch build

docs:
	cd docs && make docs