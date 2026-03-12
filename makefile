.PHONY: dev test test-cov test-v1 test-v1-cov test-all lint fmt clean build docs

all: clean dev lint fmt test

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	pip install "hatch==1.13.0" "click<8.3" "virtualenv<21"
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	hatch env create

dev: .venv/bin/python
	@hatch run which python

lint:
	hatch run verify

fmt:
	hatch run fmt

test:
	hatch run v0:test

test-v1:
	hatch run v1:test

test-v1-cov:
	hatch run v1:test-cov

test-all:
	hatch run v0:test
	hatch run v1:test

test-cov:
	hatch run v0:test-cov

test-coverage:
	hatch run v0:test-cov && open htmlcov/index.html

build:
	hatch build

docs:
	cd docs && make docs

docs-serve:
	cd docs && make docs && open build/html/index.html
