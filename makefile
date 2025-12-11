.PHONY: dev test lint fmt clean build docs

all: clean dev lint fmt test

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

dev:
	@which hatch > /dev/null || pip install hatch
	@hatch run which python

lint:
	hatch run test-pydantic.2.8.2:verify

fmt:
	hatch run test-pydantic.2.8.2:fmt

test:
	hatch run test-pydantic:test

test-coverage:
	make test && open htmlcov/index.html

build:
	hatch build

docs:
	cd docs && make docs

docs-serve:
	cd docs && make docs && open build/html/index.html