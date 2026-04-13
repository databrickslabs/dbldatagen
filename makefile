all: clean lint fmt test coverage

# Prevent uv from modifying the lock file. UV_FROZEN skips resolution entirely,
# which is required because the lock file uses public PyPI URLs while the actual
# index may be an internal proxy. Use `make lock-dependencies` to update the lock file.
export UV_FROZEN := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 10 --timeout 600 --durations 20

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

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
	$(UV_TEST) --cov=dbldatagen --cov-report=xml tests/

coverage:
	$(UV_TEST) --cov=dbldatagen --cov-report=html tests/
	open htmlcov/index.html

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
	uv sync --group docs
	$(UV_RUN) sphinx-build -M html docs/source docs/build

docs-clean:
	rm -rf docs/build

docs-serve:
	make docs-build
	open docs/build/html/index.html

.DEFAULT: all
.PHONY: all clean dev lint fmt test coverage build lock-dependencies docs-build docs-clean docs-serve
