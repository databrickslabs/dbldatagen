.PHONY: clean wheel dist tests 

NO_COLOR = \x1b[0m
OK_COLOR = \x1b[32;01m
ERROR_COLOR = \x1b[31;01m

PYCACHE := $(shell find . -name '__pycache__')
EGGS :=  $(shell find . -name '*.egg-info')
CURRENT_VERSION := $(shell awk '/current_version/ {print $$3}' .bumpversion.cfg)

clean:
	@echo "$(OK_COLOR)=> Cleaning$(NO_COLOR)"
	@echo "vars: $(EGGS) $(PYCACHE)"
	@echo "Eggs: $(EGGS)"
	@rm -fr build dist $(EGGS) $(PYCACHE) databrickslabs_testdatagenerator/lib/* databrickslabs_testdatagenerator/env_files/*

prepare: clean
	git add .
	git status
	git commit -m "cleanup before release"

# Tests

tests: 
	python3 -m unittest discover -s "unit_tests" -p "*.py"  -v

# Version commands

bump:
ifdef part
ifdef version
	bumpversion --new-version $(version) $(part) && grep current .bumpversion.cfg
else
	bumpversion $(part) && grep current .bumpversion.cfg
endif
else
	@echo "$(ERROR_COLOR)Provide part=major|minor|patch|release|build and optionally version=x.y.z...$(NO_COLOR)"
	exit 1
endif

# Dist commands

# wheel:

dist:
	@echo "$(OK_COLOR)=> building wheel$(NO_COLOR)"
	@python3 python/setup.py sdist bdist_wheel

release:
	git add .
	git status
	#git commit -m "Latest release: $(CURRENT_VERSION)"
	#git tag -a v$(CURRENT_VERSION) -m "Latest release: $(CURRENT_VERSION)"

install: dist
	@echo "$(OK_COLOR)=> Installing databrickslabs_testdatagenerator$(NO_COLOR)"
	@pip3 install --upgrade .

# dev tools

check_version:
	dev_tools/check_versions env.yml

dev_tools:
	pip install --upgrade bumpversion
	pip3 install --upgrade bumpversion
	python3 -m pip install --user --upgrade yapf pylint pyYaml
	python3 -m pip install --user --upgrade setuptools wheel
