# This Makefile is for project development purposes only.
.PHONY: clean wheel dist tests buildenv install

ENV_NAME=dbl_testdatagenerator

NO_COLOR = \x1b[0m
OK_COLOR = \x1b[32;01m
ERROR_COLOR = \x1b[31;01m

PYCACHE := $(shell find . -name '__pycache__')
EGGS :=  $(shell find . -name '*.egg-info')
CURRENT_VERSION := $(shell awk '/current_version/ {print $$3}' python/.bumpversion.cfg)

clean:
	@echo "$(OK_COLOR)=> Cleaning$(NO_COLOR)"
	@echo "Current version: $(CURRENT_VERSION)"
	@rm -fr build dist $(EGGS) $(PYCACHE) databrickslabs_testdatagenerator/lib/* databrickslabs_testdatagenerator/env_files/*


prepare: clean
	@echo "$(OK_COLOR)=> Preparing ...$(NO_COLOR)"
	git add .
	git status
	git commit -m "cleanup before release"

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7.5

install-dev-dependencies:
	pip install -r python/require.txt

build_env/bin/activate: python/require.txt
	@echo "$(OK_COLOR)=> Updating build virtual environment ...$(NO_COLOR)"
	@test -d build_env || python3 -m venv build_env
	@. build_env/bin/activate; pip install -Ur python/require.txt
	@touch build_env/bin/activate

buildenv: install-dev-dependencies
	@echo "$(OK_COLOR)=> Checking build virtual environment ...$(NO_COLOR)"

describe_buildenv: buildenv
	@echo "$(OK_COLOR)=> Validating build virtual environment ...$(NO_COLOR)"
	@echo "The following packages are installed:"
	@pip3 list

clean_buildenv:
	@echo "$(OK_COLOR)=> Cleaning build virtual environment ...$(NO_COLOR)"
	@rm -rf ./build_env
	@echo "directory is `pwd`"
	@echo "$(OK_COLOR)=> Creating build virtual environment ...$(NO_COLOR)"
	@pip install -r python/require.txt

docs: install
	@echo "$(OK_COLOR)=> Creating docs ...$(NO_COLOR)"
	@-mkdir python/docs/source/relnotes
	@cp -f python/require.txt python/docs/source/relnotes/require.md
	@cp -f CONTRIBUTING.md python/docs/source/relnotes/
	@cp -f RELEASE_NOTES.md python/docs/source/relnotes/
	@cp -f python/docs/APIDOCS.md python/docs/source/relnotes/
	@cd python/docs && make docs


# Tests

# setup exports for build on mac osx
tests: export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
#tests: export PYSPARK_PYTHON=`which python3`
#tests: export PYSPARK_DRIVER_PYTHON=`which python3`

tests:
	@echo "$(OK_COLOR)=> Running unit tests$(NO_COLOR)"
	pytest tests/ --cov databrickslabs_testdatagenerator

test-with-html-report:
	@echo "$(OK_COLOR)=> Running unit tests with HTML test coverage report$(NO_COLOR)"
	pytest --cov databrickslabs_testdatagenerator --cov-report html -s
	@echo "$(OK_COLOR)=> the test coverage report can be found at htmlcov/index.html$(NO_COLOR)"


# Version commands

bump:
ifdef part
ifdef version
	@bumpversion --config-file python/.bumpversion.cfg --allow-dirty --new-version $(version) $(part) ; \
	grep current python/.bumpversion.cfg ; \
	grep -H version setup.py ; \
	grep -H "Version" RELEASE_NOTES.md
else
	bumpversion --config-file python/.bumpversion.cfg --allow-dirty $(part) ; \
	grep current python/.bumpversion.cfg ; \
	grep -H "version" setup.py ; \
	grep -H "Version" RELEASE_NOTES.md
endif
else
	@echo "$(ERROR_COLOR)Provide part=major|minor|patch|release|build and optionally version=x.y.z...$(NO_COLOR)"
	exit 1
endif

# Dist commands
# TODO - use conda in future rather than virtual env for better compatibility to ensure we can setup correct version of python

# wheel:

dist:
	@echo "$(OK_COLOR)=> building dist of wheel$(NO_COLOR)"
	# clean out old dist files - ignore any errors flagged
	@- test -d `pwd`/dist && test -n "$(find `pwd`/dist/ -name '*.whl' -print -quit)" && echo "found" && rm `pwd`/dist/*
	@python3 setup.py sdist bdist_wheel
	@touch `pwd`/dist/dist_flag.txt
	export NEW_WHEEL=`find ./dist -name "*.whl" -print`
	@echo "new package is $(NEW_WHEEL)"

new_artifact: buildenv
	@echo "$(OK_COLOR)=> committing new artifact$(NO_COLOR)"
	-git rm --cached `pwd`/dist/"*.whl"
	git add -f `pwd`/dist/*.whl


dist/dist_flag.txt: dist

newbuild:
	bumpversion --config-file python/.bumpversion.cfg --allow-dirty part=build ; \
	grep current python/.bumpversion.cfg ; \
	grep -H "version" setup.py ; \
	grep -H "Version" RELEASE_NOTES.md
	git add -u
	git status
	#git commit -m "Latest release: $(CURRENT_VERSION)"
	#git tag -a v$(CURRENT_VERSION) -m "Latest release: $(CURRENT_VERSION)"

release:
	@echo "$(OK_COLOR)=> building and committing new artifact$(NO_COLOR)"
	tar -czf ./dist/html_help.tgz -C ./python/docs/build ./html/
	-git rm -f --cached `pwd`/dist/"*.whl"
	git add -f `pwd`/dist/*.whl
	git add -f ./dist/html_help.tgz
	git commit -m "Latest release: $(CURRENT_VERSION)"
	#git tag -a v$(CURRENT_VERSION) -m "Latest release: $(CURRENT_VERSION)"

install: buildenv dist/dist_flag.txt
	@echo "$(OK_COLOR)=> Installing databrickslabs_testdatagenerator$(NO_COLOR)"
	@cp README.md python/
	@pip3 install --upgrade .
	@touch `pwd`/dist/install_flag.txt

dist/install_flag.txt: install


# dev tools

check_version:
	dev_tools/check_versions env.yml

dev_tools:
	pip install --upgrade bumpversion
	pip3 install --upgrade bumpversion
	python3 -m pip install --user --upgrade yapf pylint pyYaml
	python3 -m pip install --user --upgrade setuptools wheel
