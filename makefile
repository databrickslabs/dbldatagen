# This Makefile is for project development purposes only.
.PHONY: clean wheel dist test buildenv install

ENV_NAME=dbl_testdatagenerator

NO_COLOR = \x1b[0m
OK_COLOR = \x1b[32;01m
ERROR_COLOR = \x1b[31;01m

PYCACHE := $(shell find . -name '__pycache__')
EGGS :=  $(shell find . -name '*.egg-info')
CURRENT_VERSION := $(shell awk '/current_version/ {print $$3}' python/.bumpversion.cfg)

PACKAGE_NAME = "dbldatagen"

clean:
	@echo "$(OK_COLOR)=> Cleaning$(NO_COLOR)"
	@echo "Current version: $(CURRENT_VERSION)"
	@rm -fr build dist $(EGGS) $(PYCACHE)

prepare: clean
	@echo "$(OK_COLOR)=> Preparing ...$(NO_COLOR)"
	git add .
	git status
	git commit -m "cleanup before release"


create-dev-env:
	@echo "$(OK_COLOR)=> making conda dev environment$(NO_COLOR)"
	conda create -n $(ENV_NAME) python=3.8.10

create-github-build-env:
	@echo "$(OK_COLOR)=> making conda dev environment$(NO_COLOR)"
	conda create -n pip_$(ENV_NAME) python=3.8

install-dev-dependencies:
	@echo "$(OK_COLOR)=> installing dev environment requirements$(NO_COLOR)"
	pip install -v -r python/dev_require.txt

clean-dev-env:
	@echo "$(OK_COLOR)=> Cleaning dev environment$(NO_COLOR)"
	@echo "Current version: $(CURRENT_VERSION)"
	@rm -fr build dist $(EGGS) $(PYCACHE)


buildenv: 
	@echo "$(OK_COLOR)=> Creating and checking build virtual environment ...$(NO_COLOR)"
	pip install pipenv
	pipenv install --dev

clean_buildenv:
	@echo "$(OK_COLOR)=> Cleaning build virtual environment ...$(NO_COLOR)"
	pipenv clean

docs: install
	@echo "$(OK_COLOR)=> Creating docs ...$(NO_COLOR)"
	@cd docs && make docs

dev-docs: dev-install
	@echo "$(OK_COLOR)=> Creating docs ...$(NO_COLOR)"
	@cd docs && make docs

prep-doc-release:
	@echo "$(OK_COLOR)=> Preparing docs for release ...$(NO_COLOR)"
	cp -r docs/build/html docs/public_docs/
	touch docs/.nojekyll
	touch docs/public_docs/.nojekyll



# Tests
test: export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

dev-test: export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

dev-test: export SPARK_MASTER_HOST='localhost'

dev-test: export SPARK_LOCAL_IP=127.0.0.1

dev-test-with-html-report: export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

dev-test-with-html-report: export SPARK_MASTER_HOST='localhost'

dev-test-with-html-report: export SPARK_LOCAL_IP=127.0.0.1

dev-test:
	@echo "$(OK_COLOR)=> Running unit tests in directory $(PWD) $(NO_COLOR)"
	pytest tests/ --cov $(PACKAGE_NAME) --cov-report=xml

dev-lint-report:
	@echo "$(OK_COLOR)=> Running Prospector lint reporting $(PWD) $(NO_COLOR)"
	prospector --profile prospector.yaml > prospector_report.txt

dev-lint:
	@echo "$(OK_COLOR)=> Running Prospector lint reporting $(PWD) $(NO_COLOR)"
	prospector --profile prospector.yaml

dev-test-with-html-report:
	@echo "$(OK_COLOR)=> Running unit tests with HTML test coverage report$(NO_COLOR)"
	pytest --cov $(PACKAGE_NAME) --cov-report html -s
	@echo "$(OK_COLOR)=> the test coverage report can be found at htmlcov/index.html$(NO_COLOR)"


test: buildenv
	@echo "$(OK_COLOR)=> Running unit tests$(NO_COLOR)"
	pipenv run pytest tests/ --cov $(PACKAGE_NAME)  --cov-report=xml

test-with-html-report: buildenv
	@echo "$(OK_COLOR)=> Running unit tests with HTML test coverage report$(NO_COLOR)"
	pipenv run pytest --cov $(PACKAGE_NAME) --cov-report html -s
	@echo "$(OK_COLOR)=> the test coverage report can be found at htmlcov/index.html$(NO_COLOR)"

# Version commands
bump:
ifdef part
ifdef version
	@bumpversion --config-file python/.bumpversion.cfg --allow-dirty --new-version $(version) $(part) ; \
	grep current python/.bumpversion.cfg ; \
	grep -H version setup.py ; \
	grep -H "Version" CHANGELOG.md
else
	bumpversion --config-file python/.bumpversion.cfg --allow-dirty $(part) ; \
	grep current python/.bumpversion.cfg ; \
	grep -H "version" setup.py ; \
	grep -H "Version" CHANGELOG.md
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
	@echo "current dir is `pwd`"
	@echo "`ls ./dist`"
	@pipenv run python setup.py sdist bdist_wheel
	@touch `pwd`/dist/dist_flag.txt
	@echo "new package is located in dist - listing wheel files"
	@find ./dist -name "*.whl" -print

dev-dist:
	@echo "$(OK_COLOR)=> building dist of wheel$(NO_COLOR)"
	# clean out old dist files - ignore any errors flagged
	@- test -d `pwd`/dist && test -n "$(find `pwd`/dist/ -name '*.whl' -print -quit)" && echo "found" && rm `pwd`/dist/*
	@echo "current dir is `pwd`"
	@echo "`ls ./dist`"
	@python3 setup.py sdist bdist_wheel
	@touch `pwd`/dist/dev-dist_flag.txt
	@echo "new package is located in dist - listing wheel files"
	@find ./dist -name "*.whl" -print

new_artifact: buildenv
	@echo "$(OK_COLOR)=> committing new artifact$(NO_COLOR)"
	-git rm --cached `pwd`/dist/"*.whl"
	git add -f `pwd`/dist/*.whl

dev-test-pkg: dev-dist
	@echo "Building test package for Test Pypi..."
	rm dist/*.txt
	python3 -m twine upload --repository testpypi dist/*



dist/dist_flag.txt: dist

dist/dev-dist_flag.txt: dev-dist

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
	@echo "$(OK_COLOR)=> Installing $(PACKAGE_NAME) $(NO_COLOR)"
	#@cp README.md python/
	@pip3 install --upgrade .
	@touch `pwd`/dist/install_flag.txt

dev-install: dist/dev-dist_flag.txt
	@echo "$(OK_COLOR)=> Installing $(PACKAGE_NAME)$(NO_COLOR)"
	#@cp README.md python/
	@pip3 install --upgrade .
	@touch `pwd`/dist/dev-install_flag.txt

dist/install_flag.txt: install

dist/dev-install_flag.txt: dev-install