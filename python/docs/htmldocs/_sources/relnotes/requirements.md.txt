# Dependencies for the Test Data Generator framework

Building the framework will install a number of packages in the virtual environment used for building the framework.
Some of these are required at runtime, while a number of packages are used for building the documentation only.

 The following packages are used in building the test data generator framework
 All packages used are already installed in the Databricks runtime environment for version 6.5 or later
* numpy==1.16.2
* pandas==0.24.2
* pickleshare==0.7.5
* py4j==0.10.7
* pyarrow==0.13.0
* pyspark==2.4.5
* python-dateutil==2.8.0
* six==1.12.0
* wheel==0.33.1
* setuptools==40.8.0
* bumpversion

 The following packages are only required for building documentation and are not required at runtime
* sphinx>=2.0.0,<3.1.0
* nbsphinx
* numpydoc==0.8
* pypandoc
* ipython==7.4.0
* pydata-sphinx-theme
* recommonmark
* sphinx-markdown-builder
