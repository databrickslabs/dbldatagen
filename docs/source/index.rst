.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Databricks Labs Data Generator documentation
============================================

The Databricks Labs Data Generator project provides a convenient way to
generate large volumes of synthetic test data from within a Databricks notebook
(or regular Spark application).

By defining a data generation spec, either in conjunction with an existing schema
or through creating a schema on the fly, you can control how synthetic data is generated.

As the data generator generates a PySpark data frame, it is simple to create a view over it to expose it
to Scala or R based Spark applications also.

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   Get Started Here <APIDOCS>
   Installation instructions <installation_notes>
   Using data ranges <DATARANGES>
   Generating text data <textdata>
   Using data distributions <DISTRIBUTIONS>
   Options for column specification <options_and_features>
   Generating repeatable data  <repeatable_data_generation>
   Troubleshooting data generation <troubleshooting>

.. toctree::
   :maxdepth: 1
   :caption: API

   Quick API index <relnotes/quickindex>
   The dbldatagen package API <reference/api/modules>

.. toctree::
   :maxdepth: 1
   :caption: Development

   Building and contributing <relnotes/CONTRIBUTING>
   Change log <relnotes/CHANGELOG>
   Build requirements <relnotes/requirements>

.. toctree::
   :maxdepth: 1
   :caption: License

   license

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`

