.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Test Data Generator documentation
=================================

The Databricks Labs Test Data Generator project provides a convenient way to
generate large volumes of synthetic test data from within a Databricks notebook
(or regular Spark application).

By defining a test data generation spec, either in conjunction with an existing schema
or through creating a schema on the fly, you can control how test data is generated.

As the data generator generates a PYSpark data frame, it is simple to create a view over it to expose it
to Scala or R based Spark applications also.

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   Get Started Here <relnotes/APIDOCS>
   Installation instructions <TOBEADDED>
   Using data ranges <relnotes/DATARANGES>
   Using data distributions <relnotes/DISTRIBUTIONS>

.. toctree::
   :maxdepth: 1
   :caption: API

   Quick index <relnotes/quickindex>
   API Documentation <reference/api/modules>

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

=======
