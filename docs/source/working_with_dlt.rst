.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Working with Delta Live Tables
==============================
The Databricks Labs Data Generator can be used within a Delta Live Tables Python pipeline to generate one or more
synthetic data sources that can be used to test or benchmark a Delta Live Tables pipeline.

To install the library in a Delta Live Tables pipeline, you will need to create a notebook cell as the first cell
of the notebook to install the library via the `%pip` command.

.. code-block:: python

   %pip install dbldatagen

Once the Data Generator has been installed, you can use it to create DLT live tables and live streaming tables.

.. note::
   We recommend using the `rate-micro-batch` streaming source when creating a Delta Live Tables streaming source.

Creating a batch data source
----------------------------

Creating a batch data source in DLT is similar to creating a batch data source in a classic Databricks notebook.


Creating a streaming data source
--------------------------------

You can use the Data Generator to generate a synthetic source for a streaming live table.

