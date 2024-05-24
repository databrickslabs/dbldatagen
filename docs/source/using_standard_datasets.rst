.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Quick and easy dataset generation with Standard Datasets
========================================================

The standard dataset mechanism is a new pluggable framework for generation of data generation patterns using
predefined data set definitions.

As the mechanism produces a Data Generator object, it can be further customized through addition of further columns,
constraints, or modification of existing columns.

The standard datasets are also self documenting so that you can dynamically see what standard datasets are available,
find further details of datasets and their data.

Simple use
----------

For the simplest use, you can simply select a standard dataset and generate a data frame from it.

.. code-block:: python

   import dbldatagen as df
   df = dg.Datasets(spark, "basic/iot").get().build()
   display(df)


Customizing rows and partitions
-------------------------------
TBD

Passing additional options
--------------------------
TBD

Listing available datasets
--------------------------
TBD

Getting details of a dataset
----------------------------
TBD

Defining additional dataset
---------------------------
TBD

Limitations of standard datasets
--------------------------------
