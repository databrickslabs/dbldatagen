.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Quick and easy dataset generation with Standard Datasets (Experimental)
=======================================================================

The standard dataset mechanism is a new pluggable framework for generation of data generation patterns using
predefined data set definitions.

Standard datasets are provided by the registration of dataset provider classes with the standard dataset mechanism.
These classes provide a mechanism for generating data frames with a data generation pattern (and optionally, a
specific schema).

They can provide:
   - Preconfigured data generator instances that will produce data once the `build()` method is called.
     These data generation specifications are produced via invoking the `Datasets.get()` method.
   - Associated tables or datasets that will produce prebuilt dataframes that can be used in conjunction with the
     preconfigured data generation instances. The are produced via invoking the `Datasets.getAssociatedDataset()`
     method.
   - Multiple named tables generators and associated datasets.

The overall goal is to allow provision of pre-canned definitions for data analysis and ML use cases such as
benchmarks, datasets needed to explore common optimizations and architectural patterns, datasets needed for
open competition style problems and to provide a convenient starting point for data generation.

As the main `Datasets.get()` mechanism produces a Data Generator object, it can be further customized through
addition of further columns, constraints, or modification of existing columns.

The role of the associated datasets can vary - they may provide lookup data to be used in conjunction with the
data generation instances produced, they may run a benchmark or validation over other datasets passed as parameters, or
provide summary derived data from the data produced by the data generation instances.

.. Note ::

   In keeping with the varied roles of the associated datasets, the associated datasets may be retrieved using one of
   several methods to get the dataframes in a way that is self descriptive. The methods are:
   - `Datasets.getAssociatedDataset()` - returns a dataframe based on the supplied parameters and provider logic
   - `Datasets.getSupportingDataset()` - alias for `Datasets.getAssociatedDataset()`
   - `Datasets.getCombinedDataset()` - alias for `Datasets.getAssociatedDataset()`
   - `Datasets.getEnrichedDataset()` - alias for `Datasets.getAssociatedDataset()`
   - `Datasets.getSummaryDataset()` - alias for `Datasets.getAssociatedDataset()`

    The method names are intended to be self descriptive and to provide a clear indication of the role of the associated
    usage, but they are all aliases of `getAssociatedDataset()` and can be used interchangeably.

The standard datasets are also self documenting so that you can dynamically see what standard datasets are available,
find further details of datasets and their data.

Simple use
----------

For the simplest use, you can simply select a standard dataset and generate a data frame from it.

.. code-block:: python

   import dbldatagen as dg
   df = dg.Datasets(spark, "basic/user").get().build()
   display(df)

In this case, the provider determines a default number of rows and partitions to generate.

Customizing rows and partitions
-------------------------------
You can pass arguments such as `rows` and `partitions` to the `get()` method to customize the number of rows and
partitions in the generated data frame.

.. code-block:: python

   import dbldatagen as dg
   df = dg.Datasets(spark, "basic/user").get(rows=1000, partitions=2).build()
   display(df)

These options are supported by all standard datasets.

Passing additional options
--------------------------
Specific dataset providers may support additional options that can be passed to the `get()` method.

For example the `basic/user` dataset supports the `dummyValues` option to specify the number of dummy values to
generate when a wider rows size is needed.

.. code-block:: python

   import dbldatagen as dg
   df = dg.Datasets(spark, "basic/user").get(rows=1000, partitions=2, dummyValues=10).build()
   display(df)

Listing available datasets
--------------------------
The `Datasets` class provides a method `list()` that can be used to list the available datasets.

The following example lists available datasets that start with the prefix `basic` and support streaming usage.

.. code-block:: python

   import dbldatagen as dg
   dg.Datasets.list(pattern="basic.*", supportsStreaming=True)

Getting details of a dataset
----------------------------
The `Datasets` class provides a method `describe()` that can be used to describe a particular dataset.
The following example describes the `basic/user` dataset.

.. code-block:: python

   import dbldatagen as dg
   dg.Datasets.describe("basic/user")

Multi-table use
---------------

You can use the multi-table provider capabilities to generate multiple datasets that can then be used in collaboratively
to generate data for common architectural patterns.

The following example gets the datasets and joins them as described in the multi-table section of the documentation.
This can be useful for benchmarking of joins.

.. code-block:: python

   import dbldatagen as dg

   multiTableDS = dg.Datasets(spark, "multi_table/telephony")
   options = {"numPlans": 50, "numCustomers": 100}

   dfPlans = multiTableDS.get(table="plans", **options).build()
   dfCustomers = multiTableDS.get(table="customers", **options).build()
   dfDeviceEvents = multiTableDS.get(table="deviceEvents", **options).build()

   dfInvoices = multiTableDS.getSummaryDataset(table="invoices",
                                              plans=dfPlans,
                                              customers=dfCustomers,
                                              deviceEvents=dfDeviceEvents)
   display(dfInvoices)







Notes for developers
--------------------

To implement a dataset provider, you need to create a class that extends the `DatasetProvider` class and implements
the `getTableGenerator` and `getAssociatedDataset` methods.

The simplest way to declare the needed metadata is to use the `@dataset_provider` decorator.
Use the option `autoRegister=True` to automatically register the dataset provider with the standard dataset mechanism.

See the `BasicUserProvider` and `MultiTableTelephonyProvider` dataset provider implementations in the datasets
package for examples.

.. Note ::

   Implementing a dataset provider only requires implementing the `getTableGenerator` and
   `getAssociatedDataset` methods.

   The `get` method is provided by the `Datasets` class and should not be overridden.

   All of the aliased methods are mapped automatically to the `getAssociatedDataset` method.
