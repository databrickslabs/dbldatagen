.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Writing Generated Data to Tables or Files
===========================================================

Generated data can be written directly to output tables or files using the ``OutputDataset`` class.

Writing generated data to a table
---------------------------------

Once you've defined a ``DataGenerator``, call the ``saveAsDataset`` method to write data to a target table.

.. code-block:: python

   import dbldatagen as dg
   from dbldatagen.config import OutputDataset

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", expr="concat('user_', id)")
       .withColumn("email_address", expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", template="555-DDD-DDDD")
   )

   # Define an output configuration:
   outputDataset = OutputDataset("main.demo.users")

   # Generate and write the output data:
   testDataSpec.saveAsDataset(dataset=outputDataset)

Writing generated data with streaming
-------------------------------------

Specify a ``trigger`` to write output data using Structured Streaming. Triggers can be passed as
Python dictionaries (e.g. ``{"processingTime": "10 seconds"}`` to write data every 10 seconds).

.. code-block:: python

   import dbldatagen as dg
   from dbldatagen.config import OutputDataset

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", expr="concat('user_', id)")
       .withColumn("email_address", expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", template="555-DDD-DDDD")
   )

   # Define an output configuration:
   outputDataset = OutputDataset(
      "main.demo.table",
      trigger={"processingTime": "10 seconds"}
   )

   # Generate and write the output data:
   testDataSpec.saveAsDataset(dataset=outputDataset)

Options for writing data
------------------------

Specify the ``output_mode`` and ``options`` to control how data is written to output tables or files.
Data will be written in append mode by default.

.. code-block:: python

   import dbldatagen as dg
   from dbldatagen.config import OutputDataset

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", expr="concat('user_', id)")
       .withColumn("email_address", expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", template="555-DDD-DDDD")
   )

   # Define an output configuration:
   outputDataset = OutputDataset(
      "/Volumes/main/demo/users_files/csv",
      options={"mergeSchema": "true"},
      output_mode="overwrite"
   )

   # Generate and write the output data:
   testDataSpec.saveAsDataset(dataset=outputDataset)

Writing generated data to files
-------------------------------

To write generated data to files (e.g. JSON or CSV), specify a ``format`` when creating your ``OutputConfig``.
File data can be written to a relative path using Databricks Volumes, an absolute path in cloud storage, or a path
in Databricks File System (DBFS).

.. code-block:: python

   import dbldatagen as dg
   from dbldatagen.config import OutputDataset

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", expr="concat('user_', id)")
       .withColumn("email_address", expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", template="555-DDD-DDDD")
   )

   # Define an output configuration:
   outputDataset = OutputDataset(
      "/Volumes/main/demo/users_files/csv",
      format="csv",
      options={"header": "true"}
   )

   # Generate and write the output data:
   testDataSpec.saveAsDataset(dataset=outputDataset)
