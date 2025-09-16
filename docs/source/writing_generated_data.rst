.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Writing Generated Data to Tables or Files
===========================================================

Generated data can be written directly to output tables or files using the ``OutputConfig`` class.

Writing Generated Data to a Table
--------------------------------------------

Once you've defined a ``DataGenerator``, call the ``writeGeneratedData`` method to write data to a target table.

.. code-block:: python

   from pyspark.sql.types import StringType
   import dbldatagen as dg
   from dbldatagen.config import OutputConfig

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", StringType(), expr="concat('user_', id)")
       .withColumn("email_address", StringType(), expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", StringType(), template="555-DDD-DDDD")
   )

   # Define an output configuration:
   outputConfig = OutputConfig(location="main.demo.table", output_mode="overwrite")

   # Get the data generation options as a Python dictionary:
   testDataSpec.writeGeneratedData(config=outputConfig)

