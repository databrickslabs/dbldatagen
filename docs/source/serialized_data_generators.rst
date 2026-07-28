.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Saving and Loading Data Generation Specs from Configuration
===========================================================

Data generation specifications can be converted to and from configuration (either Python dictionaries or JSON strings).
This section shows how to save and load data generators, columns, and constraints from configuration.

Saving data generation specs to configuration
---------------------------------------------

``DataGenerators`` can be converted to dictionaries using the ``saveToInitializationDict`` method.

.. code-block:: python

   from pyspark.sql.types import StringType
   import dbldatagen as dg

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", StringType(), expr="concat('user_', id)")
       .withColumn("email_address", StringType(), expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", StringType(), template="555-DDD-DDDD")
   )

   # Get the data generation options as a Python dictionary:
   dataSpecOptions = testDataSpec.saveToInitializationDict()

Calling  ``saveToInitializationDict`` will return properties of the ``DataGenerator``  (e.g. ``rows``,
``randomSeedMethod``) as root-level keys. Associated dictionaries for the ``ColumnGenerationSpecs`` and ``Constraints``
will be returned in the ``columns`` and ``constraints`` keys.

Creating data generators from configuration
-------------------------------------------

``DataGenerators`` and their associated objects can be created from configuration by calling
``loadFromInitializationDict``.

.. code-block:: python

   import dbldatagen as dg

   # Define the data generation options:
   dataSpecOptions = {
      "name": "users_dataset",
      "rows": 1000,
      "randomSeedMethod": "hash_fieldname",
      "columns": [
         {"colName": "user_name", "colType": "string", "expr": "concat('user_', id)"},
         {"colName": "phone_number", "colType": "string", "template": "555-DDD-DDDD"}
      ]
   }

   # Create the DataGenerator from options:
   dg.DataGenerator.loadFromInitializationDict(dataSpecOptions)

Advanced configuration syntax
-----------------------------

When adding constraints, distributions, text generators, or data ranges via configuration, specify the object's
constructor arguments as a Python dictionary and include the class name in the `kind` property.

To define a column with a data range, pass a dictionary with the ``DateRange`` or ``NRange`` options.

.. code-block:: python

   dataSpecOptions = {
      "name": "users_dataset",
      "rows": 1000,
      "randomSeedMethod": "hash_fieldname",
      "columns": [
         {"colName": "user_name", "colType": "string", "expr": "concat('user_', id)"},
         {"colName": "phone_number", "colType": "string", "template": "555-DDD-DDDD"},
         {"colName": "created_on", "colType": "date", "dataRange": {
            "kind": "DateRange", "begin": "2020-01-01", "end": "2025-01-01", "interval": "1 DAY", "datetime_format": "yyyy-MM-dd"}}
      ]
   }

To define a column with a distribution, pass a dictionary with the ``Distribution`` options.

.. code-block:: python


   dataSpecOptions = {
      "name": "users_dataset", "rows": 1000, "randomSeedMethod": "hash_fieldname",
      "columns": [
         {"colName": "user_name", "colType": "string", "expr": "concat('user_', id)"},
         {"colName": "phone_number", "colType": "string", "template": "555-DDD-DDDD"},
         {"colName": "total_logins", "colType": "int", "distribution": {
            "kind": "Normal", "mean": "100", "stddev": "10"}}
      ]
   }

To define a column with a text generator, pass a dictionary with the ``TextGenerator`` options.

.. code-block:: python


   dataSpecOptions = {
      "name": "users_dataset", "rows": 1000, "randomSeedMethod": "hash_fieldname",
      "columns": [
         {"colName": "user_name", "colType": "string", "expr": "concat('user_', id)"},
         {"colName": "phone_number", "colType": "string", "template": "555-DDD-DDDD"},
         {"colName": "description", "colType": "string", "text": {
            "kind": "ILText", "sentences": 3, "words": 10}}
      ]
   }


To define a column with a text generator, pass a dictionary with the ``TextGenerator`` options.

.. code-block:: python

   dataSpecOptions = {
      "name": "users_dataset", "rows": 1000, "randomSeedMethod": "hash_fieldname",
      "columns": [
         {"colName": "user_name", "colType": "string", "expr": "concat('user_', id)"},
         {"colName": "phone_number", "colType": "string", "template": "555-DDD-DDDD"},
         {"colName": "total_logins", "colType": "int", "distribution": {
            "kind": "Normal", "mean": "100", "stddev": "10"}}
      ],
      "constraints": [
         {"kind": "PositiveValues", "columns": "total_logins", "strict": True}
      ]
   }

.. note::

   Columns which use ``PyfuncText``, ``PyfuncTextFactory``, and ``FakerTextFactory`` are not currently serializable to
   and from configuration.

Saving and loading from JSON
----------------------------

Data generators can be converted to and from JSON. This allows users to repeatedly generate datasets via options stored
in files.

Use ``saveToJson`` to generate a JSON string from a ``DataGenerator``.

.. code-block:: python

   from pyspark.sql.types import StringType
   import dbldatagen as dg

   # Create a sample data generator with a few columns:
   testDataSpec = (
       dg.DataGenerator(spark, name="users_dataset", rows=1000)
       .withColumn("user_name", StringType(), expr="concat('user_', id)")
       .withColumn("email_address", StringType(), expr="concat(user_name, '@email.com')")
       .withColumn("phone_number", StringType(), template="555-DDD-DDDD")
   )

   # Create a JSON string with the data generation config:
   jsonStr = testDataSpec.saveToJson()


Use ``loadFromJson`` to create a ``DataGenerator`` from a JSON string.

.. code-block:: python

   import dbldatagen as dg

   # Define the data generation options:
   jsonStr = '''{
      "name": "users_dataset",
      "rows": 1000,
      "randomSeedMethod": "hash_fieldname",
      "columns": [
         {"colName": "user_name", "colType": "string", "expr": "concat('user_', id)"},
         {"colName": "phone_number", "colType": "string", "template": "555-DDD-DDDD"}
      ]
   }'''

   # Create a data generator from the JSON string:
   testDataSpec = dg.DataGenerator.loadFromJson(jsonStr)
