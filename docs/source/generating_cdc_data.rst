.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Generating Change Data Capture data
===================================

This section explores some of the features for generating CDC style data - that is exploring the abilitty to
generate a base data set and then apply changes such as updates to existing rows and
new rows that will be inserts to the existing data

See the section on repeatable data generation for the concepts that underpin the data generation.

Overview
--------
We'll generate a customer table, and write out the data.

Then we generate changes for the table and show merging them in.

To start, we'll specify some locations for our data:

.. code-block:: python

   BASE_PATH = '/tmp/dbldatagen/cdc/'
   dbutils.fs.mkdirs(BASE_PATH)

   customers1_location = BASE_PATH + "customers1"

Lets generate 10 million customer style records.

We'll add a timestamp for when the row was generated and a memo field to mark what operation added it.

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   spark.catalog.clearCache()
   shuffle_partitions_requested = 8
   partitions_requested = 32
   data_rows = 10 * 1000 * 1000

   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
   spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

   uniqueCustomers = 10 * 1000000

   dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
               .withColumn("customer_id","long", uniqueValues=uniqueCustomers)
               .withColumn("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
               .withColumn("alias", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
               .withColumn("payment_instrument_type", values=['paypal', 'Visa', 'Mastercard', 'American Express', 'discover', 'branded visa', 'branded mastercard'], random=True, distribution="normal")
               .withColumn("int_payment_instrument", "long",  minValue=100000000000000, maxValue=999999999999999,  baseColumn="customer_id", baseColumnType="hash", omit=True)
               .withColumn("payment_instrument", expr="format_number(int_payment_instrument, '#### ###### #####')", baseColumn="int_payment_instrument")
               .withColumn("email", template=r'\\w.\\w@\\w.com|\\w-\\w@\\w')
               .withColumn("email2", template=r'\\w.\\w@\\w.com')
               .withColumn("ip_address", template=r'\\n.\\n.\\n.\\n')
               .withColumn("md5_payment_instrument",
                           expr="md5(concat(payment_instrument_type, ':', payment_instrument))",
                           base_column=['payment_instrument_type', 'payment_instrument'])
               .withColumn("customer_notes", text=dg.ILText(words=(1,8)))
               .withColumn("created_ts", "timestamp", expr="now()")
               .withColumn("modified_ts", "timestamp", expr="now()")
               .withColumn("memo", expr="'original data'")
               )
   df1 = dataspec.build()

   # write table

   df1.write.format("delta").save(customers1_location)

Creating a table definition
^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can use the features of the data generator to script SQL definitions for table creation and merge
statements.

Lets create a table definition around our data. As we generate a SQL statement with an explicit location,
the table is implicitly ``external`` and will not overwrite our data.

.. code-block:: python

   customers1_location = BASE_PATH + "customers1"
   tableDefn=dataspec.scriptTable(name="customers1", location=customers1_location)

   spark.sql(tableDefn)

Now lets explore the table layout:

.. code-block:: sql

   %sql
   -- lets check our table

   select * from customers1

Creating Changes
^^^^^^^^^^^^^^^^

Lets generate some changes.

Here we want to generate a set of new rows, which we guarantee to be new by using customer ids greater than the maximum
existing customer id.

We will also generate a set of updates by sampling from the existing data and adding some modifications.

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   start_of_new_ids = df1.select(F.max('customer_id')+1).collect()[0][0]

   print(start_of_new_ids)

   df1_inserts = (dataspec.clone()
           .option("startingId", start_of_new_ids)
           .withRowCount(10 * 1000)
           .build()
           .withColumn("memo", F.lit("insert"))
           .withColumn("customer_id", F.expr(f"customer_id + {start_of_new_ids}"))
                 )

   df1_updates = (df1.sample(False, 0.1)
           .limit(50 * 1000)
           .withColumn("alias", F.lit('modified alias'))
           .withColumn("modified_ts",F.expr('current_timestamp()'))
           .withColumn("memo", F.lit("update")))


   df_changes = df1_inserts.union(df1_updates)

   display(df_changes)

Merging in the changes
^^^^^^^^^^^^^^^^^^^^^^

We can script the merge statement in the data generator.

The ``updateColumns`` argument, specifies which columns should be updated.
The corresponding ``updateColumnExprs`` argument provides SQL expressions as overrides for the
columns being updated. These do not have to provided - in which case the
values of the columns from the source table will be used.

.. code-block:: python

   df_changes.dropDuplicates(["customer_id"]).createOrReplaceTempView("customers1_changes")
   sqlStmt = dataspec.scriptMerge(tgtName="customers1", srcName="customers1_changes",
                                  joinExpr="src.customer_id=tgt.customer_id",
                                  updateColumns=["alias", "memo","modified_ts"],
                                  updateColumnExprs=[ ("memo", "'updated on merge'"),
                                                      ("modified_ts", "now()")
                                                    ])

   print(sqlStmt)

   spark.sql(sqlStmt)

That's all that's required to perform merges with the data generation framework.
Note that these merge script statements can be used as part of a streaming merge implementation also.
