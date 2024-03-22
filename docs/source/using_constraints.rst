.. Databricks Labs Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.

Using Constraints to control data generation
============================================

You can use the `constraints` subpackage to limit the data generated to only data that satisfies specific conditions.

While similar effects can be achieved by applying `where` clauses to the generated dataframe, the Constraints objects
may use a variety of effects to satisfy the constraint, including filtering, modifying the data generation strategy
or transforming the generated data.

Any statistical distribution applied to the data generation through the use of objects in the `distributions`
subpackage, or implicitly through the use of weights or other means, are only applied **before** the application
of constraints.

.. note ::
   When using constraints, the `number of rows` parameter applied to the data generation specification
   determines the number of rows generated **before** constraints are applied.

   Due to the possible filtering effects of constraints, the number of rows actually produced may be less, or even zero
   if there are no rows that satisfy the constraints.

How constraints are implemented
-------------------------------

.. image:: _static/images/generating_data_from_spec.png

Constraints are implemented in the following ways:

- The data generation specification may be modified to generate data that conforms more closely with the constraints
   - This may modify the internal column definitions
- The Spark data frame columns may be transformed to conform with constraints
   - This may impact conformance with statistical distributions
- The Spark data frame may be filtered (using `where` clauses) to conform with the constraints
   - This may reduce the final row count from the requested row count (or even completely filter all data)

Generally speaking, the SQL expression constraints will be implemented purely using filtering whereas other
constraints may use modification of the data generation spec and / or transformation.

.. note:: These modifications will be performed in-place on the the data generation specification. If the same
          data generation specification is being used to generate multiple data sets but with different constraints,
          it should be cloned before applying constraints.

Example
-------
We'll use the IOT style data from the `Getting Started` section to illustrate use of constraints.

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

   dataspec = (
       dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
         .withColumn("customer_id","long", uniqueValues=uniqueCustomers)
         .withColumn("name", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
         .withColumn("alias", percentNulls=0.01, template=r'\\w \\w|\\w a. \\w')
         .withColumn("payment_instrument_type", values=['paypal', 'Visa', 'Mastercard',
                     'American Express', 'discover', 'branded visa', 'branded mastercard'],
                     random=True, distribution="normal")
         .withColumn("int_payment_instrument", "int",  minValue=0000, maxValue=9999,
                     baseColumn="customer_id", baseColumnType="hash", omit=True)
         .withColumn("payment_instrument",
                     expr="format_number(int_payment_instrument, '**** ****** *####')",
                     baseColumn="int_payment_instrument")
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

   # read the written data - if we simply recompute, timestamps of original will be lost
   df_original = spark.read.format("delta").load(customers1_location)

   df1_updates = (df_original.sample(False, 0.1)
           .limit(50 * 1000)
           .withColumn("alias", F.lit('modified alias'))
           .withColumn("modified_ts",F.expr('now()'))
           .withColumn("memo", F.lit("update")))

   df_changes = df1_inserts.union(df1_updates)

   # randomize ordering
   df_changes = (df_changes.withColumn("order_rand", F.expr("rand()"))
                 .orderBy("order_rand")
                 .drop("order_rand")
                 )


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
