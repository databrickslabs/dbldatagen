.. Test Data Generator documentation master file, created by
   sphinx-quickstart on Sun Jun 21 10:54:30 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Generating and using data with multiple tables
==============================================

See the section repeatable data generation for the concepts that underpin the data generation.

One common scenario is the need to be able to generate multiple tables
with consistent primary and foreign keys to model join or merge scenarios.

By generating tables with repeatable data, we can generate multiple versions of the same data for different tables and
ensure that we have referential integrity across the tables.

Telephony billing example
-------------------------
To illustrate multi-table data generation and use, we'll use a simplified version of telecoms billing processes.

Let's assume we have data as follows:

- A set of customers
- A set of customer device activity events
   - text message
   - local call
   - international call
   - long distance call
   - internet activity

- A set of pricing plans indicating
   - cost per MB of internet activity
   - cost per minute of call for each of the call categories
   - cost per message

Internet activity will be priced per MB transferred

Phone calls will be priced per minute or partial minute.

Messages will be priced per actual counts

.. note::
   For simplicity, we'll ignore the free data, messages and calls threshold in most plans and the complexity of
   matching devices to customers and telecoms operators - our goal here is to show generation of join ready data,
   rather than full modelling of phone usage invoicing.

   Nothing in the example is meant to represent real world phone, internet or SMS pricing.

Some utility functions
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import re

   MARGIN_PATTERN= re.compile(r"\s*\|")  # margin detection pattern for stripMargin
   def stripMargin(s):
     """  strip margin removes leading space in multi line string before '|' """
     return "\n".join(re.split(MARGIN_PATTERN, s))

Let's model our calling plans
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Note, we use two columns, ``ld_multipler`` and ``intl_multiplier`` just as intermediate results used
in later calculations and omit them from the output.

We use ``decimal`` types for prices to avoid rounding issues.

Here we use a simple sequence for our plan ids.

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   spark.catalog.clearCache()  # clear cache so that if we run multiple times to check performance, we're not relying on cache

   UNIQUE_PLANS = 20
   PLAN_MIN_VALUE = 100

   shuffle_partitions_requested = 8
   partitions_requested = 1
   data_rows = UNIQUE_PLANS # we'll generate one row for each plan

   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
   spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


   plan_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
               .withColumn("plan_id","int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS)
               # use plan_id as root value
               .withColumn("plan_name", prefix="plan", baseColumn="plan_id")

               # note default step is 1 so you must specify a step for small number ranges,
               .withColumn("cost_per_mb", "decimal(5,3)", minValue=0.005, maxValue=0.050,
                           step=0.005, random=True)
               .withColumn("cost_per_message", "decimal(5,3)", minValue=0.001, maxValue=0.02,
                           step=0.001, random=True)
               .withColumn("cost_per_minute", "decimal(5,3)", minValue=0.001, maxValue=0.01,
                           step=0.001, random=True)

               # we're modelling long distance and international prices simplistically -
               # each is a multiplier thats applied to base rate
               .withColumn("ld_multiplier", "decimal(5,3)", minValue=1.5, maxValue=3, step=0.05,
                           random=True, distribution="normal", omit=True)
               .withColumn("ld_cost_per_minute", "decimal(5,3)",
                           expr="cost_per_minute * ld_multiplier",
                           baseColumns=['cost_per_minute', 'ld_multiplier'])
               .withColumn("intl_multiplier", "decimal(5,3)", minValue=2, maxValue=4, step=0.05,
                           random=True,  distribution="normal", omit=True)
               .withColumn("intl_cost_per_minute", "decimal(5,3)",
                           expr="cost_per_minute * intl_multiplier",
                           baseColumns=['cost_per_minute', 'intl_multiplier'])
               )

   df_plans = (plan_dataspec.build()
               .cache()
              )

   display(df_plans)

Let's model our customers
^^^^^^^^^^^^^^^^^^^^^^^^^
We'll use device id as the foreign key for device events here.

We want to ensure that our device id is unique for each customer. We could use a simple sequence as
with plans but for the purposes of illustration, we'll use a hash of the customer ids instead.

There's still a small likelihood of hash collisions so we'll remove any duplicates from the generated data -
but in practice, we do not see duplicates in most small datasets when using hashing. As all data produced by
the framework is repeatable when not using random , or when using random with a seed,
this will give us a predictable range of foreign keys.

Use of hashes and sequences is a very efficient way of generating unique predictable keys
while introducing some pseudo-randomness in the values.


Note - for real telephony systems, there's a complex set of rules around device ids (IMEI and related device ids),
subscriber numbers and techniques for matching devices to subscribers. Again, our goal here is to illustrate
generating predictable join keys not full modelling of a telephony system.

We use decimal types for ids to avoid exceeding the range of ints and longs when working
with a larger numbers of customers. Even though our data set sizes are small,
when using hashed values, the range of the hashes produced can be large.

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
   spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

   UNIQUE_CUSTOMERS = 50000
   CUSTOMER_MIN_VALUE = 1000
   DEVICE_MIN_VALUE = 1000000000
   SUBSCRIBER_NUM_MIN_VALUE = 1000000000

   spark.catalog.clearCache()  # clear cache so that if we run multiple times to check
                               # performance, we're not relying on cache
   shuffle_partitions_requested = 8
   partitions_requested = 8
   data_rows = UNIQUE_CUSTOMERS

   customer_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
               .withColumn("customer_id","decimal(10)", minValue=CUSTOMER_MIN_VALUE,
                           uniqueValues=UNIQUE_CUSTOMERS)
               .withColumn("customer_name", template=r"\\w \\w|\\w a. \\w")

               # use the following for a simple sequence
               #.withColumn("device_id","decimal(10)", minValue=DEVICE_MIN_VALUE,
               #              uniqueValues=UNIQUE_CUSTOMERS)

               .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE,
                           baseColumn="customer_id", baseColumnType="hash")

               .withColumn("phone_number","decimal(10)",  minValue=SUBSCRIBER_NUM_MIN_VALUE,
                           baseColumn=["customer_id", "customer_name"], baseColumnType="hash")

               # for email, we'll just use the formatted phone number
               .withColumn("email","string",  format="subscriber_%s@myoperator.com",
                           baseColumn="phone_number")
               .withColumn("plan", "int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS,
                           random=True)
               )

   df_customers = (customer_dataspec.build()
                   .dropDuplicates(["device_id"])
                   .dropDuplicates(["phone_number"])
                   .orderBy("customer_id")
                   .cache()
                  )

   effective_customers = df_customers.count()

   print(stripMargin(f"""revised customers : {df_customers.count()},
          |   unique customers: {df_customers.select(F.countDistinct('customer_id')).take(1)[0][0]},
          |   unique device ids: {df_customers.select(F.countDistinct('device_id')).take(1)[0][0]},
          |   unique phone numbers: {df_customers.select(F.countDistinct('phone_number')).take(1)[0][0]}""")
        )

   display(df_customers)

Now let's model our device events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Generating `master-detail` style data is one of the key challenges in data generation for join ready data.

What do we mean by `master-detail`?

This is where the goal is to model data that consists of large grained entities, that are in turn
comprised of smaller items. For example invoices and their respective line items follow this pattern.

IOT data has similar characteristics. Usually you have a series of devices that generate time series style
events from their respective systems and subsystems - each data row being an observation of
some measure from some subsystem at a point in time.

Telephony billing activity has characteristics of both IOT data and master detail data.

For the telephony events, we want to ensure that on average `n` events occur per device per day and
that text and internet browsing is more frequent than phone calls.

A simple approach is simply to multiply the
`number of customers` by `number of days in data set`  by `average events per day`

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F

   AVG_EVENTS_PER_CUSTOMER = 50

   spark.catalog.clearCache()
   shuffle_partitions_requested = 8
   partitions_requested = 8
   NUM_DAYS=31
   MB_100 = 100 * 1000 * 1000
   K_1 = 1000
   data_rows = AVG_EVENTS_PER_CUSTOMER * UNIQUE_CUSTOMERS * NUM_DAYS

   spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
   spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


   # use random seed method of 'hash_fieldname' for better spread - default in later builds
   events_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested,
                      randomSeed=42, randomSeedMethod="hash_fieldname")
                # use same logic as per customers dataset to ensure matching keys - but make them random
               .withColumn("device_id_base","decimal(10)", minValue=CUSTOMER_MIN_VALUE,
                           uniqueValues=UNIQUE_CUSTOMERS,
                           random=True, omit=True)
               .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE,
                           baseColumn="device_id_base", baseColumnType="hash")

               # use specific random seed to get better spread of values
               .withColumn("event_type","string",
                           values=[ "sms", "internet", "local call", "ld call", "intl call" ],
                           weights=[50, 50, 20, 10, 5 ], random=True)

               # use Gamma distribution for skew towards short calls
               .withColumn("base_minutes","decimal(7,2)",  minValue=1.0, maxValue=100.0, step=0.1,
                           distribution=dg.distributions.Gamma(shape=1.5, scale=2.0),
                           random=True, omit=True)

               # use Gamma distribution for skew towards short transfers
               .withColumn("base_bytes_transferred","decimal(12)",  minValue=K_1, maxValue=MB_100,
                           distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                           random=True, omit=True)

               .withColumn("minutes", "decimal(7,2)",
                           baseColumn=["event_type", "base_minutes"],
                           expr= """
                                 case when event_type in ("local call", "ld call", "intl call")
                                     then base_minutes
                                     else 0
                                 end
                                  """)
               .withColumn("bytes_transferred", "decimal(12)",
                           baseColumn=["event_type", "base_bytes_transferred"],
                           expr= """
                                 case when event_type = "internet"
                                      then base_bytes_transferred
                                      else 0
                                 end
                                  """)

               .withColumn("event_ts", "timestamp",
                            data_range=dg.DateRange("2020-07-01 00:00:00",
                                                    "2020-07-31 11:59:59",
                                                    "seconds=1"),
                           random=True)

               )

   df_events = (events_dataspec.build()
                  )

   display(df_events)

Now let's compute the invoices
------------------------------
Let's compute the customers and associated plans

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F
   import pyspark.sql.types as T

   df_customer_pricing = df_customers.join(df_plans,
                                           df_plans.plan_id == df_customers.plan)

   display(df_customer_pricing)

let's compute our summary information

.. code-block:: python

   import dbldatagen as dg
   import pyspark.sql.functions as F
   import pyspark.sql.types as T


   # lets compute the summary minutes messages and bytes transferred
   df_enriched_events = (df_events
                         .withColumn("message_count",
                                     F.expr("""case
                                                  when event_type='sms' then 1
                                                                        else 0 end"""))
                         .withColumn("ld_minutes",
                                     F.expr("""case
                                                  when event_type='ld call'
                                                  then cast(ceil(minutes) as decimal(18,3))
                                                  else 0.0 end"""))
                         .withColumn("local_minutes",
                                     F.expr("""case when event_type='local call'
                                                    then cast(ceil(minutes) as decimal(18,3))
                                                    else 0.0 end"""))
                         .withColumn("intl_minutes",
                                     F.expr("""case when event_type='intl call'
                                               then cast(ceil(minutes) as decimal(18,3))
                                               else 0.0 end"""))
                        )

   df_enriched_events.createOrReplaceTempView("telephony_events")

   df_summary = spark.sql("""select device_id,
                                    round(sum(bytes_transferred) / 1000000.0, 3) as total_mb,
                                    sum(message_count) as total_messages,
                                    sum(ld_minutes) as total_ld_minutes,
                                    sum(local_minutes) as total_local_minutes,
                                    sum(intl_minutes) as total_intl_minutes,
                                    count(device_id) as event_count
                                    from telephony_events
                                    group by device_id

   """)

   df_summary.createOrReplaceTempView("event_summary")

   display(df_summary.where("event_count > 0"))

now let's compute the invoices

.. code-block:: python

   df_customer_summary = (df_customer_pricing.join(df_summary,
                                                   df_customer_pricing.device_id == df_summary.device_id )
                          .createOrReplaceTempView("customer_summary"))

   df_invoices = spark.sql("""
                        select *,
                           internet_cost + sms_cost + ld_cost + local_cost + intl_cost
                             as total_invoice
                           from
                             (select customer_id, customer_name,
                                     phone_number, email, plan_name,
                                     cast(round(total_mb * cost_per_mb, 2) as decimal(18,3))
                                         as internet_cost,
                                     cast(round(total_ld_minutes * ld_cost_per_minute, 2)
                                          as decimal(18,2))
                                       as ld_cost,
                                     cast(round(total_local_minutes * cost_per_minute, 2)
                                          as decimal(18,2))
                                       as local_cost,
                                     cast(round(total_intl_minutes * intl_cost_per_minute, 2)
                                          as decimal(18,2))
                                       as intl_cost,
                                     cast(round(total_messages * cost_per_message, 2)
                                          as decimal(18,2))
                                       as sms_cost
                              from customer_summary)

   """)

   display(df_invoices)

You can confirm that we have invoices for all customers by issuing a ``count`` on the invoices data set.

.. code-block::

   print(df_invoices.count())