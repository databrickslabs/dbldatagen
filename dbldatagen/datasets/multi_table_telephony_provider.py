from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="multi_table/telephony", summary="Multi-table telephony dataset", supportsStreaming=True,
                    autoRegister=True,
                    tables=["plans", "customers", "deviceEvents"],
                    associatedDatasets=["invoices"])
class MultiTableTelephonyProvider(DatasetProvider):
    """ Telephony multi-table example from documentation

    See [https://databrickslabs.github.io/dbldatagen/public_docs/multi_table_data.html]

    It generates one of several possible tables on each call

    plans - which model telephony calling plans
    customers - which model cellular customers
    device_events - which are used to model device events like calls and text messages

    Once the above tables have been computed, you can retrieve the combined tables for invoices
    using `Datasets(...).getCombinedTable("invoices")`

    The following combined tables are available:
    invoices - computed from a join of the other tables to compute number of texts etc.

    While real world billing is different from this in with unlimited messaging etc, or prices per bands of usage,
    this simplified pricing model is useful in many scenarios where a dataset produced from the join of other
    datasets is needed.

    It can be used to tune join performance, or in many other benchmarking and testing applications.

    The following options are supported:
    - random - whether data is random or from each row's seed value
    - numPlans - number of unique plans
    - numCustomers - number of unique customers
    - averageEventsPerDay - number of telephony events per customer per day
    - numDays - number of days to spread events over
    - dummyValues - number of dummy values to widen the tables

    When requesting the `plans` table, the `numPlans` parameter will determine the number of unique plans.
    The `numCustomers` parameter will determine the number of customers
    For device events, the number of customers * the average number of events per day * the number of days will
    determine the number of device events possible.

    While it is possible to specify the number of rows explicitly when getting each table generator, the default will
    be to compute the number of rows from these.

    """
    MAX_LONG = 9223372036854775807
    PLAN_MIN_VALUE = 100
    DEFAULT_NUM_PLANS = 20
    DEFAULT_NUM_CUSTOMERS = 50_000
    CUSTOMER_MIN_VALUE = 1000
    DEVICE_MIN_VALUE = 1000000000
    SUBSCRIBER_NUM_MIN_VALUE = 1000000000
    DEFAULT_NUM_DAYS = 31
    DEFAULT_AVG_EVENTS_PER_CUSTOMER = 50

    def getPlans(self, sparkSession, *, rows, partitions, generateRandom, numPlans, dummyValues):
        import dbldatagen as dg

        if numPlans is None or numPlans < 0:
            numPlans = self.DEFAULT_NUM_PLANS

        if rows is None or rows < 0:
            rows = numPlans

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        plan_dataspec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("plan_id", "int", minValue=self.PLAN_MIN_VALUE, uniqueValues=numPlans)
            # use plan_id as root value
            .withColumn("plan_name", prefix="plan", baseColumn="plan_id")

            # note default step is 1 so you must specify a step for small number ranges,
            .withColumn("cost_per_mb", "decimal(5,3)", minValue=0.005, maxValue=0.050,
                        step=0.005, random=generateRandom)
            .withColumn("cost_per_message", "decimal(5,3)", minValue=0.001, maxValue=0.02,
                        step=0.001, random=generateRandom)
            .withColumn("cost_per_minute", "decimal(5,3)", minValue=0.001, maxValue=0.01,
                        step=0.001, random=generateRandom)

            # we're modelling long distance and international prices simplistically -
            # each is a multiplier thats applied to base rate
            .withColumn("ld_multiplier", "decimal(5,3)", minValue=1.5, maxValue=3, step=0.05,
                        random=generateRandom, distribution="normal", omit=True)
            .withColumn("ld_cost_per_minute", "decimal(5,3)",
                        expr="cost_per_minute * ld_multiplier",
                        baseColumns=['cost_per_minute', 'ld_multiplier'])
            .withColumn("intl_multiplier", "decimal(5,3)", minValue=2, maxValue=4, step=0.05,
                        random=generateRandom, distribution="normal", omit=True)
            .withColumn("intl_cost_per_minute", "decimal(5,3)",
                        expr="cost_per_minute * intl_multiplier",
                        baseColumns=['cost_per_minute', 'intl_multiplier'])
        )

        if dummyValues > 0:
            plan_dataspec = plan_dataspec.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                                     minValue=1, maxValue=self.MAX_LONG)

        return plan_dataspec

    def getCustomers(self, sparkSession, *, rows, partitions, generateRandom, numCustomers, numPlans, dummyValues):
        import dbldatagen as dg

        if numCustomers is None or numCustomers < 0:
            numCustomers = self.DEFAULT_NUM_CUSTOMERS

        if rows is None or rows < 0:
            rows = numCustomers

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 6 + dummyValues)

        customer_dataspec = (dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
                             .withColumn("customer_id", "decimal(10)", minValue=self.CUSTOMER_MIN_VALUE,
                                         uniqueValues=numCustomers)
                             .withColumn("customer_name", template=r"\\w \\w|\\w a. \\w")

                             # use the following for a simple sequence
                             # .withColumn("device_id","decimal(10)", minValue=DEVICE_MIN_VALUE,
                             #              uniqueValues=UNIQUE_CUSTOMERS)

                             .withColumn("device_id", "decimal(10)", minValue=self.DEVICE_MIN_VALUE,
                                         baseColumn="customer_id", baseColumnType="hash")

                             .withColumn("phone_number", "decimal(10)", minValue=self.SUBSCRIBER_NUM_MIN_VALUE,
                                         baseColumn=["customer_id", "customer_name"], baseColumnType="hash")

                             # for email, we'll just use the formatted phone number
                             .withColumn("email", "string", format="subscriber_%s@myoperator.com",
                                         baseColumn="phone_number")
                             .withColumn("plan", "int", minValue=self.PLAN_MIN_VALUE, uniqueValues=numPlans,
                                         random=generateRandom)
                             .withConstraint(dg.constraints.UniqueCombinations(columns=["device_id"]))
                             .withConstraint(dg.constraints.UniqueCombinations(columns=["phone_number"]))
                             )
        if dummyValues > 0:
            customer_dataspec = customer_dataspec.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                                             minValue=1, maxValue=self.MAX_LONG)

        return customer_dataspec

    def getDeviceEvents(self, sparkSession, *, rows, partitions, generateRandom, numCustomers, numDays, dummyValues,
                        averageEventsPerCustomer):
        import dbldatagen as dg

        MB_100 = 100 * 1000 * 1000
        K_1 = 1000

        if rows is None or rows < 0:
            rows = averageEventsPerCustomer * numCustomers * numDays

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 8 + dummyValues)

        # use random seed method of 'hash_fieldname' for better spread - default in later builds
        events_dataspec = (dg.DataGenerator(sparkSession, rows=rows, partitions=partitions,
                                            randomSeed=42, randomSeedMethod="hash_fieldname")
                           # use same logic as per customers dataset to ensure matching keys
                           # but make them random
                           .withColumn("device_id_base", "decimal(10)", minValue=self.CUSTOMER_MIN_VALUE,
                                       uniqueValues=numCustomers,
                                       random=generateRandom, omit=True)
                           .withColumn("device_id", "decimal(10)", minValue=self.DEVICE_MIN_VALUE,
                                       baseColumn="device_id_base", baseColumnType="hash")

                           # use specific random seed to get better spread of values
                           .withColumn("event_type", "string",
                                       values=["sms", "internet", "local call", "ld call", "intl call"],
                                       weights=[50, 50, 20, 10, 5], random=generateRandom)

                           # use Gamma distribution for skew towards short calls
                           .withColumn("base_minutes", "decimal(7,2)",
                                       minValue=1.0, maxValue=100.0, step=0.1,
                                       distribution=dg.distributions.Gamma(shape=1.5, scale=2.0),
                                       random=generateRandom, omit=True)

                           # use Gamma distribution for skew towards short transfers
                           .withColumn("base_bytes_transferred", "decimal(12)",
                                       minValue=K_1, maxValue=MB_100,
                                       distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                                       random=generateRandom, omit=True)

                           .withColumn("minutes", "decimal(7,2)",
                                       baseColumn=["event_type", "base_minutes"],
                                       expr="""
                                      case when event_type in ("local call", "ld call", "intl call")
                                          then base_minutes
                                          else 0
                                      end
                                       """)
                           .withColumn("bytes_transferred", "decimal(12)",
                                       baseColumn=["event_type", "base_bytes_transferred"],
                                       expr="""
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

        if dummyValues > 0:
            events_dataspec = events_dataspec.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                                         minValue=1, maxValue=self.MAX_LONG)

        return events_dataspec

    @DatasetProvider.allowed_options(options=["random", "numPlans", "numCustomers", "dummyValues", "numDays",
                                              "averageEventsPerCustomer"])
    def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1, **options):
        generateRandom = options.get("random", False)
        numPlans = options.get("numPlans", self.DEFAULT_NUM_PLANS)
        numCustomers = options.get("numCustomers", self.DEFAULT_NUM_CUSTOMERS)
        numDays = options.get("numDays", self.DEFAULT_NUM_DAYS)
        dummyValues = options.get("dummyValues", 0)
        averageEventsPerCustomer = options.get("averageEventsPerCustomer", self.DEFAULT_AVG_EVENTS_PER_CUSTOMER)

        if tableName == "plans":
            return self.getPlans(sparkSession, rows=rows, partitions=partitions, numPlans=numPlans,
                                 generateRandom=generateRandom, dummyValues=dummyValues)
        elif tableName == "customers":
            return self.getCustomers(sparkSession, rows=rows, partitions=partitions, numCustomers=numCustomers,
                                     generateRandom=generateRandom, numPlans=numPlans, dummyValues=dummyValues)
        elif tableName == "deviceEvents":
            return self.getDeviceEvents(sparkSession, rows=rows, partitions=partitions, generateRandom=generateRandom,
                                        numCustomers=numCustomers, numDays=numDays, dummyValues=dummyValues,
                                        averageEventsPerCustomer=averageEventsPerCustomer)

    @DatasetProvider.allowed_options(options=["plans", "customers", "deviceEvents"])
    def getAssociatedDataset(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                             **options):
        from pyspark.sql import DataFrame
        import pyspark.sql.functions as F

        dfPlans = options.get("plans", None)
        assert dfPlans is not None and issubclass(type(dfPlans), DataFrame), \
            "Option `plans` should be a dataframe of plan records"

        dfCustomers = options.get("customers", None)
        assert dfCustomers is not None and issubclass(type(dfCustomers), DataFrame), \
            "Option `customers` should be dataframe of customer records"

        dfDeviceEvents = options.get("deviceEvents", None)
        assert dfDeviceEvents is not None and issubclass(type(dfDeviceEvents), DataFrame), \
            "Option `device_events` should be dataframe of device_event records"

        if tableName == "invoices":
            df_customer_pricing = dfCustomers.join(dfPlans, dfPlans.plan_id == dfCustomers.plan)

            # let's compute the summary minutes messages and bytes transferred
            df_enriched_events = (dfDeviceEvents
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

            df_enriched_events.createOrReplaceTempView("mtp_telephony_events")

            # compute summary activity
            df_summary = sparkSession.sql("""select device_id,
                                             round(sum(bytes_transferred) / 1000000.0, 3) as total_mb,
                                             sum(message_count) as total_messages,
                                             sum(ld_minutes) as total_ld_minutes,
                                             sum(local_minutes) as total_local_minutes,
                                             sum(intl_minutes) as total_intl_minutes,
                                             count(device_id) as event_count
                                             from mtp_telephony_events
                                             group by device_id

            """)

            df_summary.createOrReplaceTempView("mtp_event_summary")

            df_customer_summary = (
                df_customer_pricing.join(df_summary,
                                         df_customer_pricing.device_id == df_summary.device_id)
                .createOrReplaceTempView("mtp_customer_summary"))

            df_invoices = sparkSession.sql("""
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
                                       from mtp_customer_summary)

            """)

            return df_invoices
        else:
            raise ValueError(f"Unknown table or dataset `{tableName}`!")
