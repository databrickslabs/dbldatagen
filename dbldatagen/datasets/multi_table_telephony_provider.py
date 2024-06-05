from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="multi_table/telephony", summary="Multi-table telephony dataset", supportsStreaming=True,
                    autoRegister=True,
                    tables=["plans", "customers", "calls", "messages", "data_usage", "billing"])
class MultiTableTelephonyProvider(DatasetProvider):
    """ Telephony multi-table example from documentation

    See [https://databrickslabs.github.io/dbldatagen/public_docs/multi_table_data.html]

    It generates one of several possible tables on each call

    plans - which model telephony calling plans
    customers - which model cellular customers
    device_events - which are used to model device events like calls and text messages
    invoices - computed from a join of the other tables to compute number of texts etc.

    While real world billing is different from this in with unlimited messaging etc, or prices per bands of usage,
    this simplified pricing model is useful in many scenarios where a dataset produced from the join of other
    datasets is needed.

    It can be used to tune join performance, or in many other benchmarking and testing applications.

    The following options are supported:
    - random - whether data is random or from each rows seed value
    - numPlans - number of unique plans
    - numCustomers - number of unique customers
    - averageEventsPerDay - number of telephony events per customer per day
    - numDays - number of days to spread events over

    When requesting the `plans` table, the `numPlans` parameter will determine the number of unique plans and
    the `numPlans` parameter will be ignored

    """
    MAX_LONG = 9223372036854775807
    PLAN_MIN_VALUE = 100
    DEFAULT_NUM_PLANS = 20
    DEFAULT_NUM_CUSTOMERS = 50_000
    CUSTOMER_MIN_VALUE = 1000
    DEVICE_MIN_VALUE = 1000000000
    SUBSCRIBER_NUM_MIN_VALUE = 1000000000

    def getPlans(self, sparkSession, *, rows, partitions, generateRandom, numPlans, dummyValues):
        import dbldatagen as dg

        if numPlans is None or numPlans < 0:
            numPlans = self.DEFAULT_NUM_PLANS

        if rows is None or rows < 0:
            rows = numPlans

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9)

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
        return plan_dataspec

    def getCustomers(self, sparkSession, *, rows, partitions, generateRandom, numCustomers, numPlans, dummyValues):
        import dbldatagen as dg

        if numCustomers is None or numCustomers < 0:
            numCustomers = self.DEFAULT_NUM_CUSTOMERS

        if rows is None or rows < 0:
            rows = numCustomers

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 6)

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

    @DatasetProvider.allowed_options(options=["random", "numPlans", "numCustomers", "dummyValues"])
    def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1, **options):

        if rows is None or rows < 0:
            rows = 100000

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 8)

        generateRandom = options.get("random", False)
        numPlans = options.get("numPlans", self.DEFAULT_NUM_PLANS)
        numCustomers = options.get("numCustomers", self.DEFAULT_NUM_CUSTOMERS)
        numDevices = options.get("numDevices", self.DEFAULT_NUM_CUSTOMERS)
        dummyValues = options.get("dummyValues", 0)

        if tableName == "plans":
            return self.getPlans(sparkSession, rows=rows, partitions=partitions, numPlans=numPlans,
                                 generateRandom=generateRandom, dummyValues=dummyValues)
        elif tableName == "customers":
            return self.getCustomers(sparkSession, rows=rows, partitions=partitions, numCustomers=numCustomers,
                                     generateRandom=generateRandom, numPlans=numPlans, dummyValues=dummyValues)
        elif tableName =="events":
            pass
        elif tableName == "invoices":
            pass

    def getAssociatedDataset(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                             **options):
        raise NotImplementedError("Base/user data provider does not produce any supporting tables!")
