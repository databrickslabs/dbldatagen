from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="multi-table/telephony", summary="Multi-table telephony dataset", supportsStreaming=True,
                    autoRegister=True, tables=["plans", "customers", "calls", "messages", "data_usage", "billing"])
class MultiTableTelephonyProvider(DatasetProvider):
    """ Basic User Data Set

    This is a basic user data set with customer id, name, email, ip address, and phone number.

    """

    def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=-1,
                 **options):
        import dbldatagen as dg

        random = options.get("random", False)
        dummyValues = options.get("dummyValues", 0)

        UNIQUE_PLANS = 20
        PLAN_MIN_VALUE = 100

        assert tableName in ["plans", "customers", ], "Invalid table name"

        if tableName == "plans":
            plan_dataspec = (
                dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
                .withColumn("plan_id", "int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS)
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
                            random=True, distribution="normal", omit=True)
                .withColumn("intl_cost_per_minute", "decimal(5,3)",
                            expr="cost_per_minute * intl_multiplier",
                            baseColumns=['cost_per_minute', 'intl_multiplier'])
            )
            return plan_dataspec
