from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="basic/user", summary="Basic User Data Set", autoRegister=True, supportsStreaming=True)
class BasicUserProvider(DatasetProvider):
    """
    Basic User Data Set
    ===================

    This is a basic user data set with customer id, name, email, ip address, and phone number.

    It takes the following optins when retrieving the table:
        - random: if True, generates random data
        - dummyValues: number of additional dummy value columns to generate (to widen row size if necessary)
        - rows : number of rows to generate. Default is 100000
        - partitions: number of partitions to use. If -1, it will be computed based on the number of rows
        -

    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    Note that this datset does not use any features that would prevent it from being used as a source for a
    streaming dataframe, and so the flag `supportsStreaming` is set to True.

    """
    MAX_LONG = 9223372036854775807
    COLUMN_COUNT = 5

    @DatasetProvider.allowed_options(options=["random", "dummyValues"])
    def getTableDataGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                              **options):
        import dbldatagen as dg

        generateRandom = options.get("random", False)
        dummyValues = options.get("dummyValues", 0)

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, self.COLUMN_COUNT + dummyValues)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        df_spec = (
            dg.DataGenerator(sparkSession=sparkSession, rows=rows,
                             partitions=partitions,
                             randomSeedMethod="hash_fieldname")
            .withColumn("customer_id", "long", minValue=1000000, maxValue=self.MAX_LONG, random=generateRandom)
            .withColumn("name", "string",
                        template=r'\w \w|\w \w \w', random=generateRandom)
            .withColumn("email", "string",
                        template=r'\w.\w@\w.com|\w@\w.co.u\k', random=generateRandom)
            .withColumn("ip_addr", "string",
                        template=r'\n.\n.\n.\n', random=generateRandom)
            .withColumn("phone", "string",
                        template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd',
                        random=generateRandom)
        )

        if dummyValues > 0:
            df_spec = df_spec.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                         minValue=1, maxValue=self.MAX_LONG)

        return df_spec
