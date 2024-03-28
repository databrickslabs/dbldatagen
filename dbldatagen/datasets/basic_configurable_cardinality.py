from . import DatasetProvider, dataset_definition


@dataset_definition(name="basic/configurable-cardinality",
                    summary="Basic Configurable Cardinality Data Set",
                    autoRegister=True)
class BasicConfigurableCardinalityProvider(DatasetProvider):
    """ Basic User Data Set

    This is a basic user data set with customer id, name, email, ip address, and phone number.

    """

    def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=-1,
                 **options):
        import dbldatagen as dg

        random = options.get("random", False)
        dummyValues = options.get("dummyValues", 0)

        assert tableName is None or tableName == "primary", "Invalid table name"
        df_spec = (
             dg.DataGenerator(sparkSession=sparkSession, name="test_data_set1", rows=rows,
                              partitions=4, randomSeedMethod="hash_fieldname")
            .withColumn("customer_id", "long", minValue=1000000, random=random)
            .withColumn("name", "string",
                            template=r'\w \w|\w \w \w', random=random)
            .withColumn("email", "string",
                            template=r'\w.\w@\w.com|\w@\w.co.u\k', random=random)
            .withColumn("ip_addr", "string",
                             template=r'\n.\n.\n.\n', random=random)
            .withColumn("phone", "string",
                             template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd',
                            random=random)
            )

        if dummyValues > 0:
            df_spec = df_spec.withColumn("dummy", "long", random=True, numColumns=dummyValues, minValue=1)

        return df_spec
