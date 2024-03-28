from . import DatasetProvider, dataset_definition

@dataset_definition(name="basic/user", summary="Basic User Data Set", autoRegister=True)
class BasicUserProvider(DatasetProvider):
    """ Basic User Data Set

    This is a basic user data set with customer id, name, email, ip address, and phone number.

    """

    def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=-1, dummyValues=0, random=False):
        import dbldatagen as dg

        assert tableName is None or tableName == "primary", "Invalid table name"
        df_spec = (
             dg.DataGenerator(sparkSession=sparkSession, name="test_data_set1", rows=rows,
                              partitions=4, randomSeedMethod="hash_fieldname")
            .withColumn("customer_id", "long",minValue=1000000, random=random)
            .withColumn("email", "string",
                            template=r'\w.\w@\w.com|\w@\w.co.u\k', random=random)
            .withColumn("ip_addr", "string",
                             template=r'\n.\n.\n.\n',random=random)
            .withColumn("phone", "string",
                             template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd',
                            random=random)
            )

        if dummyValues > 0:
            df_spec = df_spec.withColumn("dummy", "long", random=True, numValues=dummyValues, minValue=1)

        return df_spec

