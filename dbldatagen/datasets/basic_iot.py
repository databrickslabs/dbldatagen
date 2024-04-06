from pyspark.sql.types import LongType, StringType, IntegerType

from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="basic/iot", summary="Basic IOT Data Set", autoRegister=True)
class BasicIOTProvider(DatasetProvider):
    """
    Basic User Data Set
    ===================

    This is a basic user data set with customer id, name, email, ip address, and phone number.

    It takes the following optins when retrieving the table:
        - random: if True, generates random data
        - dummyValues: number of dummy value columns to generate (to widen row size if necessary)
        - rows : number of rows to generate
        - partitions: number of partitions to use

    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    """
    MAX_LONG = 9223372036854775807
    ALLOWED_OPTIONS = ["devicePopulation", "begin", "end", "interval", "rows", "partitions", "tableName"]

    def getTable(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                 **options):
        import dbldatagen as dg  # import locally to avoid circular imports

        device_population = options.get("devicePopulation", 100000)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"

        if rows is None or rows < 0:
            rows = 100000

        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 8)

        country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                         'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
        country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                           17]

        manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

        lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

        testDataSpec = (
            dg.DataGenerator(sparkSession, name="device_data_set", rows=rows,
                             partitions=partitions,
                             randomSeedMethod='hash_fieldname')

            # we'll use hash of the base field to generate the ids to
            # avoid a simple incrementing sequence
            .withColumn("internal_device_id", "long", minValue=0x1000000000000,
                        uniqueValues=device_population, omit=True, baseColumnType="hash")

            # note for format strings, we must use "%lx" not "%x" as the
            # underlying value is a long
            .withColumn("device_id", "string", format="0x%013x",
                        baseColumn="internal_device_id")

            # the device / user attributes will be the same for the same device id
            # so lets use the internal device id as the base column for these attribute
            .withColumn("country", "string", values=country_codes,
                        weights=country_weights,
                        baseColumn="internal_device_id")
            .withColumn("manufacturer", "string", values=manufacturers,
                        baseColumn="internal_device_id")

            # use omit = True if you don't want a column to appear in the final output
            # but just want to use it as part of generation of another column
            .withColumn("line", "string", values=lines, baseColumn="manufacturer",
                        baseColumnType="hash", omit=True)
            .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                        baseColumn="device_id",
                        baseColumnType="hash", omit=True)

            .withColumn("model_line", "string", expr="concat(line, '#', model_ser)",
                        baseColumn=["line", "model_ser"])
            .withColumn("event_type", "string",
                        values=["activation", "deactivation", "plan change",
                                "telecoms activity", "internet activity", "device error"],
                        random=True)
            .withColumn("event_ts", "timestamp",
                        begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00",
                        interval="1 minute",
                        random=True)
        )

        return testDataSpec
