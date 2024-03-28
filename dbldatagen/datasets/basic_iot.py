from . import DatasetProvider, dataset_definition
from pyspark.sql.types import LongType, StringType, IntegerType
import dbldatagen as dg

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

    def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=-1,
                 **options):

        device_population = options.get("devicePopulation", 100000)

        assert tableName is None or tableName == "primary", "Invalid table name"

        if partitions < 0:
            partitions = 4

        shuffle_partitions_requested = partitions
        partitions_requested = partitions

        oldShufffleSetting = -1
        try:
            oldShufffleSetting = int(sparkSession.conf.get("spark.sql.shuffle.partitions"))
            sparkSession.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

            country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                             'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
            country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                               17]

            manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

            lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

            testDataSpec = (
                dg.DataGenerator(sparkSession, name="device_data_set", rows=rows,
                                 partitions=partitions_requested,
                                 randomSeedMethod='hash_fieldname')
                .withIdOutput()
                # we'll use hash of the base field to generate the ids to
                # avoid a simple incrementing sequence
                .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                            uniqueValues=device_population, omit=True, baseColumnType="hash")

                # note for format strings, we must use "%lx" not "%x" as the
                # underlying value is a long
                .withColumn("device_id", StringType(), format="0x%013x",
                            baseColumn="internal_device_id")

                # the device / user attributes will be the same for the same device id
                # so lets use the internal device id as the base column for these attribute
                .withColumn("country", StringType(), values=country_codes,
                            weights=country_weights,
                            baseColumn="internal_device_id")
                .withColumn("manufacturer", StringType(), values=manufacturers,
                            baseColumn="internal_device_id")

                # use omit = True if you don't want a column to appear in the final output
                # but just want to use it as part of generation of another column
                .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                            baseColumnType="hash", omit=True)
                .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                            baseColumn="device_id",
                            baseColumnType="hash", omit=True)

                .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                            baseColumn=["line", "model_ser"])
                .withColumn("event_type", StringType(),
                            values=["activation", "deactivation", "plan change",
                                    "telecoms activity", "internet activity", "device error"],
                            random=True)
                .withColumn("event_ts", "timestamp",
                            begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00",
                            interval="1 minute",
                            random=True)

                )
        finally:
            if oldShufffleSetting >= 0:
                sparkSession.conf.set("spark.sql.shuffle.partitions", oldShufffleSetting)

        return testDataSpec
