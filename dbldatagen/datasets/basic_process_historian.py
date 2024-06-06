from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="basic/process_historian",
                    summary="Basic Historian Data for Process Manufacturing",
                    autoRegister=True,
                    supportsStreaming=True)
class BasicProcessHistorianProvider(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
    """
    Basic Process Historian Dataset
    ===============================

    This is a basic process historian data set with fields like plant ID, device ID, tag name, timestamp,
    value, and units of measure.

    It takes the following options when retrieving the table:
        - random: if True, generates random data
        - rows : number of rows to generate
        - partitions: number of partitions to use
        - numDevices: number of unique device IDs
        - numPlants: number of unique plant IDs
        - numTags: number of unique tag names
        - startTimestamp: earliest timestamp for IOT time series data
        - endTimestamp: latest timestamp for IOT time series data
        - dataQualityRatios: dictionary with `pctQuestionable`, `pctSubstituted`, and `pctAnnotated`
            values corresponding to the percentage of values to be marked `is_questionable`, `is_substituted`,
            or `is_annotated`, respectively

    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    Note that this datset does not use any features that would prevent it from being used as a source for a
    streaming dataframe, and so the flag `supportsStreaming` is set to True.

    """
    MIN_DEVICE_ID = 0x100000000
    MAX_DEVICE_ID = 9223372036854775807
    MIN_PROPERTY_VALUE = 50.0
    MAX_PROPERTY_VALUE = 60.0
    DEFAULT_NUM_DEVICES = 10000
    DEFAULT_NUM_PLANTS = 100
    DEFAULT_NUM_TAGS = 10
    DEFAULT_START_TIMESTAMP = "2024-01-01 00:00:00"
    DEFAULT_END_TIMESTAMP = "2024-02-01 00:00:00"
    COLUMN_COUNT = 10
    ALLOWED_OPTIONS = [
        "numDevices",
        "numPlants",
        "numTags", 
        "startTimestamp", 
        "endTimestamp", 
        "dataQualityRatios",
        "random"
    ]

    @DatasetProvider.allowed_options(options=ALLOWED_OPTIONS)
    def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1, **options):
        import dbldatagen as dg  # import locally to avoid circular imports
        import numpy as np

        generateRandom = options.get("random", False)
        numDevices = options.get("numDevices", self.DEFAULT_NUM_DEVICES)
        numPlants = options.get("numPlants", self.DEFAULT_NUM_PLANTS)
        numTags = options.get("numTags", self.DEFAULT_NUM_TAGS)
        startTimestamp = options.get("startTimestamp", self.DEFAULT_START_TIMESTAMP)
        endTimestamp = options.get("endTimestamp", self.DEFAULT_END_TIMESTAMP)
        dataQualityRatios = options.get("dataQualityRatios", None)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        if rows is None or rows < 0:
            rows = DatasetProvider.DEFAULT_ROWS
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, self.COLUMN_COUNT)

        tag_names = [f"HEX-{str(j).zfill(int(np.ceil(np.log10(numTags))))}_INLET_TMP" for j in range(numTags)]
        plant_ids = [f"PLANT-{str(j).zfill(int(np.ceil(np.log10(numPlants))))}" for j in range(numPlants)]
        testDataSpec = (
            dg.DataGenerator(sparkSession, name="process_historian_data", rows=rows,
                             partitions=partitions,
                             randomSeedMethod="hash_fieldname")
            .withColumn("internal_device_id", "long", minValue=self.MIN_DEVICE_ID, maxValue=self.MAX_DEVICE_ID,
                            uniqueValues=numDevices, omit=True, baseColumnType="hash")
            .withColumn("device_id", "string", format="0x%09x", baseColumn="internal_device_id")
            .withColumn("plant_id", "string", values=plant_ids, baseColumn="internal_device_id")
            .withColumn("tag_name", "string", values=tag_names, baseColumn="internal_device_id")
            .withColumn("ts", "timestamp", begin=startTimestamp, end=endTimestamp, 
                            interval="1 second", random=generateRandom)
            .withColumn("value", "float", minValue=self.MIN_PROPERTY_VALUE, maxValue=self.MAX_PROPERTY_VALUE,
                             step=1e-3, random=generateRandom)
            .withColumn("engineering_units", "string", expr="'Deg.F'")
        )
        # Add the data quality columns if they were provided
        if dataQualityRatios is not None:
            if "pctQuestionable" in dataQualityRatios.keys():
                testDataSpec = testDataSpec.withColumn(
                    "is_questionable", "boolean",
                    expr=f"rand() < {dataQualityRatios['pctQuestionable']}"
                )
            if "pctSubstituted" in dataQualityRatios.keys():
                testDataSpec = testDataSpec.withColumn(
                    "is_substituted", "boolean",
                    expr=f"rand() < {dataQualityRatios['pctSubstituted']}"
                )
            if "pctAnnotated" in dataQualityRatios.keys():
                testDataSpec = testDataSpec.withColumn(
                    "is_annotated", "boolean",
                    expr=f"rand() < {dataQualityRatios['pctAnnotated']}"
                )

        return testDataSpec
