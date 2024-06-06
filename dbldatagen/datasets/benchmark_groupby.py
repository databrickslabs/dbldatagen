from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="benchmark/groupby",
                    summary="Benchmarking dataset for GROUP BY queries in various database systems",
                    autoRegister=True,
                    supportsStreaming=True)
class BenchmarkGroupByProvider(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
    """
    Grouping Benchmark Dataset
    ==========================

    This is a benchmarking dataset for evaluating groupBy operations on columns of different type and
    cardinality.

    It takes the following options when retrieving the table:
        - random: if True, generates random data
        - rows : number of rows to generate
        - partitions: number of partitions to use
        - groups: number of groups within the dataset
        - percentNulls: percentage of nulls within the non-base columns

    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    Note that this datset does not use any features that would prevent it from being used as a source for a
    streaming dataframe, and so the flag `supportsStreaming` is set to True.

    """
    MAX_LONG = 9223372036854775807
    DEFAULT_NUM_GROUPS = 100
    DEFAULT_PCT_NULLS = 0.0
    COLUMN_COUNT = 12
    ALLOWED_OPTIONS = ["groups", "percentNulls", "rows", "partitions", "tableName", "random"]

    @DatasetProvider.allowed_options(options=ALLOWED_OPTIONS)
    def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                 **options):
        import dbldatagen as dg

        generateRandom = options.get("random", False)
        groups = options.get("groups", self.DEFAULT_NUM_GROUPS)
        percentNulls = options.get("percentNulls", self.DEFAULT_PCT_NULLS)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        if rows is None or rows < 0:
            rows = DatasetProvider.DEFAULT_ROWS
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, self.COLUMN_COUNT)
        try:
            groups = int(groups)
        except Exception as e:
            raise ValueError("groups must be a value of type 'int'") from e
        if groups <= 0:
            groups = 100
            raise UserWarning(f"Received an invalid groups value; Setting to {groups}")
        if rows < groups:
            groups = int(rows / 1000)
            raise UserWarning(f"Received more groups than rows; Setting the number of groups to {groups}")

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        df_spec = (
             dg.DataGenerator(sparkSession=sparkSession, name="test_data_set1", rows=rows,
                              partitions=4, randomSeedMethod="hash_fieldname")
            .withColumn("base1", "integer", minValue=1, maxValue=groups, random=generateRandom, omit=True)
            .withColumn("base2", "integer", minValue=1, maxValue=groups, random=generateRandom, omit=True)
            .withColumn("base3", "integer", minValue=1, maxValue=int(rows / groups), random=generateRandom, omit=True)
            .withColumn("id1", "string", baseColumn="base1", format="id%03d", percentNulls=percentNulls)
            .withColumn("id2", "string", baseColumn="base2", format="id%03d", percentNulls=percentNulls)
            .withColumn("id3", "string", baseColumn="base3", format="id%010d", percentNulls=percentNulls)
            .withColumn("id4", "integer", minValue=1, maxValue=groups, random=generateRandom, percentNulls=percentNulls)
            .withColumn("id5", "integer", minValue=1, maxValue=groups, random=generateRandom, percentNulls=percentNulls)
            .withColumn("id6", "integer", minValue=1, maxValue=int(rows / groups), random=generateRandom,
                            percentNulls=percentNulls)
            .withColumn("v1", "integer", minValue=1, maxValue=5, random=generateRandom, percentNulls=percentNulls)
            .withColumn("v2", "integer", minValue=1, maxValue=15, random=generateRandom, percentNulls=percentNulls)
            .withColumn("v3", "decimal(9,6)", minValue=0.0, maxValue=100.0,
                        step=1e-6, random=generateRandom, percentNulls=percentNulls)
        )

        return df_spec
