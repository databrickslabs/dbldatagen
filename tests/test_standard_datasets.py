import pytest
from pyspark.sql.types import IntegerType

import dbldatagen as dg
from dbldatagen.datasets import DatasetProvider, dataset_definition

spark = dg.SparkSingleton.getLocalInstance("unit tests")

__MISSING__ = "MISSING_PARAM"


@pytest.fixture
def mkTableSpec():
    dataspec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000)
                .withIdOutput()
                .withColumn("code1", IntegerType(), min=100, max=200)
                .withColumn("code2", IntegerType(), min=0, max=10)
                )
    return dataspec


class TestStandardDatasetsFramework:
    # Define some dummy providers - we will use these to check if they are found by
    # the listing and describe methods etc.
    @dataset_definition(name="test_providers/test_batch", summary="Test Data Set1", autoRegister=True,
                        tables=["green", "yellow", "red"], supportsStreaming=False)
    class SampleDatasetProviderBatch(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
        def __init__(self):
            pass

        lastTableRetrieved = None
        lastOptionsUsed = None
        lastRowsRequested = None
        lastPartitionsRequested = None

        @classmethod
        def clearRecordedArgs(cls):
            cls.lastTableRetrieved = None
            cls.lastOptionsUsed = None
            cls.lastRowsRequested = None
            cls.lastPartitionsRequested = None

        @classmethod
        def recordArgs(cls, *, table, options, rows, partitions):
            cls.lastTableRetrieved = table
            cls.lastOptionsUsed = options
            cls.lastRowsRequested = rows
            cls.lastPartitionsRequested = partitions

        @DatasetProvider.allowed_options(options=["random", "dummyValues"])
        def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                              **options):
            generateRandom = options.get("random", True)
            dummyValues = options.get("dummyValues", 0)

            ds = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, seedMethod='hash_fieldname')
                  .withColumn("code1", "int", min=100, max=200)
                  .withColumn("code2", "int", min=0, max=10)
                  .withColumn("code3", "string", values=['a', 'b', 'c'])
                  .withColumn("code4", "string", values=['a', 'b', 'c'], random=generateRandom)
                  .withColumn("code5", "string", values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                  )
            if dummyValues > 0:
                ds = ds.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                   minValue=1, maxValue=self.MAX_LONG)

            return ds

    @dataset_definition(name="test_providers/test_streaming", summary="Test Data Set2", autoRegister=True,
                        supportsStreaming=True)
    class SampleDatasetProviderStreaming(DatasetProvider):
        def __init__(self):
            pass

        def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                              **options):
            ds = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                   seedMethod='hash_fieldname')
                  .withColumn("code1", "int", min=100, max=200)
                  .withColumn("code2", "int", min=0, max=10)
                  .withColumn("code3", "string", values=['a', 'b', 'c'])
                  .withColumn("code4", "string", values=['a', 'b', 'c'], random=True)
                  .withColumn("code5", "string", values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                  )
            return ds

    @pytest.fixture
    def dataset_definition1(self):
        return DatasetProvider.DatasetDefinition(
            name="test_dataset",
            tables=["table1", "table2"],
            primaryTable="table1",
            summary="Summary of the test dataset",
            description="Description of the test dataset",
            supportsStreaming=True,
            providerClass=DatasetProvider,
            associatedDatasets=None
        )

    def test_datasets_bad_table_name(self):
        with pytest.raises(ValueError):
            ds = dg.Datasets(spark, name="test_providers/test_batch").get(table="blue")
            assert ds is not None

    def test_datasets_unsupported_streaming(self):
        with pytest.raises(ValueError):
            ds = dg.Datasets(spark, name="test_providers/test_batch", streaming=True).get()
            assert ds is not None

    def test_datasets_unsupported_provider(self):
        with pytest.raises(ValueError):
            ds = dg.Datasets(spark, name="test_providers/unknown_provider").get()
            assert ds is not None

    def test_datasets_bad_option(self):
        with pytest.raises(ValueError):
            ds = dg.Datasets(spark, name="test_providers/test_batch").get(badOption=True)
            assert ds is not None

    def test_datasets_bad_associated_dataset_name(self):
        with pytest.raises(ValueError):
            ds = dg.Datasets(spark, name="test_providers/test_batch").getAssociatedDataset(table="blue")
            assert ds is not None

    def test_datasets_bad_decorator_usage(self):
        with pytest.raises(TypeError):
            @dataset_definition(name="test_providers/badly_applied_decorator", summary="Bad Usage", autoRegister=True)
            def by_two(x):
                return x * 2

    def test_different_retrieval_mechanisms_simple(self):
        testSpec1 = dg.Datasets(spark, "test_providers/test_batch").get(table="yellow")
        assert testSpec1 is not None

        testSpec2 = dg.Datasets(spark, "test_providers/test_batch").get(rows=10)
        assert testSpec2 is not None

    def test_different_retrieval_mechanisms_alt1(self):
        testSpec3 = dg.Datasets(spark, "test_providers/test_batch").yellow()
        assert testSpec3 is not None

    def test_different_retrieval_mechanisms_alt2(self):
        testSpec4 = dg.Datasets(spark, "test_providers").test_batch.yellow()
        assert testSpec4 is not None

    def test_different_retrieval_mechanisms_alt3(self):
        testSpec5 = dg.Datasets(spark, "test_providers").test_batch()
        assert testSpec5 is not None

    def test_dataset_definition_attributes(self, dataset_definition1):
        assert dataset_definition1.name == "test_dataset"
        assert dataset_definition1.tables == ["table1", "table2"]
        assert dataset_definition1.primaryTable == "table1"
        assert dataset_definition1.summary == "Summary of the test dataset"
        assert dataset_definition1.description == "Description of the test dataset"
        assert dataset_definition1.supportsStreaming is True
        assert dataset_definition1.providerClass == DatasetProvider
        assert not dataset_definition1.associatedDatasets

    def test_decorators1(self, mkTableSpec):
        import sys
        print("sys.versioninfo", sys.version_info)

        @dataset_definition
        class X1(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):

            def getTableGenerator(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, **options):
                return mkTableSpec

        ds_definition = X1.getDatasetDefinition()
        print("ds_definition", ds_definition)
        assert ds_definition.name == "providers/X1"
        assert ds_definition.tables == [DatasetProvider.DEFAULT_TABLE_NAME]
        assert ds_definition.primaryTable == DatasetProvider.DEFAULT_TABLE_NAME
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False

        assert X1.getDatasetTables() is not None, "should have default table set"

        @dataset_definition(name="test/test", tables=["main"])
        class Y1(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
            def getTableGenerator(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, **options):
                return mkTableSpec

        ds_definition = Y1.getDatasetDefinition()
        assert ds_definition.name == "test/test"
        assert ds_definition.tables == ["main"]
        assert ds_definition.primaryTable == "main"
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False
        assert Y1.getDatasetTables() is not None, "should have default table set"

    def test_decorators1a(self, mkTableSpec):
        @dataset_definition(name="test/test", tables=["main1"])
        class Y1a(DatasetProvider):
            def getTableGenerator(self, sparkSession, *, tableName=None, rows=1000000, partitions=4,
                                  autoSizePartitions=False,
                                  **options):
                return mkTableSpec

        ds_definition = Y1a.getDatasetDefinition()
        assert ds_definition.name == "test/test"
        assert ds_definition.tables == ["main1"]
        assert ds_definition.primaryTable == "main1"
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False

        print("description\n", ds_definition.description)

        DatasetProvider.registerDataset(Y1a)
        assert Y1a.getDatasetDefinition().name in DatasetProvider.getRegisteredDatasets()
        assert Y1a.getDatasetTables() is not None

        DatasetProvider.unregisterDataset(Y1a.getDatasetDefinition().name)
        assert Y1a.getDatasetDefinition().name not in DatasetProvider.getRegisteredDatasets()
        assert Y1a.getDatasetTables() is not None

    def test_decorators1b(self, mkTableSpec):
        @dataset_definition(description="a test description")
        class X1b(DatasetProvider):
            def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                                  description="a test description",
                                  **options):
                return mkTableSpec

        ds_definition = X1b.getDatasetDefinition()
        assert ds_definition.name == "providers/X1b"
        assert ds_definition.tables == [DatasetProvider.DEFAULT_TABLE_NAME]
        assert X1b.getDatasetTables() == [DatasetProvider.DEFAULT_TABLE_NAME]
        assert ds_definition.primaryTable == DatasetProvider.DEFAULT_TABLE_NAME
        assert ds_definition.summary is not None
        assert ds_definition.description == "a test description"
        assert ds_definition.supportsStreaming is False

        print("description\n", ds_definition.description)

        registeredDatasetsVersion = DatasetProvider.getRegisteredDatasetsVersion()

        DatasetProvider.registerDataset(X1b)
        assert X1b.getDatasetDefinition().name in DatasetProvider.getRegisteredDatasets()

        registeredDatasetsVersion2 = DatasetProvider.getRegisteredDatasetsVersion()
        assert registeredDatasetsVersion2 != registeredDatasetsVersion

        DatasetProvider.unregisterDataset(X1b.getDatasetDefinition().name)
        assert X1b.getDatasetDefinition().name not in DatasetProvider.getRegisteredDatasets()

        registeredDatasetsVersion3 = DatasetProvider.getRegisteredDatasetsVersion()
        assert registeredDatasetsVersion3 != registeredDatasetsVersion2

    def test_bad_registration(self, mkTableSpec):
        @dataset_definition(description="a test description")
        class X1b(DatasetProvider):
            def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                                  description="a test description",
                                  **options):
                return mkTableSpec

        with pytest.raises(ValueError):
            DatasetProvider.registerDataset(None)

        with pytest.raises(ValueError):
            DatasetProvider.registerDataset(X1b.getDatasetDefinition())

        with pytest.raises(ValueError):
            DatasetProvider.registerDataset(X1b)
            DatasetProvider.registerDataset(X1b)

    def test_invalid_decorator_use(self):
        with pytest.raises(TypeError):
            @dataset_definition
            def my_function(x):
                return x

    def test_invalid_decorator_use2(self):
        with pytest.raises(TypeError):
            @dataset_definition(name="test/bad_decorator1")
            def my_function(x):
                return x

    @pytest.mark.parametrize("providerClass, options",
                             [(SampleDatasetProviderBatch, {}),
                              (SampleDatasetProviderBatch, {"pattern": "test.*"}),
                              (SampleDatasetProviderBatch, {"pattern": "test_providers/test_batch"}),
                              (SampleDatasetProviderBatch, {"supportsStreaming": False}),
                              (SampleDatasetProviderStreaming, {"supportsStreaming": True})
                              ])
    def test_listing(self, providerClass, options, capsys):
        print("listing datasets")

        dg.Datasets.list(**options)

        print("done listing datasets")

        captured = capsys.readouterr().out
        print("Actual captured output:", captured)

        # check for the name of provider and the summary description:
        providerMetadata = providerClass.getDatasetDefinition()
        assert providerMetadata.name in captured, "Should have found provider name in the output"
        assert providerMetadata.summary in captured, "Should have found provider summary in output"

    def test_describe_basic_usr(self, capsys):
        print("listing datasets")
        dg.Datasets.describe("basic/user")
        print("done listing datasets")

        captured = capsys.readouterr().out
        print("Actual captured output:", captured)

    @pytest.fixture
    def dataset_provider(self):
        class MyDatasetProvider(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
            def getTableGenerator(self, sparkSession, *, tableName=None, rows=1000000, partitions=4,
                                  autoSizePartitions=False,
                                  **options):
                return mkTableSpec

        return MyDatasetProvider()

    def test_get_table_raises_type_error(self, dataset_provider):
        with pytest.raises(TypeError):
            DatasetProvider().getTableGenerator(sparkSession=None)  # pylint: disable=abstract-class-instantiated

    def test_check_options_valid_options(self, dataset_provider):
        options = {"option1": "value1", "option2": "value2"}
        allowed_options = ["option1", "option2"]
        dataset_provider.checkOptions(options, allowed_options)  # This should not raise an exception

        # check that dataset provider without decorator still supports getDatasetTables
        assert dataset_provider.getDatasetTables() is not None

    def test_check_options_invalid_options(self, dataset_provider):
        options = {"option1": "value1", "option2": "value2"}
        allowed_options = ["option1"]
        with pytest.raises(AssertionError):
            dataset_provider.checkOptions(options, allowed_options)

    @pytest.mark.parametrize("rows, columns, expected_partitions", [
        (1000000, 10, 4),
        (5000000, 100, 12),
        (100, 2, 4),
        (1000_000_000, 10, 18),
        (5000_000_000, 30, 32)
    ])
    def test_auto_compute_partitions(self, dataset_provider, rows, columns, expected_partitions):
        partitions = dataset_provider.autoComputePartitions(rows, columns)
        assert partitions == expected_partitions
