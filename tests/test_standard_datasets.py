import logging

import pytest
from pyspark.sql.types import IntegerType

import dbldatagen as dg
from dbldatagen.datasets import DatasetProvider, dataset_definition

spark = dg.SparkSingleton.getLocalInstance("unit tests")

__MISSING__ = "MISSING_PARAM"


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


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
    class SampleDatasetProviderBatch(DatasetProvider):
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

        def getTable(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
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

    @dataset_definition(name="test_providers/test_streaming", summary="Test Data Set2", autoRegister=True,
                        supportsStreaming=True)
    class SampleDatasetProviderStreaming(DatasetProvider):
        def __init__(self):
            pass

        def getTable(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
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

    def setup_log_capture(self, caplog_object):
        """ set up log capture fixture

        Sets up log capture fixture to only capture messages after setup and only
        capture warnings and errors

        """
        caplog_object.set_level(logging.WARNING)

        # clear messages from setup
        caplog_object.clear()

    def get_log_capture_warnings_and_errors(self, caplog_object, searchText=None):
        """
        gets count of errors containing specified text

        :param caplog_object: log capture object from fixture
        :param searchText: text to search for to include error or warning in count
        :return: count of errors containg text specified in `textFlag`
        """
        seed_column_warnings_and_errors = 0
        for r in caplog_object.records:
            if (r.levelname in ["WARNING", "ERROR"]) and (searchText is None) or (searchText in r.message):
                seed_column_warnings_and_errors += 1

        return seed_column_warnings_and_errors

    @pytest.fixture
    def getPrintOutput(self, capsys):
        return capsys.readouterr().out

    @pytest.fixture
    def dataset_definition1(self):
        return DatasetProvider.DatasetDefinition(
            name="test_dataset",
            tables=["table1", "table2"],
            primaryTable="table1",
            summary="Summary of the test dataset",
            description="Description of the test dataset",
            supportsStreaming=True,
            providerClass=DatasetProvider
        )

    # @pytest.mark.skip(reason="disabled for now")
    @pytest.mark.skip(reason="in progress")
    def test_datasets_bad_table_name(self):
        with pytest.raises(ValueError):
            ds = dg.Datasets(spark, name="test_providers/test_batch").get(table="blue")
            assert ds is not None

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

    def test_decorators1(self, mkTableSpec):
        import sys
        print("sys.versioninfo", sys.version_info)

        @dataset_definition
        class X1(DatasetProvider):

            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
                         **options):
                return mkTableSpec

        ds_definition = X1.getDatasetDefinition()
        print("ds_definition", ds_definition)
        assert ds_definition.name == "providers/X1"
        assert ds_definition.tables == [DatasetProvider.DEFAULT_TABLE_NAME]
        assert ds_definition.primaryTable == DatasetProvider.DEFAULT_TABLE_NAME
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False

        @dataset_definition(name="test/test", tables=["main"])
        class Y1(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
                         **options):
                return mkTableSpec

        ds_definition = Y1.getDatasetDefinition()
        assert ds_definition.name == "test/test"
        assert ds_definition.tables == ["main"]
        assert ds_definition.primaryTable == "main"
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False

    def test_decorators1a(self, mkTableSpec):
        @dataset_definition(name="test/test", tables=["main1"])
        class Y1a(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
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

        DatasetProvider.unregisterDataset(Y1a.getDatasetDefinition().name)
        assert Y1a.getDatasetDefinition().name not in DatasetProvider.getRegisteredDatasets()

    def test_decorators1b(self, mkTableSpec):
        @dataset_definition(description="a test description")
        class X1b(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
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

        DatasetProvider.registerDataset(X1b)
        assert X1b.getDatasetDefinition().name in DatasetProvider.getRegisteredDatasets()

        DatasetProvider.unregisterDataset(X1b.getDatasetDefinition().name)
        assert X1b.getDatasetDefinition().name not in DatasetProvider.getRegisteredDatasets()

    def test_bad_registration(self, mkTableSpec):
        @dataset_definition(description="a test description")
        class X1b(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
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

    @pytest.mark.parametrize("providerName, rows_requested, partitions_requested, random, dummy", [
        ("basic/user", 50, 4, False, 0),
        ("basic/user", __MISSING__, __MISSING__, __MISSING__, __MISSING__),
        ("basic/user", 100, -1, False, 0),
        ("basic/user", 5000, __MISSING__, __MISSING__, 4),
        ("basic/user", 100, -1, True, 0),
    ])
    def test_basic_table_retrieval(self, providerName, rows_requested, partitions_requested, random, dummy):

        dict_params = {}

        if rows_requested != __MISSING__:
            dict_params["rows"] = rows_requested
        if partitions_requested != __MISSING__:
            dict_params["partitions"] = partitions_requested
        if random != __MISSING__:
            dict_params["random"] = random
        if dummy != __MISSING__:
            dict_params["dummyValues"] = dummy

        print("retrieving dataset for options", dict_params)

        ds = dg.Datasets(spark, providerName).get(**dict_params)
        assert ds is not None, f"""expected to get dataset specification for provider `{providerName}`
                                   with options: {dict_params} 
                                """
        df = ds.build()

        assert df.count() == (
            DatasetProvider.DEFAULT_ROWS if rows_requested is __MISSING__ or rows_requested == -1 else rows_requested)

        if dummy is not __MISSING__ and dummy is not None and dummy > 0:
            assert "dummy_0" in df.columns

        if random and random is not __MISSING__:
            leadingRows = df.limit(100).collect()
            customer_ids = [r.customer_id for r in leadingRows]
            assert customer_ids != sorted(customer_ids)

    @pytest.mark.parametrize("providerClass, pattern",
                             [(SampleDatasetProviderBatch, __MISSING__),
                              (SampleDatasetProviderBatch, "test.*"),
                              (SampleDatasetProviderBatch, "test_providers/test_batch")])
    def test_listing(self, providerClass, pattern, capsys):
        print("listing datasets")

        if pattern == __MISSING__:
            dg.Datasets.list()
        else:
            dg.Datasets.list(pattern=pattern)

        print("done listing datasets")

        captured = capsys.readouterr().out
        print("Actual captured output:", captured)

        # check for the name of provider and the summary description:
        providerMetadata = providerClass.getDatasetDefinition()
        assert providerMetadata.name in captured, "Should have found provider name in the output"
        assert providerMetadata.summary in captured, "Should have found provider summary in output"

    def test_describe_basic_usr(self):
        # caplog fixture captures log content
        # self.setup_log_capture(caplog)

        print("listing datasets")
        dg.Datasets.describe("basic/user")
        print("done listing datasets")

        # check that there are no warnings or errors due to use of the overridden seed column
        # seed_column_warnings_and_errors = self.get_log_capture_warngings_and_errors(caplog, "listing")
        # assert seed_column_warnings_and_errors == 0, "Should not have error messages about seed column"

    @pytest.fixture
    def dataset_provider(self):
        class MyDatasetProvider(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
                         **options):
                return mkTableSpec

        return MyDatasetProvider()

    def test_get_table_raises_type_error(self, dataset_provider):
        with pytest.raises(TypeError):
            DatasetProvider().getTable(sparkSession=None)  # pylint: disable=abstract-class-instantiated

    def test_check_options_valid_options(self, dataset_provider):
        options = {"option1": "value1", "option2": "value2"}
        allowed_options = ["option1", "option2"]
        dataset_provider.checkOptions(options, allowed_options)  # This should not raise an exception

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