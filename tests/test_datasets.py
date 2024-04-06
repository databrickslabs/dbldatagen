import logging

import pytest
from pyspark.sql.types import IntegerType

import dbldatagen as dg
from dbldatagen.datasets import DatasetProvider, dataset_definition

spark = dg.SparkSingleton.getLocalInstance("unit tests")


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


class TestDatasets:

    def setup_log_capture(self, caplog_object):
        """ set up log capture fixture

        Sets up log capture fixture to only capture messages after setup and only
        capture warnings and errors

        """
        caplog_object.set_level(logging.WARNING)

        # clear messages from setup
        caplog_object.clear()

    def get_log_capture_warngings_and_errors(self, caplog_object, searchText=None):
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
        assert ds_definition.tables == ["primary"]
        assert ds_definition.primaryTable == "primary"
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
        @dataset_definition(name="test/test", tables=["main"])
        class Y1a(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
                         **options):
                return mkTableSpec

        ds_definition = Y1a.getDatasetDefinition()
        assert ds_definition.name == "test/test"
        assert ds_definition.tables == ["main"]
        assert ds_definition.primaryTable == "main"
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False

        print("description\n", ds_definition.description)

    def test_decorators1b(self, mkTableSpec):
        @dataset_definition
        class X1b(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
                         **options):
                return mkTableSpec

        ds_definition = X1b.getDatasetDefinition()
        assert ds_definition.name == "providers/X1b"
        assert ds_definition.tables == ["primary"]
        assert ds_definition.primaryTable == "primary"
        assert ds_definition.summary is not None
        assert ds_definition.description is not None
        assert ds_definition.supportsStreaming is False

        print("description\n", ds_definition.description)

    def test_basic(self):
        ds = dg.Datasets(spark, "basic/user").get()
        assert ds is not None
        df = ds.build()
        assert df.count() == dg.Datasets.DEFAULT_ROWS

    @pytest.mark.skip(reason="to be debugged")
    def test_basic2(self):
        ds = dg.Datasets(spark, "basic/user").get(dummyValues=5, random=True)
        assert ds is not None
        df = ds.build()
        assert df.count() == dg.Datasets.DEFAULT_ROWS
        df.show()

    def test_basic_iot(self):
        ds = dg.Datasets(spark, "basic/iot").get()
        assert ds is not None
        df = ds.build()
        assert df.count() == dg.Datasets.DEFAULT_ROWS
        df.show()

    def test_listing(self):
        # caplog fixture captures log content
        # self.setup_log_capture(caplog)

        print("listing datasets")
        dg.Datasets.list()
        print("done listing datasets")

        # check that there are no warnings or errors due to use of the overridden seed column
        # seed_column_warnings_and_errors = self.get_log_capture_warngings_and_errors(caplog, "listing")
        # assert seed_column_warnings_and_errors == 0, "Should not have error messages about seed column"

    def test_listing2(self):
        # caplog fixture captures log content
        # self.setup_log_capture(caplog)

        print("listing datasets matching 'basic.*'")
        dg.Datasets.list(pattern="basic.*")
        print("done listing datasets")

        # check that there are no warnings or errors due to use of the overridden seed column
        # seed_column_warnings_and_errors = self.get_log_capture_warngings_and_errors(caplog, "listing")
        # assert seed_column_warnings_and_errors == 0, "Should not have error messages about seed column"

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
    def sample_navigator(self):
        tree = dg.Datasets.DatasetNavigator()
        tree.insert_path('X.a.b')
        tree.set_table('X.a.b', 'foo', 'Result of foo()')
        tree.set_table('X.a.b', 'bar', 'Result of bar()')
        tree.set_table('X.a.b', 'cod', 'Result of cod()')
        return tree

    @pytest.mark.parametrize("name, providerClass, useProviderArg", [
        ('a', None, True),
        ('x.y.z', None, True),
        ('a', None, False),
        ('x.y.z', None, False),
        ('a', DatasetProvider, True),
        ('x.y.z', DatasetProvider, True),
    ])
    def test_datasets_navigator_node(self, name, providerClass, useProviderArg):
        if useProviderArg:
            node = dg.Datasets.TreeNode(name, providerClass)
        else:
            node = dg.Datasets.TreeNode(name)
        assert node.nodeName == name
        assert not node.children
        assert node.providerClass is providerClass

    @pytest.mark.skip(reason="work in progress")
    def test_table_lookup(self, sample_navigator):
        assert sample_navigator.X.a.b.foo._table == 'Result of foo()'
        assert sample_navigator.X.a.b.bar == 'Result of bar()'
        assert sample_navigator.X.a.b.cod == 'Result of cod()'

    @pytest.mark.skip(reason="work in progress")
    def test_invalid_table_lookup(self, sample_navigator):
        assert sample_navigator.X.a.b.invalid is None
        assert sample_navigator.X.a.invalid.cod is None
        assert sample_navigator.X.invalid.b.cod is None

    @pytest.mark.skip(reason="work in progress")
    def test_invalid_path_lookup(self, sample_navigator):
        assert sample_navigator.X.invalid.path is None
        assert sample_navigator.Y.a.b.foo is None
        assert sample_navigator.Z.a.b.cod is None

    @pytest.fixture
    def dataset_provider(self):
        class MyDatasetProvider(DatasetProvider):
            def getTable(self, sparkSession, *, tableName=None, rows=1000000, partitions=4, autoSizePartitions=False,
                         **options):
                return mkTableSpec

        return MyDatasetProvider()

    def test_get_table_raises_not_implemented_error(self, dataset_provider):
        with pytest.raises(NotImplementedError):
            DatasetProvider().getTable(sparkSession=None)

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
