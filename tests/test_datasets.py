import logging

import pytest
from pyspark.sql.types import IntegerType, StringType, FloatType

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

    def test_dataset_definition(self):
        ds_definition = DatasetProvider.DatasetDefinition(name="test/test",
                                                          primaryTable="primary1",
                                                          tables=["primary1"],
                                                          supportsStreaming=True,
                                                          summary="sample data set",
                                                          description="A test description")

        # assert for each property of the ds_definition
        assert ds_definition.name == "test/test"
        assert ds_definition.primaryTable == "primary1"
        assert ds_definition.tables == ["primary1"]
        assert ds_definition.supportsStreaming
        assert ds_definition.summary == "sample data set"
        assert ds_definition.description == "A test description"

    def test_decorators1(self, mkTableSpec):
        import sys
        print("sys.versioninfo", sys.version_info)

        @dataset_definition
        class X1(DatasetProvider):
            def __init__(self):
                super().__init__()

            def getTable(self, tableName, rows=1000000, partitions=4, **options):
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
            def getTable(self, tableName, rows=1000000, partitions=4, **options):
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
            def getTable(self, tableName, rows=1000000, partitions=4, **options):
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
            def __init__(self):
                super().__init__()

            def getTable(self, tableName, rows=1000000, partitions=4, **options):
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

        print("listing datasets")
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

    def test_alt_seed_column(self, caplog):
        # caplog fixture captures log content
        self.setup_log_capture(caplog)

        dgspec = (dg.DataGenerator(sparkSession=spark, name="alt_data_set", rows=10000,
                                   partitions=4, seedMethod='hash_fieldname', verbose=True,
                                   seedColumnName="_id")
                  .withIdOutput()
                  .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                              numColumns=4)
                  .withColumn("code1", IntegerType(), min=100, max=200)
                  .withColumn("code2", IntegerType(), min=0, max=10)
                  .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                  .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                  .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                  )

        fieldsFromGenerator = set(dgspec.getOutputColumnNames())

        df_testdata = dgspec.build()

        fieldsFromSchema = set([fld.name for fld in df_testdata.schema.fields])

        assert fieldsFromGenerator == fieldsFromSchema

        assert "_id" == dgspec.seedColumnName
        assert "_id" in fieldsFromGenerator
        assert "id" not in fieldsFromGenerator

        ds_copy1 = dgspec.clone()
        fieldsFromGeneratorClone = set(ds_copy1.getOutputColumnNames())

        assert "_id" in fieldsFromGeneratorClone
        assert "id" not in fieldsFromGeneratorClone

        # check that there are no warnings or errors due to use of the overridden seed column
        seed_column_warnings_and_errors = self.get_log_capture_warngings_and_errors(caplog, "seed")
        assert seed_column_warnings_and_errors == 0, "Should not have error messages about seed column"
