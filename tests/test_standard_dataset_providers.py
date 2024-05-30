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


class TestStandardDatasetProviders:
    @dataset_definition(name="test_providers/test_batch", summary="Test Data Set1", autoRegister=True,
                        tables=["green", "yellow", "red"], supportsStreaming=False)
    class TestDatasetBatch(DatasetProvider):
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
        def recordArgs(cls, *, table, options, rows, partitions ):
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
    class TestDatasetStreaming(DatasetProvider):
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

    # @pytest.mark.parametrize("rows, columns, expected_partitions", [
    #    (1000000, 10, 4),
    #    (5000000, 100, 12),
    #    (100, 2, 4),
    #    (1000_000_000, 10, 18),
    #    (5000_000_000, 30, 32)
    # ])
    # def test_auto_compute_partitions(self, dataset_provider, rows, columns, expected_partitions):
    #    partitions = dataset_provider.autoComputePartitions(rows, columns)
    #    assert partitions == expected_partitions
