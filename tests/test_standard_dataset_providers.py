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

    @pytest.mark.parametrize("providerName, rows_requested, partitions_requested, random, dummy", [
        ("basic/user", 50, 4, False, 0),
        ("basic/user", __MISSING__, __MISSING__, __MISSING__, __MISSING__),
        ("basic/user", 100, -1, False, 0),
        ("basic/user", 5000, __MISSING__, __MISSING__, 4),
        ("basic/user", 100, -1, True, 0),
    ])
    def test_standard_table_retrieval(self, providerName, rows_requested, partitions_requested, random, dummy):

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

