import pytest

import dbldatagen as dg


class TestSimulatedServerless:
    """Serverless operation and other forms of shared spark cloud operation often have restrictions on what
    features may be used.

    In this set of tests, we'll simulate some of the common restrictions found in Databricks serverless and shared
    environments to ensure that common operations still work.

    Serverless operations have some of the following restrictions:

    - Spark config settings cannot be written

    """

    @pytest.fixture(scope="class")
    def serverlessSpark(self):
        from unittest.mock import MagicMock

        sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")

        oldSetMethod = sparkSession.conf.set
        oldGetMethod = sparkSession.conf.get

        def mock_conf_set(*args, **kwargs):
            raise ValueError("Setting value prohibited in simulated serverless env.")

        def mock_conf_get(config_key, default=None):
            # Allow internal PySpark configuration calls that are needed for basic operation
            whitelisted_configs = {
                'spark.sql.stackTracesInDataFrameContext': '1',
                'spark.sql.execution.arrow.enabled': 'false',
                'spark.sql.execution.arrow.pyspark.enabled': 'false',
                'spark.python.sql.dataFrameDebugging.enabled': 'true',
                'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
            }
            if config_key in whitelisted_configs:
                try:
                    return oldGetMethod(config_key, whitelisted_configs[config_key])
                except:
                    return whitelisted_configs[config_key]
            else:
                raise ValueError("Getting value prohibited in simulated serverless env.")

        sparkSession.conf.set = MagicMock(side_effect=mock_conf_set)
        sparkSession.conf.get = MagicMock(side_effect=mock_conf_get)

        yield sparkSession

        sparkSession.conf.set = oldSetMethod
        sparkSession.conf.get = oldGetMethod

    def test_basic_data(self, serverlessSpark):
        from pyspark.sql.types import FloatType, IntegerType, StringType

        row_count = 1000 * 100
        column_count = 10
        testDataSpec = (
            dg.DataGenerator(serverlessSpark, name="test_data_set1", rows=row_count, partitions=4)
            .withIdOutput()
            .withColumn(
                "r",
                FloatType(),
                expr="floor(rand() * 350) * (86400 + 3600)",
                numColumns=column_count,
            )
            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
            .withColumn("code2", "integer", minValue=0, maxValue=10, random=True)
            .withColumn("code3", StringType(), values=["online", "offline", "unknown"])
            .withColumn("code4", StringType(), values=["a", "b", "c"], random=True, percentNulls=0.05)
            .withColumn("code5", "string", values=["a", "b", "c"], random=True, weights=[9, 1, 1])
        )

        testDataSpec.build()

    @pytest.mark.parametrize(
        "providerName, providerOptions",
        [
            ("basic/user", {"rows": 50, "partitions": 4, "random": False, "dummyValues": 0}),
            ("basic/user", {"rows": 100, "partitions": -1, "random": True, "dummyValues": 0}),
        ],
    )
    def test_basic_user_table_retrieval(self, providerName, providerOptions, serverlessSpark):
        ds = dg.Datasets(serverlessSpark, providerName).get(**providerOptions)
        assert (
            ds is not None
        ), f"""expected to get dataset specification for provider `{providerName}`
                                   with options: {providerOptions} 
                                """
        df = ds.build()

        assert df.count() >= 0
