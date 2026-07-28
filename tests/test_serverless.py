import os
import re
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import FloatType, IntegerType, StringType

import dbldatagen as dg

_SERVERLESS_WARNING = (
    "Running on Databricks serverless compute: skipping arrow configuration. "
    "The `batchSize` option has no effect on serverless and will be ignored."
)


class TestSimulatedServerless:
    """Serverless operation and other forms of shared spark cloud operation often have restrictions on what
    features may be used.

    In this set of tests, we'll simulate some of the common restrictions found in Databricks serverless and shared
    environments to ensure that common operations still work.

    Serverless operations have some of the following restrictions:

    - Spark config settings cannot be written

    On serverless compute, dbldatagen detects the environment from the ``IS_SERVERLESS`` environment variable.``
    and skips the Arrow Spark configuration (which is already enabled there and cannot be modified), warning the
    user instead.
    """

    @pytest.fixture(scope="class")
    def serverless_spark(self):
        spark_session = dg.SparkSingleton.getLocalInstance("unit tests")
        old_set_method = spark_session.conf.set

        spark_session.conf.set = MagicMock(
            side_effect=ValueError("Setting value prohibited in simulated serverless env.")
        )

        with patch.dict(os.environ, {"IS_SERVERLESS": "TRUE"}):
            yield spark_session

        spark_session.conf.set = old_set_method

    def test_init_datagen_with_batch_size_warns_on_serverless(self, serverless_spark):
        with pytest.warns(UserWarning, match=f"^{re.escape(_SERVERLESS_WARNING)}$"):
            _fails = dg.DataGenerator(
                serverless_spark, name="test_serverless_pandas_udf", rows=100, partitions=4, batchSize=1000
            )
            serverless_spark.conf.set.assert_not_called()

    def test_pandas_udf_column_builds_and_warns_on_serverless(self, serverless_spark):
        with pytest.warns(UserWarning, match=f"^{re.escape(_SERVERLESS_WARNING)}$"):
            test_spec = (
                dg.DataGenerator(
                    serverless_spark, name="test_serverless_pandas_udf", rows=100, partitions=4, batchSize=1000
                )
                .withIdOutput()
                .withColumn("paras", text=dg.ILText(paragraphs=(1, 2), sentences=(2, 4), words=(3, 8)))
            )

        df = test_spec.build()

        assert df.count() == 100
        assert "paras" in df.columns
        # the prohibited Spark config write must never be attempted on serverless
        serverless_spark.conf.set.assert_not_called()

    def test_basic_data(self, serverless_spark):
        row_count = 1000 * 100
        column_count = 10
        test_spec = (
            dg.DataGenerator(serverless_spark, name="test_data_set1", rows=row_count, partitions=4)
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

        df = test_spec.build()
        assert df.count() == row_count

    @pytest.mark.parametrize(
        "provider_name, provider_options",
        [
            ("basic/user", {"rows": 50, "partitions": 4, "random": False, "dummyValues": 0}),
            ("basic/user", {"rows": 100, "partitions": -1, "random": True, "dummyValues": 0}),
        ],
    )
    def test_basic_user_table_retrieval(self, provider_name, provider_options, serverless_spark):
        ds = dg.Datasets(serverless_spark, provider_name).get(**provider_options)
        assert (
            ds is not None
        ), f"""expected to get dataset specification for provider `{provider_name}`
                                   with options: {provider_options} 
                                """
        df = ds.build()
        assert df.count() == provider_options.get("rows", 0)
