import logging
from unittest.mock import Mock, PropertyMock

import pytest
import dbldatagen as dg


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestSharedEnv:
    """Tests to simulate testing under a Unity Catalog shared environment. In a Unity Catalog shared environment with
    the 14.x versions of the Databricks runtime, the sparkSession object does not support use of the sparkContext
    attribute to get the default parallelism. In this case, we want to catch errors and return a default of
    200 as the default number of partitions. This is the same as the default parallelism in many versions of Spark.


    """
    SMALL_ROW_COUNT = 100000
    COLUMN_COUNT = 10

    @pytest.fixture(scope="class")
    def sparkSession(self, setupLogging):
        spark = dg.SparkSingleton.getLocalInstance("unit tests")
        return spark

    @pytest.fixture(scope="class")
    def sharedSparkSession(self, setupLogging):
        spark = Mock(wraps=dg.SparkSingleton.getLocalInstance("unit tests"))
        del spark.sparkContext
        return spark

    @pytest.fixture(scope="class")
    def sparkSessionNullContext(self, setupLogging):

        class MockSparkSession:
            def __init__(self):
                self.sparkContext = None

        spark = MockSparkSession()
        return spark

    def test_getDefaultParallelism(self, sparkSession):
        """Test that the default parallelism is returned when the sparkSession object supports use of the
        sparkContext attribute to get the default parallelism.

        :param sparkSession: The sparkSession object to use for the test.
        """
        defaultParallelism = dg.DataGenerator._getDefaultSparkParallelism(sparkSession)
        assert defaultParallelism == sparkSession.sparkContext.defaultParallelism

    def test_getSharedDefaultParallelism(self, sharedSparkSession):
        """Test that the default parallelism is returned when the sparkSession object supports use of the
        sparkContext attribute to get the default parallelism, but that a constant is return when the `sparkContext`
        attribute is not available.
        """
        defaultParallelism = dg.DataGenerator._getDefaultSparkParallelism(sharedSparkSession)
        assert defaultParallelism == dg.SPARK_DEFAULT_PARALLELISM

    def test_getNullContextDefaultParallelism(self, sparkSessionNullContext):
        """Test that the default parallelism is returned when the sparkSession object supports use of the
        sparkContext attribute to get the default parallelism.

        :param sparkSession: The sparkSession object to use for the test.
        """
        defaultParallelism = dg.DataGenerator._getDefaultSparkParallelism(sparkSessionNullContext)
        assert defaultParallelism == dg.SPARK_DEFAULT_PARALLELISM

    def test_mocked_shared_session1(self, sharedSparkSession):
        # validate that accessing the sparkContext on the shared spark session raises an exception
        with pytest.raises(Exception) as excinfo:
            context = sharedSparkSession.sparkContext

        assert "sparkContext" in str(excinfo.value)

    def test_null_context_spark_session(self, sparkSessionNullContext):
        # validate that accessing the sparkContext on the shared spark session raises an exception
        context = sparkSessionNullContext.sparkContext
        assert context is None
