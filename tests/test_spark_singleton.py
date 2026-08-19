import logging
import pytest

from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession
import dbldatagen as dg


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestSparkSingleton:

    def test_basic_spark_instance(self, setupLogging):
        sparkSession = dg.SparkSingleton.getInstance()
        assert sparkSession is not None

    def test_local_spark_instance1(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance(useAllCores=True)
        assert sparkSession is not None

    def test_local_spark_instance2(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance()
        assert sparkSession is not None

    def test_local_spark_instance3(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance(useAllCores=False)
        assert sparkSession is not None

    def test_get_local_instance_reuses_active_session_in_connect_mode(self, setupLogging):
        active_session = object()

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("dbldatagen.spark_singleton.is_spark_connect_mode", lambda: True)
            monkeypatch.setattr(dg.SparkSingleton, "getActiveSession", classmethod(lambda cls: active_session))

            assert dg.SparkSingleton.getLocalInstance() is active_session
