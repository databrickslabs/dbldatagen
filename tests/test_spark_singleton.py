import pytest
import logging

import dbldatagen as dg


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestSparkSingleton:

    def test_basic_spark_instance(self, setupLogging):
        sparkSession = dg.SparkSingleton.getInstance()
        assert sparkSession is not None

    def test_local_spark_instance(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance(useAllCores=True)
        assert sparkSession is not None

    def test_local_spark_instance2(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance()
        assert sparkSession is not None
