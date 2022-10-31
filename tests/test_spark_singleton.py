import pytest
import logging

import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestSparkSingleton:

    @pytest.fixture()
    def getSparkSession(self):
        sparkSession = dg.SparkSingleton.getLocalInstance()
        yield sparkSession
        sparkSession.stop()

    def test_basic_spark_instance(self, setupLogging):
        sparkSession = dg.SparkSingleton.getInstance()
        try:
            assert sparkSession is not None
        finally:
            sparkSession.stop()

    def test_local_spark_instance1(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance(useAllCores=True)
        try:
            assert sparkSession is not None
        finally:
            sparkSession.stop()

    def test_local_spark_instance2(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance()

        try:
            assert sparkSession is not None
        finally:
            sparkSession.stop()

    def test_local_spark_instance3(self, setupLogging):
        sparkSession = dg.SparkSingleton.getLocalInstance(useAllCores=False)

        try:
            assert sparkSession is not None
        finally:
            sparkSession.stop()

    def test_inactive_spark_session(self, setupLogging):
        with pytest.raises(dg.DataGenError):
            dataspec = (dg.DataGenerator( name="test_data_set1", rows=10000, seedMethod='hash_fieldname')
                      .withIdOutput()
                      .withColumn("code1", IntegerType(), min=100, max=200)
                      .withColumn("code2", IntegerType(), min=0, max=10)
                      .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                      .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                      .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                      )

            assert dataspec is not None

    def test_active_spark_session(self, setupLogging, getSparkSession):
        NUM_ROWS = 10000
        dataspec = (dg.DataGenerator( name="test_data_set1", rows=NUM_ROWS, seedMethod='hash_fieldname')
                  .withIdOutput()
                  .withColumn("code1", IntegerType(), min=100, max=200)
                  .withColumn("code2", IntegerType(), min=0, max=10)
                  .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                  .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                  .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                  )

        assert dataspec is not None

        dfData = dataspec.build()
        assert dfData.count() == NUM_ROWS

