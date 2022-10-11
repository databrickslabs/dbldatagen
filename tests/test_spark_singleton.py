import pytest
import os

import dbldatagen as dg

@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestSparkSingleton:

    def test_basic_spark_instance(self):
        sparkSession = dg.SparkSingleton.getInstance()

        assert sparkSession is not None

    def test_local_spark_instance(self):
        sparkSession = dg.SparkSingleton.getLocalInstance(useAllCores=True)

        assert sparkSession is not None

        print(sparkSession.sparkContext.defaultParallelism)

    def test_local_spark_instance2(self):
        sparkSession = dg.SparkSingleton.getLocalInstance()

        assert sparkSession is not None

        print(sparkSession.sparkContext.defaultParallelism)

        r1 = sparkSession.range(100000000, numPartitions=sparkSession.sparkContext.defaultParallelism-1)
        print(r1.where("id % 2 = 0").count())



    def test_recommended_core_count(self):
        recommended_tasks = dg.SparkSingleton.getRecommendedSparkTaskCount(useAllCores=True,
                                                                           limitToAvailableCores=True)
        cpu_count = os.cpu_count()
        assert recommended_tasks == cpu_count

    def test_recommended_core_count(self):
        recommended_tasks = dg.SparkSingleton.getRecommendedSparkTaskCount(useAllCores=True, minTasks=32)
        cpu_count = os.cpu_count()
        assert recommended_tasks == max(cpu_count, 32)
