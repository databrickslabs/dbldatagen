import os
import shutil
import time
import uuid
import pytest

from pyspark.sql.types import IntegerType, StringType, FloatType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("streaming tests")


class TestStreaming():
    row_count = 100000
    column_count = 10
    time_to_run = 10
    rows_per_second = 5000

    @pytest.fixture
    def getStreamingDirs(self):
        base_dir = f"/tmp/testdatagenerator/{uuid.uuid4()}"
        print("test dir created")
        data_dir = os.path.join(base_dir, "data")
        checkpoint_dir = os.path.join(base_dir, "checkpoint")
        os.makedirs(data_dir)
        os.makedirs(checkpoint_dir)

        print("\n\n*** Test directories", base_dir, data_dir, checkpoint_dir)
        yield base_dir, data_dir, checkpoint_dir

        shutil.rmtree(base_dir, ignore_errors=True)
        print(f"\n\n*** test dir [{base_dir}] deleted")

    @pytest.mark.parametrize("seedColumnName", ["id", "_id", None])
    def test_streaming(self, getStreamingDirs, seedColumnName):
        base_dir, test_dir, checkpoint_dir = getStreamingDirs

        if seedColumnName is not None:
            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                             partitions=4, seedMethod='hash_fieldname', seedColumnName=seedColumnName))
        else:
            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                             partitions=4, seedMethod='hash_fieldname'))

        testDataSpec = (testDataSpec
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                    numColumns=self.column_count)
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        dfTestData = testDataSpec.build(withStreaming=True,
                                        options={'rowsPerSecond': self.rows_per_second})

        # check that seed column is in schema
        fields = [c.name for c in dfTestData.schema.fields]

        if seedColumnName is not None:
            assert seedColumnName in fields
            assert "id" not in fields if seedColumnName != "id" else True

        sq = (dfTestData
              .writeStream
              .format("csv")
              .outputMode("append")
              .option("path", test_dir)
              .option("checkpointLocation", f"{checkpoint_dir}/{uuid.uuid4()}")
              .start())

        # loop until we get one seconds worth of data
        start_time = time.time()
        elapsed_time = 0
        rows_retrieved = 0
        time_limit = 10.0

        while elapsed_time < time_limit and rows_retrieved <= self.rows_per_second:
            time.sleep(1)

            elapsed_time = time.time() - start_time

            try:
                df2 = spark.read.format("csv").load(test_dir)
                rows_retrieved = df2.count()

            # ignore file or metadata not found issues arising from read before stream has written first batch
            except Exception as exc:  # pylint: disable=broad-exception-caught
                print("Exception:", exc)

        if sq.isActive:
            sq.stop()

        end_time = time.time()

        print("*** Done ***")
        print(f"read {rows_retrieved} rows from newly written data")
        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        assert rows_retrieved >= self.rows_per_second

    @pytest.mark.parametrize("seedColumnName", ["id", "_id", None])
    def test_streaming_trigger_once(self, getStreamingDirs, seedColumnName):
        base_dir, test_dir, checkpoint_dir = getStreamingDirs

        if seedColumnName is not None:
            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                             partitions=4, seedMethod='hash_fieldname',
                                             seedColumnName=seedColumnName))
        else:
            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                             partitions=4, seedMethod='hash_fieldname'))

        testDataSpec = (testDataSpec
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                    numColumns=self.column_count)
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        dfTestData = testDataSpec.build(withStreaming=True,
                                        options={'rowsPerSecond': self.rows_per_second})

        # check that seed column is in schema
        fields = [c.name for c in dfTestData.schema.fields]

        if seedColumnName is not None:
            assert seedColumnName in fields
            assert "id" not in fields if seedColumnName != "id" else True

        # loop until we get one seconds worth of data
        start_time = time.time()
        elapsed_time = 0
        rows_retrieved = 0
        time_limit = 10.0

        while elapsed_time < time_limit and rows_retrieved < self.rows_per_second:
            sq = (dfTestData
                  .writeStream
                  .format("csv")
                  .outputMode("append")
                  .option("path", test_dir)
                  .option("checkpointLocation", checkpoint_dir)
                  .trigger(once=True)
                  .start())

            # wait for trigger once to terminate
            sq.awaitTermination(5)

            elapsed_time = time.time() - start_time

            try:
                df2 = spark.read.format("csv").load(test_dir)
                rows_retrieved = df2.count()

            # ignore file or metadata not found issues arising from read before stream has written first batch
            except Exception as exc:  # pylint: disable=broad-exception-caught
                print("Exception:", exc)

            if sq.isActive:
                sq.stop()

        end_time = time.time()

        print("*** Done ***")
        print(f"read {rows_retrieved} rows from newly written data")
        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        assert rows_retrieved >= self.rows_per_second
