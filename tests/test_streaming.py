import os
import shutil
import time
import pytest

from pyspark.sql.types import IntegerType, StringType, FloatType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("streaming tests")


class TestStreaming:
    row_count = 100000
    column_count = 10
    time_to_run = 8
    rows_per_second = 5000

    def getTestDataSpec(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                         rows=self.row_count,
                                         partitions=spark.sparkContext.defaultParallelism,
                                         seedMethod='hash_fieldname')
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                    numColumns=self.column_count)
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )
        return testDataSpec

    def test_get_current_spark_timestamp(self):
        testDataSpec = dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                        rows=self.row_count,
                                        partitions=spark.sparkContext.defaultParallelism,
                                        seedMethod='hash_fieldname')
        ts = testDataSpec._getCurrentSparkTimestamp(asLong=False)

        assert type(ts) is str
        assert ts is not None and len(ts.strip()) > 0
        print(ts)

    def test_get_current_spark_timestamp2(self):
        testDataSpec = dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                        rows=self.row_count,
                                        partitions=spark.sparkContext.defaultParallelism,
                                        seedMethod='hash_fieldname')
        ts = testDataSpec._getCurrentSparkTimestamp(asLong=True)

        assert(type(ts) is int)
        print(ts)

    def test_get_current_spark_version(self):
        assert spark.version > "3.0.0"
        assert spark.version <= "6.0.0"

    @pytest.mark.parametrize("options_supplied,expected,spark_version_override",
                             [(None, "rate" if spark.version < "3.2.1" else "rate-micro-batch", None),
                              (None, "rate", "3.0.0"),
                              (None, "rate-micro-batch", "3.2.1"),
                              ({'streamingSource': 'rate'}, 'rate', None),
                              ({'streamingSource': 'rate-micro-batch'}, 'rate-micro-batch', None)])
    def test_streaming_source_options(self, options_supplied, expected, spark_version_override):
        print("options", options_supplied)
        testDataSpec = dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                        rows=self.row_count,
                                        partitions=spark.sparkContext.defaultParallelism,
                                        seedMethod='hash_fieldname')

        result = testDataSpec._getStreamingSource(options_supplied, spark_version_override)
        print("Options:", options_supplied, "retval:", result)

        assert result == expected

    @pytest.mark.parametrize("options_supplied,source_expected,options_expected,spark_version_override",
                             [(None, "rate" if spark.version < "3.2.1" else "rate-micro-batch",
                               {'numPartitions': spark.sparkContext.defaultParallelism,
                                'rowsPerBatch': spark.sparkContext.defaultParallelism,
                                'startTimestamp': "*"} if spark.version >= "3.2.1"
                               else {'numPartitions': spark.sparkContext.defaultParallelism,
                                     'rowsPerSecond': spark.sparkContext.defaultParallelism}, None),

                              (None, "rate", {'numPartitions': spark.sparkContext.defaultParallelism,
                                              'rowsPerSecond': spark.sparkContext.defaultParallelism}, "3.0.0"),

                              (None, "rate-micro-batch",
                               {'numPartitions': spark.sparkContext.defaultParallelism,
                                'rowsPerBatch': spark.sparkContext.defaultParallelism,
                                'startTimestamp': "*"}, "3.2.1"),

                              ({'streamingSource': 'rate'}, 'rate',
                               {'numPartitions': spark.sparkContext.defaultParallelism,
                                'streamingSource': 'rate',
                                'rowsPerSecond': spark.sparkContext.defaultParallelism}, None),

                              ({'streamingSource': 'rate', 'rowsPerSecond': 5000}, 'rate',
                               {'numPartitions': spark.sparkContext.defaultParallelism,
                                'streamingSource': 'rate',
                                'rowsPerSecond': 5000}, None),

                              ({'streamingSource': 'rate', 'numPartitions': 10}, 'rate',
                               {'numPartitions': 10, 'rowsPerSecond': 10, 'streamingSource': 'rate'}, None),

                              ({'streamingSource': 'rate', 'numPartitions': 10, 'rowsPerSecond': 5000}, 'rate',
                               {'numPartitions': 10, 'rowsPerSecond': 5000, 'streamingSource': 'rate'}, None),

                              ({'streamingSource': 'rate-micro-batch'}, 'rate-micro-batch',
                               {'streamingSource': 'rate-micro-batch',
                                'numPartitions': spark.sparkContext.defaultParallelism,
                                'startTimestamp': '*',
                                'rowsPerBatch': spark.sparkContext.defaultParallelism}, None),

                              ({'streamingSource': 'rate-micro-batch', 'numPartitions':20}, 'rate-micro-batch',
                               {'streamingSource': 'rate-micro-batch',
                                'numPartitions': 20,
                                'startTimestamp': '*',
                                'rowsPerBatch': 20}, None),

                              ({'streamingSource': 'rate-micro-batch', 'numPartitions': 20, 'rowsPerBatch': 4300},
                               'rate-micro-batch',
                               {'streamingSource': 'rate-micro-batch',
                                'numPartitions': 20,
                                'startTimestamp': '*',
                                'rowsPerBatch': 4300}, None),
                              ])
    def test_prepare_options(self, options_supplied, source_expected, options_expected, spark_version_override):
        testDataSpec = dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                        rows=self.row_count,
                                        partitions=spark.sparkContext.defaultParallelism,
                                        seedMethod='hash_fieldname')

        streaming_source, new_options = testDataSpec._prepareStreamingOptions(options_supplied, spark_version_override)
        print("Options supplied:", options_supplied, "streamingSource:", streaming_source)

        assert streaming_source == source_expected, "unexpected streaming source"

        if streaming_source == "rate-micro-batch":
            assert "startTimestamp" in new_options
            assert "startTimestamp" in options_expected
            if options_expected["startTimestamp"] == "*":
                options_expected.pop("startTimestamp")
                new_options.pop("startTimestamp")

        print("options expected:", options_expected)

        assert  new_options == options_expected, "unexpected options"

    @pytest.fixture
    def getBaseDir(self, request):
        time_now = int(round(time.time() * 1000))
        base_dir = f"/tmp/testdatagenerator_{request.node.originalname}_{time_now}"
        yield base_dir
        print("cleaning base dir")
        shutil.rmtree(base_dir)

    @pytest.fixture
    def getCheckpoint(self, getBaseDir, request):
        checkpoint_dir = os.path.join(getBaseDir, "checkpoint1")
        os.makedirs(checkpoint_dir)

        yield checkpoint_dir
        print("cleaning checkpoint dir")

    @pytest.fixture
    def getDataDir(self, getBaseDir, request):
        data_dir = os.path.join(getBaseDir, "data1")
        os.makedirs(data_dir)

        yield data_dir
        print("cleaning data dir")



    def test_fixture1(self, getCheckpoint, getDataDir):
        print(getCheckpoint)
        print(getDataDir)

    def test_streaming_basic_rate(self, getDataDir, getCheckpoint):
        test_dir = getDataDir
        checkpoint_dir = getCheckpoint

        try:

            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                         rows=self.row_count,
                                         partitions=spark.sparkContext.defaultParallelism,
                                         seedMethod='hash_fieldname')
                            .withIdOutput())

            dfTestData = testDataSpec.build(withStreaming=True,
                                            options={'rowsPerSecond': self.rows_per_second,
                                                     'ageLimit': 1,
                                                     'streamingSource': 'rate'})

            (dfTestData.writeStream
                       .option("checkpointLocation", checkpoint_dir)
                       .outputMode("append")
                       .format("parquet")
                       .start(test_dir)
             )

            start_time = time.time()
            time.sleep(self.time_to_run)

            # note stopping the stream may produce exceptions - these can be ignored
            recent_progress = []
            for x in spark.streams.active:
                recent_progress.append(x.recentProgress)
                print(x)
                x.stop()

            end_time = time.time()

            # read newly written data
            df2 = spark.read.format("parquet").load(test_dir)

            new_data_rows = df2.count()

            print("read {} rows from newly written data".format(new_data_rows))
        finally:
            pass

        print("*** Done ***")

        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        assert new_data_rows >  self.rows_per_second

    def test_streaming_basic_rate_micro_batch(self, getDataDir, getCheckpoint):
        test_dir = getDataDir
        checkpoint_dir = getCheckpoint

        try:

            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1",
                                         rows=self.row_count,
                                         partitions=spark.sparkContext.defaultParallelism,
                                         seedMethod='hash_fieldname')
                            .withIdOutput()
                            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            )

            dfTestData = testDataSpec.build(withStreaming=True,
                                            options={'rowsPerBatch': 1000,
                                                     'streamingSource': 'rate-micro-batch',
                                                     'startTimestamp': 0})

            (dfTestData.writeStream
                       .option("checkpointLocation", checkpoint_dir)
                       .outputMode("append")
                       .format("parquet")
                       .start(test_dir)
             )

            start_time = time.time()
            time.sleep(self.time_to_run)

            # note stopping the stream may produce exceptions - these can be ignored
            recent_progress = []
            for x in spark.streams.active:
                recent_progress.append(x.recentProgress)
                print(x)
                x.stop()

            end_time = time.time()

            # read newly written data
            df2 = spark.read.format("parquet").load(test_dir)

            new_data_rows = df2.count()

            print("read {} rows from newly written data".format(new_data_rows))
        finally:
            pass

        print("*** Done ***")

        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        assert new_data_rows >  self.rows_per_second


    def test_streaming_rate_source(self):
        print(spark.version)
        test_dir, checkpoint_dir, base_dir = self.getDataAndCheckpoint("test1")

        new_data_rows = 0

        self.makeDataAndCheckpointDirs(test_dir, checkpoint_dir)

        try:

            testDataSpec = self.getTestDataSpec()

            dfTestData = testDataSpec.build(withStreaming=True,
                                            options={'rowsPerSecond': self.rows_per_second,
                                                     'ageLimit': 1,
                                                     'streamingSource': 'rate'})

            start_time = time.time()
            time.sleep(self.time_to_run)

            # note stopping the stream may produce exceptions - these can be ignored
            recent_progress = []
            for x in spark.streams.active:
                recent_progress.append(x.recentProgress)
                print(x)
                x.stop()

            end_time = time.time()

            # read newly written data
            df2 = spark.read.format("parquet").load(test_dir)

            new_data_rows = df2.count()

            print("read {} rows from newly written data".format(new_data_rows))
        finally:
            shutil.rmtree(base_dir)

        print("*** Done ***")

        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        self.assertGreater(new_data_rows, self.rows_per_second)


    def test_streaming(self):
        print(spark.version)
        test_dir, checkpoint_dir, base_dir = self.getDataAndCheckpoint("test1")

        new_data_rows = 0

        self.makeDataAndCheckpointDirs(test_dir, checkpoint_dir)

        try:

            testDataSpec = self.getTestDataSpec()

            dfTestData = testDataSpec.build(withStreaming=True,
                                            options={'rowsPerSecond': self.rows_per_second,
                                                     'ageLimit': 1})

            start_time = time.time()
            time.sleep(self.time_to_run)

            # note stopping the stream may produce exceptions - these can be ignored
            recent_progress = []
            for x in spark.streams.active:
                recent_progress.append(x.recentProgress)
                print(x)
                x.stop()

            end_time = time.time()

            # read newly written data
            df2 = spark.read.format("parquet").load(test_dir)

            new_data_rows = df2.count()

            print("read {} rows from newly written data".format(new_data_rows))
        finally:
            shutil.rmtree(base_dir)

        print("*** Done ***")

        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        self.assertGreater(new_data_rows, self.rows_per_second)

    def test_streaming_with_age_limit(self):
        print(spark.version)

        time_now = int(round(time.time() * 1000))
        base_dir = "/tmp/testdatagenerator2_{}".format(time_now)
        test_dir = os.path.join(base_dir, "data")

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
                                             partitions=4, seedMethod='hash_fieldname')
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
                                            options={'rowsPerSecond': self.rows_per_second,
                                                     'ageLimit': 1})

            (dfTestData
             .writeStream
             .format("parquet")
             .outputMode("append")
             .option("path", test_dir)
             .option("checkpointLocation", checkpoint_dir)
             .start())

            start_time = time.time()
            time.sleep(self.time_to_run)

            # note stopping the stream may produce exceptions - these can be ignored
            recent_progress = []
            for x in spark.streams.active:
                recent_progress.append(x.recentProgress)
                print(x)
                x.stop()

            end_time = time.time()

            # read newly written data
            df2 = spark.read.format("parquet").load(test_dir)

            new_data_rows = df2.count()

            print("read {} rows from newly written data".format(new_data_rows))
        finally:
            shutil.rmtree(base_dir)


        print("*** Done ***")
        print("read {} rows from newly written data".format(rows_retrieved))
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
                  .format("parquet")
                  .outputMode("append")
                  .option("path", test_dir)
                  .option("checkpointLocation", checkpoint_dir)
                  .trigger(once=True)
                  .start())

            # wait for trigger once to terminate
            sq.awaitTermination(5)

            elapsed_time = time.time() - start_time

            try:
                df2 = spark.read.format("parquet").load(test_dir)
                rows_retrieved = df2.count()

            # ignore file or metadata not found issues arising from read before stream has written first batch
            except Exception as exc:
                print("Exception:", exc)

            if sq.isActive:
                sq.stop()

        end_time = time.time()

        print("*** Done ***")
        print("read {} rows from newly written data".format(rows_retrieved))
        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        self.assertGreater(new_data_rows, int(self.rows_per_second / 4))


