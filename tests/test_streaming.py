import os
import shutil
import time
import pytest
import logging

from pyspark.sql.types import IntegerType, StringType, FloatType
import pyspark.sql.functions as F

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("streaming tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestStreaming():
    row_count = 100000
    column_count = 10
    time_to_run = 10
    rows_per_second = 5000

    def setup_log_capture(self, caplog_object):
        """ set up log capture fixture

        Sets up log capture fixture to only capture messages after setup and only
        capture warnings and errors

        """
        caplog_object.set_level(logging.WARNING)

        # clear messages from setup
        caplog_object.clear()

    def get_log_capture_warnings_and_errors(self, caplog_object, textFlag):
        """
        gets count of errors containing specified text

        :param caplog_object: log capture object from fixture
        :param textFlag: text to search for to include error or warning in count
        :return: count of errors containg text specified in `textFlag`
        """
        streaming_warnings_and_errors = 0
        for r in caplog_object.records:
            if (r.levelname == "WARNING" or r.levelname == "ERROR") and textFlag in r.message:
                streaming_warnings_and_errors += 1

        return streaming_warnings_and_errors

    @pytest.fixture
    def getStreamingDirs(self):
        time_now = int(round(time.time() * 1000))
        base_dir = f"/tmp/testdatagenerator_{time_now}"
        print("test dir created")
        data_dir = os.path.join(base_dir, "data")
        checkpoint_dir = os.path.join(base_dir, "checkpoint")
        os.makedirs(data_dir)
        os.makedirs(checkpoint_dir)

        print("\n\n*** Test directories", base_dir, data_dir, checkpoint_dir)
        yield base_dir, data_dir, checkpoint_dir

        shutil.rmtree(base_dir, ignore_errors=True)
        print(f"\n\n*** test dir [{base_dir}] deleted")

    @pytest.fixture
    def getDataDir(self):
        time_now = int(round(time.time() * 1000))
        base_dir = "/tmp/testdata_{}".format(time_now)
        data_dir = os.path.join(base_dir, "data")
        print(f"test data dir created '{base_dir}'")

        # dont need to create the data dir
        os.makedirs(base_dir)

        try:
            yield data_dir
        finally:
            shutil.rmtree(base_dir, ignore_errors=True)
            print(f"\n\n*** test data dir [{base_dir}] deleted")


    @pytest.mark.parametrize("seedColumnName", ["id",
                                                "_id",
                                                None])
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
              .format("parquet")
              .outputMode("append")
              .option("path", test_dir)
              .option("checkpointLocation", checkpoint_dir)
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
                df2 = spark.read.format("parquet").load(test_dir)
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

    @pytest.mark.parametrize("seedColumnName", ["id",
                                                "_id",
                                                None])
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

    @pytest.mark.parametrize("options,optionsExpected",
                             [ ({"dbldatagen.streaming.source": "rate"},
                                ({"dbldatagen.streaming.source": "rate"}, {}, {})),
                               ({"dbldatagen.streaming.source": "rate-micro-batch", "rowsPerSecond": 50},
                                ({"dbldatagen.streaming.source": "rate-micro-batch"}, {"rowsPerSecond": 50}, {})),
                                   ({"dbldatagen.streaming.source": "rate-micro-batch",
                                     "rowsPerSecond": 50,
                                     "dbldatagen.rows": 100000},
                                    ({"dbldatagen.streaming.source": "rate-micro-batch"},
                                     {"rowsPerSecond": 50},
                                     {"dbldatagen.rows": 100000}))
                               ])
    def test_option_parsing(self, options, optionsExpected):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                         partitions=4, seedMethod='hash_fieldname')
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )

        datagen_options, passthrough_options, unsupported_options  = testDataSpec._parseBuildOptions(options)

        expected_datagen_options, expected_passthrough_options, expected_unsupported_options = optionsExpected

        assert datagen_options == expected_datagen_options
        assert passthrough_options == expected_passthrough_options
        assert unsupported_options == expected_unsupported_options

    @pytest.mark.parametrize("options",
                             [ {"dbldatagen.streaming.source": "parquet",
                                 "dbldatagen.streaming.sourcePath": "/tmp/testStreamingFiles/data1"},
                               {"dbldatagen.streaming.source": "csv",
                                "dbldatagen.streaming.sourcePath": "/tmp/testStreamingFiles/data2"},
                               ])
    def test_basic_file_streaming(self, options, getStreamingDirs):
        base_dir, test_dir, checkpoint_dir = getStreamingDirs

        # generate file for base of streaming generator
        testDataSpecBase = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                         seedMethod='hash_fieldname')
                        .withColumn('value', "long", expr="id")
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )

        dfBase = testDataSpecBase.build()
        dfBase.write.format(options["dbldatagen.streaming.source"])\
            .mode('overwrite')\
            .save(options["dbldatagen.streaming.source"])

        # generate streaming data frame
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                         partitions=4, seedMethod='hash_fieldname')
                        .withColumn("a", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("b", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )
        dfStreaming = testDataSpecBase.build(withStreaming=True, options=options)

        sq = (dfStreaming
              .writeStream
              .format("parquet")
              .outputMode("append")
              .option("path", test_dir)
              .option("checkpointLocation", checkpoint_dir)
              .trigger(once=True)
              .start())

        sq.processAllAvailable()

        dfStreamDataRead = spark.read.format("parquet").load(test_dir)
        rows_read = dfStreamDataRead.count()

        assert rows_read == self.row_count

    def test_withEventTime_batch(self):
        # test it in batch mode
        starting_datetime = "2022-06-01 01:00:00"
        testDataSpecBase = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                         partitions=4, seedMethod='hash_fieldname')
                        .withColumn('value', "long", expr="id")
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        .withColumn("timestamp", "timestamp", expr="now()")
                        .withEnhancedEventTime(startEventTime=starting_datetime, baseColumn="timestamp",
                                               eventTimeName="event_ts")
                        )

        df = testDataSpecBase.build()
        assert df.count() == self.row_count

        df.show()

    def test_withEventTime_streaming(self, getStreamingDirs):
        base_dir, test_dir, checkpoint_dir = getStreamingDirs

        # test it in streaming mode
        starting_datetime = "2022-06-01 01:00:00"
        testDataSpecBase = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                         partitions=4, seedMethod='hash_fieldname')
                        .withColumn('value', "long", expr="id")
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        .withColumn("timestamp2", "timestamp", expr="timestamp")
                        .withEnhancedEventTime(startEventTime=starting_datetime, baseColumn="timestamp2",
                                               eventTimeName="event_ts")
                        )

        dfStreaming = testDataSpecBase.build(withStreaming=True)

        sq = (dfStreaming
              .writeStream
              .format("parquet")
              .outputMode("append")
              .option("path", test_dir)
              .option("checkpointLocation", checkpoint_dir)
              .start())

        sq.awaitTermination(5)
        if sq.isActive:
            sq.stop()

        dfStreamDataRead = spark.read.format("parquet").load(test_dir)
        rows_read = dfStreamDataRead.count()
        assert rows_read > 0

    @pytest.mark.parametrize("options,optionsExpected",
                             [ ({"dbldatagen.streaming.source": "rate"},
                                ({"dbldatagen.streaming.source": "rate"},
                                 {"rowsPerSecond": 1, 'numPartitions': 10}, {})),
                               ({"dbldatagen.streaming.source": "rate-micro-batch"},
                                ({"dbldatagen.streaming.source": "rate-micro-batch"}, {'numPartitions': 10, 'rowsPerBatch':1}, {})),
                               ])
    def test_default_options(self, options, optionsExpected):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                         partitions=10, seedMethod='hash_fieldname')
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )

        datagen_options, passthrough_options, unsupported_options = testDataSpec._parseBuildOptions(options)
        testDataSpec._applyStreamingDefaults(datagen_options, passthrough_options)
        if "startTimestamp" in passthrough_options.keys():
            passthrough_options.pop("startTimestamp", None)

        # remove start timestamp from both options and expected options

        expected_datagen_options, expected_passthrough_options, expected_unsupported_options = optionsExpected

        assert datagen_options == expected_datagen_options
        assert passthrough_options == expected_passthrough_options

    def test_text_streaming(self, getDataDir, caplog, getStreamingDirs):
        datadir = getDataDir
        base_dir, test_dir, checkpoint_dir = getStreamingDirs

        # caplog fixture captures log content
        self.setup_log_capture(caplog)

        df = spark.range(10000).select(F.expr("cast(id as string)").alias("id"))
        df.write.format("text").save(datadir)

        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                         partitions=10, seedMethod='hash_fieldname')
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                        )

        streamingOptions = {
            'dbldatagen.streaming.source': 'text',
            'dbldatagen.streaming.sourcePath': datadir,

        }
        df_streaming = testDataSpec.build(withStreaming=True, options=streamingOptions)

        # check that there warnings about `text` format
        text_format_warnings_and_errors = self.get_log_capture_warnings_and_errors(caplog, "text")
        assert text_format_warnings_and_errors > 0, "Should  have error  or warning messages about text format"

        # loop until we get one seconds worth of data
        start_time = time.time()
        elapsed_time = 0
        rows_retrieved = 0
        time_limit = 10.0

        while elapsed_time < time_limit and rows_retrieved < self.rows_per_second:
            sq = (df_streaming
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
        assert rows_retrieved >= self.rows_per_second






