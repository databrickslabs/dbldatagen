from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as dg
from pyspark.sql import SparkSession
import unittest
import os
import time
import shutil

spark = dg.SparkSingleton.getLocalInstance("streaming tests")


class TestStreaming(unittest.TestCase):
    row_count = 100000
    column_count = 10
    time_to_run = 15
    rows_per_second = 5000

    def test_streaming(self):
        time_now = int(round(time.time() * 1000))
        base_dir = "/tmp/testdatagenerator_{}".format(time_now)
        test_dir = os.path.join(base_dir, "data")
        checkpoint_dir = os.path.join(base_dir, "checkpoint")
        print(time_now, test_dir, checkpoint_dir)

        new_data_rows = 0

        try:
            os.makedirs(test_dir)
            os.makedirs(checkpoint_dir)

            testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                             partitions=4, seed_method='hash_fieldname')
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=self.column_count)
                            .withColumn("code1", IntegerType(), min=100, max=200)
                            .withColumn("code2", IntegerType(), min=0, max=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )

            dfTestData = testDataSpec.build(withStreaming=True,
                                            options={'rowsPerSecond': self.rows_per_second})

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

        print("elapsed time (seconds)", end_time - start_time)

        # check that we have at least one second of data
        self.assertGreater(new_data_rows, self.rows_per_second)
