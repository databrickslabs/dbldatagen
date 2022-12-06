import unittest

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, pandas_udf
from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("PK1", StringType(), True),
    StructField("LAST_MODIFIED_UTC", TimestampType(), True),
    StructField("date", DateType(), True),
    StructField("str1", StringType(), True),
    StructField("email", StringType(), True),
    StructField("ip_addr", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("isDeleted", BooleanType(), True)
])

print("schema", schema)

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
    .getOrCreate()


# Test manipulation and generation of test data for a large schema
class TestPandasIntegration(unittest.TestCase):
    """ Test that build environment is setup correctly for pandas and numpy integration"""
    testDataSpec = None
    dfTestData = None
    row_count = 100000

    def setUp(self):
        print("setting up")

    @classmethod
    def setUpClass(cls):
        pass

    @staticmethod
    def pandas_udf_example(v, s):
        retvals = []

        vlen = v.size
        i = 0
        while i < vlen:
            retvals.append(v.at[i] + s.at[i])
            i = i + 1
        return pd.Series(retvals)

    # @unittest.skip("not yet implemented")
    def test_pandas(self):
        print("numpy version ", np.version)

        # Enable Arrow-based columnar data transfers
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        # Generate a Pandas DataFrame
        pdf = pd.DataFrame(np.random.rand(100, 3))

        # Create a Spark DataFrame from a Pandas DataFrame using Arrow
        df = spark.createDataFrame(pdf)

        # Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
        df.select("*").show()

        rowCount = df.count()
        self.assertGreater(rowCount, 0)

    @unittest.skip("not yet debugged")
    def test_pandas_udf(self):
        utest_pandas = pandas_udf(self.pandas_udf_example, returnType=StringType()).asNondeterministic()
        df = (spark.range(1000000)
              .withColumn("x", expr("cast(id as string)"))
              .withColumn("y", expr("cast(id as string)"))
              .withColumn("z", utest_pandas(col("x"), col("y")))
              )

        df.show()

        rowCount = df.count()
        self.assertGreater(rowCount, 0)

    def test_numpy(self):
        data = np.arange(10)
        print(np.sum(data))

        self.assertGreater(np.sum(data), 0)

    def test_numpy2(self):
        data = np.arange(1000000)
        print(np.sum(data))

        self.assertGreater(np.sum(data), 0)

#