from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from pyspark.sql.types import BooleanType, DateType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, udf, when, rand, pandas_udf

import unittest
import pandas as pd
import numpy as np

schema = StructType([
StructField("PK1",StringType(),True),
StructField("LAST_MODIFIED_UTC",TimestampType(),True),
StructField("date",DateType(),True),
StructField("str1",StringType(),True),
StructField("email",StringType(),True),
StructField("ip_addr",StringType(),True),
StructField("phone",StringType(),True),
StructField("isDeleted",BooleanType(),True)
])


print("schema", schema)

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

def test_pandas(v, s):
    retvals = []

    vlen = v.size
    i = 0
    while i < vlen:
        retvals.append(v.at[i]+s.at[i])
        i = i + 1
    return pd.Series(retvals)

# Test manipulation and generation of test data for a large schema
class TestPandasIntegration(unittest.TestCase):
    testDataSpec = None
    dfTestData = None
    row_count = 100000

    def setUp(self):
        print("setting up")

    @classmethod
    def setUpClass(cls):
        pass

    #@unittest.skip("not yet implemented")
    def test_pandas(self):
        import numpy as np
        import pandas as pd

        print("numpy version ", np.version)

        # Enable Arrow-based columnar data transfers
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")

        # Generate a Pandas DataFrame
        pdf = pd.DataFrame(np.random.rand(100, 3))

        # Create a Spark DataFrame from a Pandas DataFrame using Arrow
        df = spark.createDataFrame(pdf)

        # Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
        df.select("*").show()

    @unittest.skip("not yet finalized")
    def test_pandas_udf(self):
        utest_pandas = pandas_udf(test_pandas, returnType=StringType()).asNondeterministic()
        df = (spark.range(1000000)
              .withColumn("x", expr("cast(id as string)"))
              .withColumn("y", expr("cast(id as string)"))
              .withColumn("z", utest_pandas(col("x"), col("y")))
              )

        df.show()

    def test_numpy(self):
        data=np.arange(10)
        print(np.sum(data))

    def test_numpy2(self):
        data = np.arange(1000000)
        print(np.sum(data))




# run the tests
# if __name__ == '__main__':
#  print("Trying to run tests")
#  unittest.main(argv=['first-arg-is-ignored'],verbosity=2,exit=False)

# def runTests(suites):
#    suite = unittest.TestSuite()
#    result = unittest.TestResult()
#    for testSuite in suites:
#        suite.addTest(unittest.makeSuite(testSuite))
#    runner = unittest.TextTestRunner()
#    print(runner.run(suite))


# runTests([TestBasicOperation])
