from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from pyspark.sql.types import BooleanType, DateType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, udf, when, rand

import unittest


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
    .getOrCreate()

# Test manipulation and generation of test data for a large schema
class TestPandasIntegration(unittest.TestCase):
    testDataSpec = None
    dfTestData = None
    row_count = 100000

    def setUp(self):
        print("setting up")

    @classmethod
    def setUpClass(cls):
        cls.testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                                  partitions=4)
                            .withSchema(schema)
                            .withIdOutput()
                            .withColumnSpec("date", percent_nulls=10.0)
                            .withColumnSpec("email", template=r'\\w.\\w@\\w.com|\\w@\\w.co.u\\k')
                            .withColumnSpec("ip_addr", template=r'\\n.\\n.\\n.\\n')
                            .withColumnSpec("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                            )

        print("Test generation plan")
        print("=============================")
        cls.testDataSpec.explain()

        print("=============================")
        print("")
        cls.dfTestData = cls.testDataSpec.build().cache()

    @unittest.skip("not yet implemented")
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
