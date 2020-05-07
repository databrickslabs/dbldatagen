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
class TestTextGeneration(unittest.TestCase):
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

    def test_simple_data(self):
        self.dfTestData.show()

    def test_udfs(self):
        u_value_from_template = udf(datagen.TextGenerators.value_from_template, StringType()).asNondeterministic()

        df = (spark.range(100000)
              .withColumn("email", (when(rand() > lit(0.3),
                                        u_value_from_template(lit('testing'),
                                                              lit(r'\\w.\\w@\\w.com|\\w@\\w.co.u\\k')))
                                  .otherwise(lit(None))))
              .withColumn("email2", expr("case when rand() > 0.3 then email else null end"))
              .withColumn("ipaddr", u_value_from_template(lit('testing'), lit(r'\\n.\\n.\\n.\\n')))
              .withColumn("phone", u_value_from_template(lit('testing'), lit(r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')))
              .withColumn("cc", u_value_from_template(lit('testing'), lit(r'dddd-dddd-dddd-dddd')))
              )

        df.show()









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
