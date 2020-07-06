from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from pyspark.sql.types import BooleanType, DateType
import databrickslabs_testdatagenerator as dg
from databrickslabs_testdatagenerator import NRange, ILText
import databrickslabs_testdatagenerator.distributions as dist
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, udf, when, rand

import unittest
import re
import pandas as pd


schema = StructType([
    StructField("PK1",StringType(),True),
    StructField("LAST_MODIFIED_UTC",TimestampType(),True),
    StructField("date",DateType(),True),
    StructField("str1",StringType(),True),
    StructField("nint",IntegerType(),True),
    StructField("nstr1",StringType(),True),
    StructField("nstr2",StringType(),True),
    StructField("nstr3",StringType(),True),
    StructField("nstr4",StringType(),True),
    StructField("nstr5",StringType(),True),
    StructField("nstr6",StringType(),True),
    StructField("email",StringType(),True),
    StructField("ip_addr",StringType(),True),
    StructField("phone",StringType(),True),
    StructField("isDeleted",BooleanType(),True)
])

# add the following if using pandas udfs
#    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \


spark = dg.SparkSingleton.getLocalInstance("unit tests")

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "500")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Test manipulation and generation of test data for a large schema
class TestTextGeneration(unittest.TestCase):
    testDataSpec = None
    row_count = 1000000
    partitions_requested=24

    def setUp(self):
        print("setting up TestTextDataGenerationTests")
        print("schema", schema)

    @classmethod
    def setUpClass(cls):
        print("setting up class ")
        cls.testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                             partitions=cls.partitions_requested)
                            .withSchema(schema)
                            .withIdOutput()
                            .withColumnSpec("date", percent_nulls=10.0)
                            .withColumnSpec("nint", percent_nulls=10.0, min=1, max=9, step=2)
                            .withColumnSpec("nstr1", percent_nulls=10.0, min=1, max=9, step=2)
                            .withColumnSpec("nstr2", percent_nulls=10.0, min=1.5, max=2.5, step=0.3, format="%04f")
                            .withColumnSpec("nstr3", min=1.0, max=9.0, step=2.0)
                            .withColumnSpec("nstr4", percent_nulls=10.0, min=1, max=9, step=2,  format="%04d")
                            .withColumnSpec("nstr5", percent_nulls=10.0, min=1.5, max=2.5, step=0.3,  random=True)
                            .withColumnSpec("nstr6", percent_nulls=10.0, min=1.5, max=2.5, step=0.3, random=True, format="%04f")
                            .withColumnSpec("email", template=r'\\w.\\w@\\w.com|\\w@\\w.co.u\\k')
                            .withColumnSpec("ip_addr", template=r'\\n.\\n.\\n.\\n')
                            .withColumnSpec("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                            )


    def test_simple_data(self):
        self.testDataSpec.build().show()

    def test_simple_data2(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("date", percent_nulls=10.0)
                         .withColumnSpecs(patterns="n.*", match_types=StringType(),
                                        percent_nulls=10.0, min=1, max=9, step=2)
                         .withColumnSpecs(patterns="n.*", match_types=IntegerType(),
                                        percent_nulls=10.0, min=1, max=200, step=-2)
                         .withColumnSpec("email", template=r'\\w.\\w@\\w.com|\\w@\\w.co.u\\k')
                         .withColumnSpec("ip_addr", template=r'\\n.\\n.\\n.\\n')
                         .withColumnSpec("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                         )

        testDataSpec2.build().show()

    def test_iltext1(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("nstr1", text=ILText(words=(2,6)))
                         )

        testDataSpec2.build().select("id", "nstr1").show()

    def test_iltext2(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(2,6)))
                         )

        testDataSpec2.build().select("id", "phone").show()

    def test_iltext3(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(sentences=(2,6)))
                         )

        testDataSpec2.build().select("id", "phone").show()

    def test_iltext4a(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested,
                                          use_pandas=True, pandas_udf_batch_size=300)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1,4), sentences=(2,6)))
                         )

        testDataSpec2.build().select("id", "phone").show(20, truncate=False)

    def test_iltext4b(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested, use_pandas=False)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1,4), sentences=(2,6)))
                         )

        testDataSpec2.build().select("id", "phone").show(20, truncate=False)

    def test_iltext5(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1,4), words=(3,12)))
                         )

        testDataSpec2.build().select("id", "phone").show()

    def test_iltext6(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1,4), sentences=(2, 8), words=(3,12)))
                         )

        testDataSpec2.build().select("id", "phone").show()











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
