import unittest

from pyspark.sql.functions import expr
from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import databrickslabs_testdatagenerator as dg
from databrickslabs_testdatagenerator import ILText

schema = StructType([
    StructField("PK1", StringType(), True),
    StructField("LAST_MODIFIED_UTC", TimestampType(), True),
    StructField("date", DateType(), True),
    StructField("str1", StringType(), True),
    StructField("nint", IntegerType(), True),
    StructField("nstr1", StringType(), True),
    StructField("nstr2", StringType(), True),
    StructField("nstr3", StringType(), True),
    StructField("nstr4", StringType(), True),
    StructField("nstr5", StringType(), True),
    StructField("nstr6", StringType(), True),
    StructField("email", StringType(), True),
    StructField("ip_addr", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("isDeleted", BooleanType(), True)
])

# add the following if using pandas udfs
#    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \


spark = dg.SparkSingleton.getLocalInstance("unit tests")

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "500")


# Test manipulation and generation of test data for a large schema
class TestILTextGeneration(unittest.TestCase):
    testDataSpec = None
    row_count = 1000000
    partitions_requested = 24

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
                            .withColumnSpec("nint", percent_nulls=10.0, minValue=1, maxValue=9, step=2)
                            .withColumnSpec("nstr1", percent_nulls=10.0, minValue=1, maxValue=9, step=2)
                            .withColumnSpec("nstr2", percent_nulls=10.0, minValue=1.5, maxValue=2.5, step=0.3,
                                            format="%04.1f")
                            .withColumnSpec("nstr3", minValue=1.0, maxValue=9.0, step=2.0)
                            .withColumnSpec("nstr4", percent_nulls=10.0, minValue=1, maxValue=9, step=2, format="%04d")
                            .withColumnSpec("nstr5", percent_nulls=10.0, minValue=1.5, maxValue=2.5, step=0.3,
                                            random=True)
                            .withColumnSpec("nstr6", percent_nulls=10.0, minValue=1.5, maxValue=2.5, step=0.3,
                                            random=True,
                                            format="%04f")
                            .withColumnSpec("email", template=r'\\w.\\w@\\w.com|\\w@\\w.co.u\\k')
                            .withColumnSpec("ip_addr", template=r'\\n.\\n.\\n.\\n')
                            .withColumnSpec("phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
                            )

    def test_basic_data_generation(self):
        results = self.testDataSpec.build()

        # check ranged int data
        nint_values = [r[0] for r in results.select("nint").distinct().collect()]
        self.assertSetEqual(set(nint_values), {None, 1, 3, 5, 7, 9})

    def test_phone_number_generation(self):
        results = self.testDataSpec.build()

        # check phone numbers
        phone_values = [r[0] for r in
                        results.select(expr(r"regexp_replace(phone, '(\\d+)', 'num')")).distinct().collect()]
        print(phone_values)
        self.assertSetEqual(set(phone_values), {'num num', '(num)-num-num', 'num(num) num-num'})

    def test_email_generation(self):
        results = self.testDataSpec.build()

        # check email addresses
        email_values = [r[0] for r in
                        results.select(expr(r"regexp_replace(email, '(\\w+)', 'word')")).distinct().collect()]
        print(email_values)
        self.assertSetEqual(set(email_values), {'word@word.word.word', 'word.word@word.word'})

    def test_ip_address_generation(self):
        results = self.testDataSpec.build()

        # check ip address
        ip_address_values = [r[0] for r in
                             results.select(expr(r"regexp_replace(ip_addr, '(\\d+)', 'num')")).distinct().collect()]
        print(ip_address_values)
        self.assertSetEqual(set(ip_address_values), {'num.num.num.num'})

    def test_basic_text_data_generation(self):
        results = self.testDataSpec.build()

        # check ranged string data
        nstr1_values = [r[0] for r in results.select("nstr1").distinct().collect()]
        self.assertSetEqual(set(nstr1_values), {None, '1', '3', '5', '7', '9'})

        nstr2_values = [r[0] for r in results.select("nstr2").distinct().collect()]
        print("nstr2_values", nstr2_values)
        self.assertSetEqual(set(nstr2_values), {'01.5', '01.8', '02.1', '02.4', None})

        nstr3_values = [r[0] for r in results.select("nstr3").distinct().collect()]
        print("nstr3_values", nstr3_values)
        self.assertSetEqual(set(nstr3_values), {'1.0', '3.0', '5.0', '7.0', '9.0'})

        nstr4_values = [r[0] for r in results.select("nstr4").distinct().collect()]
        print("nstr4_values", nstr4_values)
        self.assertSetEqual(set(nstr4_values), {None, '0001', '0003', '0005', '0007', '0009'})

    def test_iltext1(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("nstr1", text=ILText(words=(2, 6)))
                         )

        results2 = testDataSpec2.build().select("id", "nstr1").cache()
        results2.show()

        # TODO: add validation statement

    def test_iltext2(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(2, 6)))
                         )

        testDataSpec2.build().select("id", "phone").show()

        # TODO: add validation statement

    def test_iltext3(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(sentences=(2, 6)))
                         )

        testDataSpec2.build().select("id", "phone").show()

        # TODO: add validation statement

    def test_iltext4a(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested,
                                          use_pandas=True, pandas_udf_batch_size=300)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1, 4), sentences=(2, 6)))
                         )

        testDataSpec2.build().select("id", "phone").show(20, truncate=False)

        # TODO: add validation statement

    def test_iltext4b(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested, use_pandas=False)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1, 4), sentences=(2, 6)))
                         )

        testDataSpec2.build().select("id", "phone").show(20, truncate=False)

        # TODO: add validation statement

    def test_iltext5(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1, 4), words=(3, 12)))
                         )

        testDataSpec2.build().select("id", "phone").show()

        # TODO: add validation statement

    def test_iltext6(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withSchema(schema)
                         .withIdOutput()
                         .withColumnSpec("phone", text=ILText(paragraphs=(1, 4), sentences=(2, 8), words=(3, 12)))
                         )

        testDataSpec2.build().select("id", "phone").show()

        # TODO: add validation statement

    def test_iltext7(self):
        print("test data spec 2")
        testDataSpec2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                          partitions=self.partitions_requested)
                         .withIdOutput()
                         .withColumn("sample_text", text=ILText(paragraphs=1, sentences=5, words=4))
                         )

        testDataSpec2.build().select("id", "sample_text").show()

        # TODO: add validation statement

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
