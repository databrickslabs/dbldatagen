from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as dg
from pyspark.sql import SparkSession
import unittest
from pyspark.sql import functions as F
import logging


spark = dg.SparkSingleton.getLocalInstance("basic tests")

class TestBasicOperation(unittest.TestCase):
    testDataSpec = None
    dfTestData = None
    row_count = 100000
    column_count = 50

    def setUp(self):
        print("setting up")
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(format=FORMAT)

    @classmethod
    def setUpClass(cls):
        cls.testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                             partitions=4, seed_method='hash_fieldname', verbose=True)
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=cls.column_count)
                            .withColumn("code1", IntegerType(), min=100, max=200)
                            .withColumn("code2", IntegerType(), min=0, max=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )

        cls.dfTestData = cls.testDataSpec.build().cache()

    def test_spark_version(self):
        print(spark.version)
        ds_copy1 = self.testDataSpec.clone()

        df_copy1 = (ds_copy1.setRowCount(1000)
                    .withColumn("another_column", StringType(), values=['a', 'b', 'c'], random=True)
                    .build())

        self.assertEqual(df_copy1.count(), 1000)

        fields1 = ds_copy1.getOutputColumnNames()
        fields2 = self.testDataSpec.getOutputColumnNames()
        self.assertNotEqual(fields1, fields2)

    def test_clone(self):
        ds_copy1 = self.testDataSpec.clone()

        df_copy1 = (ds_copy1.setRowCount(1000)
                    .withColumn("another_column", StringType(), values=['a', 'b', 'c'], random=True)
                    .build())

        self.assertEqual(df_copy1.count(), 1000)

        fields1 = ds_copy1.getOutputColumnNames()
        fields2 = self.testDataSpec.getOutputColumnNames()
        self.assertNotEqual(fields1, fields2)

    def test_multiple_base_columns(self):
        ds_copy1 = self.testDataSpec.clone()

        df_copy1 = (ds_copy1.setRowCount(1000)
                    .withColumn("ac1", IntegerType(), base_column=['code1','code2'], min=100, max=200)
                    .withColumn("ac2", IntegerType(), base_column=['code1','code2'], min=100, max=200, random=True)
                    .build())

        self.assertEqual(df_copy1.count(), 1000)

        df_overlimit=df_copy1.where("ac1 > 200")
        self.assertEqual(df_overlimit.count(), 0)

        df_underlimit=df_copy1.where("ac1 < 100")
        self.assertEqual(df_underlimit.count(), 0)

        df_overlimit2=df_copy1.where("ac2 > 200")
        self.assertEqual(df_overlimit2.count(), 0)

        df_underlimit2=df_copy1.where("ac2 < 100")
        self.assertEqual(df_underlimit2.count(), 0)

        df_copy1.show()

    def test_multiple_hash_methods(self):
        ds1 = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                partitions=4, seed_method='hash_fieldname')
               .withIdOutput()
               .withColumn("code2", IntegerType(), min=0, max=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        df = ds1.build()
        self.assertEqual(df.count(), 1000)

        ds2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                partitions=4, seed_method='fixed')
               .withIdOutput()
               .withColumn("code2", IntegerType(), min=0, max=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        df2 = ds2.build()
        self.assertEqual(df2.count(), 1000)

        ds3= (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                               partitions=4, seed_method=None)
              .withIdOutput()
              .withColumn("code2", IntegerType(), min=0, max=10)
              .withColumn("code3", StringType(), values=['a', 'b', 'c'])
              .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
              .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

              )

        df3 = ds3.build()
        self.assertEqual(df3.count(), 1000)


    def test_generated_data_count(self):
        count = self.dfTestData.count()
        self.assertEqual(count, self.row_count)

    def test_distinct_count(self):
        distinct_count = self.dfTestData.select('id').distinct().count()
        self.assertEqual(distinct_count, self.row_count)

    def test_column_count(self):
        column_count_observed = len(self.dfTestData.columns)
        self.assertEqual(column_count_observed, self.column_count + 6)

    def test_values_code1(self):
        values = self.dfTestData.select('code1').groupBy().agg(F.min('code1').alias('min'),
                                                               F.max('code1').alias('max')).collect()[0]
        print("min and max", values)
        self.assertEqual({100, 200}, {values.min, values.max})

    def test_values_code2(self):
        values = self.dfTestData.select('code2').groupBy().agg(F.min('code2').alias('min'),
                                                               F.max('code2').alias('max')).collect()[0]
        print("min and max", values)
        self.assertEqual({0, 10}, {values.min, values.max})

    def test_values_code3(self):
        values = [x.code3 for x in self.dfTestData.select('code3').distinct().collect()]
        self.assertEqual({'a', 'b', 'c'}, set(values))

    def test_values_code4(self):
        values = [x.code4 for x in self.dfTestData.select('code4').distinct().collect()]
        self.assertTrue({'a', 'b', 'c'}.issuperset(set(values)))

    def test_values_code5(self):
        values = [x.code5 for x in self.dfTestData.select('code5').distinct().collect()]
        self.assertTrue({'a', 'b', 'c'}.issuperset(set(values)))

    def test_basic_adhoc(self):
        testDataSpec = self.testDataSpec
        log=logging.getLogger('unit_tests')

        log.warning("testing")
        print("data generation description:", testDataSpec.describe())
        print("data generation repr:", repr(testDataSpec))
        print("data generation str:", str(testDataSpec))
        self.testDataSpec.explain()

        print("output columns", testDataSpec.getOutputColumnNames())
        testDataDf = testDataSpec.build()

        print("dataframe description", testDataDf.describe())
        print("dataframe repr", repr(testDataDf))
        print("dataframe str", str(testDataDf))
        print("dataframe schema", str(testDataDf.schema))
        self.assertEqual(testDataDf.count(), self.row_count)

    def test_basic_with_schema(self):
        schema = StructType([
            StructField("region_id", IntegerType(), True),
            StructField("region_cd", StringType(), True),
            StructField("c", StringType(), True),
            StructField("c1", StringType(), True),
            StructField("state1", StringType(), True),
            StructField("state2", StringType(), True),
            StructField("st_desc", StringType(), True),

        ])

        testDataSpec2 = self.testDataSpec.clone()
        print("data generation description:", testDataSpec2.describe())
        print("data generation repr:", repr(testDataSpec2))
        print("data generation str:", str(testDataSpec2))
        testDataSpec2.explain()

        testDataSpec3 = (testDataSpec2.withSchema(schema)
                         .withColumnSpec("state1", values=['ca', 'wa', 'ny'])
                         )

        print("output columns", testDataSpec3.getOutputColumnNames())


        testDataDf = testDataSpec3.build()
        testDataDf.show()

        print("dataframe description", testDataDf.describe())
        print("dataframe repr", repr(testDataDf))
        print("dataframe str", str(testDataDf))
        print("dataframe schema", str(testDataDf.schema))
        self.assertEqual(testDataDf.count(), self.row_count)

    def test_partitions(self):
        id_partitions =11
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=100000000, partitions=id_partitions, verbose=True)
            .withColumn("code1", IntegerType(), min=1, max=20, step=1)
            .withColumn("code2", IntegerType(), max=1000, step=5)
            .withColumn("code3", IntegerType(), min=100, max=200, step=1, random=True)
            .withColumn("xcode", StringType(), values=["a", "test", "value"], random=True)
            .withColumn("rating", FloatType(), min=1.0, max=5.0, step=0.00001, random=True))

        df = testdata_defn.build()
        df.printSchema()

        count = df.count()

        partitions_created = df.rdd.getNumPartitions()
        print("partitions created", partitions_created)
        self.assertEqual(id_partitions, partitions_created)





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

if __name__ == '__main__':
    unittest.main()