import logging
import unittest

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("basic tests")


class TestRepeatableDataGeneration(unittest.TestCase):
    row_count = 10000
    column_count = 10

    def setUp(self):
        print("setting up")
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(format=FORMAT)

    @classmethod
    def mkBasicDataspec(cls, withRandom=False, dist=None):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                             partitions=4, seed_method='hash_fieldname', verbose=False)
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand(42) * 350) * (86400 + 3600)",
                                        numColumns=cls.column_count)
                            .withColumn("code1", IntegerType(), min=100, max=200, random=withRandom)
                            .withColumn("code2", IntegerType(), min=0, max=1000000,
                                        random=withRandom, distribution=dist)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=withRandom)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'],
                                        random=withRandom, weights=[9, 1, 1])
                            )

        return testDataSpec

    def getFields(self, df ):
        return [ fld.name for fld in df.schema.fields]

    def renameFields(self, df, prefix=None):
        assert prefix is not None, "prefix must be supplied"

        new_columns = [ F.col(fld_name).alias(f"{prefix}_{fld_name}") for fld_name in self.getFields(df) ]
        df_new = df.select(*new_columns)
        return df_new

    def checkTablesEqual(self, df1, df2):

        df1 = df1.cache()
        df2 = df2.cache()

        self.assertEqual(df1.count(), self.row_count)
        self.assertEqual(df2.count(), self.row_count)

        self.assertEqual(df1.schema, df2.schema)

        fields1 = self.getFields(df1)
        fields2 =self.getFields(df2)

        self.assertEqual(set(fields1), set(fields2))

        # rename the fields on each side

        df1a = self.renameFields(df1, "a")
        df2a = self.renameFields(df2, "b")

        cond = [ df1a.a_id == df2a.b_id ]
        df_joined = df1a.join(df2a, cond, "inner").cache()

        comparison_fields = set(fields1)
        comparison_fields.remove("id")

        comparison_fields = sorted(comparison_fields)

        # do a field by field comparison
        for fld in comparison_fields:
            df_compare = df_joined.where(f"a_{fld} != b_{fld}")
            self.assertEqual(df_compare.count(), 0, f"field {fld} should be the same across both data sets ")

    def test_basic_repeatability(self):
        """Test basic data repeatability"""

        ds1 = self.mkBasicDataspec()
        df1 = ds1.build()

        ds2 = self.mkBasicDataspec()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_basic_repeatability_random(self):
        """Test basic data repeatability"""

        ds1 = self.mkBasicDataspec(withRandom=True)
        df1 = ds1.build()

        ds2 = self.mkBasicDataspec(withRandom=True)
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_basic_repeatability_random_normal(self):
        """Test basic data repeatability"""

        ds1 = self.mkBasicDataspec(withRandom=True, dist="normal")
        df1 = ds1.build()

        ds2 = self.mkBasicDataspec(withRandom=True, dist="normal")
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_basic_clone(self):
        """Test clone method"""
        ds1 = self.mkBasicDataspec(withRandom=True, dist="normal")
        df1 = ds1.build()

        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_clone_with_new_column(self):
        """Test clone method"""
        ds1 = (self.mkBasicDataspec(withRandom=True, dist="normal")
               .withColumn("another_column", StringType(), values=['a', 'b', 'c'], random=True)
               )

        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_multiple_base_columns(self):
        """Test data generation with multiple base columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
                    .withColumn("ac1", IntegerType(), base_column=['code1', 'code2'], minValue=100, maxValue=200)
                    .withColumn("ac2", IntegerType(), base_column=['code1', 'code2'],
                                minValue=100, maxValue=200, random=True)
                    )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_date_column(self):
        """Test data generation with multiple base columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
                    .withColumn("dt1", DateType(), random=True)
                    )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_timestamp_column(self):
        """Test data generation with multiple base columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
                    .withColumn("ts1", TimestampType() , random=True)
                    )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_template_column(self):
        """Test data generation with multiple base columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
                    .withColumn("txt1", "string", template=r"dr_\\v")
                    )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)


    def test_template_column_random(self):
        """Test data generation with multiple base columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
                    .withColumn("txt1", "string", template=r"dr_\\v", random=True)
                    )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)



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
