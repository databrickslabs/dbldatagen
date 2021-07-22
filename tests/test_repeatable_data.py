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
                        .withColumn("code1", IntegerType(), minValue=100, maxValue=200, random=withRandom)
                        .withColumn("code2", IntegerType(), minValue=0, maxValue=1000000,
                                    random=withRandom, distribution=dist)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=withRandom)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'],
                                    random=withRandom, weights=[9, 1, 1])
                        )

        return testDataSpec

    def getFields(self, df):
        return [fld.name for fld in df.schema.fields]

    def renameFields(self, df, prefix=None):
        assert prefix is not None, "prefix must be supplied"

        new_columns = [F.col(fld_name).alias(f"{prefix}_{fld_name}") for fld_name in self.getFields(df)]
        df_new = df.select(*new_columns)
        return df_new

    def checkTablesEqual(self, df1, df2):
        df1 = df1.cache()
        df2 = df2.cache()

        self.assertEqual(df1.count(), self.row_count)
        self.assertEqual(df2.count(), self.row_count)

        self.assertEqual(df1.schema, df2.schema)

        fields1 = self.getFields(df1)
        fields2 = self.getFields(df2)

        self.assertEqual(set(fields1), set(fields2))

        # rename the fields on each side

        df1a = self.renameFields(df1, "a")
        df2a = self.renameFields(df2, "b")

        cond = [df1a.a_id == df2a.b_id]
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
               .withColumn("ac1", IntegerType(), baseColumn=['code1', 'code2'], minValue=100, maxValue=200)
               .withColumn("ac2", IntegerType(), baseColumn=['code1', 'code2'],
                           minValue=100, maxValue=200, random=True)
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_date_column(self):
        """Test data generation with date columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("dt1", DateType(), random=True)
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_timestamp_column(self):
        """Test data generation with timestamp columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("ts1", TimestampType(), random=True)
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_template_column(self):
        """Test data generation with _template columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("txt1", "string", template=r"dr_\\v")
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_template_column_random(self):
        """Test data generation with _template columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("txt1", "string", template=r"\dr_\v", random=True)
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_template_column_random2(self):
        """Test data generation with _template columns

        """
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("txt1", "string", template=r"dr_\v", random=True, escapeSpecialChars=True)
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

        dfOutput = df1.where("id = 0")

        value0 = dfOutput.collect()[0]["txt1"]
        self.assertEqual(value0, "dr_0")

    def test_ILText_column_random2(self):
        """Test data generation with _template columns

        """
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_random_seed_flow(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        default_random_seed = dg.DEFAULT_RANDOM_SEED

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, random_seed=2021)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True)
                          )

        self.assertEqual(pluginDataspec.randomSeed, dg.DEFAULT_RANDOM_SEED)

        code1Spec = pluginDataspec.getColumnSpec("code1")
        self.assertEqual(code1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED)
        code2Spec = pluginDataspec.getColumnSpec("code2")
        self.assertEqual(code2Spec.randomSeed, 2021)

        textSpec = pluginDataspec.getColumnSpec("text")
        self.assertEqual(textSpec.randomSeed, dg.DEFAULT_RANDOM_SEED)

        textSpec2 = pluginDataspec.getColumnSpec("text2")
        self.assertEqual(textSpec2.textGenerator.randomSeed, dg.DEFAULT_RANDOM_SEED)

        ilTextSpec = pluginDataspec.getColumnSpec("paras")
        self.assertEqual(ilTextSpec.randomSeed, dg.DEFAULT_RANDOM_SEED)

        ilTextSpec2 = pluginDataspec.getColumnSpec("paras2")
        self.assertEqual(ilTextSpec2.textGenerator.randomSeed, dg.DEFAULT_RANDOM_SEED)

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed)
        self.assertEqual(textSpec2.randomSeed, textSpec2.textGenerator.randomSeed)
        self.assertEqual(ilTextSpec.randomSeed, ilTextSpec.textGenerator.randomSeed)
        self.assertEqual(ilTextSpec2.randomSeed, ilTextSpec2.textGenerator.randomSeed)


    def test_random_seed_flow2(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        effective_random_seed = dg.DEFAULT_RANDOM_SEED

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, random=True)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True)
                          )

        self.assertEqual(pluginDataspec.randomSeed, effective_random_seed)

        code1Spec = pluginDataspec.getColumnSpec("code1")
        self.assertEqual(code1Spec.randomSeed, effective_random_seed, "code1")

        textSpec = pluginDataspec.getColumnSpec("text")
        self.assertEqual(textSpec.randomSeed, effective_random_seed, "text")

        code2Spec = pluginDataspec.getColumnSpec("code2")
        self.assertEqual(code1Spec.randomSeed, effective_random_seed, "code2")

        text2Spec = pluginDataspec.getColumnSpec("text2")
        self.assertEqual(text2Spec.randomSeed, effective_random_seed, "text2")

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed)
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed)

    def test_random_seed_flow3(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        effective_random_seed = 1017

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows,
                                           partitions=partitions_requested,
                                           seed=effective_random_seed)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, random=True)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True)
                          )

        self.assertEqual(pluginDataspec.randomSeed, effective_random_seed, "dataspec")

        code1Spec = pluginDataspec.getColumnSpec("code1")
        textSpec = pluginDataspec.getColumnSpec("text")
        code2Spec = pluginDataspec.getColumnSpec("code2")
        text2Spec = pluginDataspec.getColumnSpec("text2")

        self.assertEqual(code1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED, "code1")

        self.assertEqual(textSpec.randomSeed, dg.DEFAULT_RANDOM_SEED, "text")

        self.assertEqual(code2Spec.randomSeed, effective_random_seed, "code2")

        self.assertEqual(text2Spec.randomSeed, effective_random_seed, "text2")

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed)
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed)

    def test_seed_flow4(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        effective_random_seed = dg.RANDOM_SEED_RANDOM

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows,
                                           partitions=partitions_requested,
                                           seed=effective_random_seed)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, random=True)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True)
                          )

        self.assertEqual(pluginDataspec.randomSeed, effective_random_seed, "dataspec")

        code1Spec = pluginDataspec.getColumnSpec("code1")
        textSpec = pluginDataspec.getColumnSpec("text")
        code2Spec = pluginDataspec.getColumnSpec("code2")
        text2Spec = pluginDataspec.getColumnSpec("text2")

        self.assertEqual(code1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED, "code1")
        self.assertEqual(textSpec.randomSeed, dg.DEFAULT_RANDOM_SEED, "text")
        self.assertEqual(code2Spec.randomSeed, effective_random_seed, "code2")
        self.assertEqual(text2Spec.randomSeed, effective_random_seed, "text2")

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed)
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed)



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
