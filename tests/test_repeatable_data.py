import logging
import unittest

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType, TimestampType

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
    def mkBasicDataspec(cls, withRandom=False, dist=None, randomSeed=None):

        if randomSeed is None:
            dgSpec = dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count)
        else:
            dgSpec = dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                      randomSeed=randomSeed, randomSeedMethod='hash_fieldname')

        testDataSpec = (dgSpec
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

    def test_basic_repeatability_random_normal2(self):
        """Test basic data repeatability"""

        ds1 = self.mkBasicDataspec(withRandom=True, dist="normal", randomSeed=42)
        df1 = ds1.build()

        ds2 = self.mkBasicDataspec(withRandom=True, dist="normal", randomSeed=42)
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_basic_clone(self):
        """Test clone method"""
        ds1 = self.mkBasicDataspec(withRandom=False, dist="normal")
        df1 = ds1.build()

        ds2 = ds1.clone()
        df2 = ds2.build()

        self.checkTablesEqual(df1, df2)

    def test_basic_clone_with_random(self):
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

        txt1Spec = ds1.getColumnSpec("txt1")
        self.assertEqual(txt1Spec.randomSeed, txt1Spec.textGenerator.randomSeed)

        self.checkTablesEqual(df1, df2)

    def test_template_column_random(self):
        """Test data generation with _template columns"""
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("txt1", "string", template=r"\dr_\v", random=True)
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        txt1Spec = ds1.getColumnSpec("txt1")
        self.assertEqual(txt1Spec.randomSeed, txt1Spec.textGenerator.randomSeed)

        self.checkTablesEqual(df1, df2)

    def test_template_column_random2(self):
        """Test data generation with _template columns

        """
        ds1 = (self.mkBasicDataspec(withRandom=True)
               .withColumn("txt1", "string", template=r"dr_\v", random=True, escapeSpecialChars=True)
               .withColumn("nonRandom", "string", baseColumn="code1")
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

        txt1Spec = ds1.getColumnSpec("txt1")
        txt2Spec = ds2.getColumnSpec("txt1")
        self.assertEqual(txt1Spec.randomSeed, txt2Spec.randomSeed)
        self.assertEqual(txt1Spec.randomSeed, txt1Spec.textGenerator.randomSeed)
        self.assertEqual(txt2Spec.randomSeed, txt2Spec.textGenerator.randomSeed)

        self.assertTrue(txt1Spec.isRandom)

        nonRandomColSpec = ds1.getColumnSpec("nonRandom")
        self.assertFalse(nonRandomColSpec.isRandom)

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

    def test_ILText_column_random3(self):
        """Test data generation with _template columns

        """
        ds1 = (self.mkBasicDataspec(withRandom=True, randomSeed=41)
               .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
               )
        df1 = ds1.build()
        ds2 = ds1.clone()
        df2 = ds2.build()

    def test_random_seed_flow(self):
        data_rows = 100 * 1000

        default_random_seed = dg.DEFAULT_RANDOM_SEED

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, randomSeed=2021)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True)
                          )

        self.assertEqual(pluginDataspec.randomSeed, dg.DEFAULT_RANDOM_SEED)

        code1Spec = pluginDataspec.getColumnSpec("code1")
        self.assertIsNotNone(code1Spec.randomSeed)
        code2Spec = pluginDataspec.getColumnSpec("code2")
        self.assertEqual(code2Spec.randomSeed, 2021, "code2")

        textSpec = pluginDataspec.getColumnSpec("text")
        self.assertIsNotNone(textSpec.randomSeed)

        textSpec2 = pluginDataspec.getColumnSpec("text2")
        self.assertIsNotNone(textSpec2.textGenerator.randomSeed)

        ilTextSpec = pluginDataspec.getColumnSpec("paras")
        self.assertIsNotNone(ilTextSpec.randomSeed)

        ilTextSpec2 = pluginDataspec.getColumnSpec("paras2")
        self.assertIsNotNone(ilTextSpec2.textGenerator.randomSeed)

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed)
        self.assertEqual(textSpec2.randomSeed, textSpec2.textGenerator.randomSeed)
        self.assertEqual(ilTextSpec.randomSeed, ilTextSpec.textGenerator.randomSeed)
        self.assertEqual(ilTextSpec2.randomSeed, ilTextSpec2.textGenerator.randomSeed)

    def test_random_seed_flow2(self):
        data_rows = 100 * 1000

        effective_random_seed = dg.DEFAULT_RANDOM_SEED

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, random=True)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True)
                          )

        self.assertEqual(pluginDataspec.randomSeed, effective_random_seed)

        code1Spec = pluginDataspec.getColumnSpec("code1")
        self.assertIsNotNone(code1Spec.randomSeed, "code1")

        textSpec = pluginDataspec.getColumnSpec("text")
        self.assertIsNotNone(textSpec.randomSeed, "text")

        code2Spec = pluginDataspec.getColumnSpec("code2")
        self.assertIsNotNone(code1Spec.randomSeed, "code2")

        text2Spec = pluginDataspec.getColumnSpec("text2")
        self.assertIsNotNone(text2Spec.randomSeed, "text2")

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed)
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed)

    def test_random_seed_flow_explicit_instance(self):
        """ Check the explicit random seed is applied to all columns"""
        data_rows = 100 * 1000

        effective_random_seed = 1017

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, randomSeed=effective_random_seed)
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
        paras1Spec = pluginDataspec.getColumnSpec("paras")
        paras2Spec = pluginDataspec.getColumnSpec("paras2")

        self.assertEqual(code1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED, "code1")

        self.assertEqual(textSpec.randomSeed, dg.DEFAULT_RANDOM_SEED, "text")

        self.assertEqual(code2Spec.randomSeed, effective_random_seed, "code2")

        self.assertEqual(text2Spec.randomSeed, effective_random_seed, "text2")

        self.assertEqual(paras1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED, "paras1")
        self.assertEqual(paras2Spec.randomSeed, effective_random_seed, "paras2")

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed, "textSpec with textGenerator")
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed, "text2Spec with textGenerator")
        self.assertEqual(paras1Spec.randomSeed, paras1Spec.textGenerator.randomSeed, "paras1Spec with textGenerator")
        self.assertEqual(paras2Spec.randomSeed, paras2Spec.textGenerator.randomSeed, "paras2Spec with textGenerator")

    def test_random_seed_flow_hash_fieldname(self):
        """ Check the explicit random seed is applied to all columns"""
        data_rows = 100 * 1000

        effective_random_seed = 1017

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows,
                                           randomSeed=effective_random_seed,
                                           randomSeedMethod=dg.RANDOM_SEED_HASH_FIELD_NAME)
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
        paras1Spec = pluginDataspec.getColumnSpec("paras")
        paras2Spec = pluginDataspec.getColumnSpec("paras2")

        self.assertTrue(code1Spec.randomSeed is not None and code1Spec.randomSeed >= 0)
        self.assertTrue(textSpec.randomSeed is not None and textSpec.randomSeed >= 0)
        self.assertTrue(code2Spec.randomSeed is not None and code2Spec.randomSeed >= 0)
        self.assertTrue(text2Spec.randomSeed is not None and text2Spec.randomSeed >= 0)
        self.assertTrue(paras1Spec.randomSeed is not None and paras1Spec.randomSeed >= 0)
        self.assertTrue(paras2Spec.randomSeed is not None and paras2Spec.randomSeed >= 0)

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed, "textSpec with textGenerator")
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed, "text2Spec with textGenerator")
        self.assertEqual(paras1Spec.randomSeed, paras1Spec.textGenerator.randomSeed, "paras1Spec with textGenerator")
        self.assertEqual(paras2Spec.randomSeed, paras2Spec.textGenerator.randomSeed, "paras2Spec with textGenerator")

    def test_random_seed_flow3_true_random(self):
        """ Check the explicit random seed (-1) is applied to all columns"""
        data_rows = 100 * 1000

        effective_random_seed = -1
        explicitRandomSeed = 41

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows,
                                           randomSeed=effective_random_seed)
                          .withColumn("code1", minValue=0, maxValue=100)
                          .withColumn("code2", minValue=0, maxValue=100, random=True)
                          .withColumn("text", "string", template=r"dr_\\v")
                          .withColumn("text2", "string", template=r"dr_\\v", random=True, randomSeed=explicitRandomSeed)
                          .withColumn("paras", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))
                          .withColumn("paras2", text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)), random=True,
                                      randomSeedMethod="fixed")
                          )

        self.assertEqual(pluginDataspec.randomSeed, effective_random_seed, "dataspec")

        code1Spec = pluginDataspec.getColumnSpec("code1")
        textSpec = pluginDataspec.getColumnSpec("text")
        code2Spec = pluginDataspec.getColumnSpec("code2")
        text2Spec = pluginDataspec.getColumnSpec("text2")
        paras1Spec = pluginDataspec.getColumnSpec("paras")
        paras2Spec = pluginDataspec.getColumnSpec("paras2")

        self.assertEqual(code1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED, "code1")

        self.assertEqual(textSpec.randomSeed, dg.DEFAULT_RANDOM_SEED, "text")

        self.assertEqual(code2Spec.randomSeed, effective_random_seed, "code2")

        self.assertEqual(text2Spec.randomSeed, explicitRandomSeed, "text2")

        self.assertEqual(paras1Spec.randomSeed, dg.DEFAULT_RANDOM_SEED, "paras1")
        self.assertEqual(paras2Spec.randomSeed, effective_random_seed, "paras2")

        self.assertEqual(textSpec.randomSeed, textSpec.textGenerator.randomSeed, "textSpec with textGenerator")
        self.assertEqual(text2Spec.randomSeed, text2Spec.textGenerator.randomSeed, "text2Spec with textGenerator")
        self.assertEqual(paras1Spec.randomSeed, paras1Spec.textGenerator.randomSeed, "paras1Spec with textGenerator")
        self.assertEqual(paras2Spec.randomSeed, paras2Spec.textGenerator.randomSeed, "paras2Spec with textGenerator")

    def test_random_seed_flow3a(self):
        data_rows = 100 * 1000

        effective_random_seed = 1017

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows,
                                           randomSeed=effective_random_seed,
                                           randomSeedMethod=dg.RANDOM_SEED_FIXED)
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
        paras1Spec = pluginDataspec.getColumnSpec("paras")
        paras2Spec = pluginDataspec.getColumnSpec("paras2")

        self.assertTrue(code1Spec.randomSeed is not None and code1Spec.randomSeed >= 0)
        self.assertTrue(textSpec.randomSeed is not None and textSpec.randomSeed >= 0)
        self.assertTrue(code2Spec.randomSeed is not None and code2Spec.randomSeed >= 0)
        self.assertTrue(text2Spec.randomSeed is not None and text2Spec.randomSeed >= 0)
        self.assertTrue(paras1Spec.randomSeed is not None and paras1Spec.randomSeed >= 0)
        self.assertTrue(paras2Spec.randomSeed is not None and paras2Spec.randomSeed >= 0)

    def test_seed_flow4(self):
        data_rows = 100 * 1000

        effective_random_seed = dg.RANDOM_SEED_RANDOM

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, randomSeed=effective_random_seed)
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
