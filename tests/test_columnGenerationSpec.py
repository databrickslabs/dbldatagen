import unittest

from pyspark.sql.types import StringType, TimestampType

import databrickslabs_testdatagenerator as dg


class TestColumnGenerationSpec(unittest.TestCase):
    def test_getNames(self):
        cd = dg.ColumnGenerationSpec(name="test")
        results = cd.getNames()
        self.assertEqual(results, ['test'])

    def test_getNames2(self):
        cd = dg.ColumnGenerationSpec(name="test", numColumns=3)
        results = cd.getNames()
        self.assertEqual(results, ['test_0', 'test_1', 'test_2'])

    def test_isFieldOmitted(self):
        cd = dg.ColumnGenerationSpec(name="test", omit=True)
        self.assertTrue(cd.omit)

    def test_colType(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType())
        self.assertEqual(cd.datatype, dt)

        dt2 = TimestampType()
        cd2 = dg.ColumnGenerationSpec(name="test", colType=TimestampType())
        self.assertEqual(cd2.datatype, dt2)

    def test_prefix(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), prefix="test_")
        self.assertEqual(cd.prefix, "test_")

    def test_suffix(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), suffix="_test")
        self.assertEqual(cd.suffix, "_test")

    def test_baseColumn(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), base_column='test0')
        self.assertEqual(cd.baseColumn, 'test0', "baseColumn should be as expected")
        self.assertEqual(cd.baseColumns, ['test0'])

    def test_baseColumnMultiple(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), base_column=['test0', 'test_1'])
        self.assertEqual(cd.baseColumn, ['test0', 'test_1'], "baseColumn should be as expected")
        self.assertEqual(cd.baseColumns, ['test0', 'test_1'])

    def test_baseColumnMultiple2(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), base_column='test0,test_1')
        self.assertEqual(cd.baseColumn, 'test0,test_1', "baseColumn should be as expected")
        self.assertEqual(cd.baseColumns, ['test0', 'test_1'])

    def test_expr(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), base_column='test0,test_1', expr="concat(1,2)")
        self.assertEqual(cd.expr, 'concat(1,2)')
