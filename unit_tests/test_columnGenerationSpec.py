from unittest import TestCase
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as dg

import unittest


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

    def test_baseColumn(self):
        dt = StringType()
        cd = dg.ColumnGenerationSpec(name="test", colType=StringType(), base_column='test0')
        self.assertEqual(cd.baseColumn, 'test0')
