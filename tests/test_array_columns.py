import logging
import pytest

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestArrayColumns:
    testDataSpec = None
    dfTestData = None
    row_count = 1000
    column_count = 10

    def setUp(self):
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(format=FORMAT)

    def test_basic_arrays(self):
        column_count = 10
        data_rows = 100 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   .withColumn("a", ArrayType(StringType()))
                   )

        df = df_spec.build()

        c = df.count()
        assert c == data_rows

    def test_basic_arrays2(self):
        column_count = 10
        data_rows = 100 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withColumn("a", ArrayType(StringType()))
                   )

        df = df_spec.build()
        c = df.count()
        assert c == data_rows

    def test_basic_arrays_with_lit1(self):
        data_rows = 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   .withColumn("a", ArrayType(StringType()), expr="array(1,2,3,4)")
                   .withColumn("b", "array<string>", expr="array(1,2,3,4)")
                   )

        df = df_spec.build()
        c = df.count()
        assert c == data_rows

    def test_basic_arrays_with_lit2(self):
        data_rows = 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("code1", "int", minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   .withColumn("a", ArrayType(StringType()), expr="array('one','two','three')")
                   .withColumn("b", "array<string>", expr="array('one','two','three')")
                   )

        df = df_spec.build()
        c = df.count()
        assert c == data_rows

    def test_basic_arrays_with_columns(self):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count)
                   .withColumn("r_array", "array<float>", expr="array(r_0, r_1, r_2, r_3)")
                   )

        df = df_spec.build()
        c = df.count()
        assert c == data_rows
        df.show()

    def test_basic_arrays_with_columns2(self):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count)
                   )

        df = df_spec.build()
        df.show()

    def test_basic_arrays_with_columns3(self):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", ArrayType(FloatType()), expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count)
                   )

        df = df_spec.build()
        df.show()

    def test_basic_arrays_with_columns4(self):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count, structType="array")
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   )

        df = df_spec.build()
        df.show()

    def test_basic_arrays_with_columns5(self):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", FloatType(), minValue=1.0, maxValue=10.0, step=0.1,
                               numColumns=column_count, structType="array")
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   )

        df = df_spec.build()
        df.show()
