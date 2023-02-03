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
        df.show()

    def test_basic_arrays2(self):
        column_count = 10
        data_rows = 100 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withColumn("a", ArrayType(StringType()))
                   )

        df = df_spec.build()
        df.show()

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
                   )

        df = df_spec.build()
        print(df.schema)

        field_type =  df.schema.fields['a']

        print(field_type)

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
                   )

        df = df_spec.build()
        df.show()

    def test_basic_arrays_with_columns(self):
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

    @pytest.fixture
    def arraySchema(self):
        spark.sql("create database if not exists test_array_db")
        spark.sql("create table test_array_db.array_test(id long, arrayVal array<INT>) using parquet")
        df = spark.sql("select * from test_array_db.array_test")
        yield df.schema
        spark.sql("drop table test_array_db.array_test")
        spark.sql("drop database if exists test_array_db")



    def test_basic_arrays_with_existing_schema(self, arraySchema):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumn("anotherValue")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema2(self, arraySchema):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", numColumns=4, structType="array")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema3(self, arraySchema):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="array(1,2,3)")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema4(self, arraySchema):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="array(1,2,3)", numColumns=4, structType="array")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema5(self, arraySchema):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="id", numColumns=4, structType="array")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema6(self, arraySchema):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="id+1")
                )
        df = gen1.build()
        df.show()










