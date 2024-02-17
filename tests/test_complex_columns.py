import logging

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType, MapType, \
    BinaryType, LongType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestComplexColumns:
    testDataSpec = None
    dfTestData = None
    row_count = 1000
    column_count = 10

    @pytest.mark.parametrize("complexFieldType, expectedType, invalidValueCondition",
                             [("array<int>", ArrayType(IntegerType()), "complex_field is not Null"),
                              ("array<array<string>>", ArrayType(ArrayType(StringType())), "complex_field is not Null"),
                              ("map<STRING, INT>", MapType(StringType(), IntegerType()), "complex_field is not Null"),
                              ("struct<a:binary, b:int, c:float>",
                               StructType([StructField("a", BinaryType()), StructField("b", IntegerType()),
                                           StructField("c", FloatType())]),
                               "complex_field is not Null"
                               )
                              ])
    def test_uninitialized_complex_fields(self, complexFieldType, expectedType, invalidValueCondition, setupLogging):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   .withColumn("complex_field", complexFieldType)
                   )

        df = df_spec.build()
        assert df is not None, "Ensure dataframe can be created"

        complex_type = df.schema["complex_field"].dataType
        assert complex_type == expectedType

        invalid_data_count = df.where(invalidValueCondition).count()
        assert invalid_data_count == 0, "Not expecting invalid values"

    @pytest.mark.parametrize("complexFieldType, expectedType, invalidValueCondition",
                             [("array<int>", ArrayType(IntegerType()), "complex_field is not Null"),
                              ("array<array<string>>", ArrayType(ArrayType(StringType())), "complex_field is not Null"),
                              ("map<STRING, INT>", MapType(StringType(), IntegerType()), "complex_field is not Null"),
                              ("struct<a:binary, b:int, c:float>",
                               StructType([StructField("a", BinaryType()), StructField("b", IntegerType()),
                                           StructField("c", FloatType())]),
                               "complex_field is not Null"
                               )
                              ])
    def test_unitialized_complex_fields2(self, complexFieldType, expectedType, invalidValueCondition, setupLogging):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withColumn("complex_field", complexFieldType)
                   )

        df = df_spec.build()
        assert df is not None, "Ensure dataframe can be created"

        complex_type = df.schema["complex_field"].dataType
        assert complex_type == expectedType

        invalid_data_count = df.where(invalidValueCondition).count()
        assert invalid_data_count == 0, "Not expecting invalid values"

    @pytest.mark.parametrize("complexFieldType, expectedType, valueInitializer, validValueCondition",
                             [("array<int>", ArrayType(IntegerType()), "array(1,2,3)",
                               "complex_field[1] = 2"),
                              ("array<array<string>>", ArrayType(ArrayType(StringType())), "array(array('one','two'))",
                               "complex_field is not Null and size(complex_field) = 1"),
                              ("map<STRING, INT>", MapType(StringType(), IntegerType()), "map('hello',1, 'world', 2)",
                               "complex_field is not Null and complex_field['hello'] = 1"),
                              ("struct<a:string, b:int, c:float>",
                               StructType([StructField("a", StringType()), StructField("b", IntegerType()),
                                           StructField("c", FloatType())]),
                               "named_struct('a', 'hello, world', 'b', 42, 'c', 0.25)",
                               "complex_field is not Null and complex_field.c = 0.25"
                               ),
                              ("struct<a:string, b:int, c:int>",
                               StructType([StructField("a", StringType()), StructField("b", IntegerType()),
                                           StructField("c", IntegerType())]),
                               "named_struct('a', code3, 'b', code1, 'c', code2)",
                               "complex_field is not Null and complex_field.c = code2"
                               )
                              ])
    def test_initialized_complex_fields(self, complexFieldType, expectedType, valueInitializer, validValueCondition,
                                        setupLogging):
        data_rows = 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   .withColumn("complex_field", complexFieldType, expr=valueInitializer,
                               baseColumn=['code1', 'code2', 'code3', 'code4', 'code5'])
                   )

        df = df_spec.build()
        assert df is not None, "Ensure dataframe can be created"

        complex_type = df.schema["complex_field"].dataType
        assert complex_type == expectedType

        valid_data_count = df.where(validValueCondition).count()
        assert valid_data_count == data_rows, "Not expecting invalid values"

    def test_basic_arrays_with_columns(self, setupLogging):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count, structType="array")
                   )

        df = df_spec.build()
        df.show()

    def test_basic_arrays_with_columns2(self, setupLogging):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                    partitions=spark.sparkContext.defaultParallelism)
                   .withIdOutput()
                   .withColumn("r", ArrayType(FloatType()), expr="array(floor(rand() * 350) * (86400 + 3600))",
                               numColumns=column_count)
                   )

        df = df_spec.build()
        df.show()

    def test_basic_arrays_with_columns4(self, setupLogging):
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

    def test_basic_arrays_with_columns5(self, setupLogging):
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

    def test_basic_arrays_with_existing_schema(self, arraySchema, setupLogging):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumn("anotherValue")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema2(self, arraySchema, setupLogging):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", numColumns=4, structType="array")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema3(self, arraySchema, setupLogging):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="array(1,2,3)")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema4(self, arraySchema, setupLogging):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="array(1,2,3)", numColumns=4, structType="array")
                )
        df = gen1.build()
        df.show()

    def test_basic_arrays_with_existing_schema6(self, arraySchema, setupLogging):
        print(f"schema: {arraySchema}")

        gen1 = (dg.DataGenerator(sparkSession=spark, name="array_schema", rows=10, partitions=2)
                .withSchema(arraySchema)
                .withColumnSpec("arrayVal", expr="array(id+1)")
                )
        df = gen1.build()
        assert df is not None
        df.show()

    def test_use_of_struct_in_schema1(self, setupLogging):
        # while this is not ideal form, ensure that it is tolerated to address reported issue
        # note there is no initializer for the struct and there is an override of the default `id` field
        struct_type = StructType([
            StructField('id', LongType(), True),
            StructField("city", StructType([
                StructField('id', LongType(), True),
                StructField('population', LongType(), True)
            ]), True)])

        gen1 = (dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=10000, partitions=4)
                .withSchema(struct_type)
                .withColumn("id")
                )
        res1 = gen1.build(withTempView=True)
        assert res1.count() == 10000

    def test_use_of_struct_in_schema2(self, setupLogging):
        struct_type = StructType([
            StructField('id', LongType(), True),
            StructField("city", StructType([
                StructField('id', LongType(), True),
                StructField('population', LongType(), True)
            ]), True)])

        gen1 = (dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=10000, partitions=4)
                .withSchema(struct_type)
                .withColumnSpec("city", expr="named_struct('id', id, 'population', id * 1000)")
                )
        res1 = gen1.build(withTempView=True)
        assert res1.count() == 10000

    def test_varying_arrays(self, setupLogging):
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=1000, random=True)
                   .withColumn("r", "float", minValue=1.0, maxValue=10.0, step=0.1,
                               numColumns=5)
                   .withColumn("observations", "array<float>",
                               expr="slice(array(r_0, r_1, r_2, r_3, r_4), 1, abs(hash(id)) % 5 + 1 )",
                               baseColumn="r")
                   )

        df = df_spec.build()
        df.show()

    def test_array_values(self):
        df_spec = dg.DataGenerator(spark, name="test-data", rows=2)
        df_spec = df_spec.withColumn(
            "test",
            ArrayType(StringType()),
            values=[
                F.array(F.lit("A")),
                F.array(F.lit("C")),
                F.array(F.lit("T")),
                F.array(F.lit("G")),
            ],
        )
        test_df = df_spec.build()

        rows = test_df.collect()

        for r in rows:
            assert r['test'] is not None

    def test_single_element_array(self):
        df_spec = dg.DataGenerator(spark, name="test-data", rows=2)
        df_spec = df_spec.withColumn(
            "test1",
            ArrayType(StringType()),
            values=[
                F.array(F.lit("A")),
                F.array(F.lit("C")),
                F.array(F.lit("T")),
                F.array(F.lit("G")),
            ],
        )
        df_spec = df_spec.withColumn(
            "test2", "string", structType="array", numFeatures=1, values=["one", "two", "three"]
        )
        df_spec = df_spec.withColumn(
            "test3", "string", structType="array", numFeatures=(1, 1), values=["one", "two", "three"]
        )
        df_spec = df_spec.withColumn(
            "test4", "string", structType="array", values=["one", "two", "three"]
        )

        test_df = df_spec.build()

        for field in test_df.schema:
            assert isinstance(field.dataType, ArrayType)

    def test_map_values(self):
        df_spec = dg.DataGenerator(spark, name="test-data", rows=50, random=True)
        df_spec = df_spec.withColumn(
            "v1",
            "array<string>",
            values=[
                F.array(F.lit("A")),
                F.array(F.lit("C")),
                F.array(F.lit("T")),
                F.array(F.lit("G")),
            ],
        )
        df_spec = df_spec.withColumn(
            "v2",
            "array<string>",
            values=[
                F.array(F.lit("one")),
                F.array(F.lit("two")),
                F.array(F.lit("three")),
                F.array(F.lit("four")),
            ],
        )
        df_spec = df_spec.withColumn(
            "v3",
            "array<string>",
            values=[
                F.array(F.lit("alpha")),
                F.array(F.lit("beta")),
                F.array(F.lit("delta")),
                F.array(F.lit("gamma")),
            ],
        )
        df_spec = df_spec.withColumn(
            "v4",
            "string",
            values=["this", "is", "a", "test"],
            numFeatures=1,
            structType="array"
        )

        df_spec = df_spec.withColumn(
            "test",
            "map<string,string>",
            values=[F.map_from_arrays(F.col("v1"), F.col("v2")),
                    F.map_from_arrays(F.col("v1"), F.col("v3")),
                    F.map_from_arrays(F.col("v2"), F.col("v3")),
                    F.map_from_arrays(F.col("v1"), F.col("v4")),
                    F.map_from_arrays(F.col("v2"), F.col("v4")),
                    F.map_from_arrays(F.col("v3"), F.col("v4"))
                    ],
            baseColumns=["v1", "v2", "v3", "v4"]
        )
        test_df = df_spec.build()

        rows = test_df.collect()

        for r in rows:
            assert r['test'] is not None
