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

    def test_inferred_column_types1(self, setupLogging):
        column_count = 10
        data_rows = 10 * 1000
        import dbldatagen as dg

        from pyspark.sql.types import LongType, FloatType, IntegerType, \
            StringType, DoubleType, BooleanType, ShortType, \
            TimestampType, DateType, DecimalType, ByteType, \
            BinaryType, ArrayType, MapType, StructType, StructField

        from collections import namedtuple
        import dbldatagen as dg
        from dbldatagen import INFER_DATATYPE
        import logging

        os = ['MacOS', 'Linux', 'Windows', 'iOS', 'Andoid']
        os_edition = ['RHEL 5.0', 'Windows 7', 'Windows 10', 'Windows XP', 'Mac OS X 10.8']
        linux_distro = ['Ubuntu', 'Fedora', 'Kubuntu', 'Arch', 'Alpine']
        boolean_values = ['True']
        dot_net_version = ['.NET 8.0', '.NET 7.0', '.NET 6.0', '.NET Core 3.0']
        browser_label = ['Chrome', 'Edge', 'Firefox', 'Safari']
        browserver = ['Version 113.0.5672.126']
        osver = ['13.3.1 (22E261)']

        dataspec = dg.DataGenerator(spark, name="device_data_set", rows=10000, randomSeedMethod='hash_fieldname')

        dataspec = (dataspec
                    # Design
                    #
                    # v2.0 - updated so taht script outputs JSON only to mimic flow from Cisco edge

                    # Reporting Identifiers
                    .withColumn("dd_id", "long", minValue=1, maxValue=100000, random=True, omit=True)
                    .withColumn("internal_device_id", "integer", baseColumnType="hash",
                                omit=True)  # internal id is hash of `id` column
                    .withColumn("duo_device_id", "string", format="0x%010x", baseColumnType="values",
                                baseColumn="internal_device_id")  # format hash value as string
                    .withColumn("org_id", "integer", minValue=1, maxValue=100000, random=True, omit=True)
                    .withColumn("user_id", "integer", minValue=1000000, maxValue=9999999, random=True, omit=True)
                    .withColumn("anyconnect_id", "string", format="0x%015x", minValue=1, maxValue=1000000,
                                random=True, omit=True)
                    .withColumn("ztna_device_id", "string", format="0x%015x", minValue=1, maxValue=1000000,
                                random=True, omit=True)
                    .withColumn("reporting_identifiers", INFER_DATATYPE,
                                expr="named_struct('org_id', org_id ,'dd_id', dd_id ,'user_id',user_id,'anyconnect_id',anyconnect_id,'duo_device_id',duo_device_id,'ztna_device_id',ztna_device_id)",
                                baseColumn=['org_id', 'dd_id', 'user_id', 'anyconnect_id', 'duo_device_id',
                                            'ztna_device_id']
                                , omit=True)

                    .withColumn("txid", "string",  template=r'\XXX-XXXXXXXXXXXXXXX', random=True,
                                omit=True)
                    .withColumn("duo_client_version", "string", format="1.0.%x", minValue=1, maxValue=9,
                                random=True, omit=True)
                    .withColumn("os", "string", values=os, random=True, omit=True)
                    .withColumn("os_version", "string", values=osver, omit=True)
                    # .withColumn("os_version", "string", format="10.10.%x", minValue=1, maxValue=9, random=True, omit=True)
                    .withColumn("os_build", "string", format="19042.%x", minValue=1000, maxValue=9999, random=True,
                                omit=True)
                    .withColumn("os_edition", "string", values=os_edition, random=True, omit=True)
                    .withColumn("linux_distro", "string", values=linux_distro, random=True, omit=True)
                    .withColumn("device_name", "string", template=r'\W', random=True, omit=True)
                    .withColumn("is_password_set", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("is_encryption_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("is_firewall_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("is_screen_lock_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("is_auto_login_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("seconds_before_screen_lock", "integer", minValue=1, maxValue=60, random=True,
                                omit=True)

                    .withColumn("is_active", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("product_label", "string", template=r'\W', random=True, omit=True)
                    .withColumn("version", "string", template=r'\W', random=True, omit=True)
                    .withColumn("instance_guid", "string", template=r'\XXXXXX-\XXXXXX-\XXXXXX-\XXXXXX', random=True,
                                omit=True)

                    # struct<is_active:boolean, product_label:string, version:string, instance_guid:string>
                    .withColumn("security_agents", INFER_DATATYPE,
                                expr="named_struct('is_active', is_active, 'product_label', product_label,'version', version, 'instance_guid', instance_guid)",
                                baseColumn=['is_active', 'product_label', 'version', 'instance_guid']
                                , omit=True)

                    # .withColumn("security_agents", "struct<is_active:boolean, product_label:string, version:string, instance_guid:string>",
                    #            expr="named_struct('is_active', is_active, 'product_label', product_label,'version', version, 'instance_guid', instance_guid)",
                    #            baseColumn=['is_active', 'product_label','version','instance_guid']
                    #            ,omit=True)

                    .withColumn("communication_scheme", "string", template=r'\W', random=True, omit=True)
                    .withColumn("health_check_end_timestamp", "string", percentNulls=1, omit=True)
                    .withColumn("health_check_start_timestamp", "string", percentNulls=1, omit=True)
                    .withColumn("health_check_length_millis", "string", percentNulls=1, omit=True)
                    .withColumn("browser_label", "string", percentNulls=1, omit=True)
                    # .withColumn("browser_version", "string", percentNulls=1, omit=True)
                    .withColumn("browser_version", "string", values=browserver, omit=True)
                    .withColumn("browser_process_name", "string", percentNulls=1, omit=True)

                    .withColumn("valid", "string", percentNulls=1, omit=True)
                    .withColumn("common_name", "string", percentNulls=1, omit=True)
                    .withColumn("team_id", "string", percentNulls=1, omit=True)
                    .withColumn("browser_process_info", INFER_DATATYPE,
                                expr="named_struct('valid', valid, 'common_name', common_name,'team_id', team_id)",
                                baseColumn=['valid', 'common_name', 'team_id']
                                , omit=True)

                    .withColumn("is_virtual_machine", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("is_health_check_retrying", "string", percentNulls=1, omit=True)
                    .withColumn("domain_name", "string", template=r'www.\w.com|\w.\w.co.u\k', omit=True)
                    .withColumn("host_name", "string", template=r'\w', omit=True)
                    .withColumn("dot_net_version", "string", values=dot_net_version, random=True, omit=True)
                    .withColumn("wifi_fingerprint", "string", template=r'\XXX-XXXXXXXXXXXXXXX', random=True,
                                omit=True)
                    .withColumn("wifi_fingerprint_age_seconds", "integer", minValue=1, maxValue=720, random=True,
                                omit=True)
                    .withColumn("wifi_fingerprint_includes_bssid", "boolean", values=boolean_values, random=True,
                                omit=True)

                    .withColumn("desktop_session_token", "string", template=r'\XXX-XXXXXXXXXXXX-XXX', random=True,
                                omit=True)
                    .withColumn("desktop_session", StructType([StructField('desktop_session_token', StringType())]),
                                expr="named_struct('desktop_session_token', desktop_session_token)",
                                baseColumn=['desktop_session_token']
                                , omit=True)

                    .withColumn("machine_guid", "string", template=r'\XXXXXX-\XXXXXX-\XXXXXX', random=True,
                                omit=True)
                    .withColumn("hardware_uuid", "string", template=r'\XXXXXXXX-\XXXX-\XXXX-\XXXX-\XXXXXXXXXXXX',
                                random=True, omit=True)
                    .withColumn("domain_sid", "string", format="0x%010x", minValue=1, maxValue=1000000, random=True,
                                omit=True)
                    .withColumn("computer_sid", "string", format="0x%010x", minValue=1, maxValue=1000000,
                                random=True, omit=True)
                    .withColumn("intune_id", "string", format="0x%010x", minValue=1, maxValue=1000000, random=True,
                                omit=True)
                    .withColumn("amp_guid", "string", template=r'\XXXXXX-\XXXXXX-\XXXXXX', random=True, omit=True)
                    .withColumn("omadm_device_client_id", "string", format="0x%010x", minValue=1, maxValue=1000000,
                                random=True, omit=True)
                    .withColumn("cpu_id", "string", format="0x%010x", minValue=1, maxValue=1000000, random=True,
                                omit=True)
                    .withColumn("identifiers", INFER_DATATYPE,
                                expr="named_struct('machine_guid', machine_guid, 'hardware_uuid', hardware_uuid,'domain_sid', domain_sid, \
                                'computer_sid', computer_sid, 'intune_id', intune_id, 'amp_guid', amp_guid, 'omadm_device_client_id', omadm_device_client_id, 'cpu_id',cpu_id )",
                                baseColumn=['machine_guid', 'hardware_uuid', 'domain_sid', 'computer_sid']
                                , omit=True)

                    .withColumn("mdm_id_collection_errors", "string", format="0x%010x", minValue=1,
                                maxValue=1000000, random=True, omit=True)

                    .withColumn("password_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("device_name_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("os_version_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("biometrics_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("encryption_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("firewall_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("security_agents_total_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("lock_screen_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("virtual_machine_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("auto_login_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("domain_name_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("host_name_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("machine_guid_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("domain_sid_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("compute_sid_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("intune_id_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("amp_guid_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("omadm_device_client_id_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("desktop_session_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("signature_validation_fl", FloatType(), percentNulls=1, omit=True)
                    .withColumn("individual_health_check_durations_millis",
                                INFER_DATATYPE,
                                expr="named_struct('password_fl', password_fl, 'device_name_fl', device_name_fl,'os_version_fl', os_version_fl,'biometrics_fl', biometrics_fl, 'encryption_fl', encryption_fl, 'firewall_fl', firewall_fl, 'security_agents_total_fl', security_agents_total_fl, 'lock_screen_fl',lock_screen_fl, 'virtual_machine_fl',virtual_machine_fl,'auto_login_fl',auto_login_fl,'domain_name_fl',domain_name_fl,'host_name_fl',host_name_fl,'machine_guid_fl',machine_guid_fl,'domain_sid_fl',domain_sid_fl,'compute_sid_fl',compute_sid_fl,'intune_id_fl',intune_id_fl,'amp_guid_fl',amp_guid_fl,'omadm_device_client_id_fl',omadm_device_client_id_fl,'desktop_session_fl',desktop_session_fl,'signature_validation_fl',signature_validation_fl)",
                                baseColumn=['password_fl', 'device_name_fl', 'os_version', 'biometrics_fl',
                                            'encryption_fl', 'firewall_fl', 'security_agents_total_fl',
                                            'lock_screen_fl', 'virtual_machine_fl', 'auto_login_fl', 'domain_name_fl',
                                            'host_name_fl', 'machine_guid_fl', 'domain_sid_fl', 'compute_sid_fl',
                                            'intune_id_fl', 'amp_guid_fl', 'omadm_device_client_id_fl',
                                            'desktop_session_fl', 'signature_validation_fl']
                                , omit=True)

                    .withColumn("app_updates_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("auto_launch_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("diagnostic_log_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("desktop_sessions_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("user_configuration", INFER_DATATYPE,
                                expr="named_struct('app_updates_enabled', app_updates_enabled, 'auto_launch_enabled', auto_launch_enabled,'diagnostic_log_enabled', diagnostic_log_enabled, 'desktop_sessions_enabled',desktop_sessions_enabled)",
                                baseColumn=['app_updates_enabled', 'auto_launch_enabled', 'diagnostic_log_enabled',
                                            'desktop_sessions_enabled']
                                , omit=True)

                    .withColumn("app_updates_enabled_sys", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("row_app_hidden", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("row_password_hidden", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("row_encryption_hidden", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("row_firewall_hidden", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("row_updates_hidden", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("automatic_updates_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("updater_disabled_by_admin", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("system_configuration",
                                INFER_DATATYPE,
                                expr="named_struct('app_updates_enabled_sys', app_updates_enabled_sys, 'row_app_hidden', row_app_hidden,'row_password_hidden', row_password_hidden, 'row_encryption_hidden',row_encryption_hidden, 'row_firewall_hidden', row_firewall_hidden,'row_updates_hidden',row_updates_hidden, 'automatic_updates_enabled',automatic_updates_enabled,'updater_disabled_by_admin',updater_disabled_by_admin )",
                                baseColumn=['app_updates_enabled_sys', 'row_app_hidden', 'row_password_hidden',
                                            'row_encryption_hidden', 'row_firewall_hidden', 'row_updates_hidden',
                                            'automatic_updates_enabled', 'updater_disabled_by_admin']
                                , omit=True)

                    .withColumn("feature_enabled", "boolean", values=boolean_values, random=True, omit=True)
                    .withColumn("connected_hostnames", "string", template=r'\w', omit=True)
                    .withColumn("duoconnect", INFER_DATATYPE,
                                expr="named_struct('feature_enabled', feature_enabled, 'connected_hostnames', connected_hostnames)",
                                baseColumn=['feature_enabled', 'connected_hostnames']
                                , omit=True)

                    .withColumn("supports_hardware_security", "boolean", values=boolean_values, random=True, omit=True)

                    # Posture payload
                    .withColumn("posture_payload", INFER_DATATYPE,
                                expr="named_struct('reporting_identifiers', reporting_identifiers,'txid', txid ,'duo_client_version', duo_client_version ,'os', os,'os_version', os_version ,'os_build', os_build ,'os_edition',  os_edition,'linux_distro', linux_distro ,'device_name', device_name ,'is_password_set', is_password_set,'is_encryption_enabled', is_encryption_enabled ,'is_firewall_enabled', is_firewall_enabled  ,'is_screen_lock_enabled',is_screen_lock_enabled,'is_auto_login_enabled', is_auto_login_enabled ,'seconds_before_screen_lock', seconds_before_screen_lock, 'security_agents', security_agents, 'communication_scheme',communication_scheme,'health_check_end_timestamp',health_check_end_timestamp,'health_check_start_timestamp',health_check_start_timestamp,'health_check_length_millis', health_check_length_millis,'browser_label', browser_label,'browser_version',browser_version, 'browser_process_name', browser_process_name, 'browser_process_info',browser_process_info, 'is_virtual_machine',is_virtual_machine, 'is_health_check_retrying', is_health_check_retrying,'domain_name',domain_name, 'host_name',host_name, 'dot_net_version', dot_net_version, 'wifi_fingerprint', wifi_fingerprint,'wifi_fingerprint_age_seconds',wifi_fingerprint_age_seconds,'wifi_fingerprint_includes_bssid',wifi_fingerprint_includes_bssid, 'desktop_session', desktop_session, 'identifiers',identifiers, 'mdm_id_collection_errors',mdm_id_collection_errors, 'individual_health_check_durations_millis',individual_health_check_durations_millis, 'user_configuration' , user_configuration, 'system_configuration', system_configuration, 'duoconnect',duoconnect, 'supports_hardware_security', supports_hardware_security )",
                                baseColumn=['reporting_identifiers', 'txid', 'duo_client_version', 'os', 'os_version',
                                            'os_build', 'os_edition', 'linux_distro', 'device_name', 'is_password_set',
                                            'is_encryption_enabled', 'is_firewall_enabled', 'is_screen_lock_enabled',
                                            'is_auto_login_enabled', 'seconds_before_screen_lock', 'security_agents',
                                            'communication_scheme', 'health_check_end_timestamp',
                                            'health_check_start_timestamp', 'health_check_length_millis',
                                            'browser_label', 'browser_version', 'browser_process_name',
                                            'browser_process_info', 'is_virtual_machine', 'is_health_check_retrying',
                                            'domain_name', 'host_name', 'dot_net_version', 'wifi_fingerprint',
                                            'wifi_fingerprint_age_seconds', 'wifi_fingerprint_includes_bssid',
                                            'desktop_session', 'identifiers', 'mdm_id_collection_errors',
                                            'individual_health_check_durations_millis', 'user_configuration',
                                            'system_configuration', 'duoconnect', 'supports_hardware_security']
                                )  # Close Posture Payload
                    .withColumn("last_modified", "timestamp", expr="now()")
                    )

        dfTestData = dataspec.build()

        print(dfTestData.schema)

        data = dfTestData.limit(10).collect()

        for x in data:
            print(x)

    def test_inferred_column_types2(self, setupLogging):
        with pytest.raises(ValueError):
            column_count = 10
            data_rows = 10 * 1000
            df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows)
                       .withIdOutput()
                       .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                   numColumns=column_count, structType="array")
                       .withColumn("code1", "integer", minValue=100, maxValue=200)
                       .withColumn("code2", "integer", minValue=0, maxValue=10)
                       .withColumn("code3", dg.INFER_DATATYPE, values=['a', 'b', 'c'])
                       .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                       .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                       )

            df = df_spec.build()
            df.show()

    def test_inferred_with_schema(self):
        """Test use of schema"""
        schema = StructType([
            StructField("region_id", IntegerType(), True),
            StructField("region_cd", StringType(), True),
            StructField("c", StringType(), True),
            StructField("c1", StringType(), True),
            StructField("state1", StringType(), True),
            StructField("state2", StringType(), True),
            StructField("st_desc", StringType(), True),

        ])

        testDataSpec = (dg.DataGenerator(spark, name="test_data_set1", rows=10000)
                        .withSchema(schema)
                        )

        with pytest.raises(ValueError):
            testDataSpec2 = testDataSpec.withColumnSpecs(matchTypes=[dg.INFER_DATATYPE], minValue=0, maxValue=100)
            df = testDataSpec2.build()
            df.show()

    def test_inferred_column_types3(self, setupLogging):
        column_count = 10
        data_rows = 10 * 1000
        df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows)
                   .withIdOutput()
                   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count, structType="array")
                   .withColumn("code1", "integer", minValue=100, maxValue=200)
                   .withColumn("code2", "integer", minValue=0, maxValue=10)
                   .withColumn("code3", dg.INFER_DATATYPE, expr="code1 + code2")
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                   )

        columnSpec1 = df_spec.getColumnType("code1")
        assert columnSpec1.infertype is False

        columnSpec2 = df_spec.getColumnType("code3")
        assert columnSpec3.infertype is True






