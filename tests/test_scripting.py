import unittest

from pyspark.sql.types import IntegerType, StringType, FloatType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestScripting(unittest.TestCase):
    row_count = 1000
    column_count = 10

    def checkGeneratedScript(self, script, name):
        self.assertIsNotNone(script, "script is None")
        self.assertTrue(len(script.strip()) > 0, "empty script")
        self.assertTrue(name in script, "name is not in script")

    def checkSchemaEquality(self, schema1, schema2):
        # check schemas are the same
        self.assertEqual(len(schema1.fields), len(schema2.fields), "schemas should have same numbers of fields")

        for c1, c2 in zip(schema1.fields, schema2.fields):
            self.assertEqual(c1.name, c2.name, msg=f"{c1.name} != {c2.name}")
            self.assertEqual(
                c1.dataType,
                c2.dataType,
                msg=f"{c1.name}.datatype ({c1.dataType}) != {c2.name}.datatype ({c2.dataType})",
            )

    def test_generate_table_script(self):
        tbl_name = "scripted_table1"
        spark.sql(f"drop table if exists {tbl_name}")

        testDataSpec = (
            dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count, partitions=4)
            .withIdOutput()
            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=self.column_count)
            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
        )

        creation_script = testDataSpec.scriptTable(name=tbl_name, tableFormat="parquet")

        self.checkGeneratedScript(creation_script, name=tbl_name)

        print("====")
        print("Table Creation Script:")
        print(creation_script)
        print("====")

        spark.sql(creation_script)
        df_result = spark.sql(f"select * from {tbl_name}")

        dfTestData = testDataSpec.build().cache()
        spark.sql(f"drop table if exists {tbl_name}")

        schema1 = df_result.schema
        schema2 = dfTestData.schema

        self.checkSchemaEquality(schema1, schema2)

    def test_generate_table_script2(self):
        tbl_name = "scripted_table1"
        spark.sql(f"drop table if exists {tbl_name}")

        testDataSpec = (
            dg.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count, partitions=4)
            .withIdOutput()
            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=self.column_count)
            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
        )

        creation_script = testDataSpec.scriptTable(name=tbl_name, tableFormat="parquet", location="/tmp/test")

        self.checkGeneratedScript(creation_script, name=tbl_name)
        self.assertTrue("location" in creation_script, "location is not in script")

        print("====")
        print("Table Creation Script:")
        print(creation_script)
        print("====")

        dfTestData = testDataSpec.build().cache()

        spark.sql(creation_script)

        # write the data
        dfTestData.write.mode("overwrite").saveAsTable(tbl_name)

        df_result = spark.sql(f"select * from {tbl_name}")

        schema1 = df_result.schema
        schema2 = dfTestData.schema

        self.checkSchemaEquality(schema1, schema2)

        self.assertEqual(df_result.count(), dfTestData.count())

        # cleanup
        spark.sql(f"drop table if exists {tbl_name}")
