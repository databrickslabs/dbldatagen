from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
import unittest



spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()


class TestScripting(unittest.TestCase):
    row_count = 1000
    column_count = 10


    def compareTableScriptWithDataset(self, script, name,  df):
        pass

    def checkGeneratedScript(self, script, name):
        self.assertIsNotNone(script, "script is None")
        self.assertTrue(len(script.strip()) > 0, "empty script")
        self.assertTrue(name in script, "name is not in script")

    def checkSchemaEquality(self, schema1, schema2):
        # check schemas are the same
        self.assertEqual(len(schema1.fields), len(schema2.fields), "schemas should have same numbers of fields")

        for c1,c2 in zip(schema1.fields, schema2.fields):
            self.assertEqual(c1.name, c2.name, msg="{} != {}".format(c1.name, c2.name))
            self.assertEqual(c1.dataType, c2.dataType, msg="{}.datatype ({}) != {}.datatype ({})".format(
                c1.name,c1.dataType, c2.name, c2.dataType))


    def test_generate_table_script(self):
        tbl_name = "scripted_table1"
        spark.sql("drop table if exists {}".format(tbl_name))

        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count,
                                                  partitions=4)
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=self.column_count)
                            .withColumn("code1", IntegerType(), min=100, max=200)
                            .withColumn("code2", IntegerType(), min=0, max=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )

        creation_script = testDataSpec.scriptTable(name=tbl_name, table_format="parquet")

        self.checkGeneratedScript(creation_script, name=tbl_name)

        print("====")
        print("Table Creation Script:")
        print(creation_script)
        print("====")


        result1 = spark.sql(creation_script)
        df_result = spark.sql("select * from {}".format(tbl_name))

        dfTestData = testDataSpec.build().cache()

        schema1 = df_result.schema
        schema2 = dfTestData.schema

        self.checkSchemaEquality(schema1, schema2)

    def test_generate_table_script2(self):
        tbl_name = "scripted_table1"
        spark.sql("drop table if exists {}".format(tbl_name))

        testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set2", rows=self.row_count,
                                                  partitions=4)
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=self.column_count)
                            .withColumn("code1", IntegerType(), min=100, max=200)
                            .withColumn("code2", IntegerType(), min=0, max=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )

        creation_script = testDataSpec.scriptTable(name=tbl_name, table_format="parquet", location="/tmp/test")

        self.checkGeneratedScript(creation_script, name=tbl_name)
        self.assertTrue("location" in creation_script, "location is not in script")

        print("====")
        print("Table Creation Script:")
        print(creation_script)
        print("====")


        result1 = spark.sql(creation_script)
        df_result = spark.sql("select * from {}".format(tbl_name))

        dfTestData = testDataSpec.build().cache()

        schema1 = df_result.schema
        schema2 = dfTestData.schema

        self.checkSchemaEquality(schema1, schema2)


