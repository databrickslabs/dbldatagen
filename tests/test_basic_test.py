import logging
import pytest

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestBasicOperation:
    testDataSpec = None
    dfTestData = None
    SMALL_ROW_COUNT = 100000
    TINY_ROW_COUNT = 1000
    column_count = 10
    row_count = SMALL_ROW_COUNT

    @pytest.fixture(scope="class")
    def testDataSpec(self, setupLogging):
        retval = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.SMALL_ROW_COUNT,
                                   seedMethod='hash_fieldname')
                  .withIdOutput()
                  .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                              numColumns=self.column_count)
                  .withColumn("code1", IntegerType(), min=100, max=200)
                  .withColumn("code2", IntegerType(), min=0, max=10)
                  .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                  .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                  .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                  )
        return retval

    @pytest.fixture(scope="class")
    def testData(self, testDataSpec):
        return testDataSpec.build().cache()

    def test_row_count(self, testDataSpec):
        """Test row count property"""
        assert testDataSpec is not None
        rc = testDataSpec.rowCount

        assert rc == self.row_count

    def test_default_partition_assignment(self, testDataSpec):
        # testDataSpec was created with default number of partitions
        # check that was same as default parallelism
        assert testDataSpec.partitions == spark.sparkContext.defaultParallelism

    def test_basic_data_generation(self, testData):
        """Test basic data generation of distinct values"""
        counts = testData.agg(F.countDistinct("id").alias("id_count"),
                              F.countDistinct("code1").alias("code1_count"),
                              F.countDistinct("code2").alias("code2_count"),
                              F.countDistinct("code3").alias("code3_count"),
                              F.countDistinct("code4").alias("code4_count"),
                              F.countDistinct("code5").alias("code5_count")
                              ).collect()[0]

        assert counts["id_count"] == self.row_count
        assert counts["code1_count"] == 101
        assert counts["code2_count"] == 11
        assert counts["code3_count"] == 3
        assert counts["code4_count"] <= 3
        assert counts["code5_count"] <= 3

    def test_fieldnames(self, testData, testDataSpec):
        """Test field names in data spec correspond with schema"""
        fieldsFromGenerator = set(testDataSpec.getOutputColumnNames())

        fieldsFromSchema = set([fld.name for fld in testData.schema.fields])

        assert fieldsFromGenerator == fieldsFromSchema

    def test_clone(self, testDataSpec):
        """Test clone method"""
        ds_copy1 = testDataSpec.clone()

        df_copy1 = (ds_copy1.withRowCount(1000)
                    .withColumn("another_column", StringType(), values=['a', 'b', 'c'], random=True)
                    .build())

        assert df_copy1.count() == 1000
        fields1 = ds_copy1.getOutputColumnNames()
        fields2 = testDataSpec.getOutputColumnNames()
        assert fields1 != fields2

        # check that new fields is superset of old fields
        fields_original = set(fields2)
        fields_new = set(fields1)

        assert fields_new - fields_original == set(['another_column'])

    def test_multiple_base_columns(self, testDataSpec):
        """Test data generation with multiple base columns"""
        ds_copy1 = testDataSpec.clone()

        df_copy1 = (ds_copy1.withRowCount(self.TINY_ROW_COUNT)
                    .withColumn("ac1", IntegerType(), baseColumn=['code1', 'code2'], minValue=100, maxValue=200)
                    .withColumn("ac2", IntegerType(), baseColumn=['code1', 'code2'],
                                minValue=100, maxValue=200, random=True)
                    .build().cache())

        assert df_copy1.count() == 1000
        df_overlimit = df_copy1.where("ac1 > 200")
        assert df_overlimit.count() == 0
        df_underlimit = df_copy1.where("ac1 < 100")
        assert df_underlimit.count() == 0
        df_overlimit2 = df_copy1.where("ac2 > 200")
        assert df_overlimit2.count() == 0
        df_underlimit2 = df_copy1.where("ac2 < 100")
        assert df_underlimit2.count() == 0
        df_copy1.show()

    def test_repeatable_multiple_base_columns(self, testDataSpec):
        """Test repeatable data generation with multiple base columns

        When using multiple base columns, each generated value should be same for same combination
        of base column values
        """
        ds_copy1 = testDataSpec.clone()

        df_copy1 = (ds_copy1.withRowCount(1000)
                    .withColumn("ac1", IntegerType(), baseColumn=['code1', 'code2'], minValue=100, maxValue=200)
                    .withColumn("ac2", IntegerType(), baseColumn=['code1', 'code2'],
                                minValue=100, maxValue=200, random=True)
                    .build())

        assert df_copy1.count() == 1000
        df_copy1.createOrReplaceTempView("test_data")

        # check that for each combination of code1 and code2, we only have a single value of ac1
        df_check = spark.sql("""select * from (select  count(ac1) as count_ac1 
                                from (select distinct ac1, code1, code2 from test_data)
                                group by code1, code2)
                                where count_ac1 < 1 or count_ac1 > 1
                                """)

        assert df_check.count() == 0

    def test_default_spark_instance(self):
        """ Test different types of seeding for random values"""
        ds1 = (dg.DataGenerator( name="test_data_set1", rows=1000, seedMethod='hash_fieldname')
               .withIdOutput()
               .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        df = ds1.build()
        assert df.count() == 1000

    def test_default_spark_instance2(self):
        """ Test different types of seeding for random values"""
        ds1 = (dg.DataGenerator( name="test_data_set1", rows=1000, seedMethod='hash_fieldname')
               .withIdOutput()
               .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        ds1._setupSparkSession(None)

        sparkSession = ds1.sparkSession
        assert sparkSession is not None

    def test_multiple_hash_methods(self):
        """ Test different types of seeding for random values"""
        ds1 = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, seedMethod='hash_fieldname')
               .withIdOutput()
               .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        df = ds1.build()
        assert df.count() == 1000
        df_underlimit = df.where("code2 <= 10 or code2 >= 0")
        assert df_underlimit.count() == 1000
        df_count_values = df.where("code3 not in ('a', 'b', 'c')")
        assert df_count_values.count() == 0
        df_count_values2 = df.where("code4 not in ('a', 'b', 'c')")
        assert df_count_values2.count() == 0
        df_count_values3 = df.where("code5 not in ('a', 'b', 'c')")
        assert df_count_values3.count() == 0
        ds2 = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, seedMethod='fixed')
               .withIdOutput()
               .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        df2 = ds2.build()
        assert df2.count() == 1000
        df2_underlimit = df2.where("code2 <= 10 or code2 >= 0")
        assert df2_underlimit.count() == 1000
        df2_count_values = df2.where("code3 not in ('a', 'b', 'c')")
        assert df2_count_values.count() == 0
        df2_count_values2 = df2.where("code4 not in ('a', 'b', 'c')")
        assert df2_count_values2.count() == 0
        df2_count_values3 = df2.where("code5 not in ('a', 'b', 'c')")
        assert df2_count_values3.count() == 0
        ds3 = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, seedMethod=None)
               .withIdOutput()
               .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

               )

        df3 = ds3.build()
        assert df3.count() == 1000
        df3.show()

        df3_underlimit = df3.where("code2 <= 10 and code2 >= 0")
        assert df3_underlimit.count() == 1000
        df3_count_values = df3.where("code3  in ('a', 'b', 'c')")
        assert df3_count_values.count() == 1000
        df3_count_values2 = df3.where("code4  in ('a', 'b', 'c')")
        assert df3_count_values2.count() == 1000
        df3_count_values3 = df3.where("code5  in ('a', 'b', 'c')")
        assert df3_count_values3.count() == 1000

    def test_generated_data_count(self, testData):
        """ Test that rows are generated for the number of rows indicated by the row count"""
        count = testData.count()
        assert count == self.row_count

    def test_distinct_count(self, testData):
        """ Test that ids are unique"""
        distinct_count = testData.select('id').distinct().count()
        assert distinct_count == self.row_count

    def test_column_count(self, testData):
        """Test that expected number of columns are generated"""
        column_count_observed = len(testData.columns)
        assert column_count_observed == self.column_count + 6

    def test_values_code1(self, testData):
        """Test values"""
        values = testData.select('code1').groupBy().agg(F.min('code1').alias('minValue'),
                                                        F.max('code1').alias('maxValue')).collect()[0]
        assert {100, 200} == {values.minValue, values.maxValue}

    def test_values_code2(self, testData):
        """Test values"""
        values = testData.select('code2').groupBy().agg(F.min('code2').alias('minValue'),
                                                        F.max('code2').alias('maxValue')).collect()[0]
        assert {0, 10} == {values.minValue, values.maxValue}

    def test_values_code3(self, testData):
        """Test generated values"""
        values = [x.code3 for x in testData.select('code3').distinct().collect()]
        assert {'a', 'b', 'c'} == set(values)

    def test_values_code4(self, testData):
        """Test generated values"""
        values = [x.code4 for x in testData.select('code4').distinct().collect()]
        assert {'a', 'b', 'c'}.issuperset(set(values))

    def test_values_code5(self, testData):
        """Test generated values"""
        values = [x.code5 for x in testData.select('code5').distinct().collect()]
        assert {'a', 'b', 'c'}.issuperset(set(values))

    def test_basic_adhoc(self, testDataSpec, testData):
        """Test describe, string and repr methods"""
        log = logging.getLogger('tests')

        log.warning("testing")
        print("data generation description:", testDataSpec.describe())
        print("data generation repr:", repr(testDataSpec))
        print("data generation str:", str(testDataSpec))
        testDataSpec.explain()

        print("output columns", testDataSpec.getOutputColumnNames())

        print("dataframe description", testData.describe())
        print("dataframe repr", repr(testData))
        print("dataframe str", str(testData))
        print("dataframe schema", str(testData.schema))
        assert testData.count() == self.row_count

    def test_basic_with_schema(self, testDataSpec):
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

        testDataSpec2 = testDataSpec.clone()
        print("data generation description:", testDataSpec2.describe())
        print("data generation repr:", repr(testDataSpec2))
        print("data generation str:", str(testDataSpec2))
        testDataSpec2.explain()

        testDataSpec3 = (testDataSpec2.withSchema(schema)
                         .withColumnSpec("state1", values=['ca', 'wa', 'ny'])
                         )

        print("output columns", testDataSpec3.getOutputColumnNames())

        testDataDf = testDataSpec3.build()
        testDataDf.show()

        print("dataframe description", testDataDf.describe())
        print("dataframe repr", repr(testDataDf))
        print("dataframe str", str(testDataDf))
        print("dataframe schema", str(testDataDf.schema))
        assert testDataDf.count() == self.row_count

    def test_partitions(self):
        """Test partitioning"""
        id_partitions = 5
        rows_wanted = 100000
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=rows_wanted, partitions=id_partitions)
            .withColumn("code1", IntegerType(), minValue=1, maxValue=20, step=1)
            .withColumn("code2", IntegerType(), maxValue=1000, step=5)
            .withColumn("code3", IntegerType(), minValue=100, maxValue=200, step=1, random=True)
            .withColumn("xcode", StringType(), values=["a", "test", "value"], random=True)
            .withColumn("rating", FloatType(), minValue=1.0, maxValue=5.0, step=0.00001, random=True))

        df = testdata_defn.build()
        df.printSchema()

        count = df.count()

        partitions_created = df.rdd.getNumPartitions()
        assert id_partitions == partitions_created
        assert count == rows_wanted

    def test_percent_nulls(self):
        rows_wanted = 20000
        testdata_defn = (
            dg.DataGenerator(name="basic_dataset", rows=rows_wanted)
            .withColumn("code1", IntegerType(), minValue=1, maxValue=20, step=1, percent_nulls=0.1)
        )

        df = testdata_defn.build()

        count = df.count()

        null_count = df.where("code1 is null").count()

        percent_nulls_observed = (null_count / count) * 100.0
        assert percent_nulls_observed <= 15.0
        assert percent_nulls_observed >= 5.0

    def test_library_version(self):
        lib_version = dg.__version__

        assert lib_version is not None
        assert type(lib_version) == str, "__version__ is expected to be a string"
        assert len(lib_version.strip()) > 0, "__version__ is expected to be non-empty"
