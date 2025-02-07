from datetime import timedelta, datetime
import json
import pytest
import yaml
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType, DateType, DecimalType, DoubleType, ByteType,
    ShortType, LongType
)


import dbldatagen as dg
from dbldatagen import DataGenerator, ColumnGenerationSpec
from dbldatagen import NRange, DateRange

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("state1", StringType(), True),
    StructField("state2", StringType(), True),
    StructField("sector_technology_desc", StringType(), True),

])

interval = timedelta(seconds=10)
start = datetime(2018, 10, 1, 6, 0, 0)
end = datetime.now()

src_interval = timedelta(days=1, hours=1)
src_start = datetime(2017, 10, 1, 0, 0, 0)
src_end = datetime(2018, 10, 1, 6, 0, 0)

schema = StructType([
    StructField("site_id", IntegerType(), True),
    StructField("site_cd", StringType(), True),
    StructField("c", StringType(), True),
    StructField("c1", StringType(), True)

])

# build spark session
spark = dg.SparkSingleton.getLocalInstance("quick tests")


class TestQuickTests:
    """These are a set of quick tests to validate some basic behaviors

    The goal for these tests is that they should run fast so focus is on quick execution
    """

    def test_analyzer(self):
        testDataDF = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000, partitions=4)
                      .withIdOutput()
                      .build()
                      )

        print("schema", testDataDF.schema)
        testDataDF.printSchema()
        # display(x3_output)

        analyzer = dg.DataAnalyzer(testDataDF)

        results = analyzer.summarize()
        assert results is not None
        assert 'min' in results
        assert 'max' in results
        assert 'count' in results
        assert 'stddev' in results
        print("Summary;", results)

    def test_complex_datagen(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=1000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)")
                        .withColumn("code1a", IntegerType(), unique_values=100)
                        .withColumn("code1b", IntegerType(), min=1, max=200)
                        .withColumn("code2", IntegerType(), maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        testDataDF2 = testDataSpec.build()

        rowCount = testDataDF2.count()
        assert rowCount == 1000

        print("schema", testDataDF2.schema)
        testDataDF2.printSchema()

        testDataSpec.computeBuildPlan().explain()

        # testDataDF2.show()

        testDataDF2.createOrReplaceTempView("testdata")
        df_stats = spark.sql("""select min(code1a) as min1a, 
                              max(code1a) as max1a, 
                              min(code1b) as min1b, 
                              max(code1b) as max1b,
                              min(code2) as min2, 
                              max(code2) as max2
                              from testdata""")
        stats = df_stats.collect()[0]

        print("stats", stats)

        # self.assertEqual(stats.min1, 1)
        # self.assertEqual(stats.min2, 1)
        assert stats.max1b <= 200
        assert stats.min1b >= 1

    def test_generate_name(self):
        print("test_generate_name")
        n1 = DataGenerator.generateName()
        n2 = DataGenerator.generateName()
        assert n1 is not None
        assert n2 is not None
        assert n1 != n2, "Names should be different"

    def test_column_specifications(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_column_specifications")
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc", "s"]))
        assert expectedColumns == set(([x.name for x in tgen._allColumnSpecs]))

    def test_inferred_columns(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_inferred_columns")
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc", "s"]))
        print("inferred columns", tgen.getInferredColumnNames())
        assert expectedColumns == set((tgen.getInferredColumnNames()))

    def test_output_columns(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_output_columns")
        expectedColumns = set((["site_id", "site_cd", "c", "c1", "sector_status_desc"]))
        print("output columns", tgen.getOutputColumnNames())
        assert expectedColumns == set((tgen.getOutputColumnNames()))

    def test_with_column_spec_for_missing_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_with_column_spec_for_missing_column")
        with pytest.raises(Exception):
            t2 = tgen.withColumnSpec("d", minValue=1, maxValue=200, step=1, random=True)
            assert t2 is not None, "expecting t2 to be a new generator spec"

    def test_with_column_spec_for_duplicate_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_with_column_spec_for_duplicate_column")
        with pytest.raises(Exception):
            t2 = tgen.withColumnSpec("site_id", minValue=1, maxValue=200, step=1, random=True)
            t3 = t2.withColumnSpec("site_id", minValue=1, maxValue=200, step=1, random=True)
            assert t3 is not None, "expecting t3 to be a new generator spec"

    def test_with_column_spec_for_duplicate_column2(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_with_column_spec_for_duplicate_column2")
        t2 = tgen.withColumn("site_id", "string", minValue=1, maxValue=200, step=1, random=True)
        assert t2 is not None, "expecting t2 to be a new generator spec"

    def test_with_column_spec_for_id_column(self):
        tgen = (DataGenerator(sparkSession=spark, name="test_data_set", rows=1000000, partitions=8)
                .withSchema(schema)
                .withColumn("sector_status_desc", StringType(), minValue=1, maxValue=200, step=1,
                            prefix='status', random=True)
                .withColumn("s", StringType(), minValue=1, maxValue=200, step=1, prefix='status',
                            random=True, omit=True))

        print("test_with_column_spec_for_id_column")
        t2 = tgen.withIdOutput()
        expectedColumns = set((["id", "site_id", "site_cd", "c", "c1", "sector_status_desc"]))
        print("output columns", t2.getOutputColumnNames())
        print("inferred columns", t2.getInferredColumnNames())
        assert expectedColumns == set((t2.getOutputColumnNames()))

    def test_basic_ranges_with_view(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="ranged_data", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("code1a", IntegerType(), unique_values=100)
                        .withColumn("code1b", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("code1c", IntegerType(), minValue=1, maxValue=200, unique_values=100)
                        .withColumn("code1d", IntegerType(), minValue=1, maxValue=200, step=3, unique_values=50)
                        .withColumn("code2", IntegerType(), maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        testDataSpec.build(withTempView=True).cache()

        # we refer to the view generated above
        result = spark.sql("""select count(distinct code1a), 
                                     count(distinct code1b), 
                                     count(distinct code1c) 
                                     from ranged_data""").collect()[0]
        assert 100 == result[0]
        assert 100 == result[1]
        assert 100 == result[2]

    def test_basic_formatting1(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="values")
                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_formatting2(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_formatting_discrete_values(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str6", StringType(), template=r"\v0 \v1", baseColumn=["val1", "val2"])
                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_formatting3(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)

                        .withColumn("str5b", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"])

                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_formatting3a(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)

                        # in this case values from base column are passed as array
                        .withColumn("str5b", StringType(), format="test %s", baseColumn=["val1", "val2"])

                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    @pytest.mark.skip(reason="not yet implemented for multiple base columns")
    def test_basic_formatting4(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)

                        # when specifying multiple base columns
                        .withColumn("str5b", StringType(), format="test %s %s", baseColumn=["val1", "val2"],
                                    base_column_type="values")

                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_formatting5(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)

                        .withColumn("str1", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"])
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"], weights=[3, 1, 1])

                        .withColumn("str3", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"], template=r"test \v0")
                        .withColumn("str4", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"], weights=[3, 1, 1], template=r"test \v0")

                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_formatting(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("str5b", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", baseColumn=["val1", "val2"])
                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 100000

    def test_basic_prefix(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=1000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("val3", StringType(), values=["one", "two", "three"])
                        )

        formattedDF = testDataSpec.build(withTempView=True)
        formattedDF.show()

        rowCount = formattedDF.count()
        assert rowCount == 1000

    def test_missing_range_values(self):
        column_types = [FloatType(), DoubleType(), ByteType(), ShortType(), IntegerType(), LongType()]
        for column_type in column_types:
            range_no_min = NRange(maxValue=1.0)
            range_no_max = NRange(minValue=0.0)
            range_no_min.adjustForColumnDatatype(column_type)
            assert range_no_min.min == NRange._getNumericDataTypeRange(column_type)[0]
            assert range_no_min.step == 1
            range_no_max.adjustForColumnDatatype(column_type)
            assert range_no_max.max == NRange._getNumericDataTypeRange(column_type)[1]
            assert range_no_max.step == 1

    def test_range_with_until(self):
        range_until = NRange(step=2, until=100)
        range_until.adjustForColumnDatatype(IntegerType())
        assert range_until.minValue == 0
        assert range_until.maxValue == 101

    def test_empty_range(self):
        empty_range = NRange()
        assert empty_range.isEmpty()

    def test_reversed_ranges(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="ranged_data", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), minValue=100, maxValue=1, step=-1)
                        .withColumn("val2", IntegerType(), minValue=100, maxValue=1, step=-3, unique_values=5)
                        .withColumn("val3", IntegerType(), dataRange=NRange(100, 1, -1), unique_values=5)
                        .withColumn("val4", IntegerType(), minValue=1, maxValue=100, step=3, unique_values=5)
                        .withColumn("code1b", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("code1c", IntegerType(), minValue=1, maxValue=200, unique_values=100)
                        .withColumn("code1d", IntegerType(), minValue=1, maxValue=200)
                        .withColumn("code2", IntegerType(), maxValue=10)
                        .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                        .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                        .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                        )

        rangedDF = testDataSpec.build()
        rangedDF.show()

        rowCount = rangedDF.count()
        assert rowCount == 100000

    def test_date_time_ranges(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="ranged_data", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("last_sync_ts", "timestamp",
                                    dataRange=DateRange("2017-10-01 00:00:00",
                                                        "2018-10-06 00:00:00",
                                                        "days=1,hours=1"))
                        .withColumn("last_sync_ts", "timestamp",
                                    dataRange=DateRange("2017-10-01 00:00:00",
                                                        "2018-10-06 00:00:00",
                                                        "days=1,hours=1"), unique_values=5)

                        .withColumn("last_sync_ts", "timestamp",
                                    dataRange=DateRange("2017-10-01",
                                                        "2018-10-06",
                                                        "days=7",
                                                        datetime_format="%Y-%m-%d"))

                        .withColumn("last_sync_dt1", DateType(),
                                    dataRange=DateRange("2017-10-01 00:00:00",
                                                        "2018-10-06 00:00:00",
                                                        "days=1"))
                        .withColumn("last_sync_dt2", DateType(),
                                    dataRange=DateRange("2017-10-01 00:00:00",
                                                        "2018-10-06 00:00:00",
                                                        "days=1"), unique_values=5)

                        .withColumn("last_sync_date", DateType(),
                                    dataRange=DateRange("2017-10-01",
                                                        "2018-10-06",
                                                        "days=7",
                                                        datetime_format="%Y-%m-%d"))

                        )

        rangedDF = testDataSpec.build()
        rangedDF.show()

        rowCount = rangedDF.count()
        assert rowCount == 100000

        # TODO: add additional validation statement

    @pytest.mark.parametrize("asHtml", [True, False])
    def test_script_table(self, asHtml):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("str5b", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["one", "two", "three"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", baseColumn=["val1", "val2"])
                        )

        script = testDataSpec.scriptTable(name="Test", asHtml=asHtml)
        print(script)

        assert script is not None

        output_columns = testDataSpec.getOutputColumnNames()
        print(output_columns)
        assert set(output_columns) == {'id', 'val1', 'val2', 'str1', 'str2', 'str3', 'str4', 'str5',
                                       'str5a', 'str5b', 'str6'}

        assert script is not None

        assert "CREATE TABLE IF NOT EXISTS" in script

        for col in output_columns:
            assert col in script

    @pytest.mark.parametrize("asHtml", [True, False])
    def test_script_merge1(self, asHtml):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    baseColumnType="hash")
                        .withColumn("str5", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("action", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["INS", "DEL", "UPDATE"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", baseColumn=["val1", "val2"])
                        )

        script = testDataSpec.scriptMerge(tgtName="Test", srcName="TestInc", joinExpr="src.id=tgt.id",
                                          delExpr="src.action='DEL'", updateExpr="src.action='UPDATE",
                                          asHtml=asHtml)
        print(script)

        output_columns = testDataSpec.getOutputColumnNames()
        print(output_columns)
        assert set(output_columns) == {'id', 'val1', 'val2',
                                       'str1', 'str2', 'str3', 'str4', 'str5', 'str5a', 'action', 'str6'}

        assert script is not None

        assert "WHEN MATCHED" in script
        assert "WHEN NOT MATCHED" in script
        assert "MERGE INTO" in script

        for col in output_columns:
            assert col in script

    def test_script_merge_min(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="formattedDF", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("val1", IntegerType(), unique_values=100)
                        .withColumn("val2", IntegerType(), minValue=1, maxValue=100)
                        .withColumn("str1", StringType(), format="test %d")
                        # .withColumn("str1a", StringType(), format="test %s")
                        .withColumn("str2", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="values")
                        .withColumn("str3", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str4", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    base_column_type="hash")
                        .withColumn("str5", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("str5a", StringType(), format="test %s", baseColumn=["val1", "val2"])
                        .withColumn("action", StringType(), format="test %s", baseColumn=["val1", "val2"],
                                    values=["INS", "DEL", "UPDATE"])
                        .withColumn("str6", StringType(), template=r"\v0 \v1", baseColumn=["val1", "val2"])
                        )

        script = testDataSpec.scriptMerge(tgtName="Test", srcName="TestInc", joinExpr="src.id=tgt.id")
        assert script is not None

        print(script)

        output_columns = testDataSpec.getOutputColumnNames()
        print(output_columns)
        assert set(output_columns) == \
               {'id', 'val1', 'val2', 'str1', 'str2', 'str3', 'str4', 'str5', 'str5a', 'action', 'str6'}

        assert script is not None

        assert "WHEN MATCHED" in script
        assert "WHEN NOT MATCHED" in script
        assert "MERGE INTO" in script

        for col in output_columns:
            assert col in script

    def test_strings_from_numeric_string_field1(self):
        """ Check that order_id always generates a non null value when using random values"""
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="stringsFromNumbers", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        .withColumn("order_num", minValue=1, maxValue=100000000, random=True)
                        .withColumn("order_id", prefix="order", baseColumn="order_num")
                        )

        testDataSpec.build(withTempView=True)

        newDF = spark.sql("select * from stringsFromNumbers")
        newDF.printSchema()

        nullRowsDF = spark.sql("select * from stringsFromNumbers where order_id is null")

        nullRowsDF.show()
        rowCount = nullRowsDF.count()
        assert rowCount == 0

    def test_strings_from_numeric_string_field2(self):
        """ Check that order_id always generates a non null value when using non-random values"""
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="stringsFromNumbers", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        # use step of -1 to ensure descending from max value
                        .withColumn("order_num", minValue=1, maxValue=100000000, step=-1)
                        .withColumn("order_id", prefix="order", baseColumn="order_num")
                        )

        testDataSpec.build(withTempView=True)

        newDF = spark.sql("select * from stringsFromNumbers")
        newDF.printSchema()

        nullRowsDF = spark.sql("select * from stringsFromNumbers where order_id is null")

        nullRowsDF.show()
        rowCount = nullRowsDF.count()
        assert rowCount == 0

    def test_strings_from_numeric_string_field2a(self):
        """ Check that order_id always generates a non null value when using non-random values"""
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="stringsFromNumbers", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        # use step of -1 to ensure descending from max value
                        .withColumn("order_num", minValue=1, maxValue=100000000, step=-1)
                        .withColumn("order_id", "string", minValue=None, suffix="_order", baseColumn="order_num")
                        )

        testDataSpec.build(withTempView=True)

        testDataSpec.explain()

        newDF = spark.sql("select * from stringsFromNumbers")
        newDF.printSchema()

        newDF.show()

        nullRowsDF = spark.sql("select * from stringsFromNumbers where order_id is null")

        nullRowsDF.show()
        rowCount = nullRowsDF.count()
        assert rowCount == 0

    def test_strings_from_numeric_string_field3(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="stringsFromNumbers", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        # default column type is string
                        .withColumn("order_num", minValue=1, maxValue=100000000, random=True)
                        .withColumn("order_id", prefix="order", baseColumn="order_num")
                        )

        testDataSpec.build(withTempView=True)

        newDF = spark.sql("select * from stringsFromNumbers")
        newDF.printSchema()

        nullRowsDF = spark.sql("select * from stringsFromNumbers where order_num is null or length(order_num) = 0")

        nullRowsDF.show()
        rowCount = nullRowsDF.count()
        assert rowCount == 0

    def test_strings_from_numeric_string_field4(self):
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="stringsFromNumbers", rows=100000,
                                         partitions=4)
                        .withIdOutput()
                        # default column type is string
                        .withColumn("order_num", minValue=1, maxValue=100000000, step=-1)
                        .withColumn("order_id", prefix="order", baseColumn="order_num")
                        )

        df = testDataSpec.build(withTempView=True)

        testDataSpec.explain()

        nullRowsDF = spark.sql("select * from stringsFromNumbers where order_num is null or length(order_num) = 0")

        rowCount = nullRowsDF.count()
        assert rowCount == 0

    @pytest.mark.parametrize("columnSpecOptions", [
        {"dataType": "byte", "minValue": 1, "maxValue": None},
        {"dataType": "byte", "minValue": None, "maxValue": 10},
        {"dataType": "short", "minValue": 1, "maxValue": None},
        {"dataType": "short", "minValue": None, "maxValue": 100},
        {"dataType": "integer", "minValue": 1, "maxValue": None},
        {"dataType": "integer", "minValue": None, "maxValue": 100},
        {"dataType": "long", "minValue": 1, "maxValue": None},
        {"dataType": "long", "minValue": None, "maxValue": 100},
        {"dataType": "float", "minValue": 1.0, "maxValue": None},
        {"dataType": "float", "minValue": None, "maxValue": 100.0},
        {"dataType": "double", "minValue": 1, "maxValue": None},
        {"dataType": "double", "minValue": None, "maxValue": 100.0}
    ])
    def test_random_generation_without_range_values(self, columnSpecOptions):
        dataType = columnSpecOptions.get("dataType", None)
        minValue = columnSpecOptions.get("minValue", None)
        maxValue = columnSpecOptions.get("maxValue", None)
        testDataSpec = (dg.DataGenerator(sparkSession=spark, name="randomGenerationWithoutRangeValues", rows=100,
                                         partitions=4)
                        .withIdOutput()
                        # default column type is string
                        .withColumn("randCol", colType=dataType, minValue=minValue, maxValue=maxValue, random=True)
                        )

        df = testDataSpec.build(withTempView=True)
        sortedDf = df.orderBy("randCol")
        sortedVals = sortedDf.select("randCol").collect()
        assert sortedVals != df.select("randCol").collect()

    def test_version_info(self):
        # test access to version info without explicit import
        print("Data generator version", dg.__version__)

    def test_multi_column_generation(self):
        column_specs = [
            {"colName": "col1", "colType": "int", "minValue": 0, "maxValue": 100},
            {"colName": "col2", "colType": "float", "minValue": 0.0, "maxValue": 100.0},
            {"colName": "col3", "colType": "string", "values": ["a", "b", "c"], "random": True}
        ]
        df_from_dicts = dg.DataGenerator(rows=100, partitions=1).withColumns(column_specs).build()
        assert df_from_dicts.columns == ["col1", "col2", "col3"]

    def test_generation_from_dictionary(self):
        dg_spec = {
            "name": "test_data_generator",
            "rows": 1000,
            "partitions": 10,
            "randomSeedMethod": "fixed",
            "randomSeed": 42,
            "random": True
        }
        gen_from_dict = DataGenerator.fromDict(dg_spec)
        assert gen_from_dict.name == dg_spec.get("name")
        assert gen_from_dict.rowCount == dg_spec.get("rows")
        assert gen_from_dict.partitions == dg_spec.get("partitions")
        assert gen_from_dict.random == dg_spec.get("random")
        assert gen_from_dict.randomSeed == dg_spec.get("randomSeed")

    def test_generation_from_file(self):
        path = "tests/files/test_generator_spec.json"
        with open(path, "r", encoding="utf-8") as f:
            options = json.load(f)
            gen_options = options.get("generator")
            gen_from_json = DataGenerator.fromFile(path)
            assert gen_from_json.name == gen_options.get("name")
            assert gen_from_json.rowCount == gen_options.get("rows")
            assert gen_from_json.partitions == gen_options.get("partitions")
            assert gen_from_json.random == gen_options.get("random")
            assert gen_from_json.randomSeed == gen_options.get("randomSeed")

            df_from_json = gen_from_json.build()
            assert df_from_json.columns == ["col1", "col2", "col3"]

        path = "tests/files/test_generator_spec.yml"
        with open(path, "r", encoding="utf-8") as f:
            options = yaml.safe_load(f)
            gen_options = options.get("generator")
            gen_from_yaml = DataGenerator.fromFile(path)
            assert gen_from_yaml.name == gen_options.get("name")
            assert gen_from_yaml.rowCount == gen_options.get("rows")
            assert gen_from_yaml.partitions == gen_options.get("partitions")
            assert gen_from_yaml.random == gen_options.get("random")
            assert gen_from_yaml.randomSeed == gen_options.get("randomSeed")

            df_from_json = gen_from_json.build()
            assert df_from_json.columns == ["col1", "col2", "col3"]

        path = "tests/files/test_generator_spec.txt"
        with pytest.raises(ValueError):
            DataGenerator.fromFile(path)  # Loading from .txt should raise a ValueError
