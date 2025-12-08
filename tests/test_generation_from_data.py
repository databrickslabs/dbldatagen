import logging
import ast
import pytest

import pyspark.sql as ssql

import dbldatagen as dg


spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestGenerationFromData:
    SMALL_ROW_COUNT = 10000

    @pytest.fixture
    def testLogger(self):
        logger = logging.getLogger(__name__)
        return logger

    @pytest.fixture
    def generation_spec(self):
        spec = (
            dg.DataGenerator(
                sparkSession=spark, name='test_generator', rows=self.SMALL_ROW_COUNT, seedMethod='hash_fieldname'
            )
            .withColumn('asin', 'string', template=r"adddd", random=True)
            .withColumn('brand', 'string', template=r"\w|\w \w \w|\w \w \w")
            .withColumn('helpful', 'array<bigint>', expr="array(floor(rand()*100), floor(rand()*100))")
            .withColumn(
                'img', 'string', expr="concat('http://www.acme.com/downloads/images/', asin, '.png')", baseColumn="asin"
            )
            .withColumn('price', 'double', min=1.0, max=999.0, random=True, step=0.01)
            .withColumn('rating', 'double', values=[1.0, 2, 0, 3.0, 4.0, 5.0], random=True)
            .withColumn('review', 'string', text=dg.ILText((1, 3), (1, 4), (3, 8)), random=True)
            .withColumn('time', 'bigint', expr="now()", percentNulls=0.1)
            .withColumn('title', 'string', template=r"\w|\w \w \w|\w \w \w||\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
            .withColumn(
                "event_ts",
                "timestamp",
                begin="2020-01-01 01:00:00",
                end="2020-12-31 23:59:00",
                interval="1 minute",
                random=True,
            )
            .withColumn(
                "r_value", "float", expr="floor(rand() * 350) * (86400 + 3600)", numColumns=(2, 4), structType="array"
            )
            .withColumn("tf_flag", "boolean", expr="id % 2 = 1")
            .withColumn("short_value", "short", max=32767, percentNulls=0.1)
            .withColumn("byte_value", "tinyint", max=127)
            .withColumn("decimal_value", "decimal(10,2)", max=1000000)
            .withColumn("date_value", "date", expr="current_date()", random=True)
            .withColumn("binary_value", "binary", expr="cast('spark' as binary)", random=True)
        )
        return spec

    def test_code_generation1(self, generation_spec, setupLogging):
        df_source_data = generation_spec.build()
        df_source_data.show()

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        generatedCode = analyzer.scriptDataGeneratorFromData()

        for fld in df_source_data.schema:
            assert f"withColumn('{fld.name}'" in generatedCode

        # check generated code for syntax errors
        ast_tree = ast.parse(generatedCode)
        assert ast_tree is not None

    def test_code_generation_from_schema(self, generation_spec, setupLogging):
        df_source_data = generation_spec.build()
        generatedCode = dg.DataAnalyzer.scriptDataGeneratorFromSchema(df_source_data.schema)

        for fld in df_source_data.schema:
            assert f"withColumn('{fld.name}'" in generatedCode

        # check generated code for syntax errors
        ast_tree = ast.parse(generatedCode)
        assert ast_tree is not None

    def test_summarize(self, testLogger, generation_spec):
        testLogger.info("Building test data")

        df_source_data = generation_spec.build()

        testLogger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        testLogger.info("Summarizing data analyzer results")
        analyzer.summarize()

    def test_summarize_to_df(self, generation_spec, testLogger):
        testLogger.info("Building test data")

        df_source_data = generation_spec.build()

        testLogger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        testLogger.info("Summarizing data analyzer results")
        df = analyzer.summarizeToDF()

        df.show()

    def test_df_containing_summary(self):
        df = spark.range(10).withColumnRenamed("id", "summary")
        summary_df = dg.DataAnalyzer(sparkSession=spark, df=df).summarizeToDF()

        assert summary_df.count() == 10

    def test_data_analyzer_requires_dataframe(self):
        """Validate that DataAnalyzer cannot be initialized without a DataFrame."""
        with pytest.raises(ValueError, match="Argument `df` must be supplied when initializing a `DataAnalyzer`"):
            dg.DataAnalyzer()

    def test_add_measure_to_summary_requires_dataframe(self):
        """Validate that _addMeasureToSummary enforces a non-null dfData argument."""
        with pytest.raises(
            ValueError,
            match="Input DataFrame `dfData` must be supplied when adding measures to a summary",
        ):
            dg.DataAnalyzer._addMeasureToSummary("measure_name", dfData=None)

    def test_generator_default_attributes_from_type_requires_datatype(self):
        """Validate that _generatorDefaultAttributesFromType enforces a DataType instance."""
        with pytest.raises(
            ValueError,
            match=r"Argument 'sqlType' with type .* must be an instance of `pyspark\.sql\.types\.DataType`",
        ):
            dg.DataAnalyzer._generatorDefaultAttributesFromType("not-a-sql-type")
