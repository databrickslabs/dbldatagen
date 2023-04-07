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
    def generation_spec(self):
        spec = (
            dg.DataGenerator(sparkSession=spark, name='test_generator',
                             rows=self.SMALL_ROW_COUNT, seedMethod='hash_fieldname')
            .withColumn('asin', 'string', template=r"adddd", random=True)
            .withColumn('brand', 'string', template=r"\w|\w \w \w|\w \w \w")
            .withColumn('helpful', 'array<bigint>', expr="array(floor(rand()*100), floor(rand()*100))")
            .withColumn('img', 'string', expr="concat('http://www.acme.com/downloads/images/', asin, '.png')",
                        baseColumn="asin")
            .withColumn('price', 'double', min=1.0, max=999.0, random=True, step=0.01)
            .withColumn('rating', 'double', values=[1.0, 2, 0, 3.0, 4.0, 5.0], random=True)
            .withColumn('review', 'string', text=dg.ILText((1, 3), (1, 4), (3, 8)), random=True)
            .withColumn('time', 'bigint', expr="now()")
            .withColumn('title', 'string', template=r"\w|\w \w \w|\w \w \w||\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
            .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                        end="2020-12-31 23:59:00",
                        interval="1 minute", random=True)
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

    def test_summarize(self):
        logger = logging.getLogger(__name__)

        logger.info("Building test data")

        generation_spec = (
            dg.DataGenerator(sparkSession=spark, name='test_generator', rows=self.SMALL_ROW_COUNT)
            .withColumn('asin', 'string', template=r"adddd", random=True)
            .withColumn('brand', 'string', template=r"\w|\w \w \w|\w \w \w")
            .withColumn('helpful', 'array<bigint>', expr="array(floor(rand()*100), floor(rand()*100))")
            .withColumn('img', 'string', expr="concat('http://www.acme.com/downloads/images/', asin, '.png')",
                        baseColumn="asin")
            .withColumn('price', 'double', min=1.0, max=999.0, random=True, step=0.01)
            .withColumn('rating', 'double', values=[1.0, 2, 0, 3.0, 4.0, 5.0], random=True)
            .withColumn('review', 'string', text=dg.ILText((1, 3), (1, 4), (3, 8)), random=True)
            .withColumn('time', 'bigint', expr="now()")
            .withColumn('title', 'string', template=r"\w|\w \w \w|\w \w \w||\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
        )

        df_source_data = generation_spec.build()

        logger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        logger.info("Summarizing data analyzer results")
        analyzer.summarize()

    def test_summarize_to_df(self):
        logger = logging.getLogger(__name__)

        logger.info("Building test data")

        generation_spec = (
            dg.DataGenerator(sparkSession=spark, name='test_generator', rows=self.SMALL_ROW_COUNT)
            .withColumn('asin', 'string', template=r"adddd", random=True)
            .withColumn('brand', 'string', template=r"\w|\w \w \w|\w \w \w")
            .withColumn('helpful', 'array<bigint>', expr="array(floor(rand()*100), floor(rand()*100))")
            .withColumn('img', 'string', expr="concat('http://www.acme.com/downloads/images/', asin, '.png')",
                        baseColumn="asin")
            .withColumn('price', 'double', min=1.0, max=999.0, random=True, step=0.01)
            .withColumn('rating', 'double', values=[1.0, 2, 0, 3.0, 4.0, 5.0], random=True)
            .withColumn('review', 'string', text=dg.ILText((1, 3), (1, 4), (3, 8)), random=True)
            .withColumn('time', 'bigint', expr="now()")
            .withColumn('title', 'string', template=r"\w|\w \w \w|\w \w \w||\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
            .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                        end="2020-12-31 23:59:00",
                        interval="1 minute", random=True)
        )

        df_source_data = generation_spec.build()

        df_source_data.describe().show()

        print(type(df_source_data))

        logger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        logger.info("Summarizing data analyzer results")
        df = analyzer.summarizeToDF()

        df.show()

