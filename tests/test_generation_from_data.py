import logging
import pytest

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestGenerationFromData:
    SMALL_ROW_COUNT = 100000

    def test_code_generation1(self, setupLogging):
        generation_spec = (
            dg.DataGenerator(sparkSession=spark, name='test_generator',
                             rows=self.SMALL_ROW_COUNT, seedMethod='hash_fieldname')
            .withColumn('asin', 'string', template=r"adddd", random=True)
            .withColumn('brand', 'string', template=r"\w|\w \w \w|\w \w \w")
            .withColumn('helpful', 'array<bigint>', expr="array(floor(rand()*100), floor(rand()*100))")
            .withColumn('img', 'string', expr="concat('http://www.acme.com/downloads/images/', asin, '.png')",
                        baseColumn="asin")
            .withColumn('price', 'double', min=1.0, max=999.0, random=True, step=0.01)
            .withColumn('rating', 'double', values=[1.0,2,0, 3.0, 4.0, 5.0], random=True)
            .withColumn('review', 'string', text=dg.ILText((1,3), (1,4), (3,8)), random=True)
            .withColumn('time', 'bigint', expr="now()")
            .withColumn('title', 'string',template=r"\w|\w \w \w|\w \w \w||\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
            )

        df_source_data = generation_spec.build()
        df_source_data.show()

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        generatedCode = analyzer.scriptDataGeneratorFromData()

        for fld in df_source_data.schema:
            assert f"withColumn('{fld.name}'" in generatedCode

    def test_summarize(self, setupLogging):
        generation_spec = (
            dg.DataGenerator(sparkSession=spark, name='test_generator',
                             rows=self.SMALL_ROW_COUNT, seedMethod='hash_fieldname')
            .withColumn('asin', 'string', template=r"adddd", random=True)
            .withColumn('brand', 'string', template=r"\w|\w \w \w|\w \w \w")
            .withColumn('helpful', 'array<bigint>', expr="array(floor(rand()*100), floor(rand()*100))")
            .withColumn('img', 'string', expr="concat('http://www.acme.com/downloads/images/', asin, '.png')",
                        baseColumn="asin")
            .withColumn('price', 'double', min=1.0, max=999.0, random=True, step=0.01)
            .withColumn('rating', 'double', values=[1.0,2,0, 3.0, 4.0, 5.0], random=True)
            .withColumn('review', 'string', text=dg.ILText((1,3), (1,4), (3,8)), random=True)
            .withColumn('time', 'bigint', expr="now()")
            .withColumn('title', 'string',template=r"\w|\w \w \w|\w \w \w||\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
            )

        df_source_data = generation_spec.build()
        df_source_data.show()

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source_data)

        analyzer.summarize()
