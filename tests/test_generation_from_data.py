import ast
import logging
import re
from html.parser import HTMLParser

import pyspark.sql.functions as F
import pytest

import dbldatagen as dg


@pytest.fixture(scope="class")
def spark():
    sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")
    sparkSession.conf.set("dbldatagen.data_analysis.checkpoint", "true")
    sparkSession.sparkContext.setCheckpointDir( "/tmp/dbldatagen/checkpoint")
    return sparkSession


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


class TestGenerationFromData:
    SMALL_ROW_COUNT = 10000
    MEDIUM_ROW_COUNT = 50000

    class SimpleValidator(HTMLParser):  # pylint: disable=abstract-method
        def __init__(self):
            super().__init__()
            self._errors = []
            self._tags = {}

        def handle_starttag(self, tag, attrs):
            if tag in self._tags:
                self._tags[tag] += 1
            else:
                self._tags[tag] = 1

        def handle_endtag(self, tag):
            if tag in self._tags:
                self._tags[tag] -= 1
            else:
                self._errors.append(f"end tag {tag} found without start tag")
                self._tags[tag] = -1

        def checkHtml(self, htmlText):
            """
            Check if htmlText produces errors

            :param htmlText: html text to parse
            :return: Returns the the list of errors
            """
            for tag, count in self._tags.items():
                if count > 0:
                    self._errors.append(f"tag {tag} has {count} additional start tags")
                elif count < 0:
                    self._errors.append(f"tag {tag} has {-count} additional end tags")
            return self._errors

    @pytest.fixture(scope="class")
    def testLogger(self):
        logger = logging.getLogger(__name__)
        return logger

    def mk_generation_spec(self, spark, row_count):

        country_codes = [
            "CN", "US", "FR", "CA", "IN", "JM", "IE", "PK", "GB", "IL", "AU",
            "SG", "ES", "GE", "MX", "ET", "SA", "LB", "NL",
        ]
        country_weights = [
            1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83,
            126, 109, 58, 8, 17,
        ]

        eurozone_countries = ["Austria", "Belgium", "Cyprus", "Estonia", "Finland", "France", "Germany", "Greece",
                              "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands",
                              "Portugal", "Slovakia", "Slovenia", "Spain"
                              ]

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
            .withColumn('review', 'string', text=dg.ILText((1, 3), (1, 4), (3, 8)), random=True, percentNulls=0.1)
            .withColumn('time', 'bigint', expr="now()", percentNulls=0.1)
            .withColumn('title', 'string', template=r"\w|\w \w \w|\w \w \w|\w \w \w \w", random=True)
            .withColumn('user', 'string', expr="hex(abs(hash(id)))")
            .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                        end="2020-12-31 23:59:00",
                        interval="1 minute", random=True)
            .withColumn("r_value", "float", expr="floor(rand() * 350) * (86400 + 3600)",
                        numColumns=(2, 4), structType="array")
            .withColumn("tf_flag", "boolean", expr="id % 2 = 1")
            .withColumn("short_value", "short", max=32767, percentNulls=0.1)
            .withColumn("string_values", "string", values=["one", "two", "three"])
            .withColumn("country_codes", "string", values=country_codes, weights=country_weights)
            .withColumn("euro_countries", "string", values=eurozone_countries)
            .withColumn("int_value", "int", min=100, max=200, percentNulls=0.1)
            .withColumn("byte_value", "tinyint", max=127)
            .withColumn("decimal_value", "decimal(10,2)", max=1000000)
            .withColumn("date_value", "date", expr="current_date()", random=True)
            .withColumn("binary_value", "binary", expr="cast('spark' as binary)", random=True)

        )
        return spec

    @pytest.fixture(scope="class")
    def small_source_data_df(self, spark):
        generation_spec = self.mk_generation_spec(spark, self.SMALL_ROW_COUNT)
        df_source_data = generation_spec.build()
        return df_source_data.checkpoint(eager=True)

    @pytest.fixture(scope="class")
    def medium_source_data_df(self, spark):
        generation_spec = self.mk_generation_spec(spark, self.MEDIUM_ROW_COUNT)
        df_source_data = generation_spec.build()
        return df_source_data.checkpoint(eager=True)

    def test_code_generation1(self, small_source_data_df, setupLogging, spark):
        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df)

        generatedCode = analyzer.scriptDataGeneratorFromData()

        for fld in small_source_data_df.schema:
            assert f"withColumn('{fld.name}'" in generatedCode

        # check generated code for syntax errors
        ast_tree = ast.parse(generatedCode)
        assert ast_tree is not None

    def test_code_generation_as_html(self, small_source_data_df, setupLogging, spark):
        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df)

        generatedCode = analyzer.scriptDataGeneratorFromData(asHtml=True)

        # note the generated code does not have html tags
        validator = self.SimpleValidator()
        parsing_errors = validator.checkHtml(generatedCode)

        assert len(parsing_errors) == 0, "Number of errors should be zero"

        print(generatedCode)

    def test_code_generation_from_schema(self, small_source_data_df, setupLogging):
        generatedCode = dg.DataAnalyzer.scriptDataGeneratorFromSchema(small_source_data_df.schema)

        for fld in small_source_data_df.schema:
            assert f"withColumn('{fld.name}'" in generatedCode

        # check generated code for syntax errors
        ast_tree = ast.parse(generatedCode)
        assert ast_tree is not None

    def test_code_generation_as_html_from_schema(self, small_source_data_df, setupLogging):
        generatedCode = dg.DataAnalyzer.scriptDataGeneratorFromSchema(small_source_data_df.schema, asHtml=True)

        # note the generated code does not have html tags
        validator = self.SimpleValidator()
        parsing_errors = validator.checkHtml(generatedCode)

        assert len(parsing_errors) == 0, "Number of errors should be zero"

        print(generatedCode)

    def test_summarize(self, testLogger, small_source_data_df, spark):

        testLogger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df, maxRows=1000)

        testLogger.info("Summarizing data analyzer results")
        analyzer.summarize()

    def test_summarize_to_df(self, small_source_data_df, testLogger, spark):
        testLogger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df, maxRows=1000)

        testLogger.info("Summarizing data analyzer results")
        df = analyzer.summarizeToDF()

        df.show()

    def test_generate_text_features(self, small_source_data_df, testLogger, spark):
        testLogger.info("Creating data analyzer")

        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df, maxRows=1000)

        df_text_features = analyzer.generateTextFeatures(small_source_data_df).limit(10)
        df_text_features.show()

        # data = df_text_features.selectExpr("get_json_object(asin, '$.print_len') as asin").limit(10).collect()
        data = (df_text_features.select(F.get_json_object(F.col("asin"), "$.print_len").alias("asin"))
                .limit(10).collect())
        assert data[0]['asin'] is not None

    @pytest.mark.parametrize("sampleString, expectedMatch",
                             [("0234", "digits"),
                              ("http://www.yahoo.com", "url"),
                              ("http://www.yahoo.com/test.png", "image_url"),
                              ("info+new_account@databrickslabs.com", "email_uncommon"),
                              ("abcdefg", "alpha_lower"),
                              ("ABCDEFG", "alpha_upper"),
                              ("A09", "alphanumeric"),
                              ("this is a test ", "free_text"),
                              ("test_function", "identifier"),
                              ("10.0.0.1", "ip_addr")
                              ])
    def test_match_patterns(self, sampleString, expectedMatch, small_source_data_df, spark):
        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df)

        pattern_match_result = ""
        for k, v in analyzer._regex_patterns.items():
            pattern = f"^{v}$"

            if re.match(pattern, sampleString) is not None:
                pattern_match_result = k
                break

        assert pattern_match_result == expectedMatch, f"expected match to be {expectedMatch}"

    def test_source_data_property(self, small_source_data_df, spark):
        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df, maxRows=500)

        count_rows = analyzer.sampledSourceDf.count()
        assert abs(count_rows - 500) < 50, "expected count to be close to 500"

    def test_sample_data(self, small_source_data_df, spark):
        # create a DataAnalyzer object
        analyzer = dg.DataAnalyzer(sparkSession=spark, df=small_source_data_df, maxRows=500)

        # sample the data
        df_sample = analyzer.sampleData(small_source_data_df, 100)
        assert df_sample.count() <= 100, "expected count to be 100"
