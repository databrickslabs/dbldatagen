import logging
import pytest

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

#import  dbldatagen as dg


@pytest.fixture(scope="class")
def setupSpark():
    import dbldatagen as dg
    sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")
    return sparkSession


@pytest.fixture(scope="class")
def setupLogging():
    #FORMAT = '%(asctime)-15s %(message)s'
    #logging.basicConfig(format=FORMAT)
    pass


class TestLoggingOperation:
    testDataSpec = None
    dfTestData = None
    SMALL_ROW_COUNT = 100000
    TINY_ROW_COUNT = 1000
    column_count = 10
    row_count = SMALL_ROW_COUNT

    @pytest.fixture(scope="class")
    def testDataSpec(self, setupLogging, setupSpark):
        spark = setupSpark
        import dbldatagen as dg
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

    def setup_log_capture(self, caplog_object):
        """ set up log capture fixture

        Sets up log capture fixture to only capture messages after setup and only
        capture warnings and errors

        """
        caplog_object.set_level(logging.WARNING)

        # clear messages from setup
        caplog_object.clear()

    def get_log_capture_warngings_and_errors(self, caplog_object, textFlag):
        """
        gets count of errors containing specified text

        :param caplog_object: log capture object from fixture
        :param textFlag: text to search for to include error or warning in count
        :return: count of errors containg text specified in `textFlag`
        """
        seed_column_warnings_and_errors = 0
        for r in caplog_object.records:
            if (r.levelname == "WARNING" or r.levelname == "ERROR") and textFlag in r.message:
                seed_column_warnings_and_errors += 1

        return seed_column_warnings_and_errors

    def test_alt_seed_column(self, caplog):
        # caplog fixture captures log content
        self.setup_log_capture(caplog)

        dgspec = (dg.DataGenerator(sparkSession=spark, name="alt_data_set", rows=10000,
                                             partitions=4, seedMethod='hash_fieldname', verbose=True,
                                             seedColumnName="_id")
                            .withIdOutput()
                            .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=4)
                            .withColumn("code1", IntegerType(), min=100, max=200)
                            .withColumn("code2", IntegerType(), min=0, max=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])

                            )

        fieldsFromGenerator = set(dgspec.getOutputColumnNames())

        df_testdata = dgspec.build()

        fieldsFromSchema = set([fld.name for fld in df_testdata.schema.fields])

        assert fieldsFromGenerator == fieldsFromSchema

        assert "_id" == dgspec.seedColumnName
        assert "_id" in fieldsFromGenerator
        assert "id" not in fieldsFromGenerator

        ds_copy1 = dgspec.clone()
        fieldsFromGeneratorClone = set(ds_copy1.getOutputColumnNames())

        assert "_id" in fieldsFromGeneratorClone
        assert "id" not in fieldsFromGeneratorClone

        # check that there are no warnings or errors due to use of the overridden seed column
        seed_column_warnings_and_errors = self.get_log_capture_warngings_and_errors(caplog, "seed")
        assert seed_column_warnings_and_errors == 0, "Should not have error messages about seed column"

    def test_logging_operation(self):
        import logging
        date_format = "%Y-%m-%d %H:%M:%S"
        log_format = "[%(name)s]%(asctime)s %(levelname)-8s [%(module)s][%(funcName)s] %(message)s"
        formatter = logging.Formatter(log_format, date_format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger = logging.getLogger("test_logger1")
        logger.setLevel(level=logging.INFO)
        logger.addHandler(handler)
        logger.info("Info message 1")
        # Prints: 2023-03-08 14:18:59 INFO      Info message

        from dbldatagen import DataGenerator

        # Prints: INFO: Version : VersionInfo(major='0', minor='3', patch='1', release='', build='')
        logger.info("Info message 2")

        #logging.info("Info message 3")

        # Prints:
        # 2023-03-08 14:18:59 INFO      Info message
        # INFO: Info message

    def test_logging_operation2(self):
        import logging
        date_format = "%Y-%m-%d %H:%M:%S"
        log_format = "%(asctime)s %(levelname)-8s  %(message)s"
        formatter = logging.Formatter(log_format, date_format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger = logging.getLogger("test_logger2")
        logger.setLevel(level=logging.INFO)
        logger.addHandler(handler)

        logger.info("Info message 1")
        # Prints: 2023-03-08 14:18:59 INFO      Info message

        from dbldatagen import DataGenerator
        # Prints: INFO: Version : VersionInfo(major='0', minor='3', patch='1', release='', build='')
        logger.info("Info message 2")

        #logging.info("Info message 3")

        # Prints:
        # 2023-03-08 14:18:59 INFO      Info message
        # INFO: Info message
