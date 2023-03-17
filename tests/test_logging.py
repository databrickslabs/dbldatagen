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

    def setup_log_capture(self, caplog_object):
        """ set up log capture fixture

        Sets up log capture fixture to only capture messages after setup and only
        capture warnings and errors

        """
        caplog_object.set_level(logging.INFO)

        # clear messages from setup
        caplog_object.clear()

    def get_log_capture_warnings_and_errors(self, caplog_object, textFlag):
        """
        gets count of errors containing specified text

        :param caplog_object: log capture object from fixture
        :param textFlag: text to search for to include error or warning in count
        :return: count of errors containg text specified in `textFlag`
        """
        flagged_text_warnings_and_errors = 0
        for r in caplog_object.records:
            if (r.levelname == "WARNING" or r.levelname == "ERROR") and textFlag in r.message:
                flagged_text_warnings_and_errors += 1

        return flagged_text_warnings_and_errors

    def get_log_capture_info(self, caplog_object, textFlag):
        """
        gets count of errors containing specified text

        :param caplog_object: log capture object from fixture
        :param textFlag: text to search for to include error or warning in count
        :return: count of errors containg text specified in `textFlag`
        """
        flagged_text_info = 0
        for r in caplog_object.records:
            if (r.levelname == "INFO") and textFlag in r.message:
                flagged_text_info += 1

        return flagged_text_info


    def test_logging_operation(self, caplog):
        # caplog fixture captures log content
        self.setup_log_capture(caplog)

        date_format = "%Y-%m-%d %H:%M:%S"
        log_format = "[%(name)s]%(asctime)s %(levelname)-8s [%(module)s][%(funcName)s] TESTING1 %(message)s"
        formatter = logging.Formatter(log_format, date_format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.INFO)
        logger.addHandler(handler)

        logger.warning("Info message 1")

        # Prints: 2023-03-08 14:18:59 INFO      Info message

        from dbldatagen import DataGenerator

        logger.info("Info message 2")

        for h in logger.handlers:
            h.flush()

        message1_count = self.get_log_capture_warnings_and_errors(caplog, "Info message 1")
        assert message1_count == 1, "Should only have 1 message 1"

        message2_count = self.get_log_capture_info(caplog, "Info message 2")
        assert message2_count == 1, "Should only have 1 message 2"


    def test_logging_operation2(self, setupSpark, caplog):
        self.setup_log_capture(caplog)

        spark=setupSpark

        date_format = "%Y-%m-%d %H:%M:%S"
        log_format = "%(asctime)s %(levelname)-8s TESTING2  %(message)s"
        formatter1 = logging.Formatter(log_format, date_format)
        handler1 = logging.StreamHandler()
        handler1.setFormatter(formatter1)
        logger2 = logging.getLogger("test1")
        logger2.setLevel(level=logging.INFO)
        logger2.addHandler(handler1)

        logger2.warning("Info message 1")
        # Prints: 2023-03-08 14:18:59 INFO      Info message

        from dbldatagen import DataGenerator

        spec = (DataGenerator(sparkSession=spark, name="test_data_set1", rows=10000, seedMethod='hash_fieldname')
                .withIdOutput()
                .withColumn("r", "float", expr="floor(rand() * 350) * (86400 + 3600)",
                            numColumns=10)
                .withColumn("code1", "int", min=100, max=200)
                .withColumn("code2", "int", min=0, max=10)
                .withColumn("code3", "string", values=['a', 'b', 'c'])
                .withColumn("code4", "string", values=['a', 'b', 'c'], random=True)
                .withColumn("code5", "string", values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                )

        df = spec.build()


        # Prints: INFO: Version : VersionInfo(major='0', minor='3', patch='1', release='', build='')
        logger2.info("Info message 2")

        for h in logger2.handlers:
            h.flush()

        message1_count = self.get_log_capture_warnings_and_errors(caplog, "Info message 1")
        assert message1_count == 1, "Should only have 1 message 1"

        message2_count = self.get_log_capture_info(caplog, "Info message 2")
        assert message2_count == 1, "Should only have 1 message 2"
