import logging
import sys
from datetime import timedelta
import pytest

from dbldatagen import ensure, mkBoundsList, coalesce_values, deprecated, SparkSingleton, \
    parse_time_interval, DataGenError, split_list_matching_condition

spark = SparkSingleton.getLocalInstance("unit tests")


class TestUtils:
    x = 1

    @pytest.fixture(autouse=True)
    def setupLogger(self):
        self.logger = logging.getLogger("TestUtils")

    @deprecated("testing deprecated")
    def testDeprecatedMethod(self):
        pass

    def test_ensure(self):
        with pytest.raises(Exception):
            ensure(1 == 2, "Expected error")

    def test_mkBoundsList1(self):
        """ Test utils mkBoundsList"""
        test = mkBoundsList(None, 1)

        assert len(test) == 2

        test2 = mkBoundsList(None, [1, 1])

        assert len(test2) ==  2

    @pytest.mark.parametrize("test_input,expected",
                             [
                                 ([None, 1],  1),
                                 ([2, 1],  2),
                                 ([3, None, 1], 3),
                                 ([None, None, None], None),
                             ])
    def test_coalesce(self, test_input, expected):
        """ Test utils coalesce function"""
        result = coalesce_values(*test_input)
        assert result == expected

    @pytest.mark.parametrize("test_input,expected",
                             [
                                 ("1 hours, minutes = 2",  timedelta(hours=1, minutes=2)),
                                 ("4 days, 1 hours, 2 minutes", timedelta(days=4, hours=1, minutes=2)),
                                 ("days=4, hours=1, minutes=2", timedelta(days=4, hours=1, minutes=2)),
                                 ("1 hours, 2 seconds", timedelta(hours=1, seconds=2)),
                                 ("1 hours, 2 minutes", timedelta(hours=1, minutes=2)),
                                 ("1 hours", timedelta(hours=1)),
                                 ("1 hour", timedelta(hours=1)),
                                 ("1 hour, 1 second", timedelta(hours=1, seconds=1)),
                                 ("1 hour, 10 milliseconds", timedelta(hours=1, milliseconds=10)),
                                 ("1 hour, 10 microseconds", timedelta(hours=1, microseconds=10)),
                                 ("1 year, 4 weeks", timedelta(weeks=56))
                             ])
    def testParseTimeInterval2b(self, test_input, expected):
        interval = parse_time_interval(test_input)
        assert expected == interval

    def testDatagenExceptionObject(self):
        testException = DataGenError("testing")

        assert testException is not None

        assert type(repr(testException)) is str
        self.logger.info(repr(testException))

        assert type(str(testException)) is str
        self.logger.info(str(testException))

    @pytest.mark.parametrize("lstData,matchFn, expectedData",
                             [
                                 (['id', 'city_name', 'id', 'city_id', 'city_pop', 'id', 'city_id',
                                   'city_pop','city_id', 'city_pop','id'],
                                  lambda el: el == 'id',
                                  [['id'], ['city_name'], ['id'], ['city_id', 'city_pop'], ['id'],
                                   ['city_id', 'city_pop', 'city_id', 'city_pop'], ['id']]
                                 ),
                                 (['id', 'city_name', 'id', 'city_id', 'city_pop', 'id', 'city_id',
                                   'city_pop2', 'city_id', 'city_pop', 'id'],
                                  lambda el: el in ['id', 'city_pop'],
                                  [['id'], ['city_name'], ['id'], ['city_id'], ['city_pop'], ['id'],
                                   ['city_id', 'city_pop2', 'city_id'], ['city_pop'], ['id']]
                                  ),
                                 ([], lambda el: el == 'id', []),
                                 (['id'], lambda el: el == 'id', [ ['id'] ]),
                                 (['id', 'id'], lambda el: el == 'id', [['id'], ['id']]),
                                 (['no', 'matches'], lambda el: el == 'id', [['no', 'matches']])
                             ])
    def testSplitListOnCondition(self, lstData, matchFn, expectedData):

        results = split_list_matching_condition(lstData, matchFn)
        print(results)

        assert results == expectedData



