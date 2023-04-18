import logging
from datetime import timedelta

import pytest

from dbldatagen import ensure, mkBoundsList, coalesce_values, deprecated, SparkSingleton, \
    parse_time_interval, DataGenError, strip_margins, split_list_matching_condition, topologicalSort, \
    json_value_from_path, system_time_millis

spark = SparkSingleton.getLocalInstance("unit tests")


class TestUtils:
    x = 1

    @pytest.fixture(autouse=True)
    def setupLogger(self):
        self.logger = logging.getLogger("TestUtils")  # pylint: disable=attribute-defined-outside-init

    @deprecated("testing deprecated")
    def testDeprecatedMethod(self):
        print("testing deprecated function")

    def test_ensure(self):
        with pytest.raises(Exception):
            ensure(1 == 2, "Expected error")  # pylint: disable=comparison-of-constants

    @pytest.mark.parametrize("value,defaultValues",
                             [(None, 1),
                              (None, [1, 2]),
                              (5, [1, 2]),
                              (5, 1),
                              ([1, 2], [3, 4]),
                              ])
    def test_mkBoundsList1(self, value, defaultValues):
        """ Test utils mkBoundsList"""
        test = mkBoundsList(value, defaultValues)
        assert len(test) == 2

    @pytest.mark.parametrize("test_input,expected",
                             [
                                 ([None, 1], 1),
                                 ([2, 1], 2),
                                 ([3, None, 1], 3),
                                 ([None, None, None], None),
                             ])
    def test_coalesce(self, test_input, expected):
        """ Test utils coalesce function"""
        result = coalesce_values(*test_input)
        assert result == expected

    @pytest.mark.parametrize("test_input,expected",
                             [
                                 ("1 hours, minutes = 2", timedelta(hours=1, minutes=2)),
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

    @pytest.mark.parametrize("inputText,expectedText",
                             [("""one
                                 |two
                                 |three""",
                               "one\ntwo\nthree"),
                              ("", ""),
                              ("one\ntwo", "one\ntwo"),
                              ("    one\ntwo", "    one\ntwo"),
                              ("    |one\ntwo", "one\ntwo"),
                              ])
    def test_strip_margins(self, inputText, expectedText):
        output = strip_margins(inputText, '|')

        assert output == expectedText

    @pytest.mark.parametrize("lstData,matchFn, expectedData",
                             [
                                 (['id', 'city_name', 'id', 'city_id', 'city_pop', 'id', 'city_id',
                                   'city_pop', 'city_id', 'city_pop', 'id'],
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
                                 (['id'], lambda el: el == 'id', [['id']]),
                                 (['id', 'id'], lambda el: el == 'id', [['id'], ['id']]),
                                 (['no', 'matches'], lambda el: el == 'id', [['no', 'matches']])
                             ])
    def testSplitListOnCondition(self, lstData, matchFn, expectedData):
        results = split_list_matching_condition(lstData, matchFn)
        print(results)

        assert results == expectedData

    @pytest.mark.parametrize("dependencies, raisesError",
                             [([], False),
                              ([("id", []), ("name", ["id"]), ("name2", ["name"])], False),
                              ([("id", []), ("name", ["id"]), ("name2", ["name3"]), ("name3", ["name2"])], True),
                              ])
    def test_topological_sort(self, dependencies, raisesError):
        raised_exception = False
        try:
            results = topologicalSort(dependencies)
            print("results", results)
        except ValueError as err:
            print(err)
            raised_exception = True

        assert raised_exception == raisesError

    @pytest.mark.parametrize("path,jsonData, defaultValue, expectedValue",
                             [("a", """{"a":1,"b":2,"c":[1,2,3]}""", None, 1),
                              ("b", """{"a":1,"b":2,"c":[1,2,3]}""", None, 2),
                              ("d", """{"a":1,"b":2,"c":[1,2,3]}""", 42, 42),
                              ("c[2]", """{"a":1,"b":2,"c":[1,2,3]}""", None, 3),
                              ])
    def test_json_value_from_path(self, path, jsonData, defaultValue, expectedValue):
        results = json_value_from_path(path, jsonData, defaultValue)

        assert results == expectedValue, f"Expected `{expectedValue}`, got results `{results}`"

    def test_system_time_millis(self):
        curr_time = system_time_millis()
        assert curr_time > 0
