import logging
import unittest
from datetime import timedelta

from dbldatagen import ensure, mkBoundsList, coalesce_values, deprecated, SparkSingleton, \
    parse_time_interval, DataGenError

spark = SparkSingleton.getLocalInstance("unit tests")


class TestUtils(unittest.TestCase):
    x = 1

    def setUp(self):
        print("setting up")
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(format=FORMAT)

    @classmethod
    def setUpClass(cls):
        pass

    @deprecated("testing deprecated")
    def testDeprecatedMethod(self):
        pass

    @unittest.expectedFailure
    def test_ensure(self):
        ensure(1 == 2, "Expected error")

    def testMkBoundsList1(self):
        """ Test utils mkBoundsList"""
        test = mkBoundsList(None, 1)

        self.assertEqual(len(test), 2)

        test2 = mkBoundsList(None, [1, 1])

        self.assertEqual(len(test2), 2)

        test3= mkBoundsList(1, [1, 1])

        self.assertEqual(len(test3), 2)

        test4 = mkBoundsList([2,3], [1, 1])

        self.assertEqual(len(test4), 2)

    def testCoalesce(self):
        """ Test utils coalesce function"""
        result = coalesce_values(None, 1)

        self.assertEqual(result, 1)

        result2 = coalesce_values(3, None, 1)

        self.assertEqual(result2, 3)

        result3 = coalesce_values(None, None, None)

        self.assertIsNone(result3)

    def testParseTimeInterval1(self):
        interval = parse_time_interval("1 hours")
        self.assertEqual(timedelta(hours=1), interval)

    def testParseTimeInterval2(self):
        interval = parse_time_interval("1 hours, 2 seconds")
        self.assertEqual(timedelta(hours=1, seconds=2), interval)

    def testParseTimeInterval3(self):
        interval = parse_time_interval("1 hours, 2 minutes")
        self.assertEqual(timedelta(hours=1, minutes=2), interval)

    def testParseTimeInterval4(self):
        interval = parse_time_interval("4 days, 1 hours, 2 minutes")
        self.assertEqual(timedelta(days=4, hours=1, minutes=2), interval)

    def testParseTimeInterval1a(self):
        interval = parse_time_interval("hours=1")
        self.assertEqual(timedelta(hours=1), interval)

    def testParseTimeInterval2a(self):
        interval = parse_time_interval("hours=1, seconds = 2")
        self.assertEqual(timedelta(hours=1, seconds=2), interval)

    def testParseTimeInterval2b(self):
        interval = parse_time_interval("1 hour, 1 second")
        self.assertEqual(timedelta(hours=1, seconds=1), interval)

        interval2 = parse_time_interval("1 hour, 10 milliseconds")
        self.assertEqual(timedelta(hours=1, milliseconds=10), interval2)

        interval3 = parse_time_interval("1 hour, 10 microseconds")
        self.assertEqual(timedelta(hours=1, microseconds=10), interval3)

        interval4 = parse_time_interval("1 year, 4 weeks")
        self.assertEqual(timedelta(weeks=56), interval4)

    def testParseTimeInterval3a(self):
        interval = parse_time_interval("1 hours, minutes = 2")
        self.assertEqual(timedelta(hours=1, minutes=2), interval)

    def testParseTimeInterval4a(self):
        interval = parse_time_interval("days=4, hours=1, minutes=2")
        self.assertEqual(timedelta(days=4, hours=1, minutes=2), interval)

    def testDatagenExceptionObject(self):
        testException = DataGenError("testing")

        self.assertIsNotNone(testException)

        print("error has repr", repr(testException))
        print("error has str", str(testException))

# run the tests
# if __name__ == '__main__':
#  print("Trying to run tests")
#  unittest.main(argv=['first-arg-is-ignored'],verbosity=2,exit=False)

# def runTests(suites):
#    suite = unittest.TestSuite()
#    result = unittest.TestResult()
#    for testSuite in suites:
#        suite.addTest(unittest.makeSuite(testSuite))
#    runner = unittest.TextTestRunner()
#    print(runner.run(suite))


# runTests([TestBasicOperation])
