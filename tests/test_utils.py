import databrickslabs_testdatagenerator as dg
import unittest
import logging
from databrickslabs_testdatagenerator import ensure, mkBoundsList, coalesce_values

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestUtils(unittest.TestCase):
    x = 1

    def setUp(self):
        print("setting up")
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(format=FORMAT)

    @classmethod
    def setUpClass(cls):
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

    def testCoalesce(self):
        """ Test utils coalesce function"""
        result = coalesce_values(None, 1)

        self.assertEqual(result, 1)

        result2 = coalesce_values(3, None, 1)

        self.assertEqual(result2, 3)

        result3 = coalesce_values(None, None, None)

        self.assertIsNone(result3)

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
