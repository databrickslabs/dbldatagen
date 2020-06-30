from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
import databrickslabs_testdatagenerator as dg
from pyspark.sql import SparkSession
import unittest
from pyspark.sql import functions as F
import logging
from databrickslabs_testdatagenerator import ensure, mkBoundsList

spark = dg.SparkSingleton.get_local_instance("unit tests")

class TestUtils(unittest.TestCase):

    def setUp(self):
        print("setting up")
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(format=FORMAT)

    @classmethod
    def setUpClass(cls):
        pass

    def testMkBoundsList1(self):
        test = mkBoundsList(None, 1)

        self.assertEqual(len(test), 2)

        test2 = mkBoundsList(None, [1,1])

        self.assertEquals(len(test2), 2)




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
