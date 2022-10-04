import unittest

import dbldatagen as dg
from dbldatagen._version import get_version, __version__, _get_spark_version
from dbldatagen import python_version_check


class TestVersionInfo(unittest.TestCase):

    def test_version_info(self):
        vi = get_version(__version__)
        print("version info", vi)

        self.assertIsNotNone(vi.build)
        self.assertIsNotNone(vi.major)
        self.assertIsNotNone(vi.minor)
        self.assertIsNotNone(vi.release)

    def test_python_version1(self):
        PYTHON_VERSION = (3,0)
        python_version_check(PYTHON_VERSION)

    def test_python_version2(self):
        PYTHON_VERSION = (3,6)
        python_version_check(PYTHON_VERSION)

    @unittest.expectedFailure
    def test_python_version_bad(self):
        PYTHON_VERSION = (10,1)
        python_version_check(PYTHON_VERSION)

    def test_spark_version1(self):
        sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")
        sparkVersion = sparkSession.version

        self.assertTrue( sparkVersion is not None and len(sparkVersion.strip()) > 0)

    def test_spark_version2(self):
        sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")
        sparkVersion = sparkSession.version

        sparkVersionInfo = _get_spark_version(sparkVersion)

        retval = dg.DataGenerator._checkSparkVersion(sparkVersion, (3, 0, 0))

        self.assertTrue(retval, "expecting Spark version pass")

    def test_spark_version_warning1(self):
        sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")
        sparkVersion = sparkSession.version

        sparkVersionInfo = _get_spark_version(sparkVersion)

        # check against future version
        retval = dg.DataGenerator._checkSparkVersion(sparkVersion, (10, 0, 0))

        self.assertFalse(retval, "expecting Spark version fail")

    def test_spark_version_bad_version_string(self):
        sparkSession = dg.SparkSingleton.getLocalInstance("unit tests")
        sparkVersion = "a bad spark version"

        # check against existing version
        retval = dg.DataGenerator._checkSparkVersion(sparkVersion, (1, 0, 0))

        # will pass but generate warning for bad version string
        self.assertTrue(retval, "expecting Spark version pass")




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

if __name__ == '__main__':
    unittest.main()
