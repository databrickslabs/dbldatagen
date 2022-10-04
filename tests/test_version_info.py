import unittest

from dbldatagen._version import get_version, __version__
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
