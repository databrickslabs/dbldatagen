from databrickslabs_testdatagenerator._version import get_version, __version__
import unittest
import logging



class TestVersionInfo(unittest.TestCase):

    def test_version_info(self):
        vi = get_version(__version__)
        print(vi)
        self.assertIsNotNone(vi.build)
        self.assertIsNotNone(vi.major)
        self.assertIsNotNone(vi.minor)
        self.assertIsNotNone(vi.release)



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