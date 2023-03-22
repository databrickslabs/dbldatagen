import pytest

import dbldatagen as dg
from dbldatagen._version import get_version, __version__, _get_spark_version
from dbldatagen import python_version_check


class TestVersionInfo:

    @pytest.fixture
    def sparkSession(self):
        return dg.SparkSingleton.getLocalInstance("unit tests")

    def test_version_info(self):
        vi = get_version(__version__)
        print("version info", vi)

        assert vi.build is not None
        assert vi.major is not None
        assert vi.minor is not None
        assert vi.release is not None

    def test_python_version1(self):
        PYTHON_VERSION = (3, 0)
        assert python_version_check(PYTHON_VERSION)

    def test_python_version2(self):
        PYTHON_VERSION = (3, 6)
        assert python_version_check(PYTHON_VERSION)

    def test_python_version_bad(self):
        PYTHON_VERSION = (10, 1)
        assert not python_version_check(PYTHON_VERSION)

    def test_spark_version1(self, sparkSession):
        sparkVersion = sparkSession.version

        assert sparkVersion is not None and len(sparkVersion.strip()) > 0

    def test_spark_version2(self, sparkSession):
        sparkVersion = sparkSession.version

        sparkVersionInfo = _get_spark_version(sparkVersion)

        retval = dg.DataGenerator._checkSparkVersion(sparkVersion, (3, 0, 0))

        assert retval, "expecting Spark version pass"

    def test_spark_version_warning1(self, sparkSession):
        sparkVersion = sparkSession.version

        sparkVersionInfo = _get_spark_version(sparkVersion)

        # check against future version
        retval = dg.DataGenerator._checkSparkVersion(sparkVersion, (10, 0, 0))

        assert not retval, "expecting Spark version check fail"

    def test_spark_version_bad_version_string(self, sparkSession):
        sparkVersion = "a bad spark version"

        # check against existing version
        retval = dg.DataGenerator._checkSparkVersion(sparkVersion, (1, 0, 0))

        # will pass but generate warning for bad version string
        assert retval, "expecting Spark version pass"
