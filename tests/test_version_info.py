import pytest

from dbldatagen._version import get_version, __version__


class TestVersionInfo:

    def test_version_info(self):
        vi = get_version(__version__)
        assert vi.build is not None
        assert vi.major  is not None
        assert vi.minor  is not None
        assert vi.release  is not None
