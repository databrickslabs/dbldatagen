from collections import namedtuple
import re

VersionInfo = namedtuple('VersionInfo', ['major', 'minor', 'patch', 'release', 'build'])


def get_version(version):
    r = re.compile(r'(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\-{0,1}(?P<release>\D*)(?P<build>\d*)')
    major, minor, patch, release, build = r.match(version).groups()
    return VersionInfo(major, minor, patch, release, build)


__version__ = "1.0.5"  # DO NOT EDIT THIS DIRECTLY!  It is managed by bumpversion
__version_info__ = get_version(__version__)