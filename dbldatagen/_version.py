# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the version information for the test data generator library

Management of version identifiers for releases uses the `bumpversion` python package
which supports automatic modification of files when the version labels need to be modified

See: https://pypi.org/project/bumpversion/

Note the use of `get_version` for method name to conform with bumpversion conventions
"""

import logging
import re
from collections import namedtuple


VersionInfo = namedtuple("VersionInfo", ["major", "minor", "patch", "release", "build"])


def get_version(version: str) -> VersionInfo:
    """ Get version info object for library.

    :param version: version string to parse for version information

    Layout of version string must be compatible with `bump` package"""
    r = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+){0,1}(?P<release>\D*)(?P<build>\d*)")
    match = r.match(version)

    if not match:
        raise ValueError(f"Provided version '{version}' is invalid")

    major, minor, patch, release, build = match.groups()
    version_info = VersionInfo(major, minor, patch, release, build)
    logger = logging.getLogger(__name__)
    logger.info("Version : %s", version_info)

    return version_info


__version__ = "0.4.0post2"  # DO NOT EDIT THIS DIRECTLY!  It is managed by bumpversion
__version_info__ = get_version(__version__)


def _get_spark_version(sparkVersion: str) -> VersionInfo:
    try:
        r = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?P<release>.*)")
        match = r.match(sparkVersion)

        if not match:
            raise ValueError(f"Provided Spark version '{sparkVersion}' is invalid")

        major, minor, patch, release = match.groups()
        spark_version_info = VersionInfo(int(major), int(minor), int(patch), release, build="0")

    except (RuntimeError, ValueError):
        spark_version_info = VersionInfo(major=3, minor=0, patch=1, release="unknown", build="0")
        logger = logging.getLogger(__name__)
        logger.warning("Could not parse spark version - using assumed Spark Version : %s", spark_version_info)

    return spark_version_info
