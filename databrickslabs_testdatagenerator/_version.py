# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the version information for the test data generator library

Management of version identifiers for releases uses the bumpversion python package
which supports automatic modification of files when the version labels need to be modified

See: https://pypi.org/project/bumpversion/

Note the use of `get_version` for method name to conform with bumpversion conventions
"""

from collections import namedtuple
import re

VersionInfo = namedtuple('VersionInfo', ['major', 'minor', 'patch', 'release', 'build'])


def get_version(version):
    """ Get version string for library"""
    r = re.compile(r'(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\-{0,1}(?P<release>\D*)(?P<build>\d*)')
    major, minor, patch, release, build = r.match(version).groups()
    version_info = VersionInfo(major, minor, patch, release, build)
    print("Version : ", version_info)
    return version_info


__version__ = "0.10.0-dev2"  # DO NOT EDIT THIS DIRECTLY!  It is managed by bumpversion
__version_info__ = get_version(__version__)