# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module implements configuration classes for writing generated data.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class OutputConfig:
    """
    This class implements an output sink configuration used to write generated data. Output sinks must extend from the
    `OutputConfig` base class.
    """
    location: str
    output_mode: str
    format: str = "delta"
    options: dict = field(default_factory=dict)
