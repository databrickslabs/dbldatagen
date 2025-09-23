# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module implements configuration classes for writing generated data.
"""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OutputDataset:
    """
    This class implements an output sink configuration used to write generated data. An output location must be
    provided. The output mode, format, and options can be provided.

    :param location: Output location for writing data. This could be an absolute path, a relative path to a Databricks
        Volume, or a full table location using Unity catalog's 3-level namespace.
    :param output_mode: Output mode for writing data (default is ``"append"``).
    :param format: Output data format (default is ``"delta"``).
    :param options: Optional dictionary of options for writing data (e.g. ``{"mergeSchema": "true"}``)
    """
    location: str
    output_mode: str = "append"
    format: str = "delta"
    options: dict[str, str] | None = None
    trigger: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if not self.trigger:
            return

        # Only processingTime is currently supported
        if "processingTime" not in self.trigger:
            valid_trigger_format = '{"processingTime": "10 SECONDS"}'
            raise ValueError(f"Attribute 'trigger' must be a dictionary of the form '{valid_trigger_format}'")
