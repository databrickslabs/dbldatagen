# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Compatibility helpers for Spark runtime differences.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, cast


try:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
except ImportError:
    ConnectSparkSession = None


def _get_pyspark_is_remote() -> Callable[[], bool] | None:
    """Returns PySpark's public remote-session predicate when available."""
    # The public helper lives in different modules across the supported PySpark versions.
    for import_path in ("pyspark.sql.utils", "pyspark.sql"):
        try:
            module = __import__(import_path, fromlist=["is_remote"])
            return cast(Callable[[], bool], module.is_remote)
        except (ImportError, AttributeError):
            continue

    return None


def is_spark_connect_mode() -> bool:
    """Returns whether the current PySpark process is configured for Spark Connect."""
    is_remote = _get_pyspark_is_remote()
    if is_remote is not None:
        return bool(is_remote())

    return os.environ.get("SPARK_CONNECT_MODE_ENABLED", "").lower() == "1" or bool(os.environ.get("SPARK_REMOTE"))


def is_spark_connect_session(sparkSession: Any | None = None) -> bool:  # noqa: ANN401
    """Returns whether the supplied or current Spark session is a Spark Connect session."""
    return ConnectSparkSession is not None and isinstance(sparkSession, ConnectSparkSession)
