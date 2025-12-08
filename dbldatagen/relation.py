# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``ForeignKeyRelation`` class used for describing foreign key relations between datasets.
"""

from dataclasses import dataclass

from dbldatagen.datagen_types import ColumnLike
from dbldatagen.utils import ensure_column


@dataclass(frozen=True)
class ForeignKeyRelation:
    """
    Dataclass describing a foreign key relation between two datasets managed by a ``MultiTableBuilder``.

    :param from_table: Name of the referencing table
    :param from_column: Referencing column as a string or ``pyspark.sql.Column`` expression
    :param to_table: Name of the referenced table
    :param to_column: Referenced column as a string or ``pyspark.sql.Column`` expression
    """

    from_table: str
    from_column: ColumnLike
    to_table: str
    to_column: ColumnLike

    def __post_init__(self) -> None:
        object.__setattr__(self, "from_column", ensure_column(self.from_column))
        object.__setattr__(self, "to_column", ensure_column(self.to_column))
