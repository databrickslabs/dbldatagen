# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``MultiTableBuilder`` class used for managing relational datasets.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame

from dbldatagen.data_generator import DataGenerator
from dbldatagen.datagen_types import ColumnLike
from dbldatagen.relation import ForeignKeyRelation
from dbldatagen.utils import DataGenError, ensure_column


class MultiTableBuilder:
    """
    Basic builder for managing multiple related datasets backed by ``DataGenerator`` instances.

    This initial implementation focuses on tracking datasets, static DataFrames, and foreign key relations.
    Related tables must share a single ``DataGenerator`` so the rows can be generated in a single pass.
    """

    def __init__(self) -> None:
        self._datasets: dict[str, _DatasetDefinition] = {}
        self._data_generators: list[DataGenerator] = []
        self._static_dataframes: list[DataFrame] = []
        self._foreign_key_relations: list[ForeignKeyRelation] = []
        self._generator_cache: dict[int, DataFrame] = {}

    @property
    def data_generators(self) -> list[DataGenerator]:
        """
        List of unique ``DataGenerator`` instances tracked by the builder.
        """
        return list(dict.fromkeys(self._data_generators))

    @property
    def static_dataframes(self) -> list[DataFrame]:
        """
        List of static ``DataFrame`` objects tracked by the builder.
        """
        return list(self._static_dataframes)

    @property
    def foreign_key_relations(self) -> list[ForeignKeyRelation]:
        """
        List of registered :class:`ForeignKeyRelation` objects.
        """
        return list(self._foreign_key_relations)

    def add_data_generator(
        self,
        name: str,
        generator: DataGenerator,
        columns: Sequence[ColumnLike] | None = None,
    ) -> None:
        """
        Register a dataset backed by a ``DataGenerator``.

        :param name: Dataset name
        :param generator: Generator instance capable of producing all required columns
        :param columns: Default column projection for the dataset
        """
        if name in self._datasets:
            raise DataGenError(f"Dataset '{name}' is already defined.")

        self._datasets[name] = _DatasetDefinition(
            name=name,
            generator=generator,
            columns=tuple(columns) if columns is not None else None,
        )
        self._data_generators.append(generator)

    def add_static_dataframe(
        self,
        name: str,
        dataframe: DataFrame,
        columns: Sequence[ColumnLike] | None = None,
    ) -> None:
        """
        Register a dataset backed by a pre-built ``DataFrame``.

        :param name: Dataset name
        :param dataframe: Static ``DataFrame`` instance
        :param columns: Default column projection for the dataset
        """
        if name in self._datasets:
            raise DataGenError(f"Dataset '{name}' is already defined.")

        self._datasets[name] = _DatasetDefinition(
            name=name,
            dataframe=dataframe,
            columns=tuple(columns) if columns is not None else None,
        )
        self._static_dataframes.append(dataframe)

    def add_foreign_key_relation(
        self,
        relation: ForeignKeyRelation | None = None,
        *,
        from_table: str | None = None,
        from_column: ColumnLike | None = None,
        to_table: str | None = None,
        to_column: ColumnLike | None = None,
    ) -> ForeignKeyRelation:
        """
        Register a foreign key relation between two datasets.

        The relation can be provided via a fully constructed ``ForeignKeyRelation`` or via keyword arguments.

        :param relation: Optional ``ForeignKeyRelation`` instance
        :param from_table: Referencing dataset name (required if ``relation`` not supplied)
        :param from_column: Referencing column (required if ``relation`` not supplied)
        :param to_table: Referenced dataset name (required if ``relation`` not supplied)
        :param to_column: Referenced column (required if ``relation`` not supplied)
        :return: Registered relation
        """
        if relation is None:
            if not all([from_table, from_column, to_table, to_column]):
                raise DataGenError("Foreign key relation requires table and column details.")
            relation = ForeignKeyRelation(
                from_table=from_table,  # type: ignore[arg-type]
                from_column=from_column,  # type: ignore[arg-type]
                to_table=to_table,  # type: ignore[arg-type]
                to_column=to_column,  # type: ignore[arg-type]
            )

        self._validate_dataset_exists(relation.from_table)
        self._validate_dataset_exists(relation.to_table)

        self._foreign_key_relations.append(relation)
        return relation

    def build(
        self,
        dataset_names: Sequence[str] | None = None,
        column_overrides: Mapping[str, Sequence[ColumnLike]] | None = None,
    ) -> dict[str, DataFrame]:
        """
        Materialize one or more datasets managed by the builder.

        :param dataset_names: Optional list of dataset names to build (defaults to all datasets)
        :param column_overrides: Optional mapping of dataset name to column overrides
        :return: Dictionary keyed by dataset name containing Spark ``DataFrame`` objects
        """
        targets = dataset_names or list(self._datasets)
        results: dict[str, DataFrame] = {}

        for name in targets:
            overrides = column_overrides[name] if column_overrides and name in column_overrides else None
            results[name] = self.get_dataset(name, columns=overrides)

        return results

    def get_dataset(self, name: str, columns: Sequence[ColumnLike] | None = None) -> DataFrame:
        """
        Retrieve a single dataset as a ``DataFrame`` applying optional column overrides.

        :param name: Dataset name
        :param columns: Optional select expressions to override defaults
        :return: Spark ``DataFrame`` with projected columns
        """
        dataset = self._datasets.get(name)
        if dataset is None:
            raise DataGenError(f"Dataset '{name}' is not defined.")

        if dataset.dataframe is not None:
            return dataset.select_columns(dataset.dataframe, columns)

        assert dataset.generator is not None
        self._ensure_shared_generator(name)

        base_df = self._get_or_build_generator_output(dataset.generator)
        return dataset.select_columns(base_df, columns)

    def clear_cache(self) -> None:
        """
        Clear cached ``DataFrame`` results for generator-backed datasets.
        """
        self._generator_cache.clear()

    def _validate_dataset_exists(self, name: str) -> None:
        if name not in self._datasets:
            raise DataGenError(f"Dataset '{name}' is not registered with the builder.")

    def _get_or_build_generator_output(self, generator: DataGenerator) -> DataFrame:
        generator_id = id(generator)
        if generator_id not in self._generator_cache:
            self._generator_cache[generator_id] = generator.build()
        return self._generator_cache[generator_id]

    def _ensure_shared_generator(self, name: str) -> None:
        """
        Validate that all generator-backed tables within the relation group share the same generator instance.
        """
        dataset = self._datasets[name]
        generator = dataset.generator
        if generator is None:
            return

        for related_name in self._collect_related_tables(name):
            related_dataset = self._datasets[related_name]
            if related_dataset.generator is None:
                continue
            if related_dataset.generator is not generator:
                msg = (
                    f"Datasets '{name}' and '{related_name}' participate in a foreign key relation "
                    "and must share the same DataGenerator instance."
                )
                raise DataGenError(msg)

    def _collect_related_tables(self, name: str) -> set[str]:
        """
        Collect all tables connected to the supplied table via foreign key relations.
        """
        related: set[str] = set()
        to_visit = [name]

        while to_visit:
            current = to_visit.pop()
            for relation in self._foreign_key_relations:
                neighbor = self._neighbor_for_relation(current, relation)
                if neighbor and neighbor not in related:
                    related.add(neighbor)
                    to_visit.append(neighbor)

        return related

    @staticmethod
    def _neighbor_for_relation(table: str, relation: ForeignKeyRelation) -> str | None:
        if relation.from_table == table:
            return relation.to_table
        if relation.to_table == table:
            return relation.from_table
        return None


@dataclass
class _DatasetDefinition:
    """
    Internal representation of a dataset tracked by a ``MultiTableBuilder``.
    """

    name: str
    generator: DataGenerator | None = None
    dataframe: DataFrame | None = None
    columns: tuple[ColumnLike, ...] | None = None

    def __post_init__(self) -> None:
        has_generator = self.generator is not None
        has_dataframe = self.dataframe is not None

        if has_generator == has_dataframe:
            raise DataGenError(f"Dataset '{self.name}' must specify exactly one of DataGenerator or DataFrame.")

    def select_columns(self, df: DataFrame, overrides: Sequence[ColumnLike] | None = None) -> DataFrame:
        """
        Apply column selection for the dataset using overrides when supplied.

        :param df: Source ``DataFrame`` to project
        :param overrides: Optional column expressions to use instead of defaults
        :return: Projected ``DataFrame``
        """
        select_exprs = overrides if overrides is not None else self.columns
        if not select_exprs:
            return df

        normalized_columns = [ensure_column(expr) for expr in select_exprs]
        return df.select(*normalized_columns)
