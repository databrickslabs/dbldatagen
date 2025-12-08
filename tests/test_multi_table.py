# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
from pyspark.sql.functions import col

import dbldatagen as dg
from dbldatagen.multi_table_builder import MultiTableBuilder
from dbldatagen.relation import ForeignKeyRelation
from dbldatagen.utils import DataGenError


spark = dg.SparkSingleton.getLocalInstance("unit tests")


def _build_generator(name: str, column_names: list[str], rows: int = 10) -> dg.DataGenerator:
    """
    Helper to create a ``DataGenerator`` with deterministic integer columns.
    """
    generator = dg.DataGenerator(sparkSession=spark, name=name, rows=rows, partitions=1)

    for index, column_name in enumerate(column_names):
        min_value = index * 100
        max_value = min_value + rows - 1
        generator = generator.withColumn(column_name, "int", minValue=min_value, maxValue=max_value)

    return generator


class TestMultiTableBuilder:
    def test_single_data_generator_builds_dataset(self) -> None:
        builder = MultiTableBuilder()
        generator = _build_generator("single_gen", ["order_id", "order_value"], rows=12)

        builder.add_data_generator(
            name="orders",
            generator=generator,
            columns=[col("order_id").alias("id"), "order_value"],
        )

        results = builder.build()

        assert set(results) == {"orders"}
        orders_df = results["orders"]
        assert orders_df.count() == 12
        assert orders_df.columns == ["id", "order_value"]

    def test_independent_data_generators_build_individually(self) -> None:
        builder = MultiTableBuilder()

        generator_a = _build_generator("generator_a", ["a_id", "a_value"], rows=5)
        generator_b = _build_generator("generator_b", ["b_id", "b_value"], rows=7)

        builder.add_data_generator("table_a", generator_a, columns=["a_id", "a_value"])
        builder.add_data_generator("table_b", generator_b, columns=["b_id", "b_value"])

        results = builder.build()

        assert set(results.keys()) == {"table_a", "table_b"}
        assert results["table_a"].count() == 5
        assert results["table_b"].count() == 7
        assert len(builder.data_generators) == 2

    def test_foreign_key_relation_requires_shared_generator(self) -> None:
        builder = MultiTableBuilder()

        parent_generator = _build_generator("parent_gen", ["parent_id", "parent_value"], rows=6)
        child_generator = _build_generator("child_gen", ["child_id", "child_parent_id"], rows=6)

        builder.add_data_generator("parents", parent_generator, columns=["parent_id", "parent_value"])
        builder.add_data_generator(
            "children",
            child_generator,
            columns=["child_id", "child_parent_id"],
        )

        builder.add_foreign_key_relation(
            ForeignKeyRelation(
                from_table="children",
                from_column="child_parent_id",
                to_table="parents",
                to_column="parent_id",
            )
        )

        with pytest.raises(DataGenError):
            builder.build(["children"])

    def test_partial_relation_with_mismatched_generators_raises(self) -> None:
        builder = MultiTableBuilder()

        orders_generator = _build_generator("orders_gen", ["order_id", "order_value"], rows=6)
        line_items_generator = _build_generator(
            "line_items_gen",
            ["line_item_id", "order_id"],
            rows=8,
        )
        shipments_generator = _build_generator("shipments_gen", ["shipment_id"], rows=4)

        builder.add_data_generator("orders", orders_generator, columns=["order_id", "order_value"])
        builder.add_data_generator("line_items", line_items_generator, columns=["line_item_id", "order_id"])
        builder.add_data_generator("shipments", shipments_generator, columns=["shipment_id"])

        builder.add_foreign_key_relation(
            ForeignKeyRelation(
                from_table="line_items",
                from_column="order_id",
                to_table="orders",
                to_column="order_id",
            )
        )

        with pytest.raises(DataGenError):
            builder.build()

    def test_transitive_relation_requires_shared_generator(self) -> None:
        builder = MultiTableBuilder()

        generator_a = _build_generator("gen_a", ["a_id", "b_id"], rows=5)
        generator_b = _build_generator("gen_b", ["b_id", "c_id"], rows=5)
        generator_c = _build_generator("gen_c", ["c_id"], rows=5)

        builder.add_data_generator("table_a", generator_a, columns=["a_id", "b_id"])
        builder.add_data_generator("table_b", generator_b, columns=["b_id", "c_id"])
        builder.add_data_generator("table_c", generator_c, columns=["c_id"])

        builder.add_foreign_key_relation(
            ForeignKeyRelation(
                from_table="table_a",
                from_column="b_id",
                to_table="table_b",
                to_column="b_id",
            )
        )
        builder.add_foreign_key_relation(
            ForeignKeyRelation(
                from_table="table_b",
                from_column="c_id",
                to_table="table_c",
                to_column="c_id",
            )
        )

        with pytest.raises(DataGenError):
            builder.get_dataset("table_a")
