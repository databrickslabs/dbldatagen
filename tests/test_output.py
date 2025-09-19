import os
import shutil
import uuid
import pytest

from pyspark.sql.types import IntegerType, StringType, FloatType

import dbldatagen as dg


spark = dg.SparkSingleton.getLocalInstance("output tests")


class TestOutput:
    @pytest.fixture
    def get_output_directories(self):
        base_dir = f"/tmp/testdatagenerator/{uuid.uuid4()}"
        print("test dir created")
        data_dir = os.path.join(base_dir, "data")
        checkpoint_dir = os.path.join(base_dir, "checkpoint")
        os.makedirs(data_dir)
        os.makedirs(checkpoint_dir)

        print("\n\n*** Test directories", base_dir, data_dir, checkpoint_dir)
        yield base_dir, data_dir, checkpoint_dir

        shutil.rmtree(base_dir, ignore_errors=True)
        print(f"\n\n*** test dir [{base_dir}] deleted")

    @pytest.mark.parametrize("seed_column_name, table_format, table_location", [
        ("id", "delta", "/table_folder"),
        ("_id", "json", "/json_data_folder"),
        ("id", "csv", "/csv_data_folder"),
    ])
    def test_build_output_data_batch(self, get_output_directories, seed_column_name, table_format, table_location):
        base_dir, data_dir, checkpoint_dir = get_output_directories
        table_dir = f"{data_dir}/{table_location}"

        gen = dg.DataGenerator(
            sparkSession=spark,
            name="test_data_set1",
            rows=100,
            partitions=4,
            seedMethod='hash_fieldname',
            seedColumnName=seed_column_name
        )

        gen = (
            gen
            .withIdOutput()
            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
        )

        output_dataset = dg.OutputDataset(
            location=table_dir,
            output_mode="append",
            format=table_format,
            options={"mergeSchema": "true", "checkpointLocation": f"{data_dir}/{checkpoint_dir}"},
        )

        gen.buildOutputDataset(output_dataset)
        persisted_df = spark.read.format(table_format).load(table_dir)
        assert persisted_df.count() > 0

    @pytest.mark.parametrize("seed_column_name, table_format, table_location", [
        ("id", "delta", "/table_folder"),
        ("_id", "json", "/json_data_folder"),
        ("id", "csv", "/csv_data_folder"),
    ])
    def test_build_output_data_streaming(self, get_output_directories, seed_column_name, table_format, table_location):
        base_dir, data_dir, checkpoint_dir = get_output_directories
        table_dir = f"{data_dir}/{table_location}"

        gen = dg.DataGenerator(
            sparkSession=spark,
            name="test_data_set1",
            rows=100,
            partitions=4,
            seedMethod='hash_fieldname',
            seedColumnName=seed_column_name
        )

        gen = (
            gen
            .withIdOutput()
            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
        )

        output_dataset = dg.OutputDataset(
            location=table_dir,
            output_mode="append",
            format=table_format,
            options={"mergeSchema": "true", "checkpointLocation": f"{data_dir}/{checkpoint_dir}"},
            trigger={"availableNow": True}
        )

        gen.buildOutputDataset(output_dataset)
        persisted_df = spark.read.format(table_format).load(table_dir)
        assert persisted_df.count() > 0
