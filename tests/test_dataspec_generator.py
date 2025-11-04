# from unittest.mock import MagicMock, patch
# import pytest
# from pydantic import ValidationError
# from pyspark.sql import SparkSession
#
# from dbldatagen.extensions.datagen_spec import (
#     DatagenSpec,
#     TableDefinition,
#     ColumnDefinition,
#     FilePathTarget,
# )
#
# from dbldatagen.extensions.data_generator_from_spec import Generator
#
#
# @pytest.fixture
# def spark_session():
#     """Fixture to create a SparkSession for testing."""
#     return SparkSession.builder.appName("TestDataGen").master("local[2]").getOrCreate()
#
#
# @pytest.fixture
# def sample_config():
#     """Fixture to create a sample DatagenSpec for testing."""
#     return DatagenSpec(
#         tables={
#             "test_table": TableDefinition(
#                 number_of_rows=100,
#                 partitions=2,
#                 columns=[
#                     ColumnDefinition(name="id", type="string", primary=True),
#                     ColumnDefinition(name="value", type="integer", options={
#                                      "min": 1, "max": 100}),
#                 ],
#             )
#         },
#         output_destination=FilePathTarget(
#             base_path="/Volume/testing",
#             output_format="parquet",
#         ),
#     )
#
#
# def test_init_with_spark(spark_session):
#     """Test Generator initialization with provided SparkSession."""
#     generator = Generator(spark=spark_session)
#     assert generator.spark == spark_session
#
#
# def test_init_without_spark():
#     """Test Generator initialization without SparkSession."""
#     with pytest.raises(RuntimeError, match="SparkSession cannot be None"):
#         Generator(spark=None)
#
#
# def test_prepare_data_generators(spark_session, sample_config):
#     """Test preparing data generators from config."""
#     generator = Generator(spark=spark_session)
#     prepared_generators = generator.prepare_data_generators(sample_config)
#
#     assert "test_table" in prepared_generators
#     assert prepared_generators["test_table"] is not None
#     assert prepared_generators["test_table"].rowCount == 100
#
#
# def test_prepare_data_generators_with_invalid_config(spark_session):
#     """Test preparing data generators with invalid config."""
#     generator = Generator(spark=spark_session)
#
#     with pytest.raises(ValidationError, match="Value error, Primary column 'id' must have a type defined"):
#         invalid_config = DatagenSpec(
#             tables={
#                 "test_table": TableDefinition(
#                     number_of_rows=100,
#                     partitions=2,
#                     columns=[
#                         ColumnDefinition(
#                             name="id",
#                             primary=True,  # Missing type for primary key
#                         )
#                     ],
#                 )
#             },
#             output_destination=FilePathTarget(
#                 base_path="/Volume/test_output",
#                 output_format="parquet",
#             ),
#         )
#
#
# def test_creating_column_definition_with_nullable_primary_key_fails():
#     """
#     Tests that attempting to create a ColumnDefinition instance with
#     primary=True and nullable=True raises a Pydantic ValidationError.
#     """
#     with pytest.raises(ValidationError, match="Value error, Primary column 'id' cannot be nullable"):
#         # This is the action that should trigger the Pydantic validation error
#         ColumnDefinition(
#             name="id",
#             type="string",
#             primary=True,
#             nullable=True,  # This combination is invalid by your model's rules
#         )
#
#
# # Alternatively, if you want to test it through the DatagenSpec creation:
# def test_creating_datagenspec_with_nullable_primary_key_fails():
#     """
#     Tests that attempting to create a DatagenSpec instance containing
#     a nullable primary key raises a Pydantic ValidationError.
#     """
#     with pytest.raises(ValidationError, match="Value error, Primary column 'id' cannot be nullable"):
#         DatagenSpec(
#             tables={
#                 "test_table": TableDefinition(
#                     number_of_rows=100,
#                     partitions=2,
#                     columns=[
#                         ColumnDefinition(  # This is the invalid part
#                             name="id", type="string", primary=True, nullable=True
#                         )
#                     ],
#                 )
#             },
#             output_format="parquet",
#             output_path_prefix="/Volume/test_output",
#         )
#
#
# @patch("dbldatagen.extensions.data_generator_from_spec.Generator.write_prepared_data")
# def test_generate_and_write_data(mock_write, spark_session, sample_config):
#     """Test combined generation and writing of data."""
#     generator = Generator(spark=spark_session)
#     mock_write.return_value = True
#
#     result = generator.generate_and_write_data(sample_config)
#     assert result is True
#     mock_write.assert_called_once()
#
#
# def test_write_prepared_data(spark_session, sample_config):
#     """Test writing prepared data to output."""
#     generator = Generator(spark=spark_session)
#     prepared_generators = generator.prepare_data_generators(sample_config)
#
#     # Mock the DataFrame write operation
#     with patch("pyspark.sql.DataFrame.write") as mock_write:
#         mock_write.format.return_value = mock_write
#         mock_write.mode.return_value = mock_write
#         mock_write.save.return_value = None
#
#         result = generator.write_prepared_data(
#             prepared_generators=prepared_generators,
#             output_destination=sample_config.output_destination,
#         )
#
#         assert result is True
#         # mock_write.format.assert_called_with(sample_config.output_format)
#         # mock_write.mode.assert_called_with("overwrite")
#         # mock_write.save.assert_called()
#
#
# def test_write_prepared_data_with_empty_generators(spark_session):
#     """Test writing with empty prepared generators."""
#     generator = Generator(spark=spark_session)
#     result = generator.write_prepared_data({}, "parquet", "/tmp/test_output")
#     assert result is True  # Should return True as there's nothing to write
#
#
# def test_write_prepared_data_with_failed_generator(spark_session, sample_config):
#     """Test writing with a failed generator (None value)."""
#     generator = Generator(spark=spark_session)
#     prepared_generators = {"test_table": None}  # Simulate a failed generator
#
#     result = generator.write_prepared_data(prepared_generators, output_destination=sample_config.output_destination)
#     assert result is True  # Should return True as we skip failed generators
#
#
# def test_write_prepared_data_with_write_error(spark_session, sample_config):
#     """Test writing with a DataFrame write error."""
#     generator = Generator(spark=spark_session)
#     prepared_generators, _ = generator.prepare_data_generators(sample_config)
#
#     # Mock the DataFrame write operation to raise an error
#     with patch("pyspark.sql.DataFrame.write") as mock_write:
#         mock_write.format.return_value = mock_write
#         mock_write.mode.return_value = mock_write
#         mock_write.save.side_effect = Exception("Write error")
#
#         result = generator.write_prepared_data(
#             prepared_generators, output_destination=sample_config.output_destination)
#
#         assert result is False  # Should return False on write error
