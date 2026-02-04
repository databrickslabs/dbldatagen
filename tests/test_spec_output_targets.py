import pytest
from dbldatagen.spec.output_targets import UCSchemaTarget, FilePathTarget


class TestUCSchemaTarget:
    """Tests for UCSchemaTarget class"""

    def test_create_valid_uc_schema_target(self):
        """Test creating a valid Unity Catalog schema target"""
        target = UCSchemaTarget(catalog="main", schema_="default")

        assert target.catalog == "main"
        assert target.schema_ == "default"
        assert target.output_format == "delta"  # Default

    def test_uc_schema_target_with_custom_format(self):
        """Test creating UC target with custom output format"""
        target = UCSchemaTarget(catalog="dev", schema_="sandbox", output_format="parquet")

        assert target.catalog == "dev"
        assert target.schema_ == "sandbox"
        assert target.output_format == "parquet"

    def test_uc_schema_target_empty_catalog_error(self):
        """Test that empty catalog raises validation error"""
        with pytest.raises(ValueError, match="non-empty"):
            UCSchemaTarget(catalog="", schema_="default")

    def test_uc_schema_target_empty_schema_error(self):
        """Test that empty schema raises validation error"""
        with pytest.raises(ValueError, match="non-empty"):
            UCSchemaTarget(catalog="main", schema_="")

    def test_uc_schema_target_whitespace_catalog_error(self):
        """Test that whitespace-only catalog raises validation error"""
        with pytest.raises(ValueError, match="non-empty"):
            UCSchemaTarget(catalog="   ", schema_="default")

    def test_uc_schema_target_whitespace_schema_error(self):
        """Test that whitespace-only schema raises validation error"""
        with pytest.raises(ValueError, match="non-empty"):
            UCSchemaTarget(catalog="main", schema_="   ")

    def test_uc_schema_target_non_identifier_warning(self, caplog):
        """Test that non-Python identifiers generate warnings"""
        import logging

        with caplog.at_level(logging.WARNING):
            target = UCSchemaTarget(catalog="my-catalog", schema_="my_schema")

        assert "my-catalog" in caplog.text
        assert "not a basic Python identifier" in caplog.text

    def test_uc_schema_target_strips_whitespace(self):
        """Test that whitespace is stripped from identifiers"""
        target = UCSchemaTarget(catalog="  main  ", schema_="  default  ")

        assert target.catalog == "main"
        assert target.schema_ == "default"

    def test_uc_schema_target_str_representation(self):
        """Test string representation of UC schema target"""
        target = UCSchemaTarget(catalog="main", schema_="test")

        str_repr = str(target)
        assert "main.test" in str_repr
        assert "delta" in str_repr
        assert "UC Table" in str_repr


class TestFilePathTarget:
    """Tests for FilePathTarget class"""

    def test_create_valid_filepath_target_parquet(self):
        """Test creating a valid file path target with parquet"""
        target = FilePathTarget(base_path="/tmp/data", output_format="parquet")

        assert target.base_path == "/tmp/data"
        assert target.output_format == "parquet"

    def test_create_valid_filepath_target_csv(self):
        """Test creating a valid file path target with CSV"""
        target = FilePathTarget(base_path="/dbfs/output", output_format="csv")

        assert target.base_path == "/dbfs/output"
        assert target.output_format == "csv"

    def test_filepath_target_empty_base_path_error(self):
        """Test that empty base_path raises validation error"""
        with pytest.raises(ValueError, match="non-empty"):
            FilePathTarget(base_path="", output_format="parquet")

    def test_filepath_target_whitespace_base_path_error(self):
        """Test that whitespace-only base_path raises validation error"""
        with pytest.raises(ValueError, match="non-empty"):
            FilePathTarget(base_path="   ", output_format="parquet")

    def test_filepath_target_strips_whitespace(self):
        """Test that whitespace is stripped from base_path"""
        target = FilePathTarget(base_path="  /tmp/data  ", output_format="parquet")

        assert target.base_path == "/tmp/data"

    def test_filepath_target_invalid_format_error(self):
        """Test that invalid output format raises validation error"""
        with pytest.raises(ValueError):
            FilePathTarget(base_path="/tmp/data", output_format="json")  # type: ignore

    def test_filepath_target_no_default_format(self):
        """Test that output_format has no default and must be specified"""
        with pytest.raises(Exception):  # ValidationError (Pydantic)
            FilePathTarget(base_path="/tmp/data")  # type: ignore

    def test_filepath_target_str_representation(self):
        """Test string representation of file path target"""
        target = FilePathTarget(base_path="/tmp/output", output_format="parquet")

        str_repr = str(target)
        assert "/tmp/output" in str_repr
        assert "parquet" in str_repr
        assert "File Path" in str_repr

    def test_filepath_target_with_s3_path(self):
        """Test file path target with S3 path"""
        target = FilePathTarget(base_path="s3://bucket/path/to/data", output_format="parquet")

        assert target.base_path == "s3://bucket/path/to/data"

    def test_filepath_target_with_dbfs_path(self):
        """Test file path target with DBFS path"""
        target = FilePathTarget(base_path="/dbfs/mnt/data", output_format="csv")

        assert target.base_path == "/dbfs/mnt/data"


class TestOutputTargetIntegration:
    """Integration tests for output targets"""

    def test_different_target_types_not_equal(self):
        """Test that UC and FilePath targets are distinct types"""
        uc_target = UCSchemaTarget(catalog="main", schema_="test")
        file_target = FilePathTarget(base_path="/tmp/data", output_format="parquet")

        assert type(uc_target) != type(file_target)

    def test_targets_can_be_used_in_union_type(self):
        """Test that targets can be used in Union type hints"""
        from typing import Union

        def process_target(target: Union[UCSchemaTarget, FilePathTarget, None]) -> str:
            if isinstance(target, UCSchemaTarget):
                return "uc"
            elif isinstance(target, FilePathTarget):
                return "file"
            return "none"

        assert process_target(UCSchemaTarget(catalog="main", schema_="test")) == "uc"
        assert process_target(FilePathTarget(base_path="/tmp", output_format="parquet")) == "file"
        assert process_target(None) == "none"
