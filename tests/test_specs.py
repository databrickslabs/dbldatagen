from dbldatagen.spec.generator_spec import DatagenSpec
import pytest
from dbldatagen.spec.generator_spec import (
    DatagenSpec,
    TableDefinition,
    ColumnDefinition,
    UCSchemaTarget,
    FilePathTarget,
)
from dbldatagen.spec.validation import ValidationResult


class TestValidationResult:
    """Tests for ValidationResult class"""

    def test_empty_result_is_valid(self):
        result = ValidationResult()
        assert result.is_valid()
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_result_with_errors_is_invalid(self):
        result = ValidationResult()
        result.add_error("Test error")
        assert not result.is_valid()
        assert len(result.errors) == 1

    def test_result_with_only_warnings_is_valid(self):
        result = ValidationResult()
        result.add_warning("Test warning")
        assert result.is_valid()
        assert len(result.warnings) == 1

    def test_result_string_representation(self):
        result = ValidationResult()
        result.add_error("Error 1")
        result.add_error("Error 2")
        result.add_warning("Warning 1")

        result_str = str(result)
        assert "✗ Validation failed" in result_str
        assert "Errors (2)" in result_str
        assert "Error 1" in result_str
        assert "Error 2" in result_str
        assert "Warnings (1)" in result_str
        assert "Warning 1" in result_str

    def test_valid_result_string_representation(self):
        result = ValidationResult()
        result_str = str(result)
        assert "✓ Validation passed successfully" in result_str


class TestColumnDefinitionValidation:
    """Tests for ColumnDefinition validation"""

    def test_valid_primary_column(self):
        col = ColumnDefinition(
            name="id",
            type="int",
            primary=True
        )
        assert col.primary
        assert col.type == "int"

    def test_primary_column_with_min_max_raises_error(self):
        with pytest.raises(ValueError, match="cannot have min/max options"):
            ColumnDefinition(
                name="id",
                type="int",
                primary=True,
                options={"min": 1, "max": 100}
            )

    def test_primary_column_nullable_raises_error(self):
        with pytest.raises(ValueError, match="cannot be nullable"):
            ColumnDefinition(
                name="id",
                type="int",
                primary=True,
                nullable=True
            )

    def test_primary_column_without_type_raises_error(self):
        with pytest.raises(ValueError, match="must have a type defined"):
            ColumnDefinition(
                name="id",
                primary=True
            )

    def test_non_primary_column_without_type(self):
        # Should not raise
        col = ColumnDefinition(
            name="data",
            options={"values": ["a", "b", "c"]}
        )
        assert col.name == "data"


class TestDatagenSpecValidation:
    """Tests for DatagenSpec.validate() method"""

    def test_valid_spec_passes_validation(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="name", type="string", options={"values": ["Alice", "Bob"]}),
                    ]
                )
            },
            output_destination=UCSchemaTarget(catalog="main", schema_="default")
        )

        result = spec.validate(strict=False)
        assert result.is_valid()
        assert len(result.errors) == 0

    def test_empty_tables_raises_error(self):
        spec = DatagenSpec(tables={})

        with pytest.raises(ValueError, match="at least one table"):
            spec.validate(strict=True)

    def test_table_without_columns_raises_error(self):
        spec = DatagenSpec(
            tables={
                "empty_table": TableDefinition(
                    number_of_rows=100,
                    columns=[]
                )
            }
        )

        with pytest.raises(ValueError, match="must have at least one column"):
            spec.validate()

    def test_negative_row_count_raises_error(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=-10,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                )
            }
        )

        with pytest.raises(ValueError, match="invalid number_of_rows"):
            spec.validate()

    def test_zero_row_count_raises_error(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=0,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                )
            }
        )

        with pytest.raises(ValueError, match="invalid number_of_rows"):
            spec.validate()

    def test_invalid_partitions_raises_error(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    partitions=-5,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                )
            }
        )

        with pytest.raises(ValueError, match="invalid partitions"):
            spec.validate()

    def test_duplicate_column_names_raises_error(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="duplicate", type="string"),
                        ColumnDefinition(name="duplicate", type="int"),
                    ]
                )
            }
        )

        with pytest.raises(ValueError, match="duplicate column names"):
            spec.validate()

    def test_invalid_base_column_reference_raises_error(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="email", type="string", baseColumn="nonexistent"),
                    ]
                )
            }
        )

        with pytest.raises(ValueError, match="does not exist"):
            spec.validate()

    def test_circular_dependency_raises_error(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="col_a", type="string", baseColumn="col_b"),
                        ColumnDefinition(name="col_b", type="string", baseColumn="col_c"),
                        ColumnDefinition(name="col_c", type="string", baseColumn="col_a"),
                    ]
                )
            }
        )

        with pytest.raises(ValueError, match="Circular dependency"):
            spec.validate()

    def test_multiple_primary_columns_warning(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id1", type="int", primary=True),
                        ColumnDefinition(name="id2", type="int", primary=True),
                    ]
                )
            }
        )

        # In strict mode, warnings cause errors
        with pytest.raises(ValueError, match="multiple primary columns"):
            spec.validate(strict=True)

        # In non-strict mode, should pass but have warnings
        result = spec.validate(strict=False)
        assert result.is_valid()
        assert len(result.warnings) > 0
        assert any("multiple primary columns" in w for w in result.warnings)

    def test_column_without_type_or_options_warning(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="empty_col"),
                    ]
                )
            }
        )

        result = spec.validate(strict=False)
        assert result.is_valid()
        assert len(result.warnings) > 0
        assert any("No type specified" in w for w in result.warnings)

    def test_no_output_destination_warning(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                )
            }
        )

        result = spec.validate(strict=False)
        assert result.is_valid()
        assert len(result.warnings) > 0
        assert any("No output_destination" in w for w in result.warnings)

    def test_unknown_generator_option_warning(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                )
            },
            generator_options={"unknown_option": "value"}
        )

        result = spec.validate(strict=False)
        assert result.is_valid()
        assert len(result.warnings) > 0
        assert any("Unknown generator option" in w for w in result.warnings)

    def test_multiple_errors_collected(self):
        """Test that all errors are collected before raising"""
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=-10,  # Error 1
                    partitions=0,  # Error 2
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="id", type="string"),  # Error 3: duplicate
                        ColumnDefinition(name="email", baseColumn="phone"),  # Error 4: nonexistent
                    ]
                )
            }
        )

        with pytest.raises(ValueError) as exc_info:
            spec.validate()

        error_msg = str(exc_info.value)
        # Should contain all errors
        assert "invalid number_of_rows" in error_msg
        assert "invalid partitions" in error_msg
        assert "duplicate column names" in error_msg
        assert "does not exist" in error_msg

    def test_strict_mode_raises_on_warnings(self):
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                )
            }
            # No output_destination - will generate warning
        )

        # Strict mode should raise
        with pytest.raises(ValueError):
            spec.validate(strict=True)

        # Non-strict mode should pass
        result = spec.validate(strict=False)
        assert result.is_valid()

    def test_valid_base_column_chain(self):
        """Test that valid baseColumn chains work"""
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="id", type="int", primary=True),
                        ColumnDefinition(name="code", type="string", baseColumn="id"),
                        ColumnDefinition(name="hash", type="string", baseColumn="code"),
                    ]
                )
            },
            output_destination=FilePathTarget(base_path="/tmp/data", output_format="parquet")
        )

        result = spec.validate(strict=False)
        assert result.is_valid()

    def test_multiple_tables_validation(self):
        """Test validation across multiple tables"""
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=100,
                    columns=[ColumnDefinition(name="id", type="int", primary=True)]
                ),
                "orders": TableDefinition(
                    number_of_rows=-50,  # Error in second table
                    columns=[ColumnDefinition(name="order_id", type="int", primary=True)]
                ),
                "products": TableDefinition(
                    number_of_rows=200,
                    columns=[]  # Error: no columns
                )
            }
        )

        with pytest.raises(ValueError) as exc_info:
            spec.validate()

        error_msg = str(exc_info.value)
        # Should find errors in both tables
        assert "orders" in error_msg
        assert "products" in error_msg


class TestTargetValidation:
    """Tests for output target validation"""

    def test_valid_uc_schema_target(self):
        target = UCSchemaTarget(catalog="main", schema_="default")
        assert target.catalog == "main"
        assert target.schema_ == "default"

    def test_uc_schema_empty_catalog_raises_error(self):
        with pytest.raises(ValueError, match="non-empty"):
            UCSchemaTarget(catalog="", schema_="default")

    def test_valid_file_path_target(self):
        target = FilePathTarget(base_path="/tmp/data", output_format="parquet")
        assert target.base_path == "/tmp/data"
        assert target.output_format == "parquet"

    def test_file_path_empty_base_path_raises_error(self):
        with pytest.raises(ValueError, match="non-empty"):
            FilePathTarget(base_path="", output_format="csv")

    def test_file_path_invalid_format_raises_error(self):
        with pytest.raises(ValueError):
            FilePathTarget(base_path="/tmp/data", output_format="json")


class TestValidationIntegration:
    """Integration tests for validation"""

    def test_realistic_valid_spec(self):
        """Test a realistic, valid specification"""
        spec = DatagenSpec(
            tables={
                "users": TableDefinition(
                    number_of_rows=1000,
                    partitions=4,
                    columns=[
                        ColumnDefinition(name="user_id", type="int", primary=True),
                        ColumnDefinition(name="username", type="string", options={
                            "template": r"\w{8,12}"
                        }),
                        ColumnDefinition(name="email", type="string", options={
                            "template": r"\w.\w@\w.com"
                        }),
                        ColumnDefinition(name="age", type="int", options={
                            "min": 18, "max": 99
                        }),
                    ]
                ),
                "orders": TableDefinition(
                    number_of_rows=5000,
                    columns=[
                        ColumnDefinition(name="order_id", type="int", primary=True),
                        ColumnDefinition(name="amount", type="decimal", options={
                            "min": 10.0, "max": 1000.0
                        }),
                    ]
                )
            },
            output_destination=UCSchemaTarget(
                catalog="main",
                schema_="synthetic_data"
            ),
            generator_options={
                "random": True,
                "randomSeed": 42
            }
        )

        result = spec.validate(strict=True)
        assert result.is_valid()
        assert len(result.errors) == 0
        assert len(result.warnings) == 0