import pytest
from dbldatagen.spec.generator_spec import (
    DatagenSpec,
    DatasetDefinition,
    _validate_table_basic_properties,
    _validate_duplicate_columns,
    _validate_column_references,
    _validate_primary_key_columns,
    _validate_column_types,
    _check_circular_dependencies,
)
from dbldatagen.spec.column_spec import ColumnDefinition
from dbldatagen.spec.output_targets import UCSchemaTarget, FilePathTarget
from dbldatagen.spec.validation import ValidationResult


class TestValidationHelpers:
    """Tests for validation helper functions"""

    def test_validate_table_basic_properties_no_columns(self):
        """Test validation when table has no columns"""
        result = ValidationResult()
        table_def = DatasetDefinition(number_of_rows=100, columns=[])

        can_continue = _validate_table_basic_properties("test_table", table_def, result)

        assert not can_continue
        assert len(result.errors) == 1
        assert "at least one column" in result.errors[0]

    def test_validate_table_basic_properties_invalid_row_count(self):
        """Test validation when row count is invalid"""
        result = ValidationResult()
        table_def = DatasetDefinition(number_of_rows=0, columns=[ColumnDefinition(name="id", type="long")])

        can_continue = _validate_table_basic_properties("test_table", table_def, result)

        assert can_continue  # Can still check columns
        assert len(result.errors) == 1
        assert "invalid number_of_rows" in result.errors[0]

    def test_validate_table_basic_properties_invalid_partitions(self):
        """Test validation when partitions is invalid"""
        result = ValidationResult()
        table_def = DatasetDefinition(
            number_of_rows=100, partitions=0, columns=[ColumnDefinition(name="id", type="long")]
        )

        can_continue = _validate_table_basic_properties("test_table", table_def, result)

        assert can_continue
        assert len(result.errors) == 1
        assert "invalid partitions" in result.errors[0]

    def test_validate_duplicate_columns(self):
        """Test detection of duplicate column names"""
        result = ValidationResult()
        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=[
                ColumnDefinition(name="id", type="long"),
                ColumnDefinition(name="value", type="string"),
                ColumnDefinition(name="id", type="string"),  # Duplicate
            ],
        )

        _validate_duplicate_columns("test_table", table_def, result)

        assert len(result.errors) == 1
        assert "duplicate column names" in result.errors[0]
        assert "id" in result.errors[0]

    def test_validate_column_references_missing_base_column(self):
        """Test validation when baseColumn doesn't exist"""
        result = ValidationResult()
        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=[
                ColumnDefinition(name="col1", type="long", baseColumn="nonexistent"),
            ],
        )

        _validate_column_references("test_table", table_def, result)

        assert len(result.errors) == 1
        assert "does not exist" in result.errors[0]
        assert "nonexistent" in result.errors[0]

    def test_validate_column_references_id_is_allowed(self):
        """Test that referencing 'id' as baseColumn is allowed"""
        result = ValidationResult()
        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=[
                ColumnDefinition(name="col1", type="long", baseColumn="id"),
            ],
        )

        _validate_column_references("test_table", table_def, result)

        assert len(result.errors) == 0

    def test_validate_primary_key_columns_multiple(self):
        """Test warning when multiple primary key columns exist"""
        result = ValidationResult()
        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=[
                ColumnDefinition(name="id1", type="long", primary=True),
                ColumnDefinition(name="id2", type="long", primary=True),
            ],
        )

        _validate_primary_key_columns("test_table", table_def, result)

        assert len(result.warnings) == 1
        assert "multiple primary columns" in result.warnings[0]

    def test_validate_column_types_no_type_no_options(self):
        """Test warning when column has no type and no options"""
        result = ValidationResult()
        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=[
                ColumnDefinition(name="col1"),  # No type, no options
            ],
        )

        _validate_column_types("test_table", table_def, result)

        assert len(result.warnings) == 1
        assert "No type specified" in result.warnings[0]

    def test_check_circular_dependencies_simple_cycle(self):
        """Test detection of simple circular dependency"""
        columns = [
            ColumnDefinition(name="col1", type="long", baseColumn="col2"),
            ColumnDefinition(name="col2", type="long", baseColumn="col1"),
        ]

        errors = _check_circular_dependencies("test_table", columns)

        assert len(errors) >= 1
        assert "Circular dependency" in errors[0]

    def test_check_circular_dependencies_no_cycle(self):
        """Test that no errors are returned when there's no circular dependency"""
        columns = [
            ColumnDefinition(name="col1", type="long", baseColumn="id"),
            ColumnDefinition(name="col2", type="long", baseColumn="col1"),
        ]

        errors = _check_circular_dependencies("test_table", columns)

        assert len(errors) == 0

    def test_check_circular_dependencies_self_reference(self):
        """Test detection of self-referencing column"""
        columns = [
            ColumnDefinition(name="col1", type="long", baseColumn="col1"),
        ]

        errors = _check_circular_dependencies("test_table", columns)

        assert len(errors) == 1
        assert "Circular dependency" in errors[0]


class TestDatasetDefinition:
    """Tests for DatasetDefinition class"""

    def test_create_valid_dataset_definition(self):
        """Test creating a valid dataset definition"""
        dataset = DatasetDefinition(
            number_of_rows=1000,
            partitions=4,
            columns=[
                ColumnDefinition(name="id", type="long"),
                ColumnDefinition(name="name", type="string"),
            ],
        )

        assert dataset.number_of_rows == 1000
        assert dataset.partitions == 4
        assert len(dataset.columns) == 2

    def test_dataset_definition_with_none_partitions(self):
        """Test dataset definition with None partitions"""
        dataset = DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])

        assert dataset.partitions is None


class TestDatagenSpecValidation:
    """Tests for DatagenSpec validation"""

    def test_validate_empty_spec_error(self):
        """Test validation fails when spec has no datasets"""
        spec = DatagenSpec(datasets={})

        with pytest.raises(ValueError, match="at least one table"):
            spec.validate()

    def test_validate_no_output_destination_warning(self):
        """Test validation warns when no output destination is specified"""
        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])
            }
        )

        # With strict=True, warnings also cause validation to fail
        with pytest.raises(ValueError, match="No output_destination"):
            spec.validate(strict=True)

    def test_validate_strict_false_allows_warnings(self):
        """Test validation with strict=False allows warnings"""
        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])
            },
            output_destination=UCSchemaTarget(catalog="main", schema_="test"),
        )

        # Should pass even with warnings when strict=False
        result = spec.validate(strict=False)
        assert result is not None

    def test_validate_unknown_generator_option_warning(self):
        """Test validation warns about unknown generator options"""
        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])
            },
            output_destination=UCSchemaTarget(catalog="main", schema_="test"),
            generator_options={"unknownOption": True},
        )

        with pytest.raises(ValueError, match="Unknown generator option"):
            spec.validate(strict=True)

    def test_validate_known_generator_options(self):
        """Test validation accepts known generator options"""
        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])
            },
            output_destination=UCSchemaTarget(catalog="main", schema_="test"),
            generator_options={
                "random": True,
                "randomSeed": 42,
                "verbose": True,
            },
        )

        result = spec.validate(strict=False)
        assert result is not None

    def test_validate_circular_dependency_error(self):
        """Test validation detects circular dependencies"""
        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(
                    number_of_rows=100,
                    columns=[
                        ColumnDefinition(name="col1", type="long", baseColumn="col2"),
                        ColumnDefinition(name="col2", type="long", baseColumn="col1"),
                    ],
                )
            },
            output_destination=UCSchemaTarget(catalog="main", schema_="test"),
        )

        with pytest.raises(ValueError, match="Circular dependency"):
            spec.validate()


class TestDatagenSpecDisplayAllTables:
    """Tests for DatagenSpec display_all_tables method"""

    def test_display_all_tables_with_output_destination(self, capsys, mocker):
        """Test display_all_tables with output destination"""
        # Mock IPython display
        mock_display = mocker.patch("dbldatagen.spec.generator_spec.display")
        mock_html = mocker.patch("dbldatagen.spec.generator_spec.HTML")

        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])
            },
            output_destination=UCSchemaTarget(catalog="main", schema_="test"),
        )

        spec.display_all_tables()

        # Check that display was called
        assert mock_display.called
        captured = capsys.readouterr()
        assert "Table: table1" in captured.out

    def test_display_all_tables_without_output_destination(self, capsys, mocker):
        """Test display_all_tables without output destination"""
        # Mock IPython display
        mock_display = mocker.patch("dbldatagen.spec.generator_spec.display")
        mock_html = mocker.patch("dbldatagen.spec.generator_spec.HTML")

        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")])
            }
        )

        spec.display_all_tables()

        # Check that display was called with the warning message
        assert mock_display.called
        # Check that HTML was called with warning message
        call_args = [str(call) for call in mock_html.call_args_list]
        assert any("None" in str(call) for call in call_args)

    def test_display_all_tables_multiple_tables(self, capsys, mocker):
        """Test display_all_tables with multiple tables"""
        # Mock IPython display
        mocker.patch("dbldatagen.spec.generator_spec.display")
        mocker.patch("dbldatagen.spec.generator_spec.HTML")

        spec = DatagenSpec(
            datasets={
                "table1": DatasetDefinition(number_of_rows=100, columns=[ColumnDefinition(name="id", type="long")]),
                "table2": DatasetDefinition(number_of_rows=200, columns=[ColumnDefinition(name="name", type="string")]),
            },
            output_destination=FilePathTarget(base_path="/tmp/data", output_format="parquet"),
        )

        spec.display_all_tables()

        captured = capsys.readouterr()
        assert "Table: table1" in captured.out
        assert "Table: table2" in captured.out
