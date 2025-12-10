"""Tests for DatagenSpec specifications for datasets."""

import unittest

# Import DatagenSpec classes directly to avoid Spark initialization
from dbldatagen.spec.generator_spec import DatagenSpec, DatasetDefinition
from dbldatagen.spec.column_spec import ColumnDefinition


class TestBasicUserDatagenSpec(unittest.TestCase):
    """Tests for BasicUser DatagenSpec."""

    def test_basic_user_spec_creation(self):
        """Test creating a basic user DatagenSpec."""
        columns = [
            ColumnDefinition(
                name="customer_id",
                type="long",
                options={"minValue": 1000000, "maxValue": 9999999999}
            ),
            ColumnDefinition(
                name="name",
                type="string",
                options={"template": r"\w \w"}
            ),
            ColumnDefinition(
                name="email",
                type="string",
                options={"template": r"\w@\w.com"}
            ),
        ]

        table_def = DatasetDefinition(
            number_of_rows=1000,
            partitions=2,
            columns=columns
        )

        spec = DatagenSpec(
            datasets={"users": table_def},
            output_destination=None
        )

        self.assertIsNotNone(spec)
        self.assertIn("users", spec.datasets)
        self.assertEqual(spec.datasets["users"].number_of_rows, 1000)
        self.assertEqual(spec.datasets["users"].partitions, 2)
        self.assertEqual(len(spec.datasets["users"].columns), 3)

    def test_basic_user_spec_validation(self):
        """Test validating a basic user DatagenSpec."""
        columns = [
            ColumnDefinition(
                name="customer_id",
                type="long",
                options={"minValue": 1000000}
            ),
            ColumnDefinition(
                name="name",
                type="string",
                options={"template": r"\w \w"}
            ),
        ]

        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=columns
        )

        spec = DatagenSpec(
            datasets={"users": table_def}
        )

        validation_result = spec.validate(strict=False)
        self.assertTrue(validation_result.is_valid())
        self.assertEqual(len(validation_result.errors), 0)

    def test_column_with_base_column(self):
        """Test creating columns that depend on other columns."""
        columns = [
            ColumnDefinition(
                name="symbol_id",
                type="long",
                options={"minValue": 1, "maxValue": 100}
            ),
            ColumnDefinition(
                name="symbol",
                type="string",
                options={
                    "expr": "concat('SYM', symbol_id)"
                }
            ),
        ]

        table_def = DatasetDefinition(
            number_of_rows=50,
            columns=columns
        )

        spec = DatagenSpec(
            datasets={"symbols": table_def}
        )

        validation_result = spec.validate(strict=False)
        self.assertTrue(validation_result.is_valid())


class TestBasicStockTickerDatagenSpec(unittest.TestCase):
    """Tests for BasicStockTicker DatagenSpec."""

    def test_basic_stock_ticker_spec_creation(self):
        """Test creating a basic stock ticker DatagenSpec."""
        columns = [
            ColumnDefinition(
                name="symbol",
                type="string",
                options={"template": r"\u\u\u"}
            ),
            ColumnDefinition(
                name="post_date",
                type="date",
                options={"expr": "date_add(cast('2024-10-01' as date), floor(id / 100))"}
            ),
            ColumnDefinition(
                name="open",
                type="decimal",
                options={"minValue": 100.0, "maxValue": 500.0}
            ),
            ColumnDefinition(
                name="close",
                type="decimal",
                options={"minValue": 100.0, "maxValue": 500.0}
            ),
            ColumnDefinition(
                name="volume",
                type="long",
                options={"minValue": 100000, "maxValue": 5000000}
            ),
        ]

        table_def = DatasetDefinition(
            number_of_rows=1000,
            partitions=2,
            columns=columns
        )

        spec = DatagenSpec(
            datasets={"stock_tickers": table_def},
            output_destination=None
        )

        self.assertIsNotNone(spec)
        self.assertIn("stock_tickers", spec.datasets)
        self.assertEqual(spec.datasets["stock_tickers"].number_of_rows, 1000)
        self.assertEqual(len(spec.datasets["stock_tickers"].columns), 5)

    def test_stock_ticker_with_omitted_columns(self):
        """Test creating spec with omitted intermediate columns."""
        columns = [
            ColumnDefinition(
                name="base_price",
                type="decimal",
                options={"minValue": 100.0, "maxValue": 500.0},
                omit=True  # Intermediate column
            ),
            ColumnDefinition(
                name="open",
                type="decimal",
                options={"expr": "base_price * 0.99"}
            ),
            ColumnDefinition(
                name="close",
                type="decimal",
                options={"expr": "base_price * 1.01"}
            ),
        ]

        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=columns
        )

        spec = DatagenSpec(
            datasets={"prices": table_def}
        )

        validation_result = spec.validate(strict=False)
        self.assertTrue(validation_result.is_valid())

        # Check that omitted column is present
        omitted_cols = [col for col in columns if col.omit]
        self.assertEqual(len(omitted_cols), 1)
        self.assertEqual(omitted_cols[0].name, "base_price")


class TestDatagenSpecValidation(unittest.TestCase):
    """Tests for DatagenSpec validation."""

    def test_empty_tables_validation(self):
        """Test that spec with no tables fails validation."""
        spec = DatagenSpec(datasets={})

        with self.assertRaises(ValueError) as context:
            spec.validate(strict=False)

        # Verify error message mentions missing tables
        self.assertIn("at least one table", str(context.exception))

    def test_duplicate_column_names(self):
        """Test that duplicate column names are caught."""
        columns = [
            ColumnDefinition(name="id", type="long"),
            ColumnDefinition(name="id", type="string"),  # Duplicate!
        ]

        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=columns
        )

        spec = DatagenSpec(datasets={"test": table_def})

        with self.assertRaises(ValueError) as context:
            spec.validate(strict=False)

        # Verify the error message mentions duplicates
        self.assertIn("duplicate column names", str(context.exception))
        self.assertIn("id", str(context.exception))


    def test_negative_rows_validation(self):
        """Test that negative row counts fail validation."""
        columns = [
            ColumnDefinition(name="col1", type="long")
        ]

        # Create with negative rows using dict to bypass Pydantic validation
        table_def = DatasetDefinition(
            number_of_rows=-100,  # Invalid
            columns=columns
        )

        spec = DatagenSpec(datasets={"test": table_def})

        with self.assertRaises(ValueError) as context:
            spec.validate(strict=False)

        # Verify error message mentions invalid number_of_rows
        self.assertIn("invalid number_of_rows", str(context.exception))
        self.assertIn("-100", str(context.exception))

    def test_spec_with_generator_options(self):
        """Test creating spec with generator options."""
        columns = [
            ColumnDefinition(name="value", type="long")
        ]

        table_def = DatasetDefinition(
            number_of_rows=100,
            columns=columns
        )

        spec = DatagenSpec(
            datasets={"test": table_def},
            generator_options={
                "randomSeedMethod": "hash_fieldname",
                "verbose": True
            }
        )

        self.assertIsNotNone(spec.generator_options)
        self.assertEqual(spec.generator_options["randomSeedMethod"], "hash_fieldname")
        self.assertTrue(spec.generator_options["verbose"])


if __name__ == "__main__":
    unittest.main()



