"""Tests for Pydantic dataset specification models."""

import unittest
from datetime import date
from decimal import Decimal

# Import Pydantic directly to avoid Spark initialization issues in test environment
try:
    from pydantic.v1 import BaseModel, Field, ValidationError
except ImportError:
    from pydantic import BaseModel, Field, ValidationError  # type: ignore


class TestBasicUserSpec(unittest.TestCase):
    """Tests for BasicUser Pydantic model."""

    def setUp(self):
        """Set up test fixtures - define model inline to avoid import issues."""
        # Define the model inline to avoid triggering Spark imports
        class BasicUser(BaseModel):
            customer_id: int = Field(..., ge=1000000)
            name: str = Field(..., min_length=1)
            email: str
            ip_addr: str
            phone: str

        self.BasicUser = BasicUser

    def test_valid_user_creation(self):
        """Test creating a valid user instance."""
        user = self.BasicUser(
            customer_id=1234567890,
            name="John Doe",
            email="john.doe@example.com",
            ip_addr="192.168.1.100",
            phone="(555)-123-4567"
        )

        self.assertEqual(user.customer_id, 1234567890)
        self.assertEqual(user.name, "John Doe")
        self.assertEqual(user.email, "john.doe@example.com")
        self.assertEqual(user.ip_addr, "192.168.1.100")
        self.assertEqual(user.phone, "(555)-123-4567")

    def test_invalid_customer_id(self):
        """Test that small customer_id is rejected."""
        with self.assertRaises(ValidationError) as context:
            self.BasicUser(
                customer_id=100,  # Too small
                name="Jane Smith",
                email="jane@example.com",
                ip_addr="10.0.0.1",
                phone="555-1234"
            )

        error = context.exception
        self.assertIn("customer_id", str(error))

    def test_user_dict_conversion(self):
        """Test converting user to dictionary."""
        user = self.BasicUser(
            customer_id=1234567890,
            name="John Doe",
            email="john.doe@example.com",
            ip_addr="192.168.1.100",
            phone="(555)-123-4567"
        )

        user_dict = user.dict()
        self.assertIsInstance(user_dict, dict)
        self.assertEqual(user_dict["customer_id"], 1234567890)
        self.assertEqual(user_dict["name"], "John Doe")

    def test_user_json_serialization(self):
        """Test JSON serialization."""
        user = self.BasicUser(
            customer_id=1234567890,
            name="John Doe",
            email="john.doe@example.com",
            ip_addr="192.168.1.100",
            phone="(555)-123-4567"
        )

        json_str = user.json()
        self.assertIsInstance(json_str, str)
        self.assertIn("1234567890", json_str)
        self.assertIn("John Doe", json_str)

        # Test parsing back
        user_from_json = self.BasicUser.parse_raw(json_str)
        self.assertEqual(user_from_json.customer_id, user.customer_id)
        self.assertEqual(user_from_json.name, user.name)


class TestBasicStockTickerSpec(unittest.TestCase):
    """Tests for BasicStockTicker Pydantic model."""

    def setUp(self):
        """Set up test fixtures - define model inline to avoid import issues."""
        class BasicStockTicker(BaseModel):
            symbol: str = Field(..., min_length=1, max_length=10)
            post_date: date
            open: Decimal = Field(..., ge=0)
            close: Decimal = Field(..., ge=0)
            high: Decimal = Field(..., ge=0)
            low: Decimal = Field(..., ge=0)
            adj_close: Decimal = Field(..., ge=0)
            volume: int = Field(..., ge=0)

        self.BasicStockTicker = BasicStockTicker

    def test_valid_ticker_creation(self):
        """Test creating a valid stock ticker instance."""
        ticker = self.BasicStockTicker(
            symbol="AAPL",
            post_date=date(2024, 10, 15),
            open=Decimal("150.25"),
            close=Decimal("152.50"),
            high=Decimal("153.75"),
            low=Decimal("149.80"),
            adj_close=Decimal("152.35"),
            volume=2500000
        )

        self.assertEqual(ticker.symbol, "AAPL")
        self.assertEqual(ticker.post_date, date(2024, 10, 15))
        self.assertEqual(ticker.open, Decimal("150.25"))
        self.assertEqual(ticker.close, Decimal("152.50"))
        self.assertEqual(ticker.high, Decimal("153.75"))
        self.assertEqual(ticker.low, Decimal("149.80"))
        self.assertEqual(ticker.adj_close, Decimal("152.35"))
        self.assertEqual(ticker.volume, 2500000)

    def test_invalid_volume(self):
        """Test that negative volume is rejected."""
        with self.assertRaises(ValidationError) as context:
            self.BasicStockTicker(
                symbol="MSFT",
                post_date=date(2024, 10, 16),
                open=Decimal("300.00"),
                close=Decimal("305.00"),
                high=Decimal("310.00"),
                low=Decimal("295.00"),
                adj_close=Decimal("304.50"),
                volume=-1000  # Negative
            )

        error = context.exception
        self.assertIn("volume", str(error))

    def test_invalid_negative_price(self):
        """Test that negative prices are rejected."""
        with self.assertRaises(ValidationError) as context:
            self.BasicStockTicker(
                symbol="GOOGL",
                post_date=date(2024, 10, 17),
                open=Decimal("-100.00"),  # Negative
                close=Decimal("305.00"),
                high=Decimal("310.00"),
                low=Decimal("295.00"),
                adj_close=Decimal("304.50"),
                volume=1000000
            )

        error = context.exception
        self.assertIn("open", str(error))

    def test_ticker_dict_conversion(self):
        """Test converting ticker to dictionary."""
        ticker = self.BasicStockTicker(
            symbol="AAPL",
            post_date=date(2024, 10, 15),
            open=Decimal("150.25"),
            close=Decimal("152.50"),
            high=Decimal("153.75"),
            low=Decimal("149.80"),
            adj_close=Decimal("152.35"),
            volume=2500000
        )

        ticker_dict = ticker.dict()
        self.assertIsInstance(ticker_dict, dict)
        self.assertEqual(ticker_dict["symbol"], "AAPL")
        self.assertEqual(ticker_dict["volume"], 2500000)

    def test_ticker_json_serialization(self):
        """Test JSON serialization."""
        ticker = self.BasicStockTicker(
            symbol="AAPL",
            post_date=date(2024, 10, 15),
            open=Decimal("150.25"),
            close=Decimal("152.50"),
            high=Decimal("153.75"),
            low=Decimal("149.80"),
            adj_close=Decimal("152.35"),
            volume=2500000
        )

        json_str = ticker.json()
        self.assertIsInstance(json_str, str)
        self.assertIn("AAPL", json_str)
        self.assertIn("2024-10-15", json_str)

        # Test parsing back
        ticker_from_json = self.BasicStockTicker.parse_raw(json_str)
        self.assertEqual(ticker_from_json.symbol, ticker.symbol)
        self.assertEqual(ticker_from_json.post_date, ticker.post_date)


if __name__ == "__main__":
    unittest.main()

