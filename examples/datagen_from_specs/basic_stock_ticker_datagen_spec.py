"""DatagenSpec for Basic Stock Ticker Dataset.

This module defines a declarative Pydantic-based specification for generating
the basic stock ticker dataset, corresponding to the BasicStockTickerProvider.
"""

from random import random

from dbldatagen.spec.generator_spec import DatagenSpec, DatasetDefinition
from dbldatagen.spec.column_spec import ColumnDefinition


def create_basic_stock_ticker_spec(
    number_of_rows: int = 100000,
    partitions: int | None = None,
    num_symbols: int = 100,
    start_date: str = "2024-10-01"
) -> DatagenSpec:
    """Create a DatagenSpec for basic stock ticker data generation.

    This function creates a declarative specification matching the data generated
    by BasicStockTickerProvider in the datasets module. It generates time-series
    stock data with OHLC (Open, High, Low, Close) values, adjusted close, and volume.

    Args:
        number_of_rows: Total number of rows to generate (default: 100,000)
        partitions: Number of Spark partitions to use (default: auto-computed)
        num_symbols: Number of unique stock ticker symbols to generate (default: 100)
        start_date: Starting date for stock data in 'YYYY-MM-DD' format (default: "2024-10-01")

    Returns:
        DatagenSpec configured for basic stock ticker data generation

    Example:
        >>> spec = create_basic_stock_ticker_spec(
        ...     number_of_rows=10000,
        ...     num_symbols=50,
        ...     start_date="2024-01-01"
        ... )
        >>> spec.validate()
        >>> # Use with GeneratorSpecImpl to generate data

    Note:
        The stock prices use a growth model with volatility to simulate realistic
        price movements over time. Each symbol gets its own growth rate and volatility.
    """
    # Generate random values for start_value, growth_rate, and volatility
    # These need to be pre-computed for the values option
    num_value_sets = max(1, int(num_symbols / 10))
    start_values = [1.0 + 199.0 * random() for _ in range(num_value_sets)]
    growth_rates = [-0.1 + 0.35 * random() for _ in range(num_value_sets)]
    volatility_values = [0.0075 * random() for _ in range(num_value_sets)]

    columns = [
        # Symbol ID (numeric identifier for symbol)
        ColumnDefinition(
            name="symbol_id",
            type="long",
            options={
                "minValue": 676,
                "maxValue": 676 + num_symbols - 1
            }
        ),

        # Random value helper (omitted from output)
        ColumnDefinition(
            name="rand_value",
            type="float",
            options={
                "minValue": 0.0,
                "maxValue": 1.0,
                "step": 0.1
            },
            baseColumn="symbol_id",
            omit=True
        ),

        # Stock symbol (derived from symbol_id using base-26 conversion)
        ColumnDefinition(
            name="symbol",
            type="string",
            options={
                "expr": """concat_ws('', transform(split(conv(symbol_id, 10, 26), ''),
                    x -> case when ascii(x) < 10 then char(ascii(x) - 48 + 65) else char(ascii(x) + 10) end))"""
            }
        ),

        # Days offset from start date (omitted from output)
        ColumnDefinition(
            name="days_from_start_date",
            type="int",
            options={
                "expr": f"floor(try_divide(id, {num_symbols}))"
            },
            omit=True
        ),

        # Post date (trading date)
        ColumnDefinition(
            name="post_date",
            type="date",
            options={
                "expr": f"date_add(cast('{start_date}' as date), days_from_start_date)"
            }
        ),

        # Starting price for each symbol (omitted from output)
        ColumnDefinition(
            name="start_value",
            type="decimal",
            options={
                "values": start_values
            },
            omit=True
        ),

        # Growth rate for each symbol
        ColumnDefinition(
            name="growth_rate",
            type="float",
            options={
                "values": growth_rates
            },
            baseColumn="symbol_id"
        ),

        # Volatility for each symbol (omitted from output)
        ColumnDefinition(
            name="volatility",
            type="float",
            options={
                "values": volatility_values
            },
            baseColumn="symbol_id",
            omit=True
        ),

        # Previous day's modifier sign (omitted from output)
        ColumnDefinition(
            name="prev_modifier_sign",
            type="float",
            options={
                "expr": f"case when sin((id - {num_symbols}) % 17) > 0 then -1.0 else 1.0 end"
            },
            omit=True
        ),

        # Current day's modifier sign (omitted from output)
        ColumnDefinition(
            name="modifier_sign",
            type="float",
            options={
                "expr": "case when sin(id % 17) > 0 then -1.0 else 1.0 end"
            },
            omit=True
        ),

        # Base opening price (omitted from output)
        ColumnDefinition(
            name="open_base",
            type="decimal",
            options={
                "expr": f"""start_value
                    + (volatility * prev_modifier_sign * start_value * sin((id - {num_symbols}) % 17))
                    + (growth_rate * start_value * try_divide(days_from_start_date - 1, 365))"""
            },
            omit=True
        ),

        # Base closing price (omitted from output)
        ColumnDefinition(
            name="close_base",
            type="decimal",
            options={
                "expr": """start_value
                    + (volatility * start_value * sin(id % 17))
                    + (growth_rate * start_value * try_divide(days_from_start_date, 365))"""
            },
            omit=True
        ),

        # Base high price (omitted from output)
        ColumnDefinition(
            name="high_base",
            type="decimal",
            options={
                "expr": "greatest(open_base, close_base) + rand() * volatility * open_base"
            },
            omit=True
        ),

        # Base low price (omitted from output)
        ColumnDefinition(
            name="low_base",
            type="decimal",
            options={
                "expr": "least(open_base, close_base) - rand() * volatility * open_base"
            },
            omit=True
        ),

        # Final opening price (output column)
        ColumnDefinition(
            name="open",
            type="decimal",
            options={
                "expr": "greatest(open_base, 0.0)"
            }
        ),

        # Final closing price (output column)
        ColumnDefinition(
            name="close",
            type="decimal",
            options={
                "expr": "greatest(close_base, 0.0)"
            }
        ),

        # Final high price (output column)
        ColumnDefinition(
            name="high",
            type="decimal",
            options={
                "expr": "greatest(high_base, 0.0)"
            }
        ),

        # Final low price (output column)
        ColumnDefinition(
            name="low",
            type="decimal",
            options={
                "expr": "greatest(low_base, 0.0)"
            }
        ),

        # Dividend (omitted from output)
        ColumnDefinition(
            name="dividend",
            type="decimal",
            options={
                "expr": "0.05 * rand_value * close"
            },
            omit=True
        ),

        # Adjusted closing price (output column)
        ColumnDefinition(
            name="adj_close",
            type="decimal",
            options={
                "expr": "greatest(close - dividend, 0.0)"
            }
        ),

        # Trading volume (output column)
        ColumnDefinition(
            name="volume",
            type="long",
            options={
                "minValue": 100000,
                "maxValue": 5000000,
                "random": True
            }
        ),
    ]

    table_def = DatasetDefinition(
        number_of_rows=number_of_rows,
        partitions=partitions,
        columns=columns
    )

    spec = DatagenSpec(
        tables={"stock_tickers": table_def},
        output_destination=None,  # No automatic persistence
        generator_options={
            "randomSeedMethod": "hash_fieldname"
        }
    )

    return spec


# Pre-configured specs for common use cases
BASIC_STOCK_TICKER_SPEC_SMALL = create_basic_stock_ticker_spec(
    number_of_rows=1000,
    num_symbols=10,
    start_date="2024-10-01"
)
"""Pre-configured spec for small dataset (1,000 rows, 10 symbols)"""

BASIC_STOCK_TICKER_SPEC_MEDIUM = create_basic_stock_ticker_spec(
    number_of_rows=100000,
    num_symbols=100,
    start_date="2024-10-01"
)
"""Pre-configured spec for medium dataset (100,000 rows, 100 symbols)"""

BASIC_STOCK_TICKER_SPEC_LARGE = create_basic_stock_ticker_spec(
    number_of_rows=1000000,
    num_symbols=500,
    start_date="2024-01-01"
)
"""Pre-configured spec for large dataset (1,000,000 rows, 500 symbols, full year)"""

BASIC_STOCK_TICKER_SPEC_ONE_YEAR = create_basic_stock_ticker_spec(
    number_of_rows=36500,  # 100 symbols * 365 days
    num_symbols=100,
    start_date="2024-01-01"
)
"""Pre-configured spec for one year of daily data (100 symbols, 365 days)"""



