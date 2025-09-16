from random import random
from typing import ClassVar

from pyspark.sql import SparkSession

import dbldatagen as dg
from dbldatagen.data_generator import DataGenerator
from dbldatagen.datasets.dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="basic/stock_ticker",
                    summary="Stock ticker dataset",
                    autoRegister=True,
                    supportsStreaming=True)
class BasicStockTickerProvider(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
    """
    Basic Stock Ticker Dataset
    ========================

    This is a basic stock ticker dataset with time-series `symbol`, `open`, `close`, `high`, `low`,
    `adj_close`, and `volume` values.

    It takes the following options when retrieving the table:
        - rows : number of rows to generate
        - partitions: number of partitions to use
        - numSymbols: number of unique stock ticker symbols
        - startDate: first date for stock ticker data
        - endDate: last date for stock ticker data
    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    Note that this dataset does not use any features that would prevent it from being used as a source for a
    streaming dataframe, and so the flag `supportsStreaming` is set to True.

    """
    DEFAULT_NUM_SYMBOLS = 100
    DEFAULT_START_DATE = "2024-10-01"
    COLUMN_COUNT = 8
    ALLOWED_OPTIONS: ClassVar[list[str]] = [
        "numSymbols",
        "startDate"
    ]

    @DatasetProvider.allowed_options(options=ALLOWED_OPTIONS)
    def getTableGenerator(self, sparkSession: SparkSession, *, tableName: str|None=None, rows: int=-1, partitions: int=-1, **options: object) -> DataGenerator:

        numSymbols = options.get("numSymbols", self.DEFAULT_NUM_SYMBOLS)
        startDate = options.get("startDate", self.DEFAULT_START_DATE)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        if rows is None or rows < 0:
            rows = DatasetProvider.DEFAULT_ROWS
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, self.COLUMN_COUNT)
        if numSymbols <= 0:
            raise ValueError("'numSymbols' must be > 0")

        df_spec = (
            dg.DataGenerator(sparkSession=sparkSession, rows=rows,
                             partitions=partitions, randomSeedMethod="hash_fieldname")
            .withColumn("symbol_id", "long", minValue=676, maxValue=676 + numSymbols - 1)
            .withColumn("rand_value", "float", minValue=0.0, maxValue=1.0, step=0.1,
                        baseColumn="symbol_id", omit=True)
            .withColumn("symbol", "string",
                        expr="""concat_ws('', transform(split(conv(symbol_id, 10, 26), ''),
                            x -> case when ascii(x) < 10 then char(ascii(x) - 48 + 65) else char(ascii(x) + 10) end))""")
            .withColumn("days_from_start_date", "int", expr=f"floor(try_divide(id, {numSymbols}))", omit=True)
            .withColumn("post_date", "date", expr=f"date_add(cast('{startDate}' as date), days_from_start_date)")
            .withColumn("start_value", "decimal(11,2)",
                        values=[1.0 + 199.0 * random() for _ in range(max(1, int(numSymbols / 10)))], omit=True)
            .withColumn("growth_rate", "float", values=[-0.1 + 0.35 * random() for _ in range(max(1, int(numSymbols / 10)))],
                        baseColumn="symbol_id")
            .withColumn("volatility", "float", values=[0.0075 * random() for _ in range(max(1, int(numSymbols / 10)))],
                        baseColumn="symbol_id", omit=True)
            .withColumn("prev_modifier_sign", "float",
                        expr=f"case when sin((id - {numSymbols}) % 17) > 0 then -1.0 else 1.0 end""",
                        omit=True)
            .withColumn("modifier_sign", "float",
                        expr="case when sin(id % 17) > 0 then -1.0 else 1.0 end",
                        omit=True)
            .withColumn("open_base", "decimal(11,2)",
                        expr=f"""start_value
                            + (volatility * prev_modifier_sign * start_value * sin((id - {numSymbols}) % 17))
                            + (growth_rate * start_value * try_divide(days_from_start_date - 1, 365))""",
                        omit=True)
            .withColumn("close_base", "decimal(11,2)",
                        expr="""start_value
                            + (volatility * start_value * sin(id % 17))
                            + (growth_rate * start_value * try_divide(days_from_start_date, 365))""",
                        omit=True)
            .withColumn("high_base", "decimal(11,2)",
                        expr="greatest(open_base, close_base) + rand() * volatility * open_base",
                        omit=True)
            .withColumn("low_base", "decimal(11,2)",
                        expr="least(open_base, close_base) - rand() * volatility * open_base",
                        omit=True)
            .withColumn("open", "decimal(11,2)", expr="greatest(open_base, 0.0)")
            .withColumn("close", "decimal(11,2)", expr="greatest(close_base, 0.0)")
            .withColumn("high", "decimal(11,2)", expr="greatest(high_base, 0.0)")
            .withColumn("low", "decimal(11,2)", expr="greatest(low_base, 0.0)")
            .withColumn("dividend", "decimal(4,2)", expr="0.05 * rand_value * close", omit=True)
            .withColumn("adj_close", "decimal(11,2)", expr="greatest(close - dividend, 0.0)")
            .withColumn("volume", "long", minValue=100000, maxValue=5000000, random=True)
        )

        return df_spec
