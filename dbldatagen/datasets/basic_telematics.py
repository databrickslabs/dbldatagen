import warnings as w
from typing import Any, ClassVar

from pyspark.sql import SparkSession

import dbldatagen as dg
from dbldatagen.data_generator import DataGenerator
from dbldatagen.datasets.dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="basic/telematics",
                    summary="Telematics dataset for GPS tracking",
                    autoRegister=True,
                    supportsStreaming=True)
class BasicTelematicsProvider(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
    """
    Basic Telematics Dataset
    ========================

    This is a basic telematics dataset with time-series `lat`, `lon`, and `heading` values.

    It takes the following options when retrieving the table:
        - random: if True, generates random data
        - rows : number of rows to generate
        - partitions: number of partitions to use
        - numDevices: number of unique device IDs
        - startTimestamp: earliest timestamp for IOT time series data
        - endTimestamp: latest timestamp for IOT time series data
        - minLat: minimum latitude
        - maxLat: maximum latitude
        - minLon: minimum longitude
        - maxLon: maximum longitude
        - generateWKT: if `True`, generates the well-known text representation of the location

    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    Note that this datset does not use any features that would prevent it from being used as a source for a
    streaming dataframe, and so the flag `supportsStreaming` is set to True.

    """
    MIN_DEVICE_ID = 1000000
    MAX_DEVICE_ID = 9223372036854775807
    DEFAULT_NUM_DEVICES = 1000
    DEFAULT_START_TIMESTAMP = "2024-01-01 00:00:00"
    DEFAULT_END_TIMESTAMP = "2024-02-01 00:00:00"
    DEFAULT_MIN_LAT = -90.0
    DEFAULT_MAX_LAT = 90.0
    DEFAULT_MIN_LON = -180.0
    DEFAULT_MAX_LON = 180.0
    COLUMN_COUNT = 6
    ALLOWED_OPTIONS: ClassVar[list[str]] = [
        "numDevices",
        "startTimestamp",
        "endTimestamp",
        "minLat",
        "maxLat",
        "minLon",
        "maxLon",
        "generateWkt",
        "random"
    ]

    @DatasetProvider.allowed_options(options=ALLOWED_OPTIONS)
    def getTableGenerator(self, sparkSession: SparkSession, *, tableName: str|None=None, rows: int=-1, partitions: int=-1, **options: dict[str, Any]) -> DataGenerator:

        generateRandom = options.get("random", False)
        numDevices = options.get("numDevices", self.DEFAULT_NUM_DEVICES)
        startTimestamp = options.get("startTimestamp", self.DEFAULT_START_TIMESTAMP)
        endTimestamp = options.get("endTimestamp", self.DEFAULT_END_TIMESTAMP)
        minLat = options.get("minLat", self.DEFAULT_MIN_LAT)
        maxLat = options.get("maxLat", self.DEFAULT_MAX_LAT)
        minLon = options.get("minLon", self.DEFAULT_MIN_LON)
        maxLon = options.get("maxLon", self.DEFAULT_MAX_LON)
        generateWkt = options.get("generateWkt", False)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        if rows is None or rows < 0:
            rows = DatasetProvider.DEFAULT_ROWS
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, self.COLUMN_COUNT)
        if minLat < -90.0:
            minLat = -90.0
            w.warn("Received an invalid minLat value; Setting to -90.0", stacklevel=2)
        if minLat > 90.0:
            minLat = 89.0
            w.warn("Recieved an invalid minLat value; Setting to 89.0", stacklevel=2)
        if maxLat < -90:
            maxLat = -89.0
            w.warn("Recieved an invalid maxLat value; Setting to -89.0", stacklevel=2)
        if maxLat > 90.0:
            maxLat = 90.0
            w.warn("Received an invalid maxLat value; Setting to 90.0", stacklevel=2)
        if minLon < -180.0:
            minLon = -180.0
            w.warn("Received an invalid minLon value; Setting to -180.0", stacklevel=2)
        if minLon > 180.0:
            minLon = 179.0
            w.warn("Received an invalid minLon value; Setting to 179.0", stacklevel=2)
        if maxLon < -180.0:
            maxLon = -179.0
            w.warn("Received an invalid maxLon value; Setting to -179.0", stacklevel=2)
        if maxLon > 180.0:
            maxLon = 180.0
            w.warn("Received an invalid maxLon value; Setting to 180.0", stacklevel=2)
        if minLon > maxLon:
            (minLon, maxLon) = (maxLon, minLon)
            w.warn("Received minLon > maxLon; Swapping values", stacklevel=2)
        if minLat > maxLat:
            (minLat, maxLat) = (maxLat, minLat)
            w.warn("Received minLat > maxLat; Swapping values", stacklevel=2)
        df_spec = (
             dg.DataGenerator(sparkSession=sparkSession, rows=rows,
                              partitions=partitions, randomSeedMethod="hash_fieldname")
            .withColumn("device_id", "long", minValue=self.MIN_DEVICE_ID, maxValue=self.MAX_DEVICE_ID,
                            uniqueValues=numDevices, random=generateRandom)
            .withColumn("ts", "timestamp", begin=startTimestamp, end=endTimestamp,
                            interval="1 second", random=generateRandom)
            .withColumn("base_lat", "float", minValue=minLat, maxValue=maxLat, step=0.5,
                            baseColumn="device_id", omit=True)
            .withColumn("base_lon", "float", minValue=minLon, maxValue=maxLon, step=0.5,
                            baseColumn="device_id", omit=True)
            .withColumn("unv_lat", "float", expr="base_lat + (0.5-format_number(rand(), 3))*1e-3", omit=True)
            .withColumn("unv_lon", "float", expr="base_lon + (0.5-format_number(rand(), 3))*1e-3", omit=True)
            .withColumn("lat", "float", expr=f"""CASE WHEN unv_lat > {maxLat} THEN {maxLat}
                ELSE CASE WHEN unv_lat < {minLat} THEN {minLat}
                ELSE unv_lat END END""")
            .withColumn("lon", "float", expr=f"""CASE WHEN unv_lon > {maxLon} THEN {maxLon}
                ELSE CASE WHEN unv_lon < {minLon} THEN {minLon}
                ELSE unv_lon END END""")
            .withColumn("heading", "integer", minValue=0, maxValue=359, step=1, random=generateRandom)
            .withColumn("wkt", "string", expr="concat('POINT(', lon, ' ', lat, ')')", omit=not generateWkt)
        )

        return df_spec
