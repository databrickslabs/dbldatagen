from typing import ClassVar

from pyspark.sql import SparkSession

from dbldatagen.data_generator import DataGenerator

from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="basic/geometries",
                    summary="Geometry WKT dataset",
                    autoRegister=True,
                    supportsStreaming=True)
class BasicGeometriesProvider(DatasetProvider.NoAssociatedDatasetsMixin, DatasetProvider):
    """
    Basic Geometry WKT Dataset
    ==========================

    This is a basic process geospatial dataset with WKT strings representing `POINT`, `LINESTRING`,
    and `POLYGON` geometries.

    It takes the following options when retrieving the table:
        - random: if True, generates random data
        - rows : number of rows to generate
        - partitions: number of partitions to use
        - geometryType: one of `point`, `lineString`, or `polygon`, default is `polygon`
        - maxVertices: number of points in each `lineString` or `polygon`

    As the data specification is a DataGenerator object, you can add further columns to the data set and
    add constraints (when the feature is available)

    Note that this datset does not use any features that would prevent it from being used as a source for a
    streaming dataframe, and so the flag `supportsStreaming` is set to True.

    """
    MIN_LOCATION_ID = 1000000
    MAX_LOCATION_ID = 9223372036854775807
    DEFAULT_MIN_LAT = -90.0
    DEFAULT_MAX_LAT = 90.0
    DEFAULT_MIN_LON = -180.0
    DEFAULT_MAX_LON = 180.0
    COLUMN_COUNT = 2
    ALLOWED_OPTIONS: ClassVar[list[str]]  = [
        "geometryType",
        "maxVertices",
        "minLatitude",
        "maxLatitude",
        "minLongitude",
        "maxLongitude",
        "random"
    ]

    @DatasetProvider.allowed_options(options=ALLOWED_OPTIONS)
    def getTableGenerator(self, sparkSession: SparkSession, *, tableName: str|None=None, rows: int=-1, partitions: int=-1, **options: object) -> DataGenerator:
        # ruff: noqa: I001
        import dbldatagen as dg # noqa: PLC0415
        import warnings as w # noqa: PLC0415

        generateRandom = options.get("random", False)
        geometryType = options.get("geometryType", "point")
        maxVertices = options.get("maxVertices", 1 if geometryType == "point" else 3)
        minLatitude = options.get("minLatitude", self.DEFAULT_MIN_LAT)
        maxLatitude = options.get("maxLatitude", self.DEFAULT_MAX_LAT)
        minLongitude = options.get("minLongitude", self.DEFAULT_MIN_LON)
        maxLongitude = options.get("maxLongitude", self.DEFAULT_MAX_LON)

        assert tableName is None or tableName == DatasetProvider.DEFAULT_TABLE_NAME, "Invalid table name"
        if rows is None or rows < 0:
            rows = DatasetProvider.DEFAULT_ROWS
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, self.COLUMN_COUNT)

        df_spec = (
            dg.DataGenerator(sparkSession=sparkSession, name="test_data_set1", rows=rows,
                                partitions=partitions, randomSeedMethod="hash_fieldname")
            .withColumn("location_id", "long", minValue=self.MIN_LOCATION_ID, maxValue=self.MAX_LOCATION_ID,
                            uniqueValues=rows, random=generateRandom)
        )
        if geometryType == "point":
            if maxVertices > 1:
                w.warn("Ignoring property maxVertices for point geometries", stacklevel=2)
            df_spec = (
                df_spec.withColumn("lat", "float", minValue=minLatitude, maxValue=maxLatitude,
                                   step=1e-5, random=generateRandom, omit=True)
                .withColumn("lon", "float", minValue=minLongitude, maxValue=maxLongitude,
                            step=1e-5, random=generateRandom, omit=True)
                .withColumn("wkt", "string", expr="concat('POINT(', lon, ' ', lat, ')')")
            )
        elif geometryType == "lineString":
            if maxVertices < 2:
                maxVertices = 2
                w.warn("Parameter maxVertices must be >=2 for 'lineString' geometries; Setting to 2", stacklevel=2)
            j = 0
            while j < maxVertices:
                df_spec = (
                    df_spec.withColumn(f"lat_{j}", "float", minValue=minLatitude, maxValue=maxLatitude,
                                       step=1e-5, random=generateRandom, omit=True)
                        .withColumn(f"lon_{j}", "float", minValue=minLongitude, maxValue=maxLongitude,
                                        step=1e-5, random=generateRandom, omit=True)
                )
                j = j + 1
            concatCoordinatesExpr = [f"concat(lon_{j}, ' ', lat_{j}, ', ')" for j in range(maxVertices)]
            concatPairsExpr = f"replace(concat('LINESTRING(', {', '.join(concatCoordinatesExpr)}, ')'), ', )', ')')"
            df_spec = (
                df_spec.withColumn("wkt", "string", expr=concatPairsExpr)
            )
        elif geometryType == "polygon":
            if maxVertices < 3:
                maxVertices = 3
                w.warn("Parameter maxVertices must be >=3 for 'polygon' geometries; Setting to 3", stacklevel=2)
            j = 0
            while j < maxVertices:
                df_spec = (
                    df_spec.withColumn(f"lat_{j}", "float", minValue=minLatitude, maxValue=maxLatitude,
                                        step=1e-5, random=generateRandom, omit=True)
                        .withColumn(f"lon_{j}", "float", minValue=minLongitude, maxValue=maxLongitude,
                                        step=1e-5, random=generateRandom, omit=True)
                )
                j = j + 1
            vertexIndices = [*list(range(maxVertices)), 0]
            concatCoordinatesExpr = [f"concat(lon_{j}, ' ', lat_{j}, ', ')" for j in vertexIndices]
            concatPairsExpr = f"replace(concat('POLYGON(', {', '.join(concatCoordinatesExpr)}, ')'), ', )', ')')"
            df_spec = (
                df_spec.withColumn("wkt", "string", expr=concatPairsExpr)
            )
        else:
            raise ValueError("geometryType must be 'point', 'lineString', or 'polygon'")

        return df_spec
