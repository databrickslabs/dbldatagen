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
    COLUMN_COUNT = 2
    ALLOWED_OPTIONS = [
        "geometryType", 
        "maxVertices",
        "random"
    ]

    @DatasetProvider.allowed_options(options=ALLOWED_OPTIONS)
    def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                 **options):
        import dbldatagen as dg

        generateRandom = options.get("random", False)
        geometryType = options.get("geometryType", "point")
        maxVertices = options.get("maxVertices", 1 if geometryType == "point" else 3)

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
                raise UserWarning('Ignoring property maxVertices for point geometries')
            df_spec = (
                df_spec.withColumn("lat", "float", minValue=-90.0, maxValue=90.0,
                                step=1e-5, random=generateRandom, omit=True)
                .withColumn("lon", "float", minValue=-180.0, maxValue=180.0,
                                step=1e-5, random=generateRandom, omit=True)
                .withColumn("wkt", "string", expr="concat('POINT(', lon, ' ', lat, ')')")
                )
        elif geometryType == "lineString":
            if maxVertices < 2:
                raise ValueError("maxVertices must be >= 2")
            j = 0
            while j < maxVertices:
                df_spec = (
                    df_spec.withColumn(f"lat_{j}", "float", minValue=-90.0, maxValue=90.0,
                                       step=1e-5, random=generateRandom, omit=True)
                        .withColumn(f"lon_{j}", "float", minValue=-180.0, maxValue=180.0,
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
                raise ValueError("maxVertices must be >= 3")
            j = 0
            while j < maxVertices:
                df_spec = (
                    df_spec.withColumn(f"lat_{j}", "float", minValue=-90.0, maxValue=90.0,
                                        step=1e-5, random=generateRandom, omit=True)
                        .withColumn(f"lon_{j}", "float", minValue=-180.0, maxValue=180.0, 
                                        step=1e-5, random=generateRandom, omit=True)
                )
                j = j + 1
            vertexIndices = list(range(maxVertices)) + [0]
            concatCoordinatesExpr = [f"concat(lon_{j}, ' ', lat_{j}, ', ')" for j in vertexIndices]
            concatPairsExpr = f"replace(concat('POLYGON(', {', '.join(concatCoordinatesExpr)}, ')'), ', )', ')')"
            df_spec = (
                df_spec.withColumn("wkt", "string", expr=concatPairsExpr)
            )
        else:
            raise ValueError("geometryType must be 'point', 'lineString', or 'polygon'")

        return df_spec
