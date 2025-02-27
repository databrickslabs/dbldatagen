from datetime import date
from contextlib import nullcontext as does_not_raise
import pytest
import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestStandardDatasetProviders:
    
    # BASIC GEOMETRIES tests:
    @pytest.mark.parametrize("providerName, providerOptions, expectation", [
        ("basic/geometries", {}, does_not_raise()),
        ("basic/geometries", {"rows": 50, "partitions": 4, "random": False,
                              "geometryType": "point", "maxVertices": 1}, does_not_raise()),
        ("basic/geometries", {"rows": 100, "partitions": -1, "random": False,
                              "geometryType": "point", "maxVertices": 2}, does_not_raise()),
        ("basic/geometries", {"rows": -1, "partitions": 4, "random": True,
                              "geometryType": "point"}, does_not_raise()),
        ("basic/geometries", {"rows": 5000, "partitions": -1, "random": True,
                              "geometryType": "lineString"}, does_not_raise()),
        ("basic/geometries", {"rows": -1, "partitions": -1, "random": False,
                              "geometryType": "lineString", "maxVertices": 2}, does_not_raise()),
        ("basic/geometries", {"rows": -1, "partitions": 4, "random": True,
                              "geometryType": "lineString", "maxVertices": 1}, does_not_raise()),
        ("basic/geometries", {"rows": 5000, "partitions": 4,
                              "geometryType": "lineString", "maxVertices": 2}, does_not_raise()),
        ("basic/geometries", {"rows": 5000, "partitions": -1, "random": False,
                              "geometryType": "polygon"}, does_not_raise()),
        ("basic/geometries", {"rows": -1, "partitions": -1, "random": True,
                              "geometryType": "polygon", "maxVertices": 3}, does_not_raise()),
        ("basic/geometries", {"rows": -1, "partitions": 4, "random": True,
                              "geometryType": "polygon", "maxVertices": 2}, does_not_raise()),
        ("basic/geometries", {"rows": 5000, "partitions": 4,
                              "geometryType": "polygon", "maxVertices": 5}, does_not_raise()),
        ("basic/geometries",
            {"rows": 5000, "partitions": 4, "geometryType": "polygon", "minLatitude": 45.0,
             "maxLatitude": 50.0, "minLongitude": -85.0, "maxLongitude": -75.0}, does_not_raise()),
        ("basic/geometries",
            {"rows": -1, "partitions": -1, "geometryType": "multipolygon"}, pytest.raises(ValueError))
    ])
    def test_basic_geometries_retrieval(self, providerName, providerOptions, expectation):
        with expectation:
            ds = dg.Datasets(spark, providerName).get(**providerOptions)
            assert ds is not None

            df = ds.build()
            assert df.count() >= 0
            assert "wkt" in df.columns

            geometryType = providerOptions.get("geometryType", None)
            row = df.first().asDict()
            if geometryType == "point" or geometryType is None:
                assert "POINT" in row["wkt"]

            if geometryType == "lineString":
                assert "LINESTRING" in row["wkt"]

            if geometryType == "polygon":
                assert "POLYGON" in row["wkt"]

            random = providerOptions.get("random", None)
            if random:
                print("")
                leadingRows = df.limit(100).collect()
                ids = [r.location_id for r in leadingRows]
                assert ids != sorted(ids)

    # BASIC PROCESS HISTORIAN tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("basic/process_historian", 
            {"rows": 50, "partitions": 4, "random": False, "numDevices": 1, "numPlants": 1,
            "numTags": 1, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00"}),
        ("basic/process_historian", 
            {"rows": 1000, "partitions": -1, "random": True, "numDevices": 10, "numPlants": 2,
            "numTags": 2, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00"}),
        ("basic/process_historian", 
            {"rows": 5000, "partitions": -1, "random": True, "numDevices": 100, "numPlants": 10,
            "numTags": 5, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00"}),
        ("basic/process_historian", {}),
        ("basic/process_historian", 
            {"rows": 5000, "partitions": -1, "random": True, "numDevices": 100, "numPlants": 10,
            "numTags": 5, "startTimestamp": "2020-04-01 00:00:00", "endTimestamp": "2020-01-01 00:00:00"}),
        ("basic/process_historian", 
            {"rows": 100, "partitions": -1, "random": True, "numDevices": 100, "numPlants": 10,
            "numTags": 5, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00"}),
        ("basic/process_historian",
            {"rows": 100, "partitions": -1, "random": True, "numDevices": 100, "numPlants": 10,
            "numTags": 5, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00",
            "dataQualityRatios": {"pctQuestionable": 0.1, "pctAnnotated": 0.05, "pctSubstituded": 0.12}}),
        ("basic/process_historian",
            {"rows": 100, "partitions": -1, "random": True, "numDevices": 100, "numPlants": 10,
            "numTags": 5, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00",
            "dataQualityRatios": {"pctQuestionable": 0.1, "pctSubstituded": 0.12}}),
        ("basic/process_historian",
            {"rows": 100, "partitions": -1, "random": True, "numDevices": 100, "numPlants": 10,
            "numTags": 5, "startTimestamp": "2020-01-01 00:00:00", "endTimestamp": "2020-04-01 00:00:00",
            "dataQualityRatios": {"pctAnnotated": 0.05}}),

    ])
    def test_basic_process_historian_retrieval(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0
        
        startTimestamp = providerOptions.get("startTimestamp", "2024-01-01 00:00:00")
        endTimestamp = providerOptions.get("endTimestamp", "2024-02-01 00:00:00")
        if startTimestamp > endTimestamp:
            (startTimestamp, endTimestamp) = (endTimestamp, startTimestamp)
        assert df.where(f'ts < "{startTimestamp}"').count() == 0
        assert df.where(f'ts > "{endTimestamp}"').count() == 0

        random = providerOptions.get("random", None)
        if random:
            print("")
            leadingRows = df.limit(100).collect()
            ids = [r.device_id for r in leadingRows]
            assert ids != sorted(ids)

    # BASIC STOCK TICKER tests:
    @pytest.mark.parametrize("providerName, providerOptions, expectation", [
        ("basic/stock_ticker",
         {"rows": 50, "partitions": 4, "numSymbols": 5, "startDate": "2024-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 100, "partitions": -1, "numSymbols": 5, "startDate": "2024-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": -1, "partitions": 4, "numSymbols": 10, "startDate": "2024-01-01"}, does_not_raise()),
        ("basic/stock_ticker", {}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 5000, "partitions": -1, "numSymbols": 50, "startDate": "2024-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 5000, "partitions": 4, "numSymbols": 50}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 5000, "partitions": 4, "startDate": "2024-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 5000, "partitions": 4, "numSymbols": 100, "startDate": "2024-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 1000, "partitions": -1, "numSymbols": 100, "startDate": "2025-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 1000, "partitions": -1, "numSymbols": 10, "startDate": "2020-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 50, "partitions": 2, "numSymbols": 0, "startDate": "2020-06-04"}, pytest.raises(ValueError)),
        ("basic/stock_ticker",
         {"rows": 500, "numSymbols": 12, "startDate": "2025-06-04"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 10, "partitions": 1, "numSymbols": -1, "startDate": "2009-01-02"}, pytest.raises(ValueError)),
        ("basic/stock_ticker",
         {"partitions": 2, "numSymbols": 20, "startDate": "2021-01-01"}, does_not_raise()),
        ("basic/stock_ticker",
         {"rows": 50, "partitions": 2, "numSymbols": 2}, does_not_raise()),
    ])
    def test_basic_stock_ticker_retrieval(self, providerName, providerOptions, expectation):
        with expectation:
            ds = dg.Datasets(spark, providerName).get(**providerOptions)
            assert ds is not None
            df = ds.build()
            assert df.count() >= 0

            if "numSymbols" in providerOptions:
                assert df.selectExpr("symbol").distinct().count() == providerOptions.get("numSymbols")

            if "startDate" in providerOptions:
                assert df.selectExpr("min(post_date) as min_post_date") \
                            .collect()[0] \
                            .asDict()["min_post_date"] == date.fromisoformat(providerOptions.get("startDate"))

            assert df.where("""open < 0.0 
                            or close < 0.0 
                            or high < 0.0 
                            or low < 0.0 
                            or adj_close < 0.0""").count() == 0

            assert df.where("high < low").count() == 0

    # BASIC TELEMATICS tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("basic/telematics", 
            {"rows": 50, "partitions": 4, "random": False, "numDevices": 5000, "startTimestamp": "2020-01-01 00:00:00", 
            "endTimestamp": "2020-04-01 00:00:00", "minLat": 40.0, "maxLat": 43.0, "minLon": -93.0, "maxLon": -89.0,
            "generateWkt": False}),
        ("basic/telematics", 
            {"rows": 1000, "partitions": 4, "random": True, "numDevices": 1000, "startTimestamp": "2020-01-01 00:00:00", 
            "endTimestamp": "2020-04-01 00:00:00", "minLat": 45.0, "maxLat": 35.0, "minLon": -89.0, "maxLon": -93.0,
            "generateWkt": True}),
        ("basic/telematics", 
            {"rows": -1, "partitions": -1, "numDevices": 1000, "minLat": 98.0, "maxLat": 100.0, 
            "minLon": -181.0, "maxLon": -185.0, "generateWkt": False}),
        ("basic/telematics", 
            {"rows": 5000, "partitions": -1, "startTimestamp": "2020-01-01 00:00:00", 
            "endTimestamp": "2020-04-01 00:00:00", "generateWkt": True}),
        ("basic/telematics", {}),
        ("basic/telematics", 
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 50, "startTimestamp": "2020-06-01 00:00:00", 
            "endTimestamp": "2020-04-01 00:00:00", "minLat": 40.0, "maxLat": 43.0, "minLon": -93.0, "maxLon": -89.0,
            "generateWkt": False}),
        ("basic/telematics", 
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 100, "startTimestamp": "2020-01-01 00:00:00",
             "endTimestamp": "2020-04-01 00:00:00", "maxLat": 45.0, "minLon": -93.0, "generateWkt": False}),
        ("basic/telematics", 
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 100, "startTimestamp": "2020-01-01 00:00:00",
            "endTimestamp": "2020-04-01 00:00:00", "minLat": 45.0, "generateWkt": False}),
        ("basic/telematics",
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 100, "startTimestamp": "2020-01-01 00:00:00",
            "endTimestamp": "2020-04-01 00:00:00", "minLat": -120.0, "generateWkt": False}),
        ("basic/telematics",
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 100, "startTimestamp": "2020-01-01 00:00:00",
            "endTimestamp": "2020-04-01 00:00:00", "maxLat": -120.0, "generateWkt": False}),
        ("basic/telematics",
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 100, "startTimestamp": "2020-01-01 00:00:00",
            "endTimestamp": "2020-04-01 00:00:00", "minLon": 190.0, "generateWkt": False}),
        ("basic/telematics",
            {"rows": -1, "partitions": -1, "random": False, "numDevices": 100, "startTimestamp": "2020-01-01 00:00:00",
            "endTimestamp": "2020-04-01 00:00:00", "maxLon": 190.0, "generateWkt": False}),
    ])
    def test_basic_telematics_retrieval(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0

        row = df.first().asDict()
        assert "lat" in df.columns
        assert "lon" in df.columns
        assert "heading" in df.columns
        assert df.where('heading < 0 and heading > 359').count() == 0

        minLat = providerOptions.get("minLat", -90.0)
        maxLat = providerOptions.get("maxLat", 90.0)
        minLat = max(minLat, -90.0)
        maxLat = min(maxLat, 90.0)
        if minLat > 90.0:
            minLat = 89.0
        if maxLat < -90.0:
            maxLat = -89.0
        if minLat > maxLat:
            (minLat, maxLat) = (maxLat, minLat)
        assert df.where(f'lat < {minLat}').count() == 0
        assert df.where(f'lat > {maxLat}').count() == 0

        minLon = providerOptions.get("minLon", -180.0)
        maxLon = providerOptions.get("maxLon", 180.0)
        minLon = max(minLon, -180.0)
        maxLon = min(maxLon, 180.0)
        if minLon > 180.0:
            minLon = 179.0
        if maxLon < -180.0:
            maxLon = -179.0
        if minLon > maxLon:
            (minLon, maxLon) = (maxLon, minLon)
        assert df.where(f'lon < {minLon}').count() == 0
        assert df.where(f'lon > {maxLon}').count() == 0

        startTimestamp = providerOptions.get("startTimestamp", "2024-01-01 00:00:00")
        endTimestamp = providerOptions.get("endTimestamp", "2024-02-01 00:00:00")
        if startTimestamp > endTimestamp:
            (startTimestamp, endTimestamp) = (endTimestamp, startTimestamp)
        assert df.where(f'ts < "{startTimestamp}"').count() == 0
        assert df.where(f'ts > "{endTimestamp}"').count() == 0

        generateWkt = providerOptions.get("generateWkt", False)
        if generateWkt:
            assert "wkt" in row.keys()

        random = providerOptions.get("random", None)
        if random:
            print("")
            leadingRows = df.limit(100).collect()
            ids = [r.device_id for r in leadingRows]
            assert ids != sorted(ids)

    # BASIC USER tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("basic/user", {"rows": 50, "partitions": 4, "random": False, "dummyValues": 0}),
        ("basic/user", {"rows": -1, "partitions": 4, "random": False, "dummyValues": 0}),
        ("basic/user", {}),
        ("basic/user", {"rows": 100, "partitions": -1, "random": False, "dummyValues": 10}),
        ("basic/user", {"rows": 5000, "dummyValues": 4}),
        ("basic/user", {"rows": 100, "partitions": -1, "random": True, "dummyValues": 0}),
    ])
    def test_basic_user_table_retrieval(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None, f"""expected to get dataset specification for provider `{providerName}`
                                   with options: {providerOptions} 
                                """
        df = ds.build()

        assert df.count() >= 0

        random = providerOptions.get("random", None)
        if random:
            print("")
            leadingRows = df.limit(100).collect()
            customer_ids = [r.customer_id for r in leadingRows]
            assert customer_ids != sorted(customer_ids)

    # BENCHMARK GROUPBY tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("benchmark/groupby", {"rows": 50, "partitions": 4, "random": False, "groups": 10, "percentNulls": 0.1}),
        ("benchmark/groupby", {"rows": -1, "partitions": 4, "random": True, "groups": 100}),
        ("benchmark/groupby", {}),
        ("benchmark/groupby", {"rows": 1000, "partitions": -1, "random": False}),
        ("benchmark/groupby", {"rows": -1, "groups": 1000, "percentNulls": 0.2}),
        ("benchmark/groupby", {"rows": 1000, "partitions": -1, "random": True, "groups": 5000, "percentNulls": 0.5}),
        ("benchmark/groupby", {"rows": -1, "partitions": -1, "random": True, "groups": 0}),
        ("benchmark/groupby", {"rows": 10, "partitions": -1, "random": True, "groups": 100, "percentNulls": 0.1}),
        ("benchmark/groupby", {"rows": -1, "partitions": -1, "random": False, "groups": -50}),
        ("benchmark/groupby", {"rows": -1, "partitions": -1, "random": False, "groups": -50, "percentNulls": -12.1}),
        ("benchmark/groupby", {"rows": -1, "partitions": -1, "random": True, "groups": -50, "percentNulls": 1.1}),
    ])
    def test_benchmark_groupby_retrieval(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0

        percentNulls = providerOptions.get("percentNulls", 0.0)
        if percentNulls >= 1.0:
            return

        random = providerOptions.get("random", None)
        if random:
            print("")
            leadingRows = df.limit(100).collect()
            vals = [r.v3 for r in leadingRows]
            assert vals != sorted(vals)

    # MULTI-TABLE SALES ORDER tests:
    @pytest.mark.parametrize("providerName, providerOptions, expectation", [
        ("multi_table/sales_order", {"rows": 50, "partitions": 4}, does_not_raise()),
        ("multi_table/sales_order", {"rows": -1, "partitions": 4}, does_not_raise()),
        ("multi_table/sales_order", {}, does_not_raise()),
        ("multi_table/sales_order", {"rows": 100, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"rows": 5000, "dummyValues": 4}, does_not_raise()),
        ("multi_table/sales_order", {"rows": 100, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "customers", "numCustomers": 100}, does_not_raise()),
        ("multi_table/sales_order", {"table": "customers", "numCustomers": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "customers", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "customers", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "carriers", "numCarriers": 50}, does_not_raise()),
        ("multi_table/sales_order", {"table": "carriers", "numCarriers": -1, "dummyValues": 2}, does_not_raise()),
        ("multi_table/sales_order", {"table": "carriers", "numCustomers": 100}, does_not_raise()),
        ("multi_table/sales_order", {"table": "carriers", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "carriers", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "catalog_items", "numCatalogItems": -1,
                                     "dummyValues": 5}, does_not_raise()),
        ("multi_table/sales_order", {"table": "catalog_items", "numCatalogItems": 100,
                                     "numCustomers": 1000}, does_not_raise()),
        ("multi_table/sales_order", {"table": "catalog_items", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "catalog_items", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_orders", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_orders", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_orders", "numOrders": -1, "numCustomers": -1, "startDate": None,
                                     "endDate": None}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_orders", "numOrders": 1000, "numCustomers": 10,
                                     "dummyValues": 2}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_line_items", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_line_items", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_line_items", "numOrders": 1000,
                                     "dummyValues": 5}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_line_items", "numOrders": -1, "numCatalogItems": -1,
                                     "lineItemsPerOrder": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_shipments", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_shipments", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_shipments", "numOrders": 1000,
                                     "numCarriers": 10}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_order_shipments", "numOrders": -1, "numCarriers": -1,
                                     "dummyValues": 2}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_invoices", "rows": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_invoices", "rows": -1, "partitions": -1}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_invoices", "numOrders": 1000,
                                     "numCustomers": 10}, does_not_raise()),
        ("multi_table/sales_order", {"table": "base_invoices", "numOrders": -1, "dummyValues": 2}, does_not_raise()),
        ("multi_table/sales_order", {"table": "invalid_table_name"}, pytest.raises(ValueError))
    ])
    def test_multi_table_sales_order_retrieval(self, providerName, providerOptions, expectation):
        with expectation:
            ds = dg.Datasets(spark, providerName).get(**providerOptions)
            assert ds is not None, f"""expected to get dataset specification for provider `{providerName}`
                                       with options: {providerOptions} 
                                    """
            df = ds.build()
            assert df.limit(100).count() >= 0

    def test_full_multitable_sales_order_sequence(self):
        multiTableDataSet = dg.Datasets(spark, "multi_table/sales_order")
        options = {"numCustomers": 100, "numOrders": 1000, "numCarriers": 10, "numCatalogItems": 100,
                   "startDate": "2024-01-01", "endDate": "2024-12-31", "lineItemsPerOrder": 3}
        dfCustomers = multiTableDataSet.get(table="customers", **options).build()
        dfCarriers = multiTableDataSet.get(table="carriers", **options).build()
        dfCatalogItems = multiTableDataSet.get(table="catalog_items", **options).build()
        dfBaseOrders = multiTableDataSet.get(table="base_orders", **options).build()
        dfBaseOrderLineItems = multiTableDataSet.get(table="base_order_line_items", **options).build()
        dfBaseOrderShipments = multiTableDataSet.get(table="base_order_shipments", **options).build()
        dfBaseInvoices = multiTableDataSet.get(table="base_invoices", **options).build()

        tables = ["orders", "order_line_items", "order_shipments", "invoices"]
        for table in tables:
            df = multiTableDataSet.getSummaryDataset(
                table=table,
                customers=dfCustomers,
                carriers=dfCarriers,
                catalogItems=dfCatalogItems,
                baseOrders=dfBaseOrders,
                baseOrderLineItems=dfBaseOrderLineItems,
                baseOrderShipments=dfBaseOrderShipments,
                baseInvoices=dfBaseInvoices
            )

            assert df is not None
            assert df.count() >= 0
            assert df

    # MULTI-TABLE TELEPHONY tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("multi_table/telephony", {"rows": 50, "partitions": 4, "random": False}),
        ("multi_table/telephony", {"rows": -1, "partitions": 4, "random": False}),
        ("multi_table/telephony", {}),
        ("multi_table/telephony", {"rows": 100, "partitions": -1, "random": False}),
        ("multi_table/telephony", {"rows": 5000, "dummyValues": 4}),
        ("multi_table/telephony", {"rows": 100, "partitions": -1, "random": True}),
        ("multi_table/telephony", {"table": 'plans', "numPlans": 100}),
        ("multi_table/telephony", {"table": 'plans'}),
        ("multi_table/telephony", {"table": 'customers', "numPlans": 100, "numCustomers": 1000}),
        ("multi_table/telephony", {"table": 'customers', "numPlans": 100, "numCustomers": 1000}),
        ("multi_table/telephony", {"table": 'customers'}),
        ("multi_table/telephony", {"table": 'deviceEvents', "numPlans": 100, "numCustomers": 1000}),
        ("multi_table/telephony", {"table": 'deviceEvents'}),
        ("multi_table/telephony", {"table": 'deviceEvents', "numDays": 10}),
    ])
    def test_multi_table_retrieval(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None, f"""expected to get dataset specification for provider `{providerName}`
                                   with options: {providerOptions} 
                                """
        df = ds.build()

        assert df.limit(100).count() >= 0

    def test_full_multitable_sequence(self):
        multiTableDS = dg.Datasets(spark, "multi_table/telephony")
        options = {"numPlans": 50, "numCustomers": 100}
        dfPlans = multiTableDS.get(table="plans", **options).build()
        dfCustomers = multiTableDS.get(table="customers", **options).build()
        dfDeviceEvents = multiTableDS.get(table="deviceEvents", **options).build()
        dfInvoices = multiTableDS.getSummaryDataset(table="invoices", plans=dfPlans,
                                                    customers=dfCustomers,
                                                    deviceEvents=dfDeviceEvents)

        assert dfInvoices is not None
        assert dfInvoices.count() >= 0

        assert dfInvoices
