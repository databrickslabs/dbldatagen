import pytest

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestStandardDatasetProviders:
    
    # BASIC GEOMETRIES tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("basic/geometries", 
            {"rows": 50, "partitions": 4, "random": False, "geometryType": "point", "maxVertices": 1}),
        ("basic/geometries", 
            {"rows": 100, "partitions": -1, "random": False, "geometryType": "point", "maxVertices": 1}),
        ("basic/geometries", 
            {"rows": -1, "partitions": 4, "random": True, "geometryType": "point"}),
        ("basic/geometries", {}),
        ("basic/geometries", 
            {"rows": 5000, "partitions": -1, "random": True, "geometryType": "lineString"}),
        ("basic/geometries", 
            {"rows": -1, "partitions": -1, "random": False,  "geometryType": "lineString", "maxVertices": 2}),
        ("basic/geometries", 
            {"rows": -1, "partitions": 4, "random": True,  "geometryType": "lineString", "maxVertices": 2}),
        ("basic/geometries", 
            {"rows": 5000, "partitions": 4, "geometryType": "lineString", "maxVertices": 2}),
        ("basic/geometries", 
            {"rows": 5000, "partitions": -1, "random": False, "geometryType": "polygon"}),
        ("basic/geometries", 
            {"rows": -1, "partitions": -1, "random": True,  "geometryType": "polygon", "maxVertices": 3}),
        ("basic/geometries", 
            {"rows": -1, "partitions": 4, "random": True,  "geometryType": "polygon", "maxVertices": 3}),
        ("basic/geometries", 
            {"rows": 5000, "partitions": 4, "geometryType": "polygon", "maxVertices": 5}),
    ])
    def test_basic_geometries(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0
        assert "wkt" in df.columns

        row = df.first().asDict()
        if "geometryType" not in providerOptions or providerOptions["geometryType"] == "point":
            assert "POINT" in row["wkt"]

        if providerOptions["geometryType"] == "lineString":
            assert "LINESTRING" in row["wkt"]

        if providerOptions["geometryType"] == "polygon":
            assert "POLYGON" in row["wkt"]

        if 'random' in providerOptions and providerOptions['random']:
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
    ])
    def test_basic_process_historian(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0
        
        startTimestamp = (
            "2024-01-01" if "startTimestamp" not in providerOptions else providerOptions["startTimestamp"]
        )
        assert df.where(f'ts < {startTimestamp}').count() == 0

        endTimestamp = (
            "2024-01-01" if "endTimestamp" not in providerOptions else providerOptions["endTimestamp"]
        )
        assert df.where(f'ts > {endTimestamp}').count() == 0

        if 'random' in providerOptions and providerOptions['random']:
            print("")
            leadingRows = df.limit(100).collect()
            ids = [r.device_id for r in leadingRows]
            assert ids != sorted(ids)

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
            "endTimestamp": "2020-04-01 00:00:00", "minLat": 45.0, "maxLon": -93.0, "generateWkt": False}),
    ])
    def test_basic_telematics(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0

        row = df.first().asDict()
        assert "lat" in df.columns
        assert "lon" in df.columns
        assert "heading" in df.columns
        assert df.where('heading < 0 and heading > 359').count() == 0

        minLat = -90.0 if "minLat" not in providerOptions else providerOptions["minLat"]
        assert df.where(f'lat < {minLat}').count() == 0

        maxLat = 90.0 if "maxLat" not in providerOptions else providerOptions["maxLat"]
        assert df.where(f'lat > {maxLat}').count() == 0

        minLon = -180.0 if "minLon" not in providerOptions else providerOptions["minLon"]
        assert df.where(f'lon < {minLon}').count() == 0

        maxLon = 180.0 if "maxLon" not in providerOptions else providerOptions["maxLon"]
        assert df.where(f'lon > {maxLon}').count() == 0

        startTimestamp = (
            "2024-01-01" if "startTimestamp" not in providerOptions else providerOptions["startTimestamp"]
        )
        assert df.where(f'ts < {startTimestamp}').count() == 0

        endTimestamp = (
            "2024-01-01" if "endTimestamp" not in providerOptions else providerOptions["endTimestamp"]
        )
        assert df.where(f'ts > {endTimestamp}').count() == 0

        generateWkt = False if "generateWkt" not in providerOptions else providerOptions["generateWkt"]
        if generateWkt:
            assert "wkt" in row.keys()

        if 'random' in providerOptions and providerOptions['random']:
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

        if 'random' in providerOptions and providerOptions['random']:
            print("")
            leadingRows = df.limit(100).collect()
            customer_ids = [r.customer_id for r in leadingRows]
            assert customer_ids != sorted(customer_ids)

    # BENCHMARK GROUPBY tests:
    @pytest.mark.parametrize("providerName, providerOptions", [
        ("benchmark/groupby", {"rows": 50, "partitions": 4, "random": False, "groups": 10}),
        ("benchmark/groupby", {"rows": -1, "partitions": 4, "random": True, "groups": 100}),
        ("benchmark/groupby", {}),
        ("benchmark/groupby", {"rows": 1000, "partitions": -1, "random": False}),
        ("benchmark/groupby", {"rows": -1, "groups": 1000}),
        ("benchmark/groupby", {"rows": 1000, "partitions": -1, "random": True, "groups": 5000}),
        ("benchmark/groupby", {"rows": -1, "partitions": -1, "random": True, "groups": 0}),
        ("benchmark/groupby", {"rows": -1, "partitions": -1, "random": True, "groups": -50}),
    ])
    def test_benchmark_groupby(self, providerName, providerOptions):
        ds = dg.Datasets(spark, providerName).get(**providerOptions)
        assert ds is not None

        df = ds.build()
        assert df.count() >= 0

        if 'random' in providerOptions and providerOptions['random']:
            print("")
            leadingRows = df.limit(100).collect()
            ids = [r.id4 for r in leadingRows]
            assert ids != sorted(ids)

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
