import pytest

import dbldatagen as dg

spark = dg.SparkSingleton.getLocalInstance("unit tests")


class TestStandardDatasetProviders:

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

    @pytest.mark.parametrize("providerName, providerOptions", [
        ("multi_table/telephony", {"rows": 50, "partitions": 4, "random": False}),
        ("multi_table/telephony", {"rows": -1, "partitions": 4, "random": False}),
        ("multi_table/telephony", {}),
        ("multi_table/telephony", {"rows": 100, "partitions": -1, "random": False}),
        ("multi_table/telephony", {"rows": 5000, "dummyValues": 4}),
        ("multi_table/telephony", {"rows": 100, "partitions": -1, "random": True}),
        ("multi_table/telephony", {"table": 'plans', "numPlans": 100}),
        ("multi_table/telephony", {"table": 'customers', "numPlans": 100, "numCustomers": 1000}),
        ("multi_table/telephony", {"table": 'deviceEvents', "numPlans": 100, "numCustomers": 1000}),
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
