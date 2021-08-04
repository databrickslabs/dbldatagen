import unittest

import dbldatagen as dg
from dbldatagen import PyfuncText, PyfuncTextFactory, FakerTextFactory

spark = dg.SparkSingleton.getLocalInstance("basic tests")


class TestTextGenerationPlugins(unittest.TestCase):
    row_count = 15000
    column_count = 10


    def test_plugins(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        def initPluginContext(context):
            context.prefix = "testing"

        text_generator = (lambda context, v: context.prefix + str(v))

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("text", text=PyfuncText(text_generator, init=initPluginContext))
                          )
        dfPlugin = pluginDataspec.build()

        self.assertTrue(dfPlugin.count() == data_rows)

        dfCheck = dfPlugin.where("text like 'testing%'")
        new_count = dfCheck.count()

        self.assertTrue(new_count == data_rows)

    def test_plugin_clone(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        def initPluginContext(context):
            context.prefix = "testing"

        text_generator = (lambda context, v: context.prefix + str(v))

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("text", text=PyfuncText(text_generator, init=initPluginContext))
                          )
        dfPlugin = pluginDataspec.build()

        dfCheck = dfPlugin.where("text like 'testing%'")
        new_count = dfCheck.count()

        self.assertTrue(new_count == data_rows)

        # now check the clone

        pluginDataspec_copy = pluginDataspec.clone()
        dfPlugin2 = pluginDataspec_copy.build()

        dfCheck2 = dfPlugin2.where("text like 'testing%'")
        new_count2 = dfCheck2.count()

        self.assertTrue(new_count2 == data_rows)

    def test_plugins_extended_syntax(self):
        """ test property syntax"""
        partitions_requested = 4
        data_rows = 100 * 1000

        class TestTextGen:
            def __init__(self):
                self._prefix = "testing1"

            def mkText(self):
                return self._prefix

        def initPluginContext(context):
            context.root = TestTextGen()

        CustomText = PyfuncTextFactory(name="CustomText").withInit(initPluginContext).withRootProperty("root")

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("text", text=CustomText("mkText"))
                          )
        dfPlugin = pluginDataspec.build()

        self.assertTrue(dfPlugin.count() == data_rows)

        dfCheck = dfPlugin.where("text like 'testing1'")
        new_count = dfCheck.count()

        self.assertTrue(new_count == data_rows)

    def test_plugins_extended_syntax2(self):
        """ test arg passing"""
        partitions_requested = 4
        data_rows = 100 * 1000

        class TestTextGen:
            def __init__(self):
                self._prefix = "testing1"

            @property
            def mkText(self):
                return self._prefix

        def initPluginContext(context):
            context.root = TestTextGen()

        CustomText = PyfuncTextFactory(name="CustomText").withInit(initPluginContext).withRootProperty("root")

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("text", text=CustomText("mkText", isProperty=True))
                          )
        dfPlugin = pluginDataspec.build()

        self.assertTrue(dfPlugin.count() == data_rows)

        dfCheck = dfPlugin.where("text like 'testing1'")
        new_count = dfCheck.count()

        self.assertTrue(new_count == data_rows)

    def test_plugins_extended_syntax3(self):
        partitions_requested = 4
        data_rows = 100 * 1000

        class TestTextGen:
            def __init__(self):
                self._prefix = "testing1"

            def mkText(self, extra=None):
                return self._prefix + extra

        def initPluginContext(context):
            context.root = TestTextGen()

        CustomText = PyfuncTextFactory(name="CustomText").withInit(initPluginContext).withRootProperty("root")

        pluginDataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("text", text=CustomText("mkText", extra="again"))
                          )
        dfPlugin = pluginDataspec.build()

        self.assertTrue(dfPlugin.count() == data_rows)

        dfCheck = dfPlugin.where("text like 'testing1again'")
        new_count = dfCheck.count()

        self.assertTrue(new_count == data_rows)

    @skip("wait for test integration")
    def test_plugins_faker_integration(self):
        spark.catalog.clearCache()

        shuffle_partitions_requested = 4
        partitions_requested = 4
        data_rows = 100 * 1000

        uniqueCustomers = 10 * 1000000

        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

        from faker.providers import internet

        FakerText = FakerTextFactory(providers=[internet])

        # partition parameters etc.
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        my_word_list = [
            'danish', 'cheesecake', 'sugar',
            'Lollipop', 'wafer', 'Gummies',
            'sesame', 'Jelly', 'beans',
            'pie', 'bar', 'Ice', 'oat']

        fakerDataspec2 = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                          .withColumn("customer_id", "int", uniqueValues=uniqueCustomers)
                          .withColumn("name", text=FakerText("name"))
                          .withColumn("alias", percent_nulls=0.3, text=FakerText("name"))
                          .withColumn("payment_instrument_type", text=FakerText("credit_card_provider"))
                          .withColumn("payment_instrument", text=FakerText("credit_card_number"))
                          .withColumn("email", text=FakerText("ascii_company_email"))
                          .withColumn("email2", text=FakerText("ascii_company_email"))
                          .withColumn("ip_address", text=FakerText("ipv4_private"))
                          .withColumn("md5_payment_instrument",
                                      expr="md5(concat(payment_instrument_type, ':', payment_instrument))",
                                      base_column=['payment_instrument_type', 'payment_instrument'])
                          .withColumn("customer_notes", text=FakerText("sentence", ext_word_list=my_word_list))
                          )
        dfFaker2 = fakerDataspec2.build()
        dfFaker2.show()







# run the tests
# if __name__ == '__main__':
#  print("Trying to run tests")
#  unittest.main(argv=['first-arg-is-ignored'],verbosity=2,exit=False)

# def runTests(suites):
#    suite = unittest.TestSuite()
#    result = unittest.TestResult()
#    for testSuite in suites:
#        suite.addTest(unittest.makeSuite(testSuite))
#    runner = unittest.TextTestRunner()
#    print(runner.run(suite))


# runTests([TestBasicOperation])

if __name__ == '__main__':
    unittest.main()
