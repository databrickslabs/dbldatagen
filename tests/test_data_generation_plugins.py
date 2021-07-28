import unittest

import dbldatagen as dg
from dbldatagen import PyfuncText, PyfuncTextFactory

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
