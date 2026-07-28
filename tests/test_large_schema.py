import re
import unittest

from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType

import dbldatagen as dg

schema = StructType(
    [
        StructField("PK1", StringType(), True),
        StructField("XYYZ_IDS", StringType(), True),
        StructField("R_ID", IntegerType(), True),
        StructField("CL_ID", StringType(), True),
        StructField("INGEST_DATE", TimestampType(), True),
        StructField("CMPY_ID", DecimalType(38, 0), True),
        StructField("TXN_ID", DecimalType(38, 0), True),
        StructField("SEQUENCE_NUMBER", DecimalType(38, 0), True),
        StructField("DETAIL_ORDER", DecimalType(38, 0), True),
        StructField("TX_T_ID", DecimalType(38, 0), True),
        StructField("TXN_DATE", TimestampType(), True),
        StructField("AN_ID", DecimalType(38, 0), True),
        StructField("ANC_ID", DecimalType(38, 0), True),
        StructField("ANV_ID", DecimalType(38, 0), True),
        StructField("ANE_ID", DecimalType(38, 0), True),
        StructField("AND_ID", DecimalType(38, 0), True),
        StructField("APM_ID", DecimalType(38, 0), True),
        StructField("ACL_ID", DecimalType(38, 0), True),
        StructField("MEMO_TEXT", StringType(), True),
        StructField("ITEM_ID", DecimalType(38, 0), True),
        StructField("ITEM2_ID", DecimalType(38, 0), True),
        StructField("V1_BASE", DecimalType(38, 9), True),
        StructField("V1_YTD_AMT", DecimalType(38, 9), True),
        StructField("V1_YTD_HOURS", DecimalType(38, 0), True),
        StructField("ISTT", DecimalType(38, 9), True),
        StructField("XXX_AMT", StringType(), True),
        StructField("XXX_BASE", StringType(), True),
        StructField("XXX_ISTT", StringType(), True),
        StructField("HOURS", DecimalType(38, 0), True),
        StructField("STATE", DecimalType(38, 0), True),
        StructField("LSTATE", DecimalType(38, 0), True),
        StructField("XXX_JURISDICTION_ID", DecimalType(38, 0), True),
        StructField("XXY_JURISDICTION_ID", DecimalType(38, 0), True),
        StructField("AS_OF_DATE", TimestampType(), True),
        StructField("IS_PAYOUT", StringType(), True),
        StructField("IS_PYRL_LIABILITY", StringType(), True),
        StructField("IS_PYRL_SUMMARY", StringType(), True),
        StructField("PYRL_LIABILITY_DATE", TimestampType(), True),
        StructField("PYRL_LIAB_BEGIN_DATE", TimestampType(), True),
        StructField("QTY", DecimalType(38, 9), True),
        StructField("RATE", DecimalType(38, 9), True),
        StructField("AMOUNT", DecimalType(38, 9), True),
        StructField("SPERCENT", DecimalType(38, 9), True),
        StructField("DOC_XREF", StringType(), True),
        StructField("IS_A", StringType(), True),
        StructField("IS_S", StringType(), True),
        StructField("IS_CP", StringType(), True),
        StructField("IS_VP", StringType(), True),
        StructField("IS_B", StringType(), True),
        StructField("IS_EX", StringType(), True),
        StructField("IS_I", StringType(), True),
        StructField("IS_CL", StringType(), True),
        StructField("IS_DPD", StringType(), True),
        StructField("IS_DPD2", StringType(), True),
        StructField("DPD_ID", DecimalType(38, 0), True),
        StructField("IS_NP", StringType(), True),
        StructField("TAXABLE_TYPE", DecimalType(38, 0), True),
        StructField("IS_ARP", StringType(), True),
        StructField("IS_APP", StringType(), True),
        StructField("BALANCE1", DecimalType(38, 9), True),
        StructField("BALANCE2", DecimalType(38, 9), True),
        StructField("IS_FLAG1", StringType(), True),
        StructField("IS_FLAG2", StringType(), True),
        StructField("STATEMENT_ID", DecimalType(38, 0), True),
        StructField("INVOICE_ID", DecimalType(38, 0), True),
        StructField("STATEMENT_DATE", TimestampType(), True),
        StructField("INVOICE_DATE", TimestampType(), True),
        StructField("DUE_DATE", TimestampType(), True),
        StructField("EXAMPLE1_ID", DecimalType(38, 0), True),
        StructField("EXAMPLE2_ID", DecimalType(38, 0), True),
        StructField("IS_FLAG3", StringType(), True),
        StructField("ANOTHER_ID", DecimalType(38, 0), True),
        StructField("MARKUP", DecimalType(38, 9), True),
        StructField("S_DATE", TimestampType(), True),
        StructField("SD_TYPE", DecimalType(38, 0), True),
        StructField("SOURCE_TXN_ID", DecimalType(38, 0), True),
        StructField("SOURCE_TXN_SEQUENCE", DecimalType(38, 0), True),
        StructField("PAID_DATE", TimestampType(), True),
        StructField("OFX_TXN_ID", DecimalType(38, 0), True),
        StructField("OFX_MATCH_FLAG", DecimalType(38, 0), True),
        StructField("OLB_MATCH_MODE", DecimalType(38, 0), True),
        StructField("OLB_MATCH_AMOUNT", DecimalType(38, 9), True),
        StructField("OLB_RULE_ID", DecimalType(38, 0), True),
        StructField("ETMMODE", DecimalType(38, 0), True),
        StructField("DDA_ID", DecimalType(38, 0), True),
        StructField("DDL_STATUS", DecimalType(38, 0), True),
        StructField("ICFS", DecimalType(38, 0), True),
        StructField("CREATE_DATE", TimestampType(), True),
        StructField("CREATE_USER_ID", DecimalType(38, 0), True),
        StructField("LAST_MODIFY_DATE", TimestampType(), True),
        StructField("LAST_MODIFY_USER_ID", DecimalType(38, 0), True),
        StructField("EDIT_SEQUENCE", DecimalType(38, 0), True),
        StructField("ADDED_AUDIT_ID", DecimalType(38, 0), True),
        StructField("AUDIT_ID", DecimalType(38, 0), True),
        StructField("AUDIT_FLAG", StringType(), True),
        StructField("EXCEPTION_FLAG", StringType(), True),
        StructField("IS_PENALTY", StringType(), True),
        StructField("IS_INTEREST", StringType(), True),
        StructField("NET_AMOUNT", DecimalType(38, 9), True),
        StructField("TAX_AMOUNT", DecimalType(38, 9), True),
        StructField("TAX_CODE_ID", DecimalType(38, 0), True),
        StructField("TAX_RATE_ID", DecimalType(38, 0), True),
        StructField("CURRENCY_TYPE", DecimalType(38, 0), True),
        StructField("EXCHANGE_RATE", DecimalType(38, 9), True),
        StructField("HOME_AMOUNT", DecimalType(38, 9), True),
        StructField("HOME_OPEN_BALANCE", DecimalType(38, 9), True),
        StructField("IS_FOREX_GAIN_LOSS", StringType(), True),
        StructField("SPECIAL_TAX_TYPE", DecimalType(38, 0), True),
        StructField("SPECIAL_TAX_OPEN_BALANCE", DecimalType(38, 9), True),
        StructField("TAX_OVERRIDE_DELTA_AMOUNT", DecimalType(38, 9), True),
        StructField("INCLUSIVE_AMOUNT", DecimalType(38, 9), True),
        StructField("CUSTOM_ACCOUNT_TAX_AMT", DecimalType(38, 9), True),
        StructField("J_CODE_ID", DecimalType(38, 0), True),
        StructField("DISCOUNT_ID", DecimalType(38, 0), True),
        StructField("DISCOUNT_AMOUNT", DecimalType(38, 9), True),
        StructField("TXN_DISCOUNT_AMOUNT", DecimalType(38, 9), True),
        StructField("SUBTOTAL_AMOUNT", DecimalType(38, 9), True),
        StructField("LINE_DETAIL_TYPE", DecimalType(38, 0), True),
        StructField("W_RATE_ID", DecimalType(38, 0), True),
        StructField("R_QUANTITY", DecimalType(38, 9), True),
        StructField("R_AMOUNT", DecimalType(38, 9), True),
        StructField("SRC_QTY_USED", DecimalType(38, 9), True),
        StructField("SRC_AMT_USED", DecimalType(38, 9), True),
        StructField("LM_CLOSED", StringType(), True),
        StructField("CUSTOM_FIELD_VALUES", StringType(), True),
        StructField("PROGRESS_TRACKING_TYPE", DecimalType(38, 0), True),
        StructField("ITEM_RATE_TYPE", DecimalType(38, 0), True),
        StructField("CUSTOM_FIELD_VALS", StringType(), True),
        StructField("REGION_C_CODE", StringType(), True),
        StructField("LAST_MODIFIED_UTC", TimestampType(), True),
        StructField("date", DateType(), True),
        StructField("yearMonth", StringType(), True),
        StructField("isDeleted", BooleanType(), True),
    ]
)

print("schema", schema)

spark = dg.SparkSingleton.getLocalInstance("unit tests")


# Test manipulation and generation of test data for a large schema
class TestLargeSchemaOperation(unittest.TestCase):
    testDataSpec = None
    dfTestData = None
    row_count = 100000

    def setUp(self):
        print("setting up")

    @classmethod
    def setUpClass(cls):
        cls.testDataSpec = (
            dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count, partitions=4)
            .withSchema(schema)
            .withIdOutput()
        )

        print("Test generation plan")
        print("=============================")
        cls.testDataSpec.explain()

        print("=============================")
        print("")
        cls.dfTestData = cls.testDataSpec.build().cache()

    def test_simple_data(self):
        self.assertIsNotNone(self.dfTestData)
        self.dfTestData.limit(30).show()

    def test_generated_data_count(self):
        count = self.dfTestData.count()
        self.assertEqual(count, self.row_count)

    def test_distinct_count(self):
        distinct_count = self.dfTestData.select('id').distinct().count()
        self.assertEqual(distinct_count, self.row_count)

    def test_script_table(self):
        tableScript = self.testDataSpec.scriptTable("testTable")
        self.assertIsNotNone(tableScript)
        print("tableScript", tableScript)

        # normally, the regex dot pattern does not match newlines but use of re.DOTALL relaxes matching
        pattern = re.compile(r"create\s+table\s+if\s+not\s+exists\s+\w+\s?\(.*\)", re.IGNORECASE | re.DOTALL)

        # note match for
        match = pattern.match(tableScript)
        self.assertIsNotNone(match)

        output_columns = self.testDataSpec.getOutputColumnNames()
        self.assertIn("CREATE TABLE IF NOT EXISTS", tableScript)

        for col in output_columns:
            self.assertTrue(col in tableScript)

    def test_large_clone(self):
        sale_values = ['RETAIL', 'ONLINE', 'WHOLESALE', 'RETURN']
        sale_weights = [1, 5, 5, 1]

        ds = (
            self.testDataSpec.clone()
            .withRowCount(1000)
            # test legacy argument `match_types`
            .withColumnSpecs(
                patterns=".*_ID", match_types=StringType(), format="%010d", minValue=10, maxValue=123, step=1
            )
            # test revised argument `matchTypes`
            .withColumnSpecs(
                patterns=".*_IDS", matchTypes=StringType(), format="%010d", minValue=1, maxValue=100, step=1
            )
            .withColumnSpec("R_ID", minValue=1, maxValue=100, step=1)
            .withColumnSpec("XYYZ_IDS", minValue=1, maxValue=123, step=1, format="%05d")
            # .withColumnSpec("nstr4", percentNulls=0.1, minValue=1, maxValue=9, step=2,  format="%04d")
            # example of IS_SALE
            .withColumnSpec("IS_S", values=sale_values, weights=sale_weights, random=True)
            # .withColumnSpec("nstr4", percentNulls=0.1, minValue=1, maxValue=9, step=2,  format="%04d")
        )

        df = ds.build()
        ds.explain()
        df.show()

        rowCount = df.count()
        self.assertEqual(rowCount, 1000)

        # TODO: add additional validation statements
        # check `id` values
        id_values1 = [r[0] for r in df.select("R_ID").distinct().collect()]
        self.assertSetEqual(set(id_values1), set(range(1, 101)))

        # check `cl_id` values
        cl_id_expected_values = [f"{x:010d}" for x in range(10, 124)]
        cl_id_values = [r[0] for r in df.select("CL_ID").distinct().collect()]
        self.assertSetEqual(set(cl_id_values), set(cl_id_expected_values))

        # check `xyyz id` values
        xyyz_values = [f"{x:05}" for x in range(1, 124)]
        id_values2 = [r[0] for r in df.select("XYYZ_IDS").distinct().collect()]
        self.assertSetEqual(set(id_values2), set(xyyz_values))


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
