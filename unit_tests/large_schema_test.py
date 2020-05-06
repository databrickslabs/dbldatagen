from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DecimalType
from pyspark.sql.types import BooleanType, DateType
import databrickslabs_testdatagenerator as datagen
from pyspark.sql import SparkSession
import unittest


schema = StructType([
StructField("compositePK",StringType(),True),
StructField("R3D3_CLUSTER_IDS",StringType(),True),
StructField("RUN_DETAIL_ID",DecimalType(38,0),True),
StructField("CLUSTER_ID",StringType(),True),
StructField("INGEST_DATE",TimestampType(),True),
StructField("COMPANY_ID",DecimalType(38,0),True),
StructField("TX_ID",DecimalType(38,0),True),
StructField("SEQUENCE",DecimalType(38,0),True),
StructField("LINE_ORDER",DecimalType(38,0),True),
StructField("TX_TYPE_ID",DecimalType(38,0),True),
StructField("TX_DATE",TimestampType(),True),
StructField("ACCOUNT_ID",DecimalType(38,0),True),
StructField("CUSTOMER_ID",DecimalType(38,0),True),
StructField("VENDOR_ID",DecimalType(38,0),True),
StructField("EMPLOYEE_ID",DecimalType(38,0),True),
StructField("DEPT_ID",DecimalType(38,0),True),
StructField("PAYMENT_METHOD_ID",DecimalType(38,0),True),
StructField("KLASS_ID",DecimalType(38,0),True),
StructField("MEMO_TEXT",StringType(),True),
StructField("ITEM_ID",DecimalType(38,0),True),
StructField("PITEM_ID",DecimalType(38,0),True),
StructField("WAGE_BASE",DecimalType(38,9),True),
StructField("YTD_AMT",DecimalType(38,9),True),
StructField("YTD_HOURS",DecimalType(38,0),True),
StructField("INCOME_SUBJECT_TO_TAX",DecimalType(38,9),True),
StructField("OVERRIDE_AMT",StringType(),True),
StructField("OVERRIDE_WAGE_BASE",StringType(),True),
StructField("OVERRIDE_ISTT",StringType(),True),
StructField("HOURS",DecimalType(38,0),True),
StructField("STATE",DecimalType(38,0),True),
StructField("LIVE_STATE",DecimalType(38,0),True),
StructField("LIVE_JURISDICTION_ID",DecimalType(38,0),True),
StructField("WORK_JURISDICTION_ID",DecimalType(38,0),True),
StructField("AS_OF_DATE",TimestampType(),True),
StructField("PTO_PAY_OUT",StringType(),True),
StructField("IS_PAYROLL_LIABILITY",StringType(),True),
StructField("IS_PAYROLL_SUMMARY",StringType(),True),
StructField("PAYROLL_LIABILITY_DATE",TimestampType(),True),
StructField("PAYROLL_LIAB_BEGIN_DATE",TimestampType(),True),
StructField("QUANTITY",DecimalType(38,9),True),
StructField("RATE",DecimalType(38,9),True),
StructField("AMOUNT",DecimalType(38,9),True),
StructField("SPLIT_PERCENT",DecimalType(38,9),True),
StructField("DOC_NUM",StringType(),True),
StructField("IS_PURCHASE",StringType(),True),
StructField("IS_SALE",StringType(),True),
StructField("IS_CUSTOMER_PAYMENT",StringType(),True),
StructField("IS_PAYMENT_TO_VENDOR",StringType(),True),
StructField("IS_BILL",StringType(),True),
StructField("IS_PROFITIBILITY_EXPENSE",StringType(),True),
StructField("IS_PROFITIBILITY_INCOME",StringType(),True),
StructField("IS_CLEARED",StringType(),True),
StructField("IS_DEPOSITED",StringType(),True),
StructField("IS_DEPOSITED2",StringType(),True),
StructField("DEPOSIT_ID",DecimalType(38,0),True),
StructField("IS_NO_POST",StringType(),True),
StructField("TAXABLE_TYPE",DecimalType(38,0),True),
StructField("IS_AR_PAID",StringType(),True),
StructField("IS_AP_PAID",StringType(),True),
StructField("OPEN_BALANCE",DecimalType(38,9),True),
StructField("PAYROLL_OPEN_BALANCE",DecimalType(38,9),True),
StructField("IS_STATEMENTED",StringType(),True),
StructField("IS_INVOICED",StringType(),True),
StructField("STATEMENT_ID",DecimalType(38,0),True),
StructField("INVOICE_ID",DecimalType(38,0),True),
StructField("STATEMENT_DATE",TimestampType(),True),
StructField("INVOICE_DATE",TimestampType(),True),
StructField("DUE_DATE",TimestampType(),True),
StructField("OTHER_ACCOUNT_ID",DecimalType(38,0),True),
StructField("OTHER_KLASS_ID",DecimalType(38,0),True),
StructField("IS_BILLABLE",StringType(),True),
StructField("REIMB_TXN_ID",DecimalType(38,0),True),
StructField("MARKUP",DecimalType(38,9),True),
StructField("SERVICE_DATE",TimestampType(),True),
StructField("SALES_DETAIL_TYPE",DecimalType(38,0),True),
StructField("SOURCE_TXN_ID",DecimalType(38,0),True),
StructField("SOURCE_TXN_SEQUENCE",DecimalType(38,0),True),
StructField("PAID_DATE",TimestampType(),True),
StructField("OFX_TXN_ID",DecimalType(38,0),True),
StructField("OFX_MATCH_FLAG",DecimalType(38,0),True),
StructField("OLB_MATCH_MODE",DecimalType(38,0),True),
StructField("OLB_MATCH_AMOUNT",DecimalType(38,9),True),
StructField("OLB_RULE_ID",DecimalType(38,0),True),
StructField("EXTERNAL_TXN_MATCH_MODE",DecimalType(38,0),True),
StructField("DD_ACCOUNT_ID",DecimalType(38,0),True),
StructField("DD_LINE_STATUS",DecimalType(38,0),True),
StructField("INVENTORY_COSTING_FOR_SEQ",DecimalType(38,0),True),
StructField("CREATE_DATE",TimestampType(),True),
StructField("CREATE_USER_ID",DecimalType(38,0),True),
StructField("LAST_MODIFY_DATE",TimestampType(),True),
StructField("LAST_MODIFY_USER_ID",DecimalType(38,0),True),
StructField("EDIT_SEQUENCE",DecimalType(38,0),True),
StructField("ADDED_AUDIT_ID",DecimalType(38,0),True),
StructField("AUDIT_ID",DecimalType(38,0),True),
StructField("AUDIT_FLAG",StringType(),True),
StructField("EXCEPTION_FLAG",StringType(),True),
StructField("IS_PENALTY",StringType(),True),
StructField("IS_INTEREST",StringType(),True),
StructField("NET_AMOUNT",DecimalType(38,9),True),
StructField("TAX_AMOUNT",DecimalType(38,9),True),
StructField("TAX_CODE_ID",DecimalType(38,0),True),
StructField("TAX_RATE_ID",DecimalType(38,0),True),
StructField("CURRENCY_TYPE",DecimalType(38,0),True),
StructField("EXCHANGE_RATE",DecimalType(38,9),True),
StructField("HOME_AMOUNT",DecimalType(38,9),True),
StructField("HOME_OPEN_BALANCE",DecimalType(38,9),True),
StructField("IS_FOREX_GAIN_LOSS",StringType(),True),
StructField("SPECIAL_TAX_TYPE",DecimalType(38,0),True),
StructField("SPECIAL_TAX_OPEN_BALANCE",DecimalType(38,9),True),
StructField("TAX_OVERRIDE_DELTA_AMOUNT",DecimalType(38,9),True),
StructField("INCLUSIVE_AMOUNT",DecimalType(38,9),True),
StructField("CUSTOM_ACCOUNT_TAX_AMT",DecimalType(38,9),True),
StructField("JOURNAL_CODE_ID",DecimalType(38,0),True),
StructField("DISCOUNT_ID",DecimalType(38,0),True),
StructField("DISCOUNT_AMOUNT",DecimalType(38,9),True),
StructField("TXN_DISCOUNT_AMOUNT",DecimalType(38,9),True),
StructField("SUBTOTAL_AMOUNT",DecimalType(38,9),True),
StructField("LINE_DETAIL_TYPE",DecimalType(38,0),True),
StructField("WITHHOLDING_RATE_ID",DecimalType(38,0),True),
StructField("REMAINING_QUANTITY",DecimalType(38,9),True),
StructField("REMAINING_AMOUNT",DecimalType(38,9),True),
StructField("SRC_QTY_USED",DecimalType(38,9),True),
StructField("SRC_AMT_USED",DecimalType(38,9),True),
StructField("LINE_MANUALLY_CLOSED",StringType(),True),
StructField("CUSTOM_FIELD_VALUES",StringType(),True),
StructField("PROGRESS_TRACKING_TYPE",DecimalType(38,0),True),
StructField("ITEM_RATE_TYPE",DecimalType(38,0),True),
StructField("CUSTOM_FIELD_VALS",StringType(),True),
StructField("REGION_CUSTOMS_CODE",StringType(),True),
StructField("LAST_MODIFIED_UTC",TimestampType(),True),
StructField("date",DateType(),True),
StructField("yearMonth",StringType(),True),
StructField("isDeleted",BooleanType(),True)
])


print("schema", schema)

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("spark unit tests") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()

# Test manipulation and generation of test data for a large schema
class TestLargeSchemaOperation(unittest.TestCase):
    testDataSpec = None
    dfTestData = None
    row_count = 1000
    column_count = 50

    def setUp(self):
        print("setting up")

    @classmethod
    def setUpClass(cls):
        cls.testDataSpec = (datagen.DataGenerator(sparkSession=spark, name="test_data_set1", rows=cls.row_count,
                                                  partitions=4)
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
        self.dfTestData.limit(30).show()

    def test_generated_data_count(self):
        count = self.dfTestData.count()
        self.assertEqual(count, self.row_count)

    def test_distinct_count(self):
        distinct_count = self.dfTestData.select('id').distinct().count()
        self.assertEqual(distinct_count, self.row_count)

    def test_script_table(self):
        tableScript=self.testDataSpec.scriptTable("testTable")
        print("tableScript", tableScript)





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
