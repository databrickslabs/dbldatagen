import logging
import pytest

from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType

import dbldatagen as dg

schema = StructType([
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
    StructField("HA", DecimalType(38, 9), True),
    StructField("HO_AMT", DecimalType(38, 9), True),
    StructField("IS_FGL", StringType(), True),
    StructField("ST_TYPE", DecimalType(38, 0), True),
    StructField("STO_BALANCE", DecimalType(38, 9), True),
    StructField("TO_AMT", DecimalType(38, 9), True),
    StructField("INC_AMOUNT", DecimalType(38, 9), True),
    StructField("CA_TAX_AMT", DecimalType(38, 9), True),
    StructField("HGS_CODE_ID", DecimalType(38, 0), True),
    StructField("DISC_ID", DecimalType(38, 0), True),
    StructField("DISC_AMT", DecimalType(38, 9), True),
    StructField("TXN_DISCOUNT_AMOUNT", DecimalType(38, 9), True),
    StructField("SUBTOTAL_AMOUNT", DecimalType(38, 9), True),
    StructField("LINE_DETAIL_TYPE", DecimalType(38, 0), True),
    StructField("W_RATE_ID", DecimalType(38, 0), True),
    StructField("R_QTY", DecimalType(38, 9), True),
    StructField("R_AMOUNT", DecimalType(38, 9), True),
    StructField("AMT_2", DecimalType(38, 9), True),
    StructField("AMT_3", DecimalType(38, 9), True),
    StructField("FLAG_5", StringType(), True),
    StructField("CUSTOM_FIELD_VALUES", StringType(), True),
    StructField("PTT", DecimalType(38, 0), True),
    StructField("IRT", DecimalType(38, 0), True),
    StructField("CUSTOM_FIELD_VALS", StringType(), True),
    StructField("RCC", StringType(), True),
    StructField("LAST_MODIFIED_UTC", TimestampType(), True),
    StructField("date", DateType(), True),
    StructField("yearMonth", StringType(), True),
    StructField("isDeleted", BooleanType(), True)
])

spark = dg.SparkSingleton.getLocalInstance("unit tests")


@pytest.fixture(scope="class")
def setupLogging():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)


# Test manipulation and generation of test data for a large schema
class TestBuildPlanning:
    testDataSpec = None
    dfTestData = None
    row_count = 100000

    @pytest.fixture(scope="class")
    def sampleDataSpec(self):
        sale_values = ['RETAIL', 'ONLINE', 'WHOLESALE', 'RETURN']
        sale_weights = [1, 5, 5, 1]

        testDataspec = (dg.DataGenerator(sparkSession=spark, name="test_data_set1", rows=self.row_count, partitions=4)
                        .withSchema(schema)
                        .withIdOutput()
                        .withColumnSpecs(patterns=".*_ID", match_types=StringType(), format="%010d",
                                         minValue=1, maxValue=123,
                                         step=1)
                        .withColumnSpecs(patterns=".*_IDS", match_types="string", format="%010d", minValue=1,
                                         maxValue=100, step=1)
                        # .withColumnSpec("R3D3_CLUSTER_IDS", minValue=1, maxValue=100, step=1)
                        .withColumnSpec("XYYZ_IDS", minValue=1, maxValue=123, step=1,
                                        format="%05d")
                        # .withColumnSpec("nstr4", percentNulls=0.1,
                        # minValue=1, maxValue=9, step=2,  format="%04d")
                        # example of IS_SALE
                        .withColumnSpec("IS_S", values=sale_values, weights=sale_weights, random=True)
                        # .withColumnSpec("nstr4", percentNulls=0.1,
                        # minValue=1, maxValue=9, step=2,  format="%04d")
                        )

        return testDataspec

    @pytest.fixture()
    def sampleDataSet(self, sampleDataSpec):
        print("Test generation plan")
        print("=============================")
        sampleDataSpec.explain()

        df = sampleDataSpec.build()

        return df

    def setup_log_capture(self, caplog_object):
        """ set up log capture fixture

        Sets up log capture fixture to only capture messages after setup and only
        capture warnings and errors

        """
        caplog_object.set_level(logging.WARNING)

        # clear messages from setup
        caplog_object.clear()

    def get_log_capture_warngings_and_errors(self, caplog_object, textFlag):
        """
        gets count of errors containing specified text

        :param caplog_object: log capture object from fixture
        :param textFlag: text to search for to include error or warning in count
        :return: count of errors containg text specified in `textFlag`
        """
        seed_column_warnings_and_errors = 0
        for r in caplog_object.records:
            if (r.levelname in ["WARNING", "ERROR"]) and textFlag in r.message:
                seed_column_warnings_and_errors += 1

        return seed_column_warnings_and_errors

    def test_fieldnames_for_schema(self, sampleDataSpec):
        """Test field names in data spec correspond with schema"""
        fieldsFromGenerator = set(sampleDataSpec.getOutputColumnNames())

        fieldsFromSchema = set([fld.name for fld in schema.fields])

        # output fields should be same + 'id' field
        assert fieldsFromGenerator - fieldsFromSchema == set(['id'])

    def test_explain(self, sampleDataSpec):
        sampleDataSpec.computeBuildPlan()
        explain_results = sampleDataSpec.explain()
        assert explain_results is not None

    def test_explain_on_clone(self, sampleDataSpec):
        testDataSpec2 = sampleDataSpec.clone()
        testDataSpec2.computeBuildPlan()
        explain_results = testDataSpec2.explain(suppressOutput=True)

        assert explain_results is not None

    def test_build_ordering_basic(self, sampleDataSpec):
        build_order = sampleDataSpec.build_order

        # make sure that build order is list of lists
        assert build_order is not None
        assert isinstance(build_order, list)

        for el in build_order:
            assert isinstance(el, list)

    def builtBefore(self, field1, field2, build_order):
        """ check if field1 is built before field2"""

        fieldsBuilt = []

        for phase in build_order:
            for el in phase:
                if el == field1:
                    return field2 in fieldsBuilt
                fieldsBuilt.append(el)

        return False

    def builtInSeparatePhase(self, field1, field2, build_order):
        """ check if field1 is built in separate phase to field2"""

        fieldsBuilt = []

        for phase in build_order:
            for el in phase:
                if el == field1:
                    return field2 not in phase
                fieldsBuilt.append(el)

        return False

    def test_build_ordering_explicit_dependency(self):
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                        baseColumns=["city2"]) \
            .withColumn("city2", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                        baseColumns=["city_pop"]) \
            .withColumn("city_id2", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True,
                        baseColumn="city_id")

        build_order = gen1.build_order

        assert self.builtBefore("city_id", "city_name", build_order)
        assert self.builtBefore("city", "city2", build_order)
        assert self.builtBefore("city2", "city_pop", build_order)
        assert self.builtBefore("city_id2", "city_id", build_order)

        assert self.builtBefore("city", "city_name", build_order)
        assert self.builtBefore("city", "city_id", build_order)
        assert self.builtBefore("city", "city_pop", build_order)

        assert self.builtInSeparatePhase("city", "city_name", build_order)
        assert self.builtInSeparatePhase("city", "city_id", build_order)
        assert self.builtInSeparatePhase("city", "city_pop", build_order)

        print(gen1.build_order)

    def test_build_ordering_explicit_dependency2(self):
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                        baseColumns=["city_name", "city_id", "city_pop"]) \
            .withColumn("city2", "struct<name:string, id:long, population:long>",
                        expr="city",
                        baseColumns=["city"]) \
            .withColumn("city_id2", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True,
                        baseColumn="city_id")

        build_order = gen1.build_order

        assert self.builtBefore("city", "city_name", build_order)
        assert self.builtBefore("city", "city_id", build_order)
        assert self.builtBefore("city", "city_pop", build_order)
        assert self.builtInSeparatePhase("city", "city_name", build_order)
        assert self.builtInSeparatePhase("city", "city_id", build_order)
        assert self.builtInSeparatePhase("city", "city_pop", build_order)

        print(gen1.build_order)

    def test_build_ordering_implicit_dependency(self):
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)")

        build_order = gen1.build_order
        print(gen1.build_order)

        assert self.builtBefore("city", "city_name", build_order)
        assert self.builtBefore("city", "city_id", build_order)
        assert self.builtBefore("city", "city_pop", build_order)
        assert self.builtInSeparatePhase("city", "city_name", build_order), "fields should be built in separate phase"
        assert self.builtInSeparatePhase("city", "city_id", build_order), "fields should be built in separate phase"
        assert self.builtInSeparatePhase("city", "city_pop", build_order), "fields should be built in separate phase"

    # TODO: build ordering should initially try and build in the order supplied but separate into phases

    def test_build_ordering_implicit_dependency2(self):
        DEVICE_STATES = ['RUNNING', 'IDLE', 'DOWN']
        DEVICE_WEIGHTS = [10, 5, 1]
        SITES = ['alpha', 'beta', 'gamma', 'delta', 'phi', 'mu', 'lambda']
        AREAS = ['area 1', 'area 2', 'area 3', 'area 4', 'area 5']
        LINES = ['line 1', 'line 2', 'line 3', 'line 4', 'line 5', 'line 6']
        TAGS = ['statusCode', 'another notification 1', 'another notification 2', 'another notification 3']
        NUM_LOCAL_DEVICES = 20

        STARTING_DATETIME = "2022-06-01 01:00:00"
        END_DATETIME = "2022-09-01 23:59:00"
        EVENT_INTERVAL = "10 seconds"

        gen1 = (
            dg.DataGenerator(spark, rows=1000, partitions=4)
            # can combine internal site id computation with value lookup but clearer to use a separate internal column
            .withColumn("site", "string", values=SITES, random=True)
            .withColumn("area", "string", values=AREAS, random=True, omit=True)
            .withColumn("line", "string", values=LINES, random=True, omit=True)
            .withColumn("local_device_id", "int", maxValue=NUM_LOCAL_DEVICES - 1, omit=True, random=True)

            .withColumn("local_device", "string", prefix="device", baseColumn="local_device_id")

            .withColumn("device_key", "string",
                        expr="concat('/', site, '/', area, '/', line, '/', local_device)")

            # used to compute the device id
            .withColumn("internal_device_key", "long", expr="hash(site,  area,  line, local_device)",
                        omit=True)

            .withColumn("deviceId", "string", format="0x%013x",
                        baseColumn="internal_device_key")

            # tag name is name of device signal
            .withColumn("tagName", "string", values=TAGS, random=True)

            # tag value is state
            .withColumn("tagValue", "string",
                        values=DEVICE_STATES, weights=DEVICE_WEIGHTS,
                        random=True)

            .withColumn("tag_ts", "timestamp",
                        begin=STARTING_DATETIME,
                        end=END_DATETIME,
                        interval=EVENT_INTERVAL,
                        random=True)

            .withColumn("event_date", "date", expr="to_date(tag_ts)")
        )

        build_order = gen1.build_order
        print(gen1.build_order)

        assert self.builtBefore("event_date", "tag_ts", build_order)
        assert self.builtBefore("device_key", "site", build_order)
        assert self.builtBefore("device_key", "area", build_order)
        assert self.builtBefore("device_key", "line", build_order)
        assert self.builtBefore("device_key", "local_device", build_order)
        assert self.builtBefore( "internal_device_key", "site", build_order)
        assert self.builtBefore( "internal_device_key", "area", build_order)
        assert self.builtBefore( "internal_device_key", "line", build_order)
        assert self.builtBefore("internal_device_key", "local_device", build_order)
        assert self.builtBefore("device_key", "site",  build_order)
        assert self.builtBefore("device_key", "area", build_order)
        assert self.builtBefore("device_key", "line", build_order)
        assert self.builtBefore("local_device", "device_key", build_order)

        assert self.builtInSeparatePhase("tag_ts", "event_date", build_order)
        assert self.builtInSeparatePhase("site", "device_key", build_order)
        assert self.builtInSeparatePhase("area", "device_key", build_order)
        assert self.builtInSeparatePhase("line", "device_key", build_order)
        assert self.builtInSeparatePhase("local_device", "device_key", build_order)
        assert self.builtInSeparatePhase("site", "internal_device_key", build_order)
        assert self.builtInSeparatePhase("area", "internal_device_key", build_order)
        assert self.builtInSeparatePhase("line", "internal_device_key", build_order)
        assert self.builtInSeparatePhase("local_device", "internal_device_key", build_order)
        assert self.builtInSeparatePhase("site", "device_key", build_order)
        assert self.builtInSeparatePhase("area", "device_key", build_order)
        assert self.builtInSeparatePhase("line", "device_key", build_order)
        assert self.builtInSeparatePhase("local_device", "device_key", build_order)



    def test_expr_attribute(self):
        sql_expr = "named_struct('name', city_name, 'id', city_id, 'population', city_pop)"
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr=sql_expr)

        columnSpec = gen1.getColumnSpec("city")

        assert columnSpec.expr == sql_expr

    def test_expr_identifier_with_spaces(self):
        sql_expr = "named_struct('name', city_name, 'id', city_id, 'population', city_pop)"
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city 2", "struct<name:string, id:long, population:long>",
                        expr=sql_expr)

        columnSpec = gen1.getColumnSpec("city 2")

        assert columnSpec.expr == sql_expr

    def test_build_ordering_duplicate_names1(self):
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("extra_field", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("extra_field", "string", template=r"\w", random=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)")

        build_order = gen1.build_order
        print(gen1.build_order)

        df = gen1.build()
        df.show()

        # Behavior is allow creation of a duplicate named column. It replaces the previous column
        # of the same name

        fieldList = [ f.name for f in df.schema.fields]
        print(fieldList)

        # ensure we have no duplicate names
        #assert len(fieldList) == len(set(fieldList))

        fieldMap = { f.name: f.dataType for f in df.schema.fields}
        print("fieldMap", fieldMap)
        #assert isinstance(fieldMap["extra_field"], StringType)

    def test_build_ordering_forward_ref(self, caplog):
        # caplog fixture captures log content
        self.setup_log_capture(caplog)

        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)") \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True)

        build_order = gen1.build_order
        print(gen1.build_order)

        seed_column_warnings_and_errors = self.get_log_capture_warngings_and_errors(caplog, "forward references")
        assert seed_column_warnings_and_errors >= 1, "Should not have error messages about forward references"

    def test_build_ordering_duplicate_names2(self):
        gen1 = dg.DataGenerator(sparkSession=spark, name="nested_schema", rows=1000, partitions=4,
                                seedColumnName="_id") \
            .withColumn("id", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "long", minValue=1000000, uniqueValues=10000, random=True) \
            .withColumn("city_name", "string", template=r"\w", random=True, omit=True) \
            .withColumn("city_id", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city_pop", "long", minValue=1000000, uniqueValues=10000, random=True, omit=True) \
            .withColumn("city", "struct<name:string, id:long, population:long>",
                        expr="named_struct('name', city_name, 'id', city_id, 'population', city_pop)",
                        baseColumns=["city_name", "city_id", "city_pop"])

        build_order = gen1.build_order
        print(gen1.build_order)

        df = gen1.build()

        df.show()

        # assert self.builtBefore("city", "city_name", build_order)
        # assert self.builtBefore("city", "city_id", build_order)
        # assert self.builtBefore("city", "city_pop", build_order)
        # assert self.builtInSeparatePhase("city", "city_name", build_order), "fields should be built in separate phase"
        # assert self.builtInSeparatePhase("city", "city_id", build_order), "fields should be built in separate phase"
        # assert self.builtInSeparatePhase("city", "city_pop", build_order), "fields should be built in separate phase"
