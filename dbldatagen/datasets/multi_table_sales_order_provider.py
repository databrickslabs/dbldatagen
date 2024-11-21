from .dataset_provider import DatasetProvider, dataset_definition


@dataset_definition(name="multi_table/sales_order", summary="Multi-table sales order dataset", supportsStreaming=True,
                    autoRegister=True,
                    tables=["customers", "carriers", "catalog_items", "base_orders", "base_order_line_items",
                            "base_order_shipments", "base_invoices"],
                    associatedDatasets=["orders", "order_line_items", "order_shipments", "invoices"])
class MultiTableSalesOrderProvider(DatasetProvider):
    """ Generates a multi-table sales order scenario

    See [https://databrickslabs.github.io/dbldatagen/public_docs/multi_table_data.html]

    It generates one of several possible tables:

    customers - which model customers
    carriers - which model shipping carriers
    catalog_items - which model items in a sales catalog
    base_orders - which model basic sales order data without relations
    base_order_line_items - which model basic sales order line item data without relations
    base_order_shipments - which model basic sales order shipment data without relations
    base_invoices - which model basic invoice data without relations

    Once the above tables have been computed, you can retrieve the combined tables for:

    orders - which model complete sales orders
    order_line_items - which model complete sales order line items
    order_shipments - which model complete sales order shipments
    invoices - which model complete invoices

    using `Datasets(...).getCombinedTable("orders")`, `Datasets(...).getCombinedTable("order_line_items")`,
    `Datasets(...).getCombinedTable("order_shipments")`, `Datasets(...).getCombinedTable("invoices")`

    The following options are supported:
    - numCustomers - number of unique customers
    - numCarriers - number of unique shipping carriers
    - numCatalogItems - number of unique catalog items
    - numOrders - number of unique orders
    - lineItemsPerOrder - number of line items per order
    - startDate - earliest order date
    - endDate - latest order date
    - dummyValues - number of dummy values to widen the tables

    While it is possible to specify the number of rows explicitly when getting each table generator, the default will
    be to compute the number of rows from these options.
    """
    MAX_LONG = 9223372036854775807
    DEFAULT_NUM_CUSTOMERS = 1_000
    DEFAULT_NUM_CARRIERS = 100
    DEFAULT_NUM_CATALOG_ITEMS = 1_000
    DEFAULT_NUM_ORDERS = 100_000
    DEFAULT_LINE_ITEMS_PER_ORDER = 3
    DEFAULT_START_DATE = "2024-01-01"
    DEFAULT_END_DATE = "2025-01-01"
    CUSTOMER_MIN_VALUE = 10_000
    CARRIER_MIN_VALUE = 100
    CATALOG_ITEM_MIN_VALUE = 10_000
    ORDER_MIN_VALUE = 10_000_000
    ORDER_LINE_ITEM_MIN_VALUE = 100_000_000
    SHIPMENT_MIN_VALUE = 10_000_000
    INVOICE_MIN_VALUE = 1_000_000

    def getCustomers(self, sparkSession, *, rows, partitions, numCustomers, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numCustomers is None or numCustomers < 0:
            numCustomers = self.DEFAULT_NUM_CUSTOMERS
        if rows is None or rows < 0:
            rows = numCustomers
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        customers_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("customer_id", "integer", minValue=self.CUSTOMER_MIN_VALUE, uniqueValues=numCustomers)
            .withColumn("customer_name", "string", prefix="CUSTOMER", baseColumn="customer_id")
            .withColumn("sic_code", "integer", minValue=100, maxValue=9_995, random=True)
            .withColumn("num_employees", "integer", minValue=1, maxValue=10_000, random=True)
            .withColumn("region", "string", values=["AMER", "EMEA", "APAC", "NONE"], random=True)
            .withColumn("phone_number", "string", template="ddd-ddd-dddd")
            .withColumn("email_user_name", "string",
                        values=["billing", "procurement", "office", "purchasing", "buyer"], omit=True)
            .withColumn("email_address", "string", expr="concat(email_user_name, '@', lower(customer_name), '.com')")
            .withColumn("payment_terms", "string", values=["DUE_ON_RECEIPT", "NET30", "NET60", "NET120"])
            .withColumn("created_on", "date", begin="2000-01-01", end=self.DEFAULT_START_DATE, interval="1 DAY")
            .withColumn("created_by", "integer", minValue=1_000, maxValue=9_999, random=True)
            .withColumn("is_updated", "boolean", expr="rand() > 0.75", omit=True)
            .withColumn("updated_after_days", "integer", minValue=0, maxValue=1_000, random=True, omit=True)
            .withColumn("updated_on", "date", expr="""case when is_updated then created_on
                                                    else date_add(created_on, updated_after_days) end""")
            .withColumn("updated_by_user", "integer", minValue=1_000, maxValue=9_999, random=True, omit=True)
            .withColumn("updated_by", "integer", expr="case when is_updated then updated_by_user else created_by end")
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            customers_data_spec = customers_data_spec.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                                                 minValue=1, maxValue=self.MAX_LONG)

        return customers_data_spec

    def getCarriers(self, sparkSession, *, rows, partitions, numCarriers, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numCarriers is None or numCarriers < 0:
            numCarriers = self.DEFAULT_NUM_CARRIERS
        if rows is None or rows < 0:
            rows = numCarriers
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        carriers_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("carrier_id", "integer", minValue=self.CARRIER_MIN_VALUE, uniqueValues=numCarriers)
            .withColumn("carrier_name", "string", prefix="CARRIER", baseColumn="carrier_id")
            .withColumn("phone_number", "string", template="ddd-ddd-dddd")
            .withColumn("email_user_name", "string",
                        values=["shipping", "parcel", "logistics", "carrier"], omit=True)
            .withColumn("email_address", "string", expr="concat(email_user_name, '@', lower(carrier_name), '.com')")
            .withColumn("created_on", "date", begin="2000-01-01", end=self.DEFAULT_START_DATE, interval="1 DAY")
            .withColumn("created_by", "integer", minValue=1_000, maxValue=9_999, random=True)
            .withColumn("is_updated", "boolean", expr="rand() > 0.75", omit=True)
            .withColumn("updated_after_days", "integer", minValue=0, maxValue=1_000, random=True, omit=True)
            .withColumn("updated_on", "date", expr="""case when is_updated then created_on
                                                    else date_add(created_on, updated_after_days) end""")
            .withColumn("updated_by_user", "integer", minValue=1_000, maxValue=9_999, random=True, omit=True)
            .withColumn("updated_by", "integer", expr="case when is_updated then updated_by_user else created_by end")
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            carriers_data_spec = carriers_data_spec.withColumn("dummy", "long", random=True, numColumns=dummyValues,
                                                               minValue=1, maxValue=self.MAX_LONG)

        return carriers_data_spec

    def getCatalogItems(self, sparkSession, *, rows, partitions, numCatalogItems, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numCatalogItems is None or numCatalogItems < 0:
            numCatalogItems = self.DEFAULT_NUM_CATALOG_ITEMS
        if rows is None or rows < 0:
            rows = numCatalogItems
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        catalog_items_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("catalog_item_id", "integer", minValue=self.CATALOG_ITEM_MIN_VALUE,
                        uniqueValues=numCatalogItems)
            .withColumn("item_name", "string", prefix="ITEM", baseColumn="catalog_item_id")
            .withColumn("unit_price", "decimal(8,2)", minValue=1.50, maxValue=500.0, random=True)
            .withColumn("discount_rate", "decimal(3,2)", minValue=0.00, maxValue=9.99, random=True)
            .withColumn("min_inventory_qty", "integer", minValue=0, maxValue=10_000, random=True)
            .withColumn("inventory_qty_range", "integer", minValue=0, maxValue=10_000, random=True, omit=True)
            .withColumn("max_inventory_qty", "integer", expr="min_inventory_qty + inventory_qty_range")
            .withColumn("created_on", "date", begin="2000-01-01", end=self.DEFAULT_START_DATE, interval="1 DAY")
            .withColumn("created_by", "integer", minValue=1_000, maxValue=9_999, random=True)
            .withColumn("is_updated", "boolean", expr="rand() > 0.75", omit=True)
            .withColumn("updated_after_days", "integer", minValue=0, maxValue=1_000, random=True, omit=True)
            .withColumn("updated_on", "date", expr="""case when is_updated then created_on 
                                                    else date_add(created_on, updated_after_days) end""")
            .withColumn("updated_by_user", "integer", minValue=1_000, maxValue=9_999, random=True, omit=True)
            .withColumn("updated_by", "integer", expr="case when is_updated then updated_by_user else created_by end")
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            catalog_items_data_spec = (
                catalog_items_data_spec.withColumn("dummy", "long", random=True,
                                                   numColumns=dummyValues, minValue=1, maxValue=self.MAX_LONG))

        return catalog_items_data_spec

    def getBaseOrders(self, sparkSession, *, rows, partitions, numOrders, numCustomers, startDate,
                      endDate, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numOrders is None or numOrders < 0:
            numOrders = self.DEFAULT_NUM_ORDERS
        if numCustomers is None or numCustomers < 0:
            numCustomers = self.DEFAULT_NUM_CUSTOMERS
        if startDate is None:
            startDate = self.DEFAULT_START_DATE
        if endDate is None:
            endDate = self.DEFAULT_END_DATE
        if rows is None or rows < 0:
            rows = numOrders
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        base_orders_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("order_id", "integer", minValue=self.ORDER_MIN_VALUE, uniqueValues=numOrders)
            .withColumn("order_title", "string", prefix="ORDER", baseColumn="order_id")
            .withColumn("customer_id", "integer", minValue=self.CUSTOMER_MIN_VALUE,
                        maxValue=self.CUSTOMER_MIN_VALUE + numCustomers, random=True)
            .withColumn("purchase_order_number", "string", template="KKKK-KKKK-DDDD-KKKK", random=True)
            .withColumn("order_open_date", "date", begin=startDate, end=endDate,
                        interval="1 DAY", random=True)
            .withColumn("order_open_to_close_days", "integer", minValue=0, maxValue=30, random=True, omit=True)
            .withColumn("order_close_date", "date", expr=f"""least(cast('{endDate}' as date),
                                                        date_add(order_open_date, order_open_to_close_days))""")
            .withColumn("sales_rep_id", "integer", minValue=1_000, maxValue=9_999, random=True)
            .withColumn("sales_group_id", "integer", minValue=100, maxValue=999, random=True)
            .withColumn("created_on", "date", expr="order_open_date")
            .withColumn("created_by", "integer", minValue=1_000, maxValue=9_999, random=True)
            .withColumn("updated_after_days", "integer", minValue=0, maxValue=5, random=True, omit=True)
            .withColumn("updated_on", "date", expr="date_add(order_close_date, updated_after_days)")
            .withColumn("updated_by", "integer", minValue=1_000, maxValue=9_999, random=True)
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            base_orders_data_spec = base_orders_data_spec.withColumn(
                "dummy", "long", random=True, numColumns=dummyValues, minValue=1, maxValue=self.MAX_LONG)

        return base_orders_data_spec

    def getBaseOrderLineItems(self, sparkSession, *, rows, partitions, numOrders, numCatalogItems,
                              lineItemsPerOrder, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numOrders is None or numOrders < 0:
            numOrders = self.DEFAULT_NUM_ORDERS
        if numCatalogItems is None or numCatalogItems < 0:
            numCatalogItems = self.DEFAULT_NUM_CATALOG_ITEMS
        if lineItemsPerOrder is None or lineItemsPerOrder < 0:
            lineItemsPerOrder = self.DEFAULT_LINE_ITEMS_PER_ORDER
        if rows is None or rows < 0:
            rows = numOrders
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        base_order_line_items_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("order_line_item_id", "integer", minValue=self.ORDER_LINE_ITEM_MIN_VALUE,
                        uniqueValues=numOrders*lineItemsPerOrder)
            .withColumn("order_id", "integer", minValue=self.ORDER_MIN_VALUE, maxValue=self.ORDER_MIN_VALUE + numOrders,
                        uniqueValues=numOrders, random=True)
            .withColumn("catalog_item_id", "integer", minValue=self.CATALOG_ITEM_MIN_VALUE,
                        maxValue=self.CATALOG_ITEM_MIN_VALUE + numCatalogItems, uniqueValues=numCatalogItems,
                        random=True)
            .withColumn("has_discount", "boolean", expr="rand() > 0.9")
            .withColumn("units", "integer", minValue=1, maxValue=100, random=True)
            .withColumn("added_after_order_creation_days", "integer", minValue=0, maxValue=30, random=True)
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            base_order_line_items_data_spec = base_order_line_items_data_spec.withColumn(
                "dummy", "long", random=True, numColumns=dummyValues, minValue=1, maxValue=self.MAX_LONG)

        return base_order_line_items_data_spec

    def getBaseOrderShipments(self, sparkSession, *, rows, partitions, numOrders, numCarriers, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numOrders is None or numOrders < 0:
            numOrders = self.DEFAULT_NUM_ORDERS
        if numCarriers is None or numCarriers < 0:
            numCarriers = self.DEFAULT_NUM_CARRIERS
        if rows is None or rows < 0:
            rows = numOrders
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        base_order_shipments_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("order_shipment_id", "integer", minValue=self.ORDER_MIN_VALUE, uniqueValues=numOrders)
            .withColumn("order_id", "integer", minValue=self.ORDER_MIN_VALUE, maxValue=self.ORDER_MIN_VALUE + numOrders,
                        uniqueValues=numOrders, random=True)
            .withColumn("carrier_id", "integer", minValue=self.CARRIER_MIN_VALUE,
                        maxValue=self.CARRIER_MIN_VALUE + numCarriers, uniqueValues=numCarriers, random=True)
            .withColumn("house_number", "integer", minValue=1, maxValue=9999, random=True, omit=True)
            .withColumn("street_number", "integer", minValue=1, maxValue=150, random=True, omit=True)
            .withColumn("street_direction", "string", values=["", "N", "S", "E", "W", "NW", "NE", "SW", "SE"],
                        random=True)
            .withColumn("ship_to_address_line", "string", expr="""concat_ws(' ', house_number, street_direction, 
                                                                            street_number, 'ST')""")
            .withColumn("ship_to_country_code", "string", values=["US", "CA"], weights=[8, 3], random=True)
            .withColumn("order_open_to_ship_days", "integer", minValue=0, maxValue=30, random=True)
            .withColumn("estimated_transit_days", "integer", minValue=1, maxValue=5, random=True)
            .withColumn("actual_transit_days", "integer", expr="greatest(1, estimated_transit_days - ceil(3*rand()))")
            .withColumn("receipt_on_delivery", "boolean", expr="rand() > 0.7")
            .withColumn("method", "string", values=["GROUND", "AIR"], weights=[7, 4], random=True)
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            base_order_shipments_data_spec = base_order_shipments_data_spec.withColumn(
                "dummy", "long", random=True, numColumns=dummyValues, minValue=1, maxValue=self.MAX_LONG)

        return base_order_shipments_data_spec

    def getBaseInvoices(self, sparkSession, *, rows, partitions, numOrders, dummyValues):
        import dbldatagen as dg

        # Validate the options:
        if numOrders is None or numOrders < 0:
            numOrders = self.DEFAULT_NUM_ORDERS
        if rows is None or rows < 0:
            rows = numOrders
        if partitions is None or partitions < 0:
            partitions = self.autoComputePartitions(rows, 9 + dummyValues)

        # Create the base data generation spec:
        base_invoices_data_spec = (
            dg.DataGenerator(sparkSession, rows=rows, partitions=partitions)
            .withColumn("invoice_id", "integer", minValue=self.INVOICE_MIN_VALUE, uniqueValues=numOrders)
            .withColumn("order_id", "integer", minValue=self.ORDER_MIN_VALUE, maxValue=self.ORDER_MIN_VALUE + numOrders,
                        uniqueValues=numOrders, random=True)
            .withColumn("house_number", "integer", minValue=1, maxValue=9999, random=True, omit=True)
            .withColumn("street_number", "integer", minValue=1, maxValue=150, random=True, omit=True)
            .withColumn("street_direction", "string", values=["", "N", "S", "E", "W", "NW", "NE", "SW", "SE"],
                        random=True)
            .withColumn("bill_to_address_line", "string", expr="""concat_ws(' ', house_number, street_direction, 
                                                                            street_number, 'ST')""")
            .withColumn("bill_to_country_code", "string", values=["US", "CA"], weights=[8, 3], random=True)
            .withColumn("order_close_to_invoice_days", "integer", minValue=0, maxValue=5, random=True)
            .withColumn("order_close_to_create_days", "integer", minValue=0, maxValue=2, random=True)
            .withColumn("created_by", "integer", minValue=1_000, maxValue=9_999, random=True)
            .withColumn("is_updated", "boolean", expr="rand() > 0.75")
            .withColumn("updated_after_days", "integer", minValue=0, maxValue=5, random=True)
            .withColumn("updated_by_user", "integer", minValue=1_000, maxValue=9_999, random=True, omit=True)
            .withColumn("updated_by", "integer", expr="case when is_updated then updated_by_user else created_by end")
        )

        # Add dummy values if they were requested:
        if dummyValues > 0:
            base_invoices_data_spec = base_invoices_data_spec.withColumn(
                "dummy", "long", random=True, numColumns=dummyValues, minValue=1, maxValue=self.MAX_LONG)

        return base_invoices_data_spec

    @DatasetProvider.allowed_options(options=["numCustomers", "numCarriers", "numCatalogItems", "numOrders",
                                              "lineItemsPerOrder", "startDate", "endDate", "dummyValues"])
    def getTableGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1, **options):
        # Get the option values:
        numCustomers = options.get("numCustomers", self.DEFAULT_NUM_CUSTOMERS)
        numCarriers = options.get("numCarriers", self.DEFAULT_NUM_CARRIERS)
        numCatalogItems = options.get("numCatalogItems", self.DEFAULT_NUM_CATALOG_ITEMS)
        numOrders = options.get("numOrders", self.DEFAULT_NUM_ORDERS)
        lineItemsPerOrder = options.get("lineItemsPerOrder", self.DEFAULT_LINE_ITEMS_PER_ORDER)
        startDate = options.get("startDate", self.DEFAULT_START_DATE)
        endDate = options.get("endDate", self.DEFAULT_END_DATE)
        dummyValues = options.get("dummyValues", 0)

        # Get table generation specs for the base tables:
        if tableName == "customers":
            return self.getCustomers(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numCustomers=numCustomers,
                dummyValues=dummyValues
            )
        elif tableName == "carriers":
            return self.getCarriers(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numCarriers=numCarriers,
                dummyValues=dummyValues
            )
        elif tableName == "catalog_items":
            return self.getCatalogItems(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numCatalogItems=numCatalogItems,
                dummyValues=dummyValues
            )
        elif tableName == "base_orders":
            return self.getBaseOrders(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numOrders=numOrders,
                numCustomers=numCustomers,
                startDate=startDate,
                endDate=endDate,
                dummyValues=dummyValues
            )
        elif tableName == "base_order_line_items":
            return self.getBaseOrderLineItems(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numOrders=numOrders,
                numCatalogItems=numCatalogItems,
                lineItemsPerOrder=lineItemsPerOrder,
                dummyValues=dummyValues
            )
        elif tableName == "base_order_shipments":
            return self.getBaseOrderShipments(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numOrders=numOrders,
                numCarriers=numCarriers,
                dummyValues=dummyValues
            )
        elif tableName == "base_invoices":
            return self.getBaseInvoices(
                sparkSession,
                rows=rows,
                partitions=partitions,
                numOrders=numOrders,
                dummyValues=dummyValues
            )
        raise ValueError("tableName must be 'customers', 'carriers', 'catalog_items', 'base_orders',"
                         "'base_order_line_items', 'base_order_shipments', 'base_invoices'")

    @DatasetProvider.allowed_options(options=[
        "customers",
        "carriers",
        "catalogItems",
        "baseOrders",
        "baseOrderLineItems",
        "baseOrderShipments",
        "baseInvoices"
    ])
    def getAssociatedDataset(self, sparkSession, *, tableName=None, rows=-1, partitions=-1, **options):
        from pyspark.sql import DataFrame
        import pyspark.sql.functions as F

        dfCustomers = options.get("customers", None)
        assert dfCustomers is not None and issubclass(type(dfCustomers), DataFrame), \
            "Option `customers` should be a dataframe of customer records"

        dfCarriers = options.get("carriers", None)
        assert dfCarriers is not None and issubclass(type(dfCarriers), DataFrame), \
            "Option `carriers` should be dataframe of carrier records"

        dfCatalogItems = options.get("catalogItems", None)
        assert dfCatalogItems is not None and issubclass(type(dfCatalogItems), DataFrame), \
            "Option `catalogItems` should be dataframe of catalog item records"

        dfBaseOrders = options.get("baseOrders", None)
        assert dfBaseOrders is not None and issubclass(type(dfBaseOrders), DataFrame), \
            "Option `baseOrders` should be dataframe of base order records"

        dfBaseOrderLineItems = options.get("baseOrderLineItems", None)
        assert dfBaseOrderLineItems is not None and issubclass(type(dfBaseOrderLineItems), DataFrame), \
            "Option `baseOrderLineItems` should be dataframe of base order line item records"

        dfBaseOrderShipments = options.get("baseOrderShipments", None)
        assert dfBaseOrderShipments is not None and issubclass(type(dfBaseOrderShipments), DataFrame), \
            "Option `baseOrderLineItems` should be dataframe of base order shipment records"

        dfBaseInvoices = options.get("baseInvoices", None)
        assert dfBaseInvoices is not None and issubclass(type(dfBaseInvoices), DataFrame), \
            "Option `baseInvoices` should be dataframe of base invoice records"

        if tableName == "orders":
            dfOrderTotals = (
                dfBaseOrderLineItems.alias("a")
                .join(dfCatalogItems.alias("b"), on="catalog_item_id")
                .selectExpr("a.order_id as order_id",
                            "a.order_line_item_id as order_line_item_id",
                            """case when a.has_discount then (b.unit_price * 1 - (b.discount_rate / 100)) 
                            else b.unit_price end as unit_price""",
                            "a.units as units")
                .selectExpr("order_id", "order_line_item_id", "unit_price * units as total_price")
                .groupBy("order_id")
                .agg(F.count("order_line_item_id").alias("num_line_items"),
                     F.sum("total_price").alias("order_total"))
            )
            return (
                dfBaseOrders.alias("a")
                .join(dfOrderTotals.alias("b"), on="order_id")
                .join(dfCustomers.alias("c"), on="customer_id")
                .join(dfBaseOrderShipments.alias("d"), on="order_id")
                .selectExpr(
                    "a.order_id",
                    "concat(c.customer_name, ' ', a.order_title) AS order_title",
                    "a.customer_id",
                    "b.order_total",
                    "b.num_line_items",
                    "a.order_open_date",
                    "a.order_close_date",
                    "a.sales_rep_id",
                    "a.sales_group_id",
                    "a.created_on",
                    "a.created_by",
                    "a.updated_on",
                    "a.updated_by")
            )

        if tableName == "order_line_items":
            return (
                dfBaseOrderLineItems.alias("a")
                .join(dfBaseOrders.alias("b"), on="order_id")
                .join(dfCatalogItems.alias("c"), on="catalog_item_id")
                .selectExpr(
                    "a.order_line_item_id",
                    "a.order_id",
                    "a.catalog_item_id",
                    "a.units",
                    "c.unit_price",
                    "a.units * c.unit_price as gross_price",
                    """case when a.has_discount then a.units * c.unit_price * (1 - (c.discount_rate / 100)) 
                        else a.units * c.unit_price end as net_price""",
                    "date_add(b.created_on, a.added_after_order_creation_days) as created_on",
                    "b.created_by")
            )

        if tableName == "order_shipments":
            return (
               dfBaseOrderShipments.alias("a")
               .join(dfBaseOrders.alias("b"), on="order_id")
               .selectExpr(
                    "a.order_shipment_id",
                    "a.order_id",
                    "a.carrier_id",
                    "a.method",
                    "a.ship_to_address_line",
                    "a.ship_to_country_code",
                    """least(b.order_close_date, 
                        date_add(b.order_open_date, a.order_open_to_ship_days)) as order_shipment_date""",
                    "a.estimated_transit_days",
                    "a.actual_transit_days",
                    "b.created_on",
                    "b.created_by")
            )

        if tableName == "invoices":
            dfOrderTotals = (
                dfBaseOrderLineItems.alias("a")
                .join(dfCatalogItems.alias("b"), on="catalog_item_id")
                .selectExpr("a.order_id as order_id",
                            "a.order_line_item_id as order_line_item_id",
                            """case when a.has_discount then (b.unit_price * 1 - (b.discount_rate / 100)) 
                            else b.unit_price end as unit_price""",
                            "a.units as units")
                .selectExpr("order_id", "order_line_item_id", "unit_price * units as total_price")
                .groupBy("order_id")
                .agg(F.count("order_line_item_id").alias("num_line_items"),
                     F.sum("total_price").alias("order_total"))
            )
            return (
               dfBaseInvoices.alias("a")
               .join(dfBaseOrders.alias("b"), on="order_id")
               .join(dfCustomers.alias("c"), on="customer_id")
               .join(dfOrderTotals.alias("d"), on="order_id")
               .selectExpr(
                    "a.invoice_id",
                    "a.order_id",
                    "b.purchase_order_number",
                    "d.order_total",
                    "b.customer_id",
                    "c.payment_terms",
                    "a.bill_to_address_line",
                    "a.bill_to_country_code",
                    "date_add(b.order_close_date, a.order_close_to_invoice_days) as invoice_date",
                    "date_add(b.order_close_date, a.order_close_to_create_days) as created_on",
                    "a.created_by",
                    """case when a.is_updated then 
                        date_add(b.order_close_date, a.order_close_to_create_days + a.updated_after_days)
                        else date_add(b.order_close_date, a.order_close_to_create_days) end as updated_on""",
                    "case when a.is_updated then a.updated_by else a.created_by end as updated_by")
            )
