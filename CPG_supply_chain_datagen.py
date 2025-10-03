# Databricks notebook source
# MAGIC %md
# MAGIC # CPG Supply Chain Dummy Data Generator
# MAGIC
# MAGIC ## Educational Guide to dbldatagen
# MAGIC
# MAGIC This notebook demonstrates how to use  [**dbldatagen**](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) to create realistic CPG supply chain data for demos.
# MAGIC
# MAGIC ### What is dbldatagen?
# MAGIC `dbldatagen` is a Databricks Labs open-source library for generating synthetic data at scale using Spark. 
# MAGIC It's perfect for creating demo datasets, testing pipelines, and prototyping analytics solutions.
# MAGIC
# MAGIC ### Key Features:
# MAGIC - 🚀 **Scalable**: Generates millions of rows using Spark parallelization
# MAGIC - 🎲 **Realistic**: Control distributions, correlations, and data patterns
# MAGIC - 🔗 **Relational**: Create foreign key relationships between tables
# MAGIC - 🎯 **Flexible**: Support for dates, numbers, strings, and complex types
# MAGIC
# MAGIC ### Datasets We'll Create:
# MAGIC 1. **Products** - SKU master data with categories and pricing
# MAGIC 2. **Distribution Centers** - Network locations with capacity
# MAGIC 3. **Retail Stores** - Customer-facing locations
# MAGIC 4. **Production Orders** - Manufacturing execution data
# MAGIC 5. **Inventory Snapshots** - Multi-echelon inventory with risk metrics
# MAGIC 6. **Shipments** - Transportation and logistics data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation & Setup

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime, timedelta

print(f"Using dbldatagen version: {dg.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC **Best Practice**: Define all configuration parameters at the top for easy adjustment.

# COMMAND ----------

# Data generation parameters - adjust these to scale up/down
NUM_PRODUCTS = 500
NUM_DCS = 25
NUM_STORES = 1000
NUM_PRODUCTION_ORDERS = 10000
NUM_INVENTORY_RECORDS = 50000
NUM_SHIPMENTS = 30000

# Catalog configuration
CATALOG_NAME = 'CATALOG_NAME'
SCHEMA_NAME = 'SCHEMA_NAME'

# Set up the Catalog
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# Disable ANSI mode to prevent divide by zero errors during data generation
spark.conf.set("spark.sql.ansi.enabled", "false")

print(f"✅ Generating data in: {CATALOG_NAME}.{SCHEMA_NAME}")
print(f"📊 Total records to generate: {NUM_PRODUCTS + NUM_DCS + NUM_STORES + NUM_PRODUCTION_ORDERS + NUM_INVENTORY_RECORDS + NUM_SHIPMENTS:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Product Master Data
# MAGIC
# MAGIC ### Learning Objectives:
# MAGIC - How to use `withIdOutput()` for unique IDs
# MAGIC - Creating string expressions with `concat()` and `lpad()`
# MAGIC - Using `values` parameter for categorical data
# MAGIC - Working with different data types (string, decimal, integer, date)
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC - **uniqueValues**: Ensures the column has exactly N unique values
# MAGIC - **template**: Generates random words (\\w pattern)
# MAGIC - **minValue/maxValue**: Range for numeric values
# MAGIC - **begin/end**: Date range parameters

# COMMAND ----------

# Define categorical values for products
product_categories = ["Beverages", "Snacks", "Dairy", "Bakery", "Frozen Foods", "Personal Care", "Household"]
brands = ["Premium Brand A", "Value Brand B", "Store Brand C", "Organic Brand D", "Brand E"]

# Build the data generator specification
products_spec = (
    dg.DataGenerator(spark, name="products", rows=NUM_PRODUCTS, partitions=4)
    
    # withIdOutput() creates an 'id' column with sequential integers starting at 1
    .withIdOutput()
    
    # Create SKU codes: SKU-000001, SKU-000002, etc.
    # expr allows SQL expressions; cast(id as string) converts the id to string
    # lpad pads to 6 digits; uniqueValues ensures exactly NUM_PRODUCTS unique SKUs
    .withColumn("sku", "string", 
                expr="concat('SKU-', lpad(cast(id as string), 6, '0'))", 
                uniqueValues=NUM_PRODUCTS)
    
    # template uses \\w to generate random words
    .withColumn("product_name", "string", template=r"\\w \\w Product")
    
    # values with random=True picks randomly from the list
    .withColumn("category", "string", values=product_categories, random=True)
    .withColumn("brand", "string", values=brands, random=True)
    
    # Numeric ranges for costs and pricing
    .withColumn("unit_cost", "decimal(10,2)", minValue=0.5, maxValue=50.0, random=True)
    .withColumn("unit_price", "decimal(10,2)", minValue=1.0, maxValue=100.0, random=True)
    
    # Pick from specific values (case sizes)
    .withColumn("units_per_case", "integer", values=[6, 12, 24, 48], random=True)
    .withColumn("weight_kg", "decimal(8,2)", minValue=0.1, maxValue=25.0, random=True)
    .withColumn("shelf_life_days", "integer", minValue=30, maxValue=730, random=True)
    
    # Date range for when products were created
    .withColumn("created_date", "date", begin="2020-01-01", end="2024-01-01", random=True)
)

# Build the dataframe from the specification
df_products = products_spec.build()

# Write to table
df_products.write.mode("overwrite").saveAsTable("products")

print(f"✅ Created products table with {df_products.count():,} records")
display(df_products.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Distribution Centers
# MAGIC
# MAGIC ### Learning Objectives:
# MAGIC - Creating location codes with expressions
# MAGIC - Generating geographic coordinates (latitude/longitude)
# MAGIC - Using realistic ranges for capacity and utilization metrics
# MAGIC
# MAGIC ### Pro Tip:
# MAGIC When generating geographic data, use realistic ranges:
# MAGIC - US Latitude: 25.0 to 49.0 (southern border to Canadian border)
# MAGIC - US Longitude: -125.0 to -65.0 (west coast to east coast)

# COMMAND ----------

dc_spec = (
    dg.DataGenerator(spark, name="distribution_centers", rows=NUM_DCS, partitions=4)
    .withIdOutput()
    
    # DC codes: DC-0001, DC-0002, etc.
    .withColumn("dc_code", "string", 
                expr="concat('DC-', lpad(cast(id as string), 4, '0'))", 
                uniqueValues=NUM_DCS)
    
    .withColumn("dc_name", "string", template=r"\\w Distribution Center")
    
    # Regional distribution for US
    .withColumn("region", "string", 
                values=["Northeast", "Southeast", "Midwest", "Southwest", "West"], 
                random=True)
    
    # Warehouse capacity metrics
    .withColumn("capacity_pallets", "integer", minValue=5000, maxValue=50000, random=True)
    .withColumn("current_utilization_pct", "decimal(5,2)", minValue=45.0, maxValue=95.0, random=True)
    
    # Geographic coordinates for mapping
    .withColumn("latitude", "decimal(9,6)", minValue=25.0, maxValue=49.0, random=True)
    .withColumn("longitude", "decimal(9,6)", minValue=-125.0, maxValue=-65.0, random=True)
    
    # Operating costs
    .withColumn("operating_cost_daily", "decimal(10,2)", minValue=5000, maxValue=50000, random=True)
    .withColumn("opened_date", "date", begin="2015-01-01", end="2023-01-01", random=True)
)

df_dcs = dc_spec.build()
df_dcs.write.mode("overwrite").saveAsTable("distribution_centers")

print(f"✅ Created distribution_centers table with {df_dcs.count():,} records")
display(df_dcs.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Retail Stores
# MAGIC
# MAGIC ### Learning Objectives:
# MAGIC - Creating foreign key relationships (dc_id references DCs)
# MAGIC - Generating realistic store attributes
# MAGIC - Using longer store codes (6 digits vs 4 for DCs)

# COMMAND ----------

store_formats = ["Hypermarket", "Supermarket", "Convenience", "Online", "Club Store"]
retailers = ["RetailCo", "MegaMart", "QuickStop", "FreshGrocer", "ValueMart"]

stores_spec = (
    dg.DataGenerator(spark, name="stores", rows=NUM_STORES, partitions=8)
    .withIdOutput()
    
    # Store codes: STORE-000001, STORE-000002, etc.
    .withColumn("store_code", "string", 
                expr="concat('STORE-', lpad(cast(id as string), 6, '0'))", 
                uniqueValues=NUM_STORES)
    
    .withColumn("retailer", "string", values=retailers, random=True)
    .withColumn("store_format", "string", values=store_formats, random=True)
    .withColumn("region", "string", 
                values=["Northeast", "Southeast", "Midwest", "Southwest", "West"], 
                random=True)
    
    # Store size range from small convenience to large hypermarket
    .withColumn("square_footage", "integer", minValue=2000, maxValue=200000, random=True)
    
    # FOREIGN KEY: Links to distribution_centers table
    # Each store gets a DC ID between 1 and NUM_DCS
    .withColumn("dc_id", "integer", minValue=1, maxValue=NUM_DCS, random=True)
    
    .withColumn("latitude", "decimal(9,6)", minValue=25.0, maxValue=49.0, random=True)
    .withColumn("longitude", "decimal(9,6)", minValue=-125.0, maxValue=-65.0, random=True)
    .withColumn("opened_date", "date", begin="2010-01-01", end="2024-01-01", random=True)
)

df_stores = stores_spec.build()
df_stores.write.mode("overwrite").saveAsTable("stores")

print(f"✅ Created stores table with {df_stores.count():,} records")
print(f"🔗 Each store is linked to a DC via dc_id foreign key")
display(df_stores.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Production Orders
# MAGIC
# MAGIC ### Learning Objectives:
# MAGIC - Working with **timestamp** columns
# MAGIC - Using intermediate random columns for complex calculations
# MAGIC - Post-processing with PySpark transformations
# MAGIC - Using modulo operations for distributing categorical values
# MAGIC
# MAGIC ### Advanced Pattern:
# MAGIC When you need complex logic that depends on random values:
# MAGIC 1. Generate random "seed" columns in the spec
# MAGIC 2. Build the dataframe
# MAGIC 3. Use PySpark `.withColumn()` to create derived columns
# MAGIC 4. Drop the intermediate seed columns

# COMMAND ----------

production_status = ["Scheduled", "In Progress", "Completed", "Delayed", "Quality Hold"]

production_spec = (
    dg.DataGenerator(spark, name="production_orders", rows=NUM_PRODUCTION_ORDERS, partitions=8)
    .withIdOutput()
    
    .withColumn("order_number", "string", 
                expr="concat('PO-', lpad(cast(id as string), 8, '0'))", 
                uniqueValues=NUM_PRODUCTION_ORDERS)
    
    # FOREIGN KEYS
    .withColumn("dc_id", "integer", minValue=1, maxValue=NUM_DCS, random=True)
    .withColumn("product_id", "integer", minValue=1, maxValue=NUM_PRODUCTS, random=True)
    
    # Base timestamp for the order
    .withColumn("order_date", "timestamp", 
                begin="2024-01-01 00:00:00", 
                end="2025-09-29 23:59:59", 
                random=True)
    
    # Random seed columns for calculations (will be used then dropped)
    .withColumn("scheduled_start_days", "integer", minValue=0, maxValue=10, random=True)
    .withColumn("scheduled_duration_days", "integer", minValue=1, maxValue=6, random=True)
    .withColumn("start_delay_hours", "integer", minValue=-12, maxValue=12, random=True)
    .withColumn("actual_duration_hours", "integer", minValue=24, maxValue=144, random=True)
    .withColumn("start_probability", "double", minValue=0, maxValue=1, random=True)
    .withColumn("completion_probability", "double", minValue=0, maxValue=1, random=True)
    .withColumn("quantity_ordered", "integer", minValue=500, maxValue=50000, random=True)
    .withColumn("production_variance", "double", minValue=0.85, maxValue=1.0, random=True)
    
    # Use modulo to distribute status values evenly
    # status_rand % 5 gives values 0-4, which we'll map to our 5 status values
    .withColumn("status_rand", "integer", minValue=1, maxValue=10000, random=True)
    
    .withColumn("line_efficiency_pct", "decimal(5,2)", minValue=75.0, maxValue=98.0, random=True)
    .withColumn("production_cost", "decimal(12,2)", minValue=5000, maxValue=500000, random=True)
)

# Build the base dataframe
df_production = production_spec.build()

# POST-PROCESSING: Add calculated columns using PySpark
df_production = (
    df_production
    # Calculate scheduled start by adding days to order_date
    .withColumn("scheduled_start",
                F.expr("date_add(order_date, scheduled_start_days)"))
    
    # Calculate scheduled end
    .withColumn("scheduled_end",
                F.expr("date_add(scheduled_start, scheduled_duration_days)"))
    
    # Actual start: only if probability > 0.3, add delay hours
    .withColumn("actual_start",
                F.when(F.col("start_probability") > 0.3,
                       F.expr("timestampadd(HOUR, start_delay_hours, scheduled_start)"))
                .otherwise(None))
    
    # Actual end: only if started AND probability > 0.2
    .withColumn("actual_end",
                F.when((F.col("actual_start").isNotNull()) & 
                       (F.col("completion_probability") > 0.2),
                       F.expr("timestampadd(HOUR, actual_duration_hours, actual_start)"))
                .otherwise(None))
    
    # Quantity produced: apply variance if completed
    .withColumn("quantity_produced",
                F.when(F.col("actual_end").isNotNull(),
                       (F.col("quantity_ordered") * F.col("production_variance")).cast("integer"))
                .otherwise(0))
    
    # Map status_rand to status using modulo and array indexing
    .withColumn("status_index", F.col("status_rand") % 5)
    .withColumn("status",
                F.array([F.lit(s) for s in production_status]).getItem(F.col("status_index")))
    
    # Clean up: drop intermediate columns
    .drop("scheduled_start_days", "scheduled_duration_days", "start_delay_hours",
          "actual_duration_hours", "start_probability", "completion_probability", 
          "production_variance", "status_rand", "status_index")
)

df_production.write.mode("overwrite").saveAsTable("production_orders")

print(f"✅ Created production_orders table with {df_production.count():,} records")
print(f"📊 Status distribution:")
df_production.groupBy("status").count().orderBy("status").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inventory Snapshots
# MAGIC
# MAGIC ### Learning Objectives:
# MAGIC - Using **CASE expressions** in SQL for conditional logic
# MAGIC - Creating weighted distributions with seed columns
# MAGIC - Handling division by zero with conditional logic
# MAGIC - Post-processing for complex foreign key relationships
# MAGIC
# MAGIC ### Pattern: Weighted Categorical Distribution
# MAGIC To get 30% DC and 70% Store:
# MAGIC 1. Create a seed column with values 0-1
# MAGIC 2. Use CASE: when seed < 0.3 then 'DC' else 'Store'
# MAGIC
# MAGIC ### Pattern: Safe Division
# MAGIC Always check denominator before dividing to avoid errors

# COMMAND ----------

inventory_spec = (
    dg.DataGenerator(spark, name="inventory", rows=NUM_INVENTORY_RECORDS, partitions=8)
    .withIdOutput()
    
    # Date range for inventory snapshots
    .withColumn("snapshot_date", "date", 
                begin="2024-01-01", 
                end="2025-09-29", 
                random=True)
    
    # Weighted distribution: 30% DC, 70% Store
    .withColumn("location_type_seed", "double", minValue=0, maxValue=1, random=True)
    .withColumn("location_type", "string", expr="""
        CASE 
            WHEN location_type_seed < 0.3 THEN 'DC'
            ELSE 'Store'
        END
    """)
    
    # FOREIGN KEY
    .withColumn("product_id", "integer", minValue=1, maxValue=NUM_PRODUCTS, random=True)
    
    # Inventory quantities
    .withColumn("quantity_on_hand", "integer", minValue=0, maxValue=10000, random=True)
    .withColumn("reserve_factor", "double", minValue=0, maxValue=0.5, random=True)
    .withColumn("reorder_point", "integer", minValue=100, maxValue=2000, random=True)
    
    # Demand rate for calculations
    .withColumn("daily_demand", "double", minValue=50.0, maxValue=150.0, random=True)
    
    .withColumn("inventory_value", "decimal(12,2)", minValue=1000, maxValue=500000, random=True)
    .withColumn("days_offset", "integer", minValue=0, maxValue=60, random=True)
)

df_inventory = inventory_spec.build()

# POST-PROCESSING
df_inventory = (
    df_inventory
    # Create location_id based on location_type
    # Use modulo to cycle through valid IDs
    .withColumn("location_id",
                F.when(F.col("location_type") == "DC", 
                       (F.col("id") % 25) + 1)  # DC IDs: 1-25
                .otherwise((F.col("id") % 1000) + 1))  # Store IDs: 1-1000
    
    # Calculate reserved quantity
    .withColumn("quantity_reserved", 
                (F.col("quantity_on_hand") * F.col("reserve_factor")).cast("integer"))
    
    # Available = on hand - reserved
    .withColumn("quantity_available", 
                F.col("quantity_on_hand") - F.col("quantity_reserved"))
    
    # SAFE DIVISION: Check daily_demand > 0 before dividing
    .withColumn("days_of_supply",
                F.when(F.col("daily_demand") > 0,
                       (F.col("quantity_available") / F.col("daily_demand")).cast("decimal(8,2)"))
                .otherwise(None))
    
    # Date arithmetic
    .withColumn("last_received_date",
                F.date_sub(F.col("snapshot_date"), F.col("days_offset")))
    
    # Risk categorization based on days of supply
    .withColumn("stockout_risk",
                F.when((F.col("days_of_supply").isNull()) | 
                       (F.col("days_of_supply") < 3), "High")
                .when(F.col("days_of_supply") < 7, "Medium")
                .otherwise("Low"))
    
    # Drop intermediate columns
    .drop("reserve_factor", "days_offset", "location_type_seed")
)

df_inventory.write.mode("overwrite").saveAsTable("inventory")

print(f"✅ Created inventory table with {df_inventory.count():,} records")
print(f"📊 Location type distribution:")
df_inventory.groupBy("location_type").count().show()
print(f"⚠️ Stockout risk distribution:")
df_inventory.groupBy("stockout_risk").count().orderBy("stockout_risk").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Shipments
# MAGIC
# MAGIC ### Learning Objectives:
# MAGIC - Creating **multiple weighted categorical columns**
# MAGIC - Working with date arithmetic (transit times)
# MAGIC - Computing derived metrics (on_time, delay_hours)
# MAGIC - Handling NULL values in calculations
# MAGIC
# MAGIC ### Pattern: Multiple Weighted Categories
# MAGIC For transport_mode with weights [60%, 15%, 20%, 5%]:
# MAGIC - 0.00-0.60: Truck (60%)
# MAGIC - 0.60-0.75: Rail (15%)
# MAGIC - 0.75-0.95: Intermodal (20%)
# MAGIC - 0.95-1.00: Air (5%)

# COMMAND ----------

shipment_status = ["In Transit", "Delivered", "Delayed", "At Hub", "Out for Delivery"]
transport_modes = ["Truck", "Rail", "Intermodal", "Air"]

shipments_spec = (
    dg.DataGenerator(spark, name="shipments", rows=NUM_SHIPMENTS, partitions=8)
    .withIdOutput()
    
    .withColumn("shipment_id", "string", 
                expr="concat('SHP-', lpad(cast(id as string), 10, '0'))", 
                uniqueValues=NUM_SHIPMENTS)
    
    # FOREIGN KEY: Origin is always a DC
    .withColumn("origin_dc_id", "integer", minValue=1, maxValue=NUM_DCS, random=True)
    
    # Destination can be DC or Store (30% DC, 70% Store)
    .withColumn("destination_type_seed", "double", minValue=0, maxValue=1, random=True)
    .withColumn("destination_type", "string", expr="""
        CASE 
            WHEN destination_type_seed < 0.3 THEN 'DC'
            ELSE 'Store'
        END
    """)
    
    .withColumn("product_id", "integer", minValue=1, maxValue=NUM_PRODUCTS, random=True)
    
    # Shipment dates
    .withColumn("ship_date", "timestamp", 
                begin="2024-01-01 00:00:00", 
                end="2025-09-29 23:59:59", 
                random=True)
    
    # Transit time ranges
    .withColumn("transit_days", "integer", minValue=1, maxValue=6, random=True)
    .withColumn("actual_transit_days", "integer", minValue=1, maxValue=8, random=True)
    .withColumn("delivery_probability", "double", minValue=0, maxValue=1, random=True)
    
    .withColumn("quantity", "integer", minValue=100, maxValue=5000, random=True)
    
    # Transport mode with weighted distribution: 60% Truck, 15% Rail, 20% Intermodal, 5% Air
    .withColumn("transport_mode_seed", "double", minValue=0, maxValue=1, random=True)
    .withColumn("transport_mode", "string", expr="""
        CASE 
            WHEN transport_mode_seed < 0.60 THEN 'Truck'
            WHEN transport_mode_seed < 0.75 THEN 'Rail'
            WHEN transport_mode_seed < 0.95 THEN 'Intermodal'
            ELSE 'Air'
        END
    """)
    
    .withColumn("carrier", "string", 
                values=["FastFreight", "ReliableLogistics", "ExpressTransport", "GlobalShippers"], 
                random=True)
    
    # Status with weighted distribution: 25% In Transit, 50% Delivered, 5% Delayed, 10% At Hub, 10% Out for Delivery
    .withColumn("status_seed", "double", minValue=0, maxValue=1, random=True)
    .withColumn("status", "string", expr="""
        CASE 
            WHEN status_seed < 0.25 THEN 'In Transit'
            WHEN status_seed < 0.75 THEN 'Delivered'
            WHEN status_seed < 0.80 THEN 'Delayed'
            WHEN status_seed < 0.90 THEN 'At Hub'
            ELSE 'Out for Delivery'
        END
    """)
    
    .withColumn("shipping_cost", "decimal(10,2)", minValue=50, maxValue=5000, random=True)
    .withColumn("distance_miles", "integer", minValue=50, maxValue=2500, random=True)
)

df_shipments = shipments_spec.build()

# POST-PROCESSING: Calculate derived metrics
df_shipments = (
    df_shipments
    # Map destination_id based on type
    .withColumn("destination_id",
                F.when(F.col("destination_type") == "DC", 
                       (F.col("id") % 25) + 1)
                .otherwise((F.col("id") % 1000) + 1))
    
    # Expected delivery = ship_date + transit_days
    .withColumn("expected_delivery",
                F.date_add(F.col("ship_date"), F.col("transit_days")))
    
    # Actual delivery: only 80% of shipments are delivered
    .withColumn("actual_delivery",
                F.when(F.col("delivery_probability") > 0.2,
                       F.date_add(F.col("ship_date"), F.col("actual_transit_days")))
                .otherwise(None))
    
    # On-time check: delivered AND before/at expected time
    .withColumn("on_time",
                (F.col("actual_delivery").isNotNull()) & 
                (F.col("actual_delivery") <= F.col("expected_delivery")))
    
    # Calculate delay in hours (can be negative for early deliveries)
    .withColumn("delay_hours",
                F.when(F.col("actual_delivery").isNotNull(),
                       ((F.unix_timestamp(F.col("actual_delivery")) - 
                         F.unix_timestamp(F.col("expected_delivery"))) / 3600).cast("integer"))
                .otherwise(None))
    
    # Clean up
    .drop("transit_days", "actual_transit_days", "delivery_probability", 
          "destination_type_seed", "transport_mode_seed", "status_seed")
)

df_shipments.write.mode("overwrite").saveAsTable("shipments")

print(f"✅ Created shipments table with {df_shipments.count():,} records")
print(f"🚚 Transport mode distribution:")
df_shipments.groupBy("transport_mode").count().orderBy(F.desc("count")).show()
print(f"📦 Shipment status distribution:")
df_shipments.groupBy("status").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Data Quality Checks
# MAGIC
# MAGIC Let's verify our data generation was successful and check some relationships.

# COMMAND ----------

print("=" * 80)
print(f"✅ DATA GENERATION COMPLETE")
print("=" * 80)
print()

# Show all tables created
tables = spark.sql(f"SHOW TABLES IN {SCHEMA_NAME}").collect()
print(f"📊 Tables created in {SCHEMA_NAME}:")
print()

total_records = 0
for table in tables:
    table_name = table.tableName
    count = spark.table(f"{SCHEMA_NAME}.{table_name}").count()
    total_records += count
    print(f"   • {table_name:.<30} {count:>10,} records")

print()
print(f"   TOTAL: {total_records:,} records across all tables")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Use Cases
# MAGIC
# MAGIC This dataset enables the following analytics use cases:
# MAGIC
# MAGIC ### 📦 Inventory Optimization
# MAGIC - Multi-echelon inventory visibility across DCs and stores
# MAGIC - Stockout risk identification and prediction
# MAGIC - Days of supply analysis by product/location
# MAGIC - Slow-moving inventory identification
# MAGIC
# MAGIC ### 🚚 Logistics & Transportation
# MAGIC - Carrier performance scorecards (OTD%, cost, speed)
# MAGIC - Route optimization opportunities
# MAGIC - Transport mode analysis (cost vs speed tradeoffs)
# MAGIC - Delay root cause analysis
# MAGIC
# MAGIC ### 🏭 Production Planning
# MAGIC - Production schedule optimization
# MAGIC - Line efficiency tracking
# MAGIC - Capacity planning and utilization
# MAGIC - Production-to-inventory flow analysis
# MAGIC
# MAGIC ### 📊 Supply Chain Analytics
# MAGIC - End-to-end supply chain visibility
# MAGIC - Network optimization (DC placement, capacity)
# MAGIC - Working capital optimization
# MAGIC - Cost-to-serve analysis by region/channel
# MAGIC
# MAGIC ### 🤖 AI/ML Use Cases
# MAGIC - Demand forecasting
# MAGIC - Predictive maintenance (production efficiency)
# MAGIC - Shipment delay prediction
# MAGIC - Inventory replenishment optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries to Get Started
# MAGIC
# MAGIC Here are some queries you can run to explore the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Current Inventory Health

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inventory health by location type and risk level
# MAGIC SELECT 
# MAGIC   location_type,
# MAGIC   stockout_risk,
# MAGIC   COUNT(*) as item_count,
# MAGIC   SUM(inventory_value) as total_value,
# MAGIC   ROUND(AVG(days_of_supply), 1) as avg_days_supply
# MAGIC FROM inventory
# MAGIC WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory)
# MAGIC GROUP BY location_type, stockout_risk
# MAGIC ORDER BY location_type, 
# MAGIC   CASE stockout_risk 
# MAGIC     WHEN 'High' THEN 1 
# MAGIC     WHEN 'Medium' THEN 2 
# MAGIC     WHEN 'Low' THEN 3 
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Carrier Performance Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare carriers on key metrics
# MAGIC SELECT 
# MAGIC   carrier,
# MAGIC   COUNT(*) as total_shipments,
# MAGIC   ROUND(AVG(CASE WHEN on_time = true THEN 100.0 ELSE 0.0 END), 1) as otd_pct,
# MAGIC   ROUND(AVG(shipping_cost), 2) as avg_cost,
# MAGIC   ROUND(AVG(distance_miles), 0) as avg_distance,
# MAGIC   ROUND(AVG(shipping_cost / distance_miles), 3) as cost_per_mile
# MAGIC FROM shipments
# MAGIC WHERE actual_delivery IS NOT NULL
# MAGIC GROUP BY carrier
# MAGIC ORDER BY total_shipments DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Supply Chain Network Overview

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DC performance and utilization
# MAGIC SELECT 
# MAGIC   dc.dc_code,
# MAGIC   dc.region,
# MAGIC   dc.capacity_pallets,
# MAGIC   ROUND(dc.current_utilization_pct, 1) as utilization_pct,
# MAGIC   COUNT(DISTINCT i.product_id) as active_skus,
# MAGIC   SUM(i.inventory_value) as inventory_value,
# MAGIC   COUNT(DISTINCT s.id) as outbound_shipments_last_30d,
# MAGIC   ROUND(AVG(CASE WHEN s.on_time = true THEN 100.0 ELSE 0.0 END), 1) as otd_pct
# MAGIC FROM distribution_centers dc
# MAGIC LEFT JOIN inventory i ON dc.id = i.location_id 
# MAGIC   AND i.location_type = 'DC'
# MAGIC   AND i.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory)
# MAGIC LEFT JOIN shipments s ON dc.id = s.origin_dc_id
# MAGIC   AND s.ship_date >= CURRENT_DATE - INTERVAL 30 DAY
# MAGIC GROUP BY dc.dc_code, dc.region, dc.capacity_pallets, dc.current_utilization_pct
# MAGIC ORDER BY inventory_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Congratulations!
# MAGIC
# MAGIC You've successfully generated a complete CPG supply chain dataset using dbldatagen!
# MAGIC
# MAGIC ### What You've Learned:
# MAGIC ✅ How to install and import dbldatagen  
# MAGIC ✅ Basic column generation with different data types  
# MAGIC ✅ Creating foreign key relationships  
# MAGIC ✅ Weighted categorical distributions  
# MAGIC ✅ Date/timestamp generation  
# MAGIC ✅ Post-processing with PySpark  
# MAGIC ✅ Safe handling of division and NULL values  
# MAGIC
# MAGIC ### Your Dataset Includes:
# MAGIC - 500 Products across 7 categories  
# MAGIC - 25 Distribution Centers  
# MAGIC - 1,000 Retail Stores  
# MAGIC - 10,000 Production Orders  
# MAGIC - 50,000 Inventory Records  
# MAGIC - 30,000 Shipments  
# MAGIC
# MAGIC **Total: 91,525 records ready for analytics!**
# MAGIC
# MAGIC Now go build some amazing dashboards! 📊✨