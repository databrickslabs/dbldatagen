# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Getting Started with the Databricks Labs Data Generator
# MAGIC
# MAGIC This notebook provides an introduction to synthetic data generation using the [Databricks Labs Data Generator (`dbldatagen`)](https://databrickslabs.github.io/dbldatagen/public_docs/index.html). This data generator is useful for generating large synthetic datasets for development, testing, benchmarking, proofs-of-concept, and other use-cases.

# COMMAND ----------

# DBTITLE 1,Install dbldatagen
# MAGIC %pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Import Modules
import dbldatagen as dg
import dbldatagen.distributions as dist

from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType, BooleanType, LongType, ArrayType
from pyspark.sql.functions import current_timestamp

import random
import string
from datetime import datetime
from pyspark.sql.functions import col, format_string, expr, sha2

# COMMAND ----------

# DBTITLE 1,Set up Parameters
# Set up how many rows we want along with how many users, devices and IPs we want
ROW_COUNT=4144000
NUMBER_OF_USERS=200000
NUMBER_OF_DEVICES= NUMBER_OF_USERS+50000
NUMBER_OF_IPS=40000
# ROW_COUNT=1000000000
# NUMBER_OF_USERS=2000000
# NUMBER_OF_DEVICES= NUMBER_OF_USERS+500000
# NUMBER_OF_IPS=400000
START_TIMESTAMP="2025-03-01 00:00:00"
END_TIMESTAMP="2025-03-30 00:00:00"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Specifications
# MAGIC
# MAGIC Let's start by generating a DataFrame with rows representing unique login information. Data generation is controlled by a `DataGenerator` object. Each `DataGenerator` can be extended with rules specifying the output schema and value generation.

# COMMAND ----------

# DBTITLE 1,Generate a DataFrame
default_annotations_spec = (
    dg.DataGenerator(
        spark,
        name="default_annotations_spec",
        rows=ROW_COUNT
    )
    .withIdOutput()  # Add a unique id column for each row
    .withColumn(
        "EVENT_TIMESTAMP",
        TimestampType(),
        data_range=dg.DateRange(START_TIMESTAMP, END_TIMESTAMP, "seconds=1"),
        random=True,
    )  # Random event timestamp within the specified range
    .withColumn(
        "internal_ACCOUNTID",
        LongType(),
        minValue=0x1000000000000,
        uniqueValues=NUMBER_OF_USERS,
        omit=True,
        baseColumnType="hash",
    )  # Internal unique account id, omitted from output, used for deterministic hashing
    .withColumn(
        "ACCOUNTID", StringType(), format="0x%032x", baseColumn="internal_ACCOUNTID"
    )  # Public account id as hex string
    .withColumn(
        "internal_DEVICEID",
        LongType(),
        minValue=0x1000000000000,
        uniqueValues=NUMBER_OF_DEVICES,
        omit=True,
        baseColumnType="hash",
        baseColumn="internal_ACCOUNTID"
    )  # Internal device id, unique per account, omitted from output
    .withColumn(
        "DEVICEID", StringType(), format="0x%032x", baseColumn="internal_DEVICEID"
    )  # Public device id as hex string
    .withColumn("app_version", StringType(), values=["current"])  # Static app version
    .withColumn("authMethod", StringType(), values=["OAuth", "password"])  # Auth method, random selection
    # Assign clientName based on DEVICEID deterministically
    .withColumn(
        "clientName",
        StringType(),
        expr="""
            element_at(
                array('SwitchGameClient','XboxGameClient','PlaystationGameClient','PCGameClient'),
                (pmod(abs(hash(DEVICEID)), 4) + 1)
            )
        """
    )
    .withColumn(
        "clientId",
        StringType(),
        expr="sha2(concat(ACCOUNTID, clientName), 256)",
        baseColumn=["ACCOUNTID", "clientName"]
    )  # Deterministic clientId based on ACCOUNTID and clientName
    .withColumn(
        "correlationId",
        StringType(),
        expr="sha2(concat(ACCOUNTID, clientId), 256)",
    )  # Session correlation id, deterministic hash
    .withColumn("country", StringType(), values=["USA", "UK", "AUS"], weights=[0.6, 0.2, 0.2], baseColumn="ACCOUNTID", random=True)  # Assign country with 60% USA, 20% UK, 20% AUS
    .withColumn("environment", StringType(), values=["prod"])  # Static environment value
    .withColumn("EVENT_TYPE", StringType(), values=["account_login_success"])  # Static event type
    # Assign geoip_city_name based on country and ACCOUNTID
    .withColumn(
        "CITY",
        StringType(),
        expr="""
            CASE
                WHEN country = 'USA' THEN element_at(array('New York', 'San Francisco', 'Chicago'), pmod(abs(hash(ACCOUNTID)), 3) + 1)
                WHEN country = 'UK' THEN 'London'
                WHEN country = 'AUS' THEN 'Sydney'
            END
        """,
        baseColumn=["country", "ACCOUNTID"]
    )
    .withColumn("countrycode2", StringType(), expr="CASE WHEN country = 'USA' THEN 'US' WHEN country = 'UK' THEN 'UK' WHEN country = 'AUS' THEN 'AU' END", baseColumn=["country"])  # Country code
    # Assign ISP based on country and ACCOUNTID
    .withColumn(
        "ISP",
        StringType(),
        expr="""
            CASE
                WHEN country = 'USA' THEN element_at(array('Comcast', 'AT&T', 'Verizon', 'Spectrum', 'Cox'), pmod(abs(hash(ACCOUNTID)), 5) + 1)
                WHEN country = 'UK' THEN element_at(array('BT', 'Sky', 'Virgin Media', 'TalkTalk', 'EE'), pmod(abs(hash(ACCOUNTID)), 5) + 1)
                WHEN country = 'AUS' THEN element_at(array('Telstra', 'Optus', 'TPG', 'Aussie Broadband', 'iiNet'), pmod(abs(hash(ACCOUNTID)), 5) + 1)
                ELSE 'Unknown ISP'
            END
        """,
        baseColumn=["country", "ACCOUNTID"]
    )
    # Assign latitude based on city
    .withColumn(
        "latitude",
        FloatType(),
        expr="""
            CASE
                WHEN CITY = 'New York' THEN 40.7128
                WHEN CITY = 'San Francisco' THEN 37.7749
                WHEN CITY = 'Chicago' THEN 41.8781
                WHEN CITY = 'London' THEN 51.5074
                WHEN CITY = 'Sydney' THEN -33.8688
                ELSE 0.0
            END
        """,
        baseColumn="CITY"
    )
    # Assign longitude based on city
    .withColumn(
        "longitude",
        FloatType(),
        expr="""
            CASE
                WHEN CITY = 'New York' THEN -74.0060
                WHEN CITY = 'San Francisco' THEN -122.4194
                WHEN CITY = 'Chicago' THEN -87.6298
                WHEN CITY = 'London' THEN -0.1278
                WHEN CITY = 'Sydney' THEN 151.2093
                ELSE 0.0
            END
        """,
        baseColumn="CITY"
    )
    # Assign region name based on country and city
    .withColumn(
        "region_name",
        StringType(),
        expr="""
            CASE
                WHEN country = 'USA' THEN
                    CASE
                        WHEN CITY = 'New York' THEN 'New York'
                        WHEN CITY = 'San Francisco' THEN 'California'
                        WHEN CITY = 'Chicago' THEN 'Illinois'
                        ELSE 'Unknown'
                    END
                WHEN country = 'UK' THEN 'England'
                WHEN country = 'AUS' THEN 'New South Wales'
                ELSE 'Unknown'
            END
        """,
        baseColumn=["country", "CITY"]
    )
    # Internal IP address as integer, unique per device, omitted from output
    .withColumn(
        "internal_REQUESTIPADDRESS",
        LongType(),
        minValue=0x1000000000000,
        uniqueValues=NUMBER_OF_IPS,
        omit=True,
        baseColumnType="hash",
        baseColumn="internal_DEVICEID"
    )
    # Convert internal IP integer to dotted quad string
    .withColumn(
        "REQUESTIPADDRESS",
        StringType(),
        expr="""
            concat(
                cast((internal_REQUESTIPADDRESS >> 24) & 255 as string), '.',
                cast((internal_REQUESTIPADDRESS >> 16) & 255 as string), '.',
                cast((internal_REQUESTIPADDRESS >> 8) & 255 as string), '.',
                cast(internal_REQUESTIPADDRESS & 255 as string)
            )
        """,
        baseColumn="internal_REQUESTIPADDRESS",
    )
    # Generate user agent string using clientName and correlationId
    .withColumn(
        "userAgent",
        StringType(),
        expr="concat('Launch/1.0+', clientName, '(', clientName, '/)/', correlationId)",
        baseColumn=["clientName", "correlationId"]
    )
)
default_annotations = default_annotations_spec.build()

# COMMAND ----------

# DBTITLE 1,Transform the Dataframe
from pyspark.sql.functions import col, to_date, hour
# Transform the DataFrame
transformed_df = default_annotations.select(
    col("EVENT_TYPE").alias("EVENT_TYPE"),
    col("EVENT_TIMESTAMP").alias("EVENT_TIMESTAMP"),
    hour(col("EVENT_TIMESTAMP")).alias("EVENT_HOUR"),
    to_date(col("EVENT_TIMESTAMP")).alias("EVENT_DATE"),
    col("ACCOUNTID").alias("ACCOUNTID"),
    col("environment").alias("APPENV"),
    col("app_version").alias("APP_VERSION"),
    col("authMethod").alias("AUTHMETHOD"),
    col("clientId").alias("CLIENTID"),
    col("clientName").alias("CLIENTNAME"),
    col("DEVICEID").alias("DEVICEID"),
    col("environment").alias("ENVIRONMENT"),
    col("CITY").alias("CITY"),
    col("countrycode2").alias("COUNTRY_CODE2"),
    col("ISP").alias("ISP"),
    col("latitude").cast("double").alias("LATITUDE"),
    col("longitude").cast("double").alias("LONGITUDE"),
    col("region_name").alias("REGION_NAME"),
    col("REQUESTIPADDRESS").alias("REQUESTIPADDRESS"),
    col("correlationId").alias("SESSION_ID"),
    col("correlationId").alias("TRACKINGUUID"),
    col("userAgent").alias("USERAGENT"),
)

# COMMAND ----------

# DBTITLE 1,Look at the Data
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------

transformed_df.write.mode("overwrite").saveAsTable("main.test.EVENT_ACCOUNT_LOGIN_SUCCESS")