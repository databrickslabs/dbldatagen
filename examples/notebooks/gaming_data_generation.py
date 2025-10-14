# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Getting Started with the Databricks Labs Data Generator
# MAGIC This notebook provides an introduction to synthetic data generation using the [Databricks Labs Data Generator (`dbldatagen`)](https://databrickslabs.github.io/dbldatagen/public_docs/index.html). This data generator is useful for generating large synthetic datasets for development, testing, benchmarking, proofs-of-concept, and other use-cases.
# MAGIC
# MAGIC The notebook simulates data for a user login scenario for the gaming industry.

# COMMAND ----------

# DBTITLE 1,Install dbldatagen
# dbldatagen can be installed using pip install commands, as a cluster-scoped library, or as a serverless environment-scoped library.
%pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Import Modules
import dbldatagen as dg

from pyspark.sql.types import DoubleType, StringType, TimestampType, LongType
from pyspark.sql.functions import col, expr, sha2, to_date, hour

# COMMAND ----------

# DBTITLE 1,Set up Parameters
# Set up how many rows we want along with how many users, devices and IPs we want
ROW_COUNT = 4500000
NUMBER_OF_USERS = 200000
NUMBER_OF_DEVICES = NUMBER_OF_USERS + 50000
NUMBER_OF_IPS = 40000

START_TIMESTAMP = "2025-03-01 00:00:00"
END_TIMESTAMP = "2025-03-30 00:00:00"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Specifications
# MAGIC
# MAGIC Let's start by generating a DataFrame with rows representing unique login information. Data generation is controlled by a `DataGenerator` object. Each `DataGenerator` can be extended with rules specifying the output schema and value generation. Columns can be defined using `withColumn(...)` with a variety of parameters.
# MAGIC
# MAGIC **colName** – Name of column to add. If this conflicts with the underlying seed column (id), it is recommended that the seed column name is customized during the construction of the data generator spec.
# MAGIC
# MAGIC **colType** – Data type for column. This may be specified as either a type from one of the possible pyspark.sql.types (e.g. StringType, DecimalType(10,3) etc) or as a string containing a Spark SQL type definition (i.e String, array<Integer>, map<String, Float>)
# MAGIC
# MAGIC **omit** – if True, the column will be omitted from the final set of columns in the generated data. Used to create columns that are used by other columns as intermediate results. Defaults to False
# MAGIC
# MAGIC **expr** – Specifies SQL expression used to create column value. If specified, overrides the default rules for creating column value. Defaults to None
# MAGIC
# MAGIC **baseColumn** – String or list of columns to control order of generation of columns. If not specified, column is dependent on base seed column (which defaults to id)

# COMMAND ----------

# DBTITLE 1,Generate a DataFrame
default_annotations_spec = (
    dg.DataGenerator(spark, name="default_annotations_spec", rows=ROW_COUNT)
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
        baseColumn="internal_ACCOUNTID",
    )  # Internal device id, based on account, omitted from output
    .withColumn(
        "DEVICEID", StringType(), format="0x%032x", baseColumn="internal_DEVICEID"
    )  # Public device id as hex string
    .withColumn("APP_VERSION", StringType(), values=["current"])  # Static app version
    .withColumn(
        "AUTHMETHOD", StringType(), values=["OAuth", "password"]
    )  # Auth method, random selection
    # Assign clientName based on DEVICEID deterministically
    .withColumn(
        "CLIENTNAME",
        StringType(),
        expr="""
            element_at(
                array('SwitchGameClient','XboxGameClient','PlaystationGameClient','PCGameClient'),
                (pmod(abs(hash(DEVICEID)), 4) + 1)
            )
        """,
    )
    .withColumn(
        "CLIENTID",
        StringType(),
        expr="sha2(concat(ACCOUNTID, CLIENTNAME), 256)",
        baseColumn=["ACCOUNTID", "CLIENTNAME"],
    )  # Deterministic clientId based on ACCOUNTID and clientName
    .withColumn(
        "SESSION_ID",
        StringType(),
        expr="sha2(concat(ACCOUNTID, CLIENTID), 256)",
    )  # Session correlation id, deterministic hash
    .withColumn(
        "country",
        StringType(),
        values=["USA", "UK", "AUS"],
        weights=[0.6, 0.2, 0.2],
        baseColumn="ACCOUNTID",
        random=True,
    )  # Assign country with 60% USA, 20% UK, 20% AUS
    .withColumn(
        "APPENV", StringType(), values=["prod"]
    )  # Static environment value
    .withColumn(
        "EVENT_TYPE", StringType(), values=["account_login_success"]
    )  # Static event type
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
        baseColumn=["country", "ACCOUNTID"],
    )
    .withColumn(
        "COUNTRY_CODE2",
        StringType(),
        expr="CASE WHEN country = 'USA' THEN 'US' WHEN country = 'UK' THEN 'UK' WHEN country = 'AUS' THEN 'AU' END",
        baseColumn=["country"],
    )  # Country code
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
        baseColumn=["country", "ACCOUNTID"],
    )
    # Assign latitude based on city
    .withColumn(
        "LATITUDE",
        DoubleType(),
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
        baseColumn="CITY",
    )
    # Assign longitude based on city
    .withColumn(
        "LONGITUDE",
        DoubleType(),
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
        baseColumn="CITY",
    )
    # Assign region name based on country and city
    .withColumn(
        "REGION_NAME",
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
        baseColumn=["country", "CITY"],
    )
    # Internal IP address as integer, unique per device, omitted from output
    .withColumn(
        "internal_REQUESTIPADDRESS",
        LongType(),
        minValue=0x1000000000000,
        uniqueValues=NUMBER_OF_IPS,
        omit=True,
        baseColumnType="hash",
        baseColumn="internal_DEVICEID",
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
    # Generate user agent string using clientName and SESSION_ID
    .withColumn(
        "USERAGENT",
        StringType(),
        expr="concat('Launch/1.0+', CLIENTNAME, '(', CLIENTNAME, '/)/', SESSION_ID)",
        baseColumn=["CLIENTNAME", "SESSION_ID"],
    )
)
# Build creates a DataFrame from the DataGenerator
default_logins_df = default_annotations_spec.build()

# COMMAND ----------

# DBTITLE 1,Transform the Dataframe
logins_df = default_logins_df.withColumn(
    "EVENT_HOUR", hour(col("EVENT_TIMESTAMP"))
).withColumn("EVENT_DATE", to_date(col("EVENT_TIMESTAMP")))

# COMMAND ----------

# DBTITLE 1,Look at the Data
display(logins_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------


transformed_df.write.mode("overwrite").saveAsTable("main.test.EVENT_ACCOUNT_LOGIN_SUCCESS")
