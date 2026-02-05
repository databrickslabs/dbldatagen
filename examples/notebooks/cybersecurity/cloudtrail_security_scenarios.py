# Databricks notebook source
# MAGIC %md
# MAGIC # CloudTrail Security Scenarios
# MAGIC
# MAGIC Generates records for each security scenario:
# MAGIC 1. High frequency login attempts from same IP
# MAGIC 2. Large number of failed login attempts
# MAGIC 3. Unusual geographies (with mock IP geolocation)
# MAGIC 4. High rate of error codes
# MAGIC 5. Privilege escalation / IAM abuse (AccessDenied/UnauthorizedException)

# COMMAND ----------

# MAGIC %pip install dbldatagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Configuration Parameters
# MAGIC
# MAGIC This notebook uses **Databricks Widgets** to make it flexible and reusable. You can customize the following parameters:
# MAGIC
# MAGIC - **`catalog_name`**: The Unity Catalog name where tables will be created (default: `aa_catalog`)
# MAGIC - **`schema_name`**: The schema/database name within the catalog (default: `datagen`)
# MAGIC - **`base_rows`**: Number of records to generate per security scenario (default: `10000`)
# MAGIC - **`partitions`**: Number of data partitions/files to generate - controls parallelism (default: `4`)
# MAGIC
# MAGIC ### Impact:
# MAGIC - All Delta tables will be saved to: `{catalog_name}.{schema_name}.ctrail_*`
# MAGIC - JSON exports will be saved to: `/Volumes/{catalog_name}/{schema_name}/cloudtrails/`
# MAGIC - Total records generated: `{base_rows} × 5 scenarios`
# MAGIC - Data parallelism: Higher partition count = more parallel processing but more output files
# MAGIC
# MAGIC You can modify these values using the widgets at the top of the notebook.
# MAGIC

# COMMAND ----------

# Configure notebook parameters using widgets
# These widgets allow you to customize the notebook execution without modifying code

# Widget 1: Unity Catalog name - defines where Delta tables are stored
dbutils.widgets.text("catalog_name", "aa_catalog", "Catalog Name")

# Widget 2: Schema/database name - organizes tables within the catalog
dbutils.widgets.text("schema_name", "datagen", "Schema Name")

# Widget 3: Number of records per scenario - controls data volume (e.g., 10000 = 50K total records for 5 scenarios)
dbutils.widgets.text("base_rows", "10000", "Number of Records per Scenario")

# Widget 4: Number of partitions - controls parallelism and output file count
dbutils.widgets.text("partitions", "4", "Number of data partitions / files to generate")


# COMMAND ----------

# Retrieve widget values and convert to appropriate types
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
base_rows = int(dbutils.widgets.get("base_rows"))  # Convert to integer for numeric operations
partitions = int(dbutils.widgets.get("partitions"))  # Convert to integer for Spark partitioning

# COMMAND ----------

# List all widget names and their values
widgets = dbutils.widgets.getAll()

# Print the variables to verify
for name, value in widgets.items():
    print(f"{name}: {eval(name)}")


# COMMAND ----------

from dbldatagen import DataGenerator
from pyspark.sql.functions import col, struct, when, lit, array, expr, from_json, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mock IP Geolocation Lookup Table

# COMMAND ----------

# Create IP geolocation mock data
ip_geo_data = [
    # Normal region IPs
    ("192.168.1.*", "Region Alpha", "Alpha District", "Northport", 37.7749, -122.4194, "normal"),
    ("10.0.0.*", "Region Alpha", "Alpha District", "Westbridge", 47.6062, -122.3321, "normal"),
    ("172.16.0.*", "Region Alpha", "Alpha District", "Centralview", 40.7128, -74.0060, "normal"),
    # Suspicious region IPs (fictitious locations)
    ("203.0.113.*", "Region Beta", "Beta District", "Shadowmere", 55.7558, 37.6173, "suspicious"),
    ("198.51.100.*", "Region Beta", "Beta District", "Darkwater", 39.9042, 116.4074, "suspicious"),
    ("45.33.32.*", "Region Beta", "Beta District", "Greystone", 39.0392, 125.7625, "suspicious"),
    ("185.220.101.*", "Region Beta", "Beta District", "Phantom Bay", 35.6892, 51.3890, "suspicious"),
    ("91.108.56.*", "Region Beta", "Beta District", "Nebula City", 50.4501, 30.5234, "suspicious"),
    # More suspicious locations
    ("104.244.42.*", "Region Gamma", "Gamma District", "Voidhaven", 6.5244, 3.3792, "suspicious"),
    ("200.152.38.*", "Region Gamma", "Gamma District", "Twilight Zone", -23.5505, -46.6333, "suspicious"),
]

ip_geo_df = spark.createDataFrame(ip_geo_data, ["ip_pattern", "country", "region", "city", "latitude", "longitude", "risk_level"])
ip_geo_df.createOrReplaceTempView("ip_geolocation")

display(ip_geo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1: High Frequency Login Attempts from Same IP

# COMMAND ----------

def generate_high_frequency_login_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Multiple login attempts from same IPs (brute force)
    - 80% of logins from just 5 suspicious IPs
    - Rapid succession (within minutes)
    - Mix of success and failures
    """
    
    # Suspicious IPs doing brute force
    suspicious_ips = ["203.0.113.42", "198.51.100.88", "45.33.32.156", "185.220.101.23", "91.108.56.99"]
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("eventVersion", "string", values=["1.11"])
        .withColumn("eventTime", "string", 
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 0.5 as int)), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")") # Within 12 hours
        .withColumn("awsRegion", "string", values=["us-east-1"])
        .withColumn("eventSource", "string", values=["signin.amazonaws.com"])
        .withColumn("eventName", "string", values=["ConsoleLogin"])
        
        # 80% from suspicious IPs, 20% normal
        .withColumn("sourceIPAddress", "string", 
                   expr=f"case when rand() < 0.8 then " +
                        f"case cast(rand() * {len(suspicious_ips)} as int) " +
                        f"when 0 then '{suspicious_ips[0]}' " +
                        f"when 1 then '{suspicious_ips[1]}' " +
                        f"when 2 then '{suspicious_ips[2]}' " +
                        f"when 3 then '{suspicious_ips[3]}' " +
                        f"else '{suspicious_ips[4]}' end " +
                        f"else concat('192.168.1.', cast(rand() * 254 + 1 as int)) end")
        
        .withColumn("userAgent", "string", values=["Mozilla/5.0", "aws-cli/2.0.0"])
        
        # 60% failed, 40% success for brute force attempts
        .withColumn("errorCode", "string",
                   expr="case when rand() < 0.6 then 'AccessDenied' else null end")
        .withColumn("errorMessage", "string",
                   expr="case when errorCode is not null then 'Invalid username or password' else null end")
        
        .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
        .withColumn("eventID", "string", expr="concat(substring(md5(cast(rand() as string)), 1, 36))")
        .withColumn("readOnly", "boolean", values=[False])
        .withColumn("managementEvent", "boolean", values=[True])
        .withColumn("eventType", "string", values=["AwsConsoleSignIn"])
        .withColumn("eventCategory", "string", values=["Management"])
        .withColumn("recipientAccountId", "string", values=["123456789012"])
        .withColumn("sessionCredentialFromConsole", "string", values=["true"])
        
        .withColumn("_userName", "string", 
                   expr="case cast(rand() * 5 as int) when 0 then 'admin' when 1 then 'root' when 2 then 'administrator' when 3 then 'user' else 'test' end")
        .withColumn("_principalId", "string", expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')")
    )
    
    return data_gen.build()

# Generate high frequency login attempts from same IPs (brute force pattern)
high_frequency_login_logs = generate_high_frequency_login_scenario(spark, base_rows, partitions)
display(high_frequency_login_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Large Number of Failed Login Attempts

# COMMAND ----------

def generate_failed_login_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Credential stuffing / password spraying
    - 95% failure rate
    - Multiple usernames tried
    - AccessDenied errors
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("eventVersion", "string", values=["1.11"])
        .withColumn("eventTime", "string", 
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 1 as int)), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")")
        .withColumn("awsRegion", "string", values=["us-east-1"])
        .withColumn("eventSource", "string", values=["signin.amazonaws.com", "iam.amazonaws.com"])
        .withColumn("eventName", "string", 
                   expr="case eventSource when 'signin.amazonaws.com' then 'ConsoleLogin' else 'GetUser' end")
        
        .withColumn("sourceIPAddress", "string", 
                   expr="concat(case cast(rand() * 3 as int) when 0 then '203.0.113.' when 1 then '198.51.100.' else '45.33.32.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("userAgent", "string", values=["Mozilla/5.0", "Python-urllib/3.8", "aws-cli/2.0.0"])
        
        # 95% failed
        .withColumn("errorCode", "string",
                   expr="case when rand() < 0.95 then case cast(rand() * 2 as int) when 0 then 'AccessDenied' else 'UnauthorizedOperation' end else null end")
        .withColumn("errorMessage", "string",
                   expr="case errorCode when 'AccessDenied' then 'User is not authorized to perform this action' when 'UnauthorizedOperation' then 'Invalid credentials' else null end")
        
        .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
        .withColumn("eventID", "string", expr="concat(substring(md5(cast(rand() as string)), 1, 36))")
        .withColumn("readOnly", "boolean", values=[False])
        .withColumn("managementEvent", "boolean", values=[True])
        .withColumn("eventType", "string", values=["AwsApiCall", "AwsConsoleSignIn"])
        .withColumn("eventCategory", "string", values=["Management"])
        .withColumn("recipientAccountId", "string", values=["123456789012"])
        .withColumn("sessionCredentialFromConsole", "string", values=["true"])
        
        # Try many different usernames
        .withColumn("_userName", "string", 
                   expr="concat('user', cast(rand() * 1000 as int))")
        .withColumn("_principalId", "string", expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')")
    )
    
    return data_gen.build()

# Generate large number of failed login attempts (credential stuffing attacks)
failed_login_logs = generate_failed_login_scenario(spark, base_rows, partitions)
display(failed_login_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Unusual Geographies

# COMMAND ----------

def generate_unusual_geography_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Access from suspicious countries
    - 70% from high-risk countries
    - Map IPs to geographic locations
    """
    
    suspicious_countries = {
        "Russia": "203.0.113.",
        "China": "198.51.100.",
        "North Korea": "45.33.32.",
        "Iran": "185.220.101.",
        "Nigeria": "104.244.42."
    }
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("eventVersion", "string", values=["1.11"])
        .withColumn("eventTime", "string", 
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 3 as int)), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")")
        .withColumn("awsRegion", "string", values=["us-east-1", "us-west-2", "eu-west-1"])
        .withColumn("eventSource", "string", 
                   values=["s3.amazonaws.com", "ec2.amazonaws.com", "iam.amazonaws.com", "dynamodb.amazonaws.com"])
        .withColumn("eventName", "string", 
                   expr="case eventSource " +
                        "when 's3.amazonaws.com' then case cast(rand() * 3 as int) when 0 then 'GetObject' when 1 then 'PutObject' else 'ListBucket' end " +
                        "when 'ec2.amazonaws.com' then case cast(rand() * 2 as int) when 0 then 'DescribeInstances' else 'RunInstances' end " +
                        "when 'iam.amazonaws.com' then case cast(rand() * 3 as int) when 0 then 'ListUsers' when 1 then 'GetUser' else 'CreateAccessKey' end " +
                        "else 'Query' end")
        
        # 70% from suspicious countries
        .withColumn("sourceIPAddress", "string", 
                   expr="case when rand() < 0.7 then " +
                        "concat(case cast(rand() * 5 as int) when 0 then '203.0.113.' when 1 then '198.51.100.' when 2 then '45.33.32.' when 3 then '185.220.101.' else '104.244.42.' end, cast(rand() * 254 + 1 as int)) " +
                        "else concat('192.168.1.', cast(rand() * 254 + 1 as int)) end")
        
        .withColumn("userAgent", "string", 
                   values=["aws-cli/2.0.0", "aws-sdk-python/1.26.0", "Mozilla/5.0", "curl/7.68.0"])
        
        .withColumn("errorCode", "string",
                   expr="case when rand() < 0.4 then case cast(rand() * 3 as int) when 0 then 'AccessDenied' when 1 then 'UnauthorizedOperation' else 'InvalidParameter' end else null end")
        .withColumn("errorMessage", "string",
                   expr="case when errorCode is not null then 'Operation failed' else null end")
        
        .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
        .withColumn("eventID", "string", expr="concat(substring(md5(cast(rand() as string)), 1, 36))")
        .withColumn("readOnly", "boolean", 
                   expr="case when eventName like 'Get%' or eventName like 'List%' or eventName like 'Describe%' then true else false end")
        .withColumn("managementEvent", "boolean", values=[True])
        .withColumn("eventType", "string", values=["AwsApiCall"])
        .withColumn("eventCategory", "string", values=["Management"])
        .withColumn("recipientAccountId", "string", values=["123456789012"])
        .withColumn("sessionCredentialFromConsole", "string", values=["true"])
        
        .withColumn("_userName", "string", 
                   expr="case cast(rand() * 4 as int) when 0 then 'Mary' when 1 then 'Paulo' when 2 then 'Alice' else 'Bob' end")
        .withColumn("_principalId", "string", expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')")
    )
    
    df = data_gen.build()
    
    # Add geolocation data
    df_with_geo = df.withColumn("ip_prefix", expr("concat(split(sourceIPAddress, '\\\\.')[0], '.', split(sourceIPAddress, '\\\\.')[1], '.', split(sourceIPAddress, '\\\\.')[2], '.*')"))
    
    df_with_geo = df_with_geo.join(
        spark.table("ip_geolocation"),
        df_with_geo.ip_prefix == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern")
    
    return df_with_geo

# Generate access from unusual geographies (suspicious countries and locations)
unusual_geography_logs = generate_unusual_geography_scenario(spark, base_rows, partitions)
display(unusual_geography_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: High Rate of Error Codes

# COMMAND ----------

def generate_high_error_rate_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: System under attack or misconfiguration
    - 85% error rate
    - Various error types
    - Multiple services affected
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("eventVersion", "string", values=["1.11"])
        .withColumn("eventTime", "string", 
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 2 as int)), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")")
        .withColumn("awsRegion", "string", 
                   values=["us-east-1", "us-west-2", "eu-west-1"])
        .withColumn("eventSource", "string", 
                   values=["s3.amazonaws.com", "ec2.amazonaws.com", "iam.amazonaws.com", "lambda.amazonaws.com", "dynamodb.amazonaws.com"])
        .withColumn("eventName", "string", 
                   expr="case eventSource " +
                        "when 's3.amazonaws.com' then case cast(rand() * 4 as int) when 0 then 'GetObject' when 1 then 'PutObject' when 2 then 'DeleteObject' else 'ListBucket' end " +
                        "when 'ec2.amazonaws.com' then case cast(rand() * 3 as int) when 0 then 'RunInstances' when 1 then 'TerminateInstances' else 'StopInstances' end " +
                        "when 'iam.amazonaws.com' then case cast(rand() * 3 as int) when 0 then 'CreateUser' when 1 then 'DeleteUser' else 'AttachUserPolicy' end " +
                        "when 'lambda.amazonaws.com' then case cast(rand() * 2 as int) when 0 then 'InvokeFunction' else 'CreateFunction' end " +
                        "else 'PutItem' end")
        
        .withColumn("sourceIPAddress", "string", 
                   expr="concat(case cast(rand() * 3 as int) when 0 then '192.168.1.' when 1 then '203.0.113.' else '198.51.100.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("userAgent", "string", 
                   values=["aws-cli/2.0.0", "aws-sdk-python/1.26.0", "boto3/1.20.0", "aws-sdk-java/1.12.0"])
        
        # 85% errors with various types
        .withColumn("errorCode", "string",
                   expr="case when rand() < 0.85 then " +
                        "case cast(rand() * 6 as int) when 0 then 'AccessDenied' when 1 then 'UnauthorizedOperation' when 2 then 'InvalidParameter' when 3 then 'ThrottlingException' when 4 then 'ResourceNotFoundException' else 'ServiceUnavailable' end " +
                        "else null end")
        .withColumn("errorMessage", "string",
                   expr="case errorCode " +
                        "when 'AccessDenied' then 'User is not authorized to perform this action' " +
                        "when 'UnauthorizedOperation' then 'You are not authorized to perform this operation' " +
                        "when 'InvalidParameter' then 'Invalid parameter value provided' " +
                        "when 'ThrottlingException' then 'Rate exceeded' " +
                        "when 'ResourceNotFoundException' then 'The specified resource does not exist' " +
                        "when 'ServiceUnavailable' then 'Service is temporarily unavailable' " +
                        "else null end")
        
        .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
        .withColumn("eventID", "string", expr="concat(substring(md5(cast(rand() as string)), 1, 36))")
        .withColumn("readOnly", "boolean", 
                   expr="case when eventName like 'Get%' or eventName like 'List%' or eventName like 'Describe%' then true else false end")
        .withColumn("managementEvent", "boolean", values=[True])
        .withColumn("eventType", "string", values=["AwsApiCall"])
        .withColumn("eventCategory", "string", values=["Management"])
        .withColumn("recipientAccountId", "string", values=["123456789012"])
        .withColumn("sessionCredentialFromConsole", "string", values=["true"])
        
        .withColumn("_userName", "string", 
                   expr="case cast(rand() * 6 as int) when 0 then 'Mary' when 1 then 'Paulo' when 2 then 'Alice' when 3 then 'Bob' when 4 then 'Charlie' else 'David' end")
        .withColumn("_principalId", "string", expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')")
    )
    
    return data_gen.build()

# Generate high rate of API errors (throttling, permissions issues, service failures)
high_error_rate_logs = generate_high_error_rate_scenario(spark, base_rows, partitions)
display(high_error_rate_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Privilege Escalation & IAM Abuse

# COMMAND ----------

def generate_privilege_escalation_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Privilege escalation and IAM abuse attempts
    - Focus on IAM operations
    - High rate of AccessDenied and UnauthorizedOperation
    - Attempts to create keys, attach policies, escalate privileges
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("eventVersion", "string", values=["1.11"])
        .withColumn("eventTime", "string", 
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 1 as int)), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")")
        .withColumn("awsRegion", "string", values=["us-east-1"]) # IAM is global
        .withColumn("eventSource", "string", values=["iam.amazonaws.com", "sts.amazonaws.com"])
        
        # Suspicious IAM operations
        .withColumn("eventName", "string", 
                   expr="case eventSource " +
                        "when 'iam.amazonaws.com' then case cast(rand() * 11 as int) " +
                        "when 0 then 'CreateAccessKey' when 1 then 'CreateUser' when 2 then 'AttachUserPolicy' " +
                        "when 3 then 'AttachRolePolicy' when 4 then 'PutUserPolicy' when 5 then 'PutRolePolicy' " +
                        "when 6 then 'CreatePolicyVersion' when 7 then 'SetDefaultPolicyVersion' when 8 then 'UpdateAssumeRolePolicy' " +
                        "when 9 then 'AddUserToGroup' else 'CreateRole' end " +
                        "else case cast(rand() * 2 as int) when 0 then 'AssumeRole' else 'GetSessionToken' end end")
        
        .withColumn("sourceIPAddress", "string", 
                   expr="concat(case cast(rand() * 3 as int) when 0 then '203.0.113.' when 1 then '198.51.100.' else '192.168.1.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("userAgent", "string", 
                   values=["aws-cli/2.0.0", "aws-sdk-python/1.26.0", "Boto3/1.20.0", "aws-cli/1.18.0"])
        
        # Mix of normal users and potentially compromised accounts (define BEFORE errorMessage)
        .withColumn("_userName", "string", 
                   expr="case cast(rand() * 8 as int) " +
                        "when 0 then 'developer1' " +
                        "when 1 then 'contractor' " +
                        "when 2 then 'temp_user' " +
                        "when 3 then 'service_account' " +
                        "when 4 then 'admin_backup' " +
                        "when 5 then 'test_user' " +
                        "when 6 then 'Mary' " +
                        "else 'Bob' end")
        .withColumn("_principalId", "string", expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')")
        
        # 75% AccessDenied or UnauthorizedOperation (privilege escalation blocked)
        .withColumn("errorCode", "string",
                   expr="case when rand() < 0.75 then case cast(rand() * 2 as int) when 0 then 'AccessDenied' else 'UnauthorizedOperation' end else null end")
        .withColumn("errorMessage", "string",
                   expr="case errorCode " +
                        "when 'AccessDenied' then concat('User: arn:aws:iam::123456789012:user/', _userName, ' is not authorized to perform: iam:', eventName) " +
                        "when 'UnauthorizedOperation' then 'You are not authorized to perform this operation' " +
                        "else null end")
        
        .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
        .withColumn("eventID", "string", expr="concat(substring(md5(cast(rand() as string)), 1, 36))")
        .withColumn("readOnly", "boolean", values=[False]) # All write operations
        .withColumn("managementEvent", "boolean", values=[True])
        .withColumn("eventType", "string", values=["AwsApiCall"])
        .withColumn("eventCategory", "string", values=["Management"])
        .withColumn("recipientAccountId", "string", values=["123456789012"])
        .withColumn("sessionCredentialFromConsole", "string", values=["true"])
        
        # Target resources for privilege escalation
        .withColumn("_targetUser", "string",
                   expr="case cast(rand() * 4 as int) when 0 then 'admin' when 1 then 'root' when 2 then 'superuser' else 'privileged_user' end")
        .withColumn("_targetPolicy", "string",
                   expr="case cast(rand() * 3 as int) when 0 then 'AdministratorAccess' when 1 then 'PowerUserAccess' else 'IAMFullAccess' end")
    )
    
    return data_gen.build()

# Generate privilege escalation and IAM abuse attempts (unauthorized admin operations)
privilege_escalation_logs = generate_privilege_escalation_scenario(spark, base_rows, partitions)
display(privilege_escalation_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Struct Fields to All Scenarios

# COMMAND ----------

def add_cloudtrail_structs(df):
    """Add proper CloudTrail struct fields"""
    
    request_params_schema = StructType([
        StructField("userName", StringType(), True),
        StructField("policyArn", StringType(), True),
        StructField("targetUser", StringType(), True),
    ])
    
    df_final = df.select(
        col("*"),
        
        # userIdentity struct
        struct(
            lit("IAMUser").alias("type"),
            col("_principalId").alias("principalId"),
            expr("concat('arn:aws:iam::123456789012:user/', _userName)").alias("arn"),
            lit("123456789012").alias("accountId"),
            lit("EXAMPLEACCESSKEY123456").alias("accessKeyId"),
            col("_userName").alias("userName"),
            struct(
                struct(
                    expr("date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")").alias("creationDate"),
                    lit("false").alias("mfaAuthenticated")
                ).alias("attributes")
            ).alias("sessionContext")
        ).alias("userIdentity"),
        
        # tlsDetails struct
        struct(
            lit("TLSv1.2").alias("tlsVersion"),
            lit("ECDHE-RSA-AES128-GCM-SHA256").alias("cipherSuite"),
            when(col("eventSource").contains("s3"), lit("s3.amazonaws.com"))
                .when(col("eventSource").contains("iam"), lit("iam.amazonaws.com"))
                .when(col("eventSource").contains("ec2"), lit("ec2.amazonaws.com"))
                .when(col("eventSource").contains("signin"), lit("signin.amazonaws.com"))
                .otherwise(col("eventSource")).alias("clientProvidedHostHeader")
        ).alias("tlsDetails")
    ).drop("_userName", "_principalId", "_targetUser", "_targetPolicy")
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save All Scenarios to Delta Tables

# COMMAND ----------

# Transform raw logs into proper CloudTrail schema with nested structures
high_frequency_login_with_structs = add_cloudtrail_structs(high_frequency_login_logs)
failed_login_with_structs = add_cloudtrail_structs(failed_login_logs)
unusual_geography_with_structs = add_cloudtrail_structs(unusual_geography_logs)
high_error_rate_with_structs = add_cloudtrail_structs(high_error_rate_logs)
privilege_escalation_with_structs = add_cloudtrail_structs(privilege_escalation_logs)

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Save Scenario 1: High Frequency Login Attempts
high_frequency_login_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.ctrail_high_frequency_login")

# COMMAND ----------

# Save Scenario 2: Failed Login Attempts
failed_login_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.ctrail_failed_logins")

# COMMAND ----------

# Save Scenario 3: Unusual Geographies
unusual_geography_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.ctrail_unusual_geo")

# COMMAND ----------

# Save Scenario 4: High Error Rate
high_error_rate_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.ctrail_high_errors")

# COMMAND ----------

# Save Scenario 5: Privilege Escalation & IAM Abuse
privilege_escalation_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.ctrail_privilege_escalation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

## Scenario 1: High Frequency Login Attempts
display(spark.sql(f"""SELECT 
  'High Frequency Login' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT sourceIPAddress) as unique_ips,
  SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) as failed_attempts,
  ROUND(100.0 * SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM {catalog_name}.{schema_name}.ctrail_high_frequency_login"""))

# COMMAND ----------

## Scenario 2: Failed Logins
display(spark.sql(f"""SELECT 
  'Failed Logins' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT userIdentity.userName) as unique_users_tried,
  SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) as failed_attempts,
  ROUND(100.0 * SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM {catalog_name}.{schema_name}.ctrail_failed_logins"""))

# COMMAND ----------

## Scenario 3: Unusual Geographies
display(spark.sql(f"""SELECT 
  'Unusual Geography' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT country) as unique_countries,
  SUM(CASE WHEN risk_level = 'suspicious' THEN 1 ELSE 0 END) as suspicious_country_access,
  ROUND(100.0 * SUM(CASE WHEN risk_level = 'suspicious' THEN 1 ELSE 0 END) / COUNT(*), 2) as suspicious_pct
    FROM {catalog_name}.{schema_name}.ctrail_unusual_geo"""))

# COMMAND ----------

## Scenario 4: High Errors
display(spark.sql(f"""SELECT 
  'High Error Rate' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT errorCode) as unique_error_types,
  SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) as total_errors,
  ROUND(100.0 * SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate_pct
FROM {catalog_name}.{schema_name}.ctrail_high_errors"""))

# COMMAND ----------

## Scenario 5: Privilege Escalation
display(spark.sql(f"""SELECT 
  'Privilege Escalation' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT eventName) as unique_operations,
  SUM(CASE WHEN errorCode IN ('AccessDenied', 'UnauthorizedOperation') THEN 1 ELSE 0 END) as blocked_attempts,
  ROUND(100.0 * SUM(CASE WHEN errorCode IN ('AccessDenied', 'UnauthorizedOperation') THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.ctrail_privilege_escalation"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Complete!
# MAGIC
# MAGIC Generated 5 security scenarios:
# MAGIC
# MAGIC 1. **ctrail_high_frequency_login** - Brute force login attempts
# MAGIC 2. **ctrail_failed_logins** - Credential stuffing
# MAGIC 3. **ctrail_unusual_geo** - Access from suspicious countries (with geolocation)
# MAGIC 4. **ctrail_high_errors** - System under attack / misconfiguration
# MAGIC 5. **ctrail_privilege_escalation** - IAM abuse and privilege escalation attempts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON Files (CloudTrail Format)
# MAGIC
# MAGIC Export each table to a Volume as JSON files in CloudTrail format

# COMMAND ----------

# Base path for JSON exports
base_path = f"/Volumes/{catalog_name}/{schema_name}/cloudtrails/"

# Table configurations
tables = [
    (f"{catalog_name}.{schema_name}.ctrail_high_frequency_login", "high_frequency_login"),
    (f"{catalog_name}.{schema_name}.ctrail_failed_logins", "failed_logins"),
    (f"{catalog_name}.{schema_name}.ctrail_unusual_geo", "unusual_geo"),
    (f"{catalog_name}.{schema_name}.ctrail_high_errors", "high_errors"),
    (f"{catalog_name}.{schema_name}.ctrail_privilege_escalation", "privilege_escalation")
]

print("Exporting CloudTrail data to JSON files...")
print("=" * 60)

for table_name, file_name in tables:
    output_path = f"{base_path}{file_name}"
    print(f"\n📄 Exporting: {table_name}")
    print(f"   → {output_path}")
    
    # Read table
    df = spark.table(table_name)
    
    # Write as JSON
    df.write.format("json").mode("overwrite").save(output_path)
    
    print(f"   ✅ Exported successfully!")

print("\n" + "=" * 60)
print("✅ All exports complete!")
print("\nJSON files available at:")
for _, file_name in tables:
    print(f"  - {base_path}{file_name}/")
print("\nThese JSON files can be used with:")
print("  • AWS Athena / Glue")
print("  • Security analytics tools")
print("  • ML model training")
print("  • CloudTrail log analysis tools")


# COMMAND ----------


