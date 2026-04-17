# Databricks notebook source
# MAGIC %md
# MAGIC # Microsoft Entra ID Security Scenarios
# MAGIC
# MAGIC Generates records for each security scenario:
# MAGIC 1. Brute force sign-in attempts
# MAGIC 2. Impossible travel (geographically distant sign-ins)
# MAGIC 3. Risky sign-ins (high risk levels)
# MAGIC 4. Failed MFA attempts
# MAGIC 5. Privilege escalation (role assignments, admin operations)
# MAGIC 6. Guest user abuse
# MAGIC
# MAGIC Based on Microsoft Entra ID (Azure AD) log schema for Sign-in and Audit logs

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
# MAGIC - All Delta tables will be saved to: `{catalog_name}.{schema_name}.entra_*`
# MAGIC - JSON exports will be saved to: `/Volumes/{catalog_name}/{schema_name}/entra_id/`
# MAGIC - Total records generated: `{base_rows} × 6 scenarios`
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

# Widget 3: Number of records per scenario - controls data volume (e.g., 10000 = 60K total records for 6 scenarios)
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
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, DoubleType
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mock Geolocation Data for Entra ID

# COMMAND ----------

# Create geolocation data for sign-ins
geo_locations = [
    # Normal locations
    ("192.168.1.*", "Northport", "Alpha District", "ZZ", 37.7749, -122.4194, "normal", "low"),
    ("10.0.0.*", "Westbridge", "Alpha District", "ZZ", 47.6062, -122.3321, "normal", "low"),
    ("172.16.0.*", "Centralview", "Alpha District", "ZZ", 40.7128, -74.0060, "normal", "low"),
    ("198.51.100.*", "Eastshore", "Alpha District", "ZZ", 41.8781, -87.6298, "normal", "low"),
    # High-risk locations (fictitious)
    ("203.0.113.*", "Shadowmere", "Beta District", "XX", 55.7558, 37.6173, "suspicious", "high"),
    ("45.33.32.*", "Darkwater", "Beta District", "XX", 39.9042, 116.4074, "suspicious", "high"),
    ("185.220.101.*", "Greystone", "Beta District", "XX", 35.6892, 51.3890, "suspicious", "high"),
    ("91.108.56.*", "Nebula City", "Beta District", "XX", 50.4501, 30.5234, "suspicious", "medium"),
    ("104.244.42.*", "Voidhaven", "Gamma District", "YY", 6.5244, 3.3792, "suspicious", "medium"),
    ("200.152.38.*", "Twilight Zone", "Gamma District", "YY", -23.5505, -46.6333, "suspicious", "medium"),
]

geo_df = spark.createDataFrame(geo_locations, ["ip_pattern", "city", "state", "countryOrRegion", "latitude", "longitude", "risk_category", "risk_level"])
geo_df.createOrReplaceTempView("entra_geolocation")

display(geo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1: Brute Force Sign-in Attempts

# COMMAND ----------

def generate_brute_force_signin_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Brute force password attacks
    - 80% from 5 suspicious IPs
    - 85% failure rate
    - Rapid succession
    - Multiple usernames attempted
    """
    
    suspicious_ips = ["203.0.113.42", "198.51.100.88", "45.33.32.156", "185.220.101.23", "91.108.56.99"]
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("time", "string", 
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 0.5 as int)), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")")
        .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
        .withColumn("operationName", "string", values=["Sign-in activity"])
        .withColumn("category", "string", values=["SignInLogs"])
        
        # 85% failures
        .withColumn("resultType", "string",
                   expr="case when rand() < 0.85 then 'Failure' else 'Success' end")
        .withColumn("resultDescription", "string",
                   expr="case when resultType = 'Success' then 'Sign-in successful' else 'Invalid username or password' end")
        
        .withColumn("durationMs", "integer", expr="cast(rand() * 2000 as int)")
        
        # 80% from suspicious IPs
        .withColumn("callerIpAddress", "string",
                   expr=f"case when rand() < 0.8 then " +
                        f"case cast(rand() * {len(suspicious_ips)} as int) " +
                        f"when 0 then '{suspicious_ips[0]}' " +
                        f"when 1 then '{suspicious_ips[1]}' " +
                        f"when 2 then '{suspicious_ips[2]}' " +
                        f"when 3 then '{suspicious_ips[3]}' " +
                        f"else '{suspicious_ips[4]}' end " +
                        f"else concat('192.168.1.', cast(rand() * 254 + 1 as int)) end")
        
        .withColumn("correlationId", "string", expr="uuid()")
        
        # Multiple usernames (brute force pattern)
        .withColumn("_userPrincipalName", "string",
                   expr="concat('user', cast(rand() * 500 as int), '@contoso.com')")
        .withColumn("_userDisplayName", "string",
                   expr="concat('User ', cast(rand() * 500 as int))")
        .withColumn("_userId", "string", expr="uuid()")
        
        .withColumn("_appDisplayName", "string", values=["Microsoft Azure Portal", "Office 365", "Microsoft Teams"])
        .withColumn("_appId", "string", expr="uuid()")
        .withColumn("_clientAppUsed", "string", values=["Browser", "Mobile Apps and Desktop clients", "Exchange ActiveSync"])
        
        .withColumn("_conditionalAccessStatus", "string",
                   expr="case when resultType = 'Success' then 'success' when rand() < 0.5 then 'notApplied' else 'failure' end")
        
        # Device details
        .withColumn("_deviceOS", "string", 
                   expr="case cast(rand() * 5 as int) when 0 then 'Windows 10' when 1 then 'Windows 11' when 2 then 'MacOS' when 3 then 'iOS' else 'Android' end")
        .withColumn("_browser", "string",
                   expr="case cast(rand() * 4 as int) when 0 then 'Chrome' when 1 then 'Edge' when 2 then 'Firefox' else 'Safari' end")
        .withColumn("_deviceId", "string", expr="uuid()")
        
        # Risk indicators (higher for failed attempts)
        .withColumn("_riskDetail", "string",
                   expr="case when resultType = 'Failure' then case cast(rand() * 3 as int) when 0 then 'userPassedMFADrivenByRiskBasedPolicy' when 1 then 'adminGeneratedTemporaryPassword' else 'none' end else 'none' end")
        .withColumn("_riskLevelAggregated", "string",
                   expr="case when resultType = 'Failure' and rand() < 0.6 then 'high' when resultType = 'Failure' then 'medium' else 'low' end")
        .withColumn("_riskState", "string",
                   expr="case when _riskLevelAggregated = 'high' then 'atRisk' when _riskLevelAggregated = 'medium' then 'confirmedCompromised' else 'none' end")
        
        .withColumn("_errorCode", "integer",
                   expr="case when resultType = 'Failure' then case cast(rand() * 3 as int) when 0 then 50126 when 1 then 50053 else 50055 end else 0 end")
        .withColumn("_failureReason", "string",
                   expr="case _errorCode when 50126 then 'Invalid username or password' when 50053 then 'Account is locked' when 50055 then 'Password expired' else null end")
    )
    
    return data_gen.build()

# Generate brute force sign-in attempts (high failure rate from suspicious IPs)
brute_force_signin_logs = generate_brute_force_signin_scenario(spark, base_rows, partitions)
display(brute_force_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Impossible Travel

# COMMAND ----------

def generate_impossible_travel_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Impossible travel - sign-ins from geographically distant locations
    - Same user signs in from Russia, then US within minutes
    - High risk indicators
    - Mix of success and failures
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("time", "string",
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 2 as int)), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")")
        .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
        .withColumn("operationName", "string", values=["Sign-in activity"])
        .withColumn("category", "string", values=["SignInLogs"])
        
        # 70% success (impossible travel detected but allowed)
        .withColumn("resultType", "string",
                   expr="case when rand() < 0.7 then 'Success' else 'Failure' end")
        .withColumn("resultDescription", "string",
                   expr="case when resultType = 'Success' then 'Sign-in successful' else 'Suspicious activity detected' end")
        
        .withColumn("durationMs", "integer", expr="cast(rand() * 3000 as int)")
        
        # Mix of distant locations
        .withColumn("callerIpAddress", "string",
                   expr="concat(case cast(rand() * 6 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '45.33.32.' " +
                        "when 2 then '185.220.101.' " +
                        "when 3 then '192.168.1.' " +
                        "when 4 then '198.51.100.' " +
                        "else '10.0.0.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("correlationId", "string", expr="uuid()")
        
        # Same users appearing from different locations
        .withColumn("_userPrincipalName", "string",
                   expr="concat('executive', cast(rand() * 50 as int), '@contoso.com')")
        .withColumn("_userDisplayName", "string",
                   expr="concat('Executive ', cast(rand() * 50 as int))")
        .withColumn("_userId", "string", expr="uuid()")
        
        .withColumn("_appDisplayName", "string", values=["Microsoft Azure Portal", "Office 365", "Outlook", "OneDrive"])
        .withColumn("_appId", "string", expr="uuid()")
        .withColumn("_clientAppUsed", "string", values=["Browser", "Mobile Apps and Desktop clients"])
        
        .withColumn("_conditionalAccessStatus", "string", values=["success", "notApplied"])
        
        .withColumn("_deviceOS", "string",
                   expr="case cast(rand() * 4 as int) when 0 then 'Windows 10' when 1 then 'MacOS' when 2 then 'iOS' else 'Android' end")
        .withColumn("_browser", "string", values=["Chrome", "Edge", "Safari", "Firefox"])
        .withColumn("_deviceId", "string", expr="uuid()")
        
        # HIGH risk for impossible travel
        .withColumn("_riskDetail", "string", values=["impossibleTravel", "anonymizedIPAddress", "unfamiliarFeatures"])
        .withColumn("_riskLevelAggregated", "string",
                   expr="case when rand() < 0.8 then 'high' else 'medium' end")
        .withColumn("_riskState", "string", values=["atRisk", "confirmedCompromised"])
        
        .withColumn("_errorCode", "integer",
                   expr="case when resultType = 'Failure' then 50058 else 0 end")
        .withColumn("_failureReason", "string",
                   expr="case when resultType = 'Failure' then 'Suspicious activity detected - impossible travel' else null end")
    )
    
    df = data_gen.build()
    
    # Add geolocation
    df_with_geo = df.withColumn("ip_prefix", expr("concat(split(callerIpAddress, '\\\\.')[0], '.', split(callerIpAddress, '\\\\.')[1], '.', split(callerIpAddress, '\\\\.')[2], '.*')"))
    
    df_with_geo = df_with_geo.join(
        spark.table("entra_geolocation"),
        df_with_geo.ip_prefix == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern")
    
    return df_with_geo

# Generate impossible travel scenario (rapid sign-ins from geographically distant locations)
impossible_travel_signin_logs = generate_impossible_travel_scenario(spark, base_rows, partitions)
display(impossible_travel_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Risky Sign-ins

# COMMAND ----------

def generate_risky_signin_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: High-risk sign-in attempts
    - Anonymous IPs (Tor, VPN)
    - Malware-linked IPs
    - Leaked credentials
    - 60% blocked by Conditional Access
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("time", "string",
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 1 as int)), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")")
        .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
        .withColumn("operationName", "string", values=["Sign-in activity"])
        .withColumn("category", "string", values=["SignInLogs"])
        
        # 60% failures (blocked by CA policies)
        .withColumn("resultType", "string",
                   expr="case when rand() < 0.6 then 'Failure' else 'Success' end")
        .withColumn("resultDescription", "string",
                   expr="case when resultType = 'Success' then 'Sign-in successful' else 'Blocked by Conditional Access policy' end")
        
        .withColumn("durationMs", "integer", expr="cast(rand() * 1500 as int)")
        
        # Suspicious IPs
        .withColumn("callerIpAddress", "string",
                   expr="concat(case cast(rand() * 5 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '45.33.32.' " +
                        "when 2 then '185.220.101.' " +
                        "when 3 then '91.108.56.' " +
                        "else '104.244.42.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("correlationId", "string", expr="uuid()")
        
        .withColumn("_userPrincipalName", "string",
                   expr="concat('user', cast(rand() * 200 as int), '@contoso.com')")
        .withColumn("_userDisplayName", "string",
                   expr="concat('User ', cast(rand() * 200 as int))")
        .withColumn("_userId", "string", expr="uuid()")
        
        .withColumn("_appDisplayName", "string",
                   values=["Microsoft Azure Portal", "Office 365", "SharePoint", "Exchange Online"])
        .withColumn("_appId", "string", expr="uuid()")
        .withColumn("_clientAppUsed", "string", values=["Browser", "Exchange ActiveSync", "Other clients"])
        
        # High rate of CA failures
        .withColumn("_conditionalAccessStatus", "string",
                   expr="case when resultType = 'Failure' then 'failure' when rand() < 0.3 then 'success' else 'notApplied' end")
        
        .withColumn("_deviceOS", "string", values=["Windows 10", "Linux", "Unknown"])
        .withColumn("_browser", "string", values=["Chrome", "Firefox", "Tor Browser", "Unknown"])
        .withColumn("_deviceId", "string", expr="uuid()")
        
        # HIGH risk indicators
        .withColumn("_riskDetail", "string",
                   expr="case cast(rand() * 5 as int) " +
                        "when 0 then 'anonymousIPAddress' " +
                        "when 1 then 'maliciousIPAddress' " +
                        "when 2 then 'leakedCredentials' " +
                        "when 3 then 'malwareInfectedIPAddress' " +
                        "else 'suspiciousIPAddress' end")
        .withColumn("_riskLevelAggregated", "string",
                   expr="case when rand() < 0.85 then 'high' else 'medium' end")
        .withColumn("_riskState", "string",
                   expr="case when rand() < 0.7 then 'atRisk' else 'confirmedCompromised' end")
        
        .withColumn("_errorCode", "integer",
                   expr="case when resultType = 'Failure' then case cast(rand() * 3 as int) when 0 then 53003 when 1 then 50058 else 50074 end else 0 end")
        .withColumn("_failureReason", "string",
                   expr="case _errorCode " +
                        "when 53003 then 'Blocked by Conditional Access policy - risky sign-in' " +
                        "when 50058 then 'Suspicious activity detected' " +
                        "when 50074 then 'Strong authentication required' " +
                        "else null end")
    )
    
    df = data_gen.build()
    
    # Add geolocation
    df_with_geo = df.withColumn("ip_prefix", expr("concat(split(callerIpAddress, '\\\\.')[0], '.', split(callerIpAddress, '\\\\.')[1], '.', split(callerIpAddress, '\\\\.')[2], '.*')"))
    
    df_with_geo = df_with_geo.join(
        spark.table("entra_geolocation"),
        df_with_geo.ip_prefix == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern")
    
    return df_with_geo

# Generate risky sign-ins (high risk levels, anonymous IPs, suspicious locations)
risky_signin_logs = generate_risky_signin_scenario(spark, base_rows, partitions)
display(risky_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: Failed MFA Attempts

# COMMAND ----------

def generate_failed_mfa_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Failed MFA attempts
    - 90% MFA failures
    - Multiple MFA methods tried
    - Legitimate users but compromised credentials
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("time", "string",
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 1 as int)), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")")
        .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
        .withColumn("operationName", "string", values=["Sign-in activity"])
        .withColumn("category", "string", values=["SignInLogs"])
        
        # 90% MFA failures
        .withColumn("resultType", "string",
                   expr="case when rand() < 0.9 then 'Failure' else 'Success' end")
        .withColumn("resultDescription", "string",
                   expr="case when resultType = 'Success' then 'MFA completed successfully' else 'MFA denied' end")
        
        .withColumn("durationMs", "integer", expr="cast(rand() * 5000 as int)")
        
        .withColumn("callerIpAddress", "string",
                   expr="concat(case cast(rand() * 4 as int) " +
                        "when 0 then '192.168.1.' " +
                        "when 1 then '10.0.0.' " +
                        "when 2 then '203.0.113.' " +
                        "else '198.51.100.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("correlationId", "string", expr="uuid()")
        
        .withColumn("_userPrincipalName", "string",
                   expr="concat('employee', cast(rand() * 300 as int), '@contoso.com')")
        .withColumn("_userDisplayName", "string",
                   expr="concat('Employee ', cast(rand() * 300 as int))")
        .withColumn("_userId", "string", expr="uuid()")
        
        .withColumn("_appDisplayName", "string", values=["Microsoft Azure Portal", "Office 365", "Microsoft Teams"])
        .withColumn("_appId", "string", expr="uuid()")
        .withColumn("_clientAppUsed", "string", values=["Browser", "Mobile Apps and Desktop clients"])
        
        # MFA required
        .withColumn("_conditionalAccessStatus", "string",
                   expr="case when resultType = 'Success' then 'success' else 'failure' end")
        
        .withColumn("_deviceOS", "string", values=["Windows 10", "Windows 11", "MacOS", "iOS", "Android"])
        .withColumn("_browser", "string", values=["Chrome", "Edge", "Safari", "Firefox"])
        .withColumn("_deviceId", "string", expr="uuid()")
        
        # MFA-specific fields
        .withColumn("_mfaMethod", "string",
                   expr="case cast(rand() * 4 as int) " +
                        "when 0 then 'PhoneAppNotification' " +
                        "when 1 then 'PhoneAppOTP' " +
                        "when 2 then 'OneWaySMS' " +
                        "else 'VoiceCall' end")
        
        .withColumn("_riskDetail", "string",
                   expr="case when resultType = 'Failure' then 'userPassedMFADrivenByRiskBasedPolicy' else 'none' end")
        .withColumn("_riskLevelAggregated", "string",
                   expr="case when resultType = 'Failure' then case when rand() < 0.5 then 'medium' else 'high' end else 'low' end")
        .withColumn("_riskState", "string",
                   expr="case when resultType = 'Failure' and rand() < 0.4 then 'atRisk' else 'none' end")
        
        .withColumn("_errorCode", "integer",
                   expr="case when resultType = 'Failure' then case cast(rand() * 3 as int) when 0 then 50074 when 1 then 50076 else 500121 end else 0 end")
        .withColumn("_failureReason", "string",
                   expr="case _errorCode " +
                        "when 50074 then 'Strong authentication is required' " +
                        "when 50076 then 'MFA required but user did not complete' " +
                        "when 500121 then 'Authentication failed during MFA request' " +
                        "else null end")
    )
    
    return data_gen.build()

# Generate failed MFA attempts (authentication failures due to MFA issues)
failed_mfa_signin_logs = generate_failed_mfa_scenario(spark, base_rows, partitions)
display(failed_mfa_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Privilege Escalation (Audit Logs)

# COMMAND ----------

def generate_privilege_escalation_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Privilege escalation via role assignments
    - Admin role assignments
    - Global admin operations
    - 70% blocked (AccessDenied)
    - Suspicious account activity
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("time", "string",
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 1 as int)), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")")
        .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
        .withColumn("operationName", "string",
                   expr="case cast(rand() * 8 as int) " +
                        "when 0 then 'Add member to role' " +
                        "when 1 then 'Add user' " +
                        "when 2 then 'Update user' " +
                        "when 3 then 'Update role' " +
                        "when 4 then 'Add service principal' " +
                        "when 5 then 'Add owner to application' " +
                        "when 6 then 'Add app role assignment to service principal' " +
                        "else 'Update application' end")
        .withColumn("category", "string", values=["AuditLogs"])
        
        # 70% failures (blocked attempts)
        .withColumn("resultType", "string",
                   expr="case when rand() < 0.7 then 'Failure' else 'Success' end")
        
        .withColumn("correlationId", "string", expr="uuid()")
        
        # Initiator (who is trying to escalate)
        .withColumn("_initiatorUserPrincipalName", "string",
                   expr="concat(case cast(rand() * 6 as int) " +
                        "when 0 then 'contractor' " +
                        "when 1 then 'temp_user' " +
                        "when 2 then 'developer' " +
                        "when 3 then 'service_account' " +
                        "when 4 then 'backup_admin' " +
                        "else 'test_user' end, cast(rand() * 20 as int), '@contoso.com')")
        .withColumn("_initiatorDisplayName", "string",
                   expr="concat('Initiator ', cast(rand() * 50 as int))")
        .withColumn("_initiatorId", "string", expr="uuid()")
        .withColumn("_initiatorIpAddress", "string",
                   expr="concat(case cast(rand() * 3 as int) when 0 then '203.0.113.' when 1 then '192.168.1.' else '198.51.100.' end, cast(rand() * 254 + 1 as int))")
        
        # Target resource (what is being modified)
        .withColumn("_targetUserPrincipalName", "string",
                   expr="concat(case cast(rand() * 4 as int) " +
                        "when 0 then 'admin' " +
                        "when 1 then 'globaladmin' " +
                        "when 2 then 'privileged_user' " +
                        "else 'elevated_account' end, cast(rand() * 10 as int), '@contoso.com')")
        .withColumn("_targetDisplayName", "string",
                   expr="concat('Target User ', cast(rand() * 30 as int))")
        .withColumn("_targetId", "string", expr="uuid()")
        
        # Role being assigned
        .withColumn("_roleName", "string",
                   expr="case cast(rand() * 7 as int) " +
                        "when 0 then 'Global Administrator' " +
                        "when 1 then 'User Administrator' " +
                        "when 2 then 'Security Administrator' " +
                        "when 3 then 'Privileged Role Administrator' " +
                        "when 4 then 'Application Administrator' " +
                        "when 5 then 'Authentication Administrator' " +
                        "else 'Intune Administrator' end")
        
        .withColumn("_activityDisplayName", "string", expr="operationName")
        .withColumn("_activityDateTime", "string", expr="time")
        
        .withColumn("_categoryAudit", "string",
                   expr="case cast(rand() * 3 as int) " +
                        "when 0 then 'UserManagement' " +
                        "when 1 then 'RoleManagement' " +
                        "else 'ApplicationManagement' end")
        
        .withColumn("_result", "string",
                   expr="case when resultType = 'Success' then 'success' else 'failure' end")
        .withColumn("_resultReason", "string",
                   expr="case when resultType = 'Failure' then 'User does not have permissions to perform this action' else '' end")
    )
    
    return data_gen.build()

# Generate privilege escalation events (admin role assignments and privilege abuse)
privilege_escalation_audit_logs = generate_privilege_escalation_scenario(spark, base_rows, partitions)
display(privilege_escalation_audit_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6: Guest User Abuse

# COMMAND ----------

def generate_guest_user_abuse_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: External guest users accessing sensitive resources
    - Guest users (#EXT#)
    - Access to SharePoint, Teams, sensitive apps
    - 50% from suspicious locations
    - Data exfiltration patterns
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("time", "string",
                   expr="date_format(date_add(current_timestamp(), -cast(rand() * 2 as int)), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")")
        .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
        .withColumn("operationName", "string", values=["Sign-in activity"])
        .withColumn("category", "string", values=["SignInLogs"])
        
        # 80% success (guests getting in)
        .withColumn("resultType", "string",
                   expr="case when rand() < 0.8 then 'Success' else 'Failure' end")
        .withColumn("resultDescription", "string",
                   expr="case when resultType = 'Success' then 'Sign-in successful' else 'Access denied' end")
        
        .withColumn("durationMs", "integer", expr="cast(rand() * 2000 as int)")
        
        # 50% from suspicious locations
        .withColumn("callerIpAddress", "string",
                   expr="concat(case cast(rand() * 6 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '45.33.32.' " +
                        "when 2 then '185.220.101.' " +
                        "when 3 then '192.168.1.' " +
                        "when 4 then '10.0.0.' " +
                        "else '104.244.42.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("correlationId", "string", expr="uuid()")
        
        # Guest user pattern (external email with #EXT#)
        .withColumn("_userPrincipalName", "string",
                   expr="concat('guest', cast(rand() * 100 as int), '_', " +
                        "case cast(rand() * 4 as int) when 0 then 'gmail.com' when 1 then 'yahoo.com' when 2 then 'outlook.com' else 'hotmail.com' end, " +
                        "'#EXT#@contoso.onmicrosoft.com')")
        .withColumn("_userDisplayName", "string",
                   expr="concat('Guest User ', cast(rand() * 100 as int))")
        .withColumn("_userId", "string", expr="uuid()")
        .withColumn("_userType", "string", values=["Guest"])
        
        # Sensitive apps
        .withColumn("_appDisplayName", "string",
                   expr="case cast(rand() * 6 as int) " +
                        "when 0 then 'SharePoint Online' " +
                        "when 1 then 'Microsoft Teams' " +
                        "when 2 then 'OneDrive for Business' " +
                        "when 3 then 'Microsoft Power BI' " +
                        "when 4 then 'Azure Portal' " +
                        "else 'Office 365' end")
        .withColumn("_appId", "string", expr="uuid()")
        .withColumn("_clientAppUsed", "string", values=["Browser", "Mobile Apps and Desktop clients"])
        
        .withColumn("_conditionalAccessStatus", "string",
                   expr="case when resultType = 'Success' then case when rand() < 0.7 then 'notApplied' else 'success' end else 'failure' end")
        
        .withColumn("_deviceOS", "string", values=["Windows 10", "MacOS", "iOS", "Android", "Linux"])
        .withColumn("_browser", "string", values=["Chrome", "Edge", "Safari", "Firefox"])
        .withColumn("_deviceId", "string", expr="uuid()")
        
        # Risk for guest users
        .withColumn("_riskDetail", "string",
                   expr="case when rand() < 0.4 then case cast(rand() * 3 as int) when 0 then 'unfamiliarFeatures' when 1 then 'anonymizedIPAddress' else 'none' end else 'none' end")
        .withColumn("_riskLevelAggregated", "string",
                   expr="case when rand() < 0.3 then 'high' when rand() < 0.6 then 'medium' else 'low' end")
        .withColumn("_riskState", "string",
                   expr="case when _riskLevelAggregated = 'high' then 'atRisk' else 'none' end")
        
        .withColumn("_errorCode", "integer",
                   expr="case when resultType = 'Failure' then case cast(rand() * 2 as int) when 0 then 50105 else 50126 end else 0 end")
        .withColumn("_failureReason", "string",
                   expr="case _errorCode when 50105 then 'Guest user not allowed' when 50126 then 'Invalid credentials' else null end")
    )
    
    df = data_gen.build()
    
    # Add geolocation
    df_with_geo = df.withColumn("ip_prefix", expr("concat(split(callerIpAddress, '\\\\.')[0], '.', split(callerIpAddress, '\\\\.')[1], '.', split(callerIpAddress, '\\\\.')[2], '.*')"))
    
    df_with_geo = df_with_geo.join(
        spark.table("entra_geolocation"),
        df_with_geo.ip_prefix == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern")
    
    return df_with_geo

# Generate guest user abuse events (suspicious external account activities)
guest_user_abuse_audit_logs = generate_guest_user_abuse_scenario(spark, base_rows, partitions)
display(guest_user_abuse_audit_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Nested JSON Structures for Entra ID

# COMMAND ----------

def add_entra_id_structs_signin(df):
    """Add proper Entra ID Sign-in log nested structures"""
    
    # Check if geolocation columns exist
    has_geo = "city" in df.columns
    
    # Build location struct conditionally
    if has_geo:
        location_struct = when(col("city").isNotNull(),
            struct(
                col("city").alias("city"),
                col("state").alias("state"),
                col("countryOrRegion").alias("countryOrRegion"),
                struct(
                    col("latitude").alias("latitude"),
                    col("longitude").alias("longitude")
                ).alias("geoCoordinates")
            )
        ).alias("location")
    else:
        # Create an empty struct with proper schema instead of NullType
        location_struct = lit(None).cast(
            "struct<city:string,state:string,countryOrRegion:string,geoCoordinates:struct<latitude:double,longitude:double>>"
        ).alias("location")
    
    # Build MFA detail conditionally
    has_mfa = "_mfaMethod" in df.columns
    if has_mfa:
        mfa_struct = when(col("_mfaMethod").isNotNull(),
            struct(
                col("_mfaMethod").alias("authMethod"),
                col("resultType").alias("authDetail")
            )
        ).alias("mfaDetail")
    else:
        # Create an empty struct with proper schema instead of NullType
        mfa_struct = lit(None).cast(
            "struct<authMethod:string,authDetail:string>"
        ).alias("mfaDetail")
    
    df_final = df.select(
        col("time"),
        col("resourceId"),
        col("operationName"),
        col("category"),
        col("resultType"),
        col("resultDescription"),
        col("durationMs"),
        col("callerIpAddress"),
        col("correlationId"),
        expr("concat(_userPrincipalName, '|', _userDisplayName)").alias("identity"),
        
        # Nested properties struct
        struct(
            expr("uuid()").alias("id"),
            col("time").alias("createdDateTime"),
            col("_userDisplayName").alias("userDisplayName"),
            col("_userPrincipalName").alias("userPrincipalName"),
            col("_userId").alias("userId"),
            col("_appId").alias("appId"),
            col("_appDisplayName").alias("appDisplayName"),
            col("callerIpAddress").alias("ipAddress"),
            col("_clientAppUsed").alias("clientAppUsed"),
            col("_conditionalAccessStatus").alias("conditionalAccessStatus"),
            
            # deviceDetail struct
            struct(
                col("_deviceId").alias("deviceId"),
                col("_deviceOS").alias("operatingSystem"),
                col("_browser").alias("browser")
            ).alias("deviceDetail"),
            
            location_struct,
            
            # status struct
            struct(
                col("_errorCode").alias("errorCode"),
                col("_failureReason").alias("failureReason"),
                lit(None).cast("string").alias("additionalDetails")
            ).alias("status"),
            
            col("_riskDetail").alias("riskDetail"),
            col("_riskLevelAggregated").alias("riskLevelAggregated"),
            col("_riskState").alias("riskState"),
            
            mfa_struct
            
        ).alias("properties")
    )
    
    # Drop columns conditionally
    cols_to_drop = ["_userPrincipalName", "_userDisplayName", "_userId", "_appId", "_appDisplayName",
                    "_clientAppUsed", "_conditionalAccessStatus", "_deviceId", "_deviceOS", "_browser",
                    "_errorCode", "_failureReason", "_riskDetail", "_riskLevelAggregated", "_riskState"]
    
    if has_mfa:
        cols_to_drop.append("_mfaMethod")
    if has_geo:
        cols_to_drop.extend(["city", "state", "countryOrRegion", "latitude", "longitude", "risk_category", "risk_level"])
    
    df_final = df_final.drop(*cols_to_drop)
    
    return df_final

def add_entra_id_structs_audit(df):
    """Add proper Entra ID Audit log nested structures"""
    
    df_final = df.select(
        col("time"),
        col("resourceId"),
        col("operationName"),
        col("category"),
        col("resultType"),
        col("correlationId"),
        expr("_initiatorUserPrincipalName").alias("identity"),
        
        # Nested properties struct for Audit logs
        struct(
            expr("uuid()").alias("id"),
            col("_activityDateTime").alias("activityDateTime"),
            col("_activityDisplayName").alias("activityDisplayName"),
            col("_categoryAudit").alias("category"),
            col("_result").alias("result"),
            col("_resultReason").alias("resultReason"),
            
            # initiatedBy struct
            struct(
                struct(
                    col("_initiatorDisplayName").alias("displayName"),
                    col("_initiatorUserPrincipalName").alias("userPrincipalName"),
                    col("_initiatorId").alias("id"),
                    col("_initiatorIpAddress").alias("ipAddress")
                ).alias("user")
            ).alias("initiatedBy"),
            
            # targetResources array
            array(
                struct(
                    lit("User").alias("type"),
                    col("_targetDisplayName").alias("displayName"),
                    col("_targetUserPrincipalName").alias("userPrincipalName"),
                    col("_targetId").alias("id"),
                    array(
                        struct(
                            lit("Role").alias("displayName"),
                            lit(None).cast("string").alias("oldValue"),
                            col("_roleName").alias("newValue")
                        )
                    ).alias("modifiedProperties")
                )
            ).alias("targetResources"),
            
            array().cast("array<string>").alias("additionalDetails")
            
        ).alias("properties")
    ).drop("_initiatorUserPrincipalName", "_initiatorDisplayName", "_initiatorId", "_initiatorIpAddress",
           "_targetUserPrincipalName", "_targetDisplayName", "_targetId", "_roleName",
           "_activityDisplayName", "_activityDateTime", "_categoryAudit", "_result", "_resultReason")
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save All Scenarios to Delta Tables

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Transform raw logs into proper Entra ID schema with nested structures
brute_force_signin_with_structs = add_entra_id_structs_signin(brute_force_signin_logs)
impossible_travel_signin_with_structs = add_entra_id_structs_signin(impossible_travel_signin_logs)
risky_signin_with_structs = add_entra_id_structs_signin(risky_signin_logs)
failed_mfa_signin_with_structs = add_entra_id_structs_signin(failed_mfa_signin_logs)
privilege_escalation_audit_with_structs = add_entra_id_structs_audit(privilege_escalation_audit_logs)
guest_user_abuse_with_structs = add_entra_id_structs_signin(guest_user_abuse_audit_logs)

# COMMAND ----------

# Save Scenario 1: Brute Force Sign-ins
brute_force_signin_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.entra_brute_force_signin")

# COMMAND ----------

# Save Scenario 2: Impossible Travel
impossible_travel_signin_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.entra_impossible_travel")

# COMMAND ----------

# Save Scenario 3: Risky Sign-ins
risky_signin_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.entra_risky_signin")

# COMMAND ----------

# Save Scenario 4: Failed MFA Attempts
failed_mfa_signin_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.entra_failed_mfa")

# COMMAND ----------

# Save Scenario 5: Privilege Escalation (Audit)
privilege_escalation_audit_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.entra_privilege_escalation")

# COMMAND ----------

# Save Scenario 6: Guest User Abuse
guest_user_abuse_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.entra_guest_user_abuse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

## Scenario 1: Brute Force Sign-ins
display(spark.sql(f"""SELECT 
  'Brute Force Sign-ins' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT callerIpAddress) as unique_ips,
  SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) as failed_attempts,
  ROUND(100.0 * SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM {catalog_name}.{schema_name}.entra_brute_force_signin"""))

# COMMAND ----------

##Scenario 2: Impossible Travel
display(spark.sql(f"""SELECT 
  'Impossible Travel' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT properties.location.countryOrRegion) as unique_countries,
  SUM(CASE WHEN properties.riskLevelAggregated = 'high' THEN 1 ELSE 0 END) as high_risk_events,
  ROUND(100.0 * SUM(CASE WHEN properties.riskLevelAggregated = 'high' THEN 1 ELSE 0 END) / COUNT(*), 2) as high_risk_pct
FROM {catalog_name}.{schema_name}.entra_impossible_travel"""))

# COMMAND ----------

## Scenario 3: Risky Sign-ins
display(spark.sql(f"""SELECT 
  'Risky Sign-ins' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT properties.riskDetail) as unique_risk_types,
  SUM(CASE WHEN properties.conditionalAccessStatus = 'failure' THEN 1 ELSE 0 END) as ca_blocked,
  ROUND(100.0 * SUM(CASE WHEN properties.conditionalAccessStatus = 'failure' THEN 1 ELSE 0 END) / COUNT(*), 2) as ca_block_rate_pct
FROM {catalog_name}.{schema_name}.entra_risky_signin"""))

# COMMAND ----------

##Scenario 4: Failed MFA
display(spark.sql(f"""SELECT 
  'Failed MFA' as scenario,
  COUNT(*) as total_records,
  SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) as mfa_failures,
  ROUND(100.0 * SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) / COUNT(*), 2) as mfa_failure_rate_pct
FROM {catalog_name}.{schema_name}.entra_failed_mfa"""))

# COMMAND ----------

## Scenario 5: Privilege Escalation
display(spark.sql(f"""SELECT 
  'Privilege Escalation' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT operationName) as unique_operations,
  SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) as blocked_attempts,
  ROUND(100.0 * SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.entra_privilege_escalation"""))

# COMMAND ----------

##Scenario 6: Guest User Abuse
display(spark.sql(f"""SELECT 
  'Guest User Abuse' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT properties.userPrincipalName) as unique_guests,
  SUM(CASE WHEN resultType = 'Success' THEN 1 ELSE 0 END) as successful_access,
  ROUND(100.0 * SUM(CASE WHEN resultType = 'Success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM {catalog_name}.{schema_name}.entra_guest_user_abuse"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Complete!
# MAGIC
# MAGIC Generated 6 Microsoft Entra ID security scenarios:
# MAGIC
# MAGIC 1. **entra_brute_force_signin** - Brute force password attacks
# MAGIC 2. **entra_impossible_travel** - Impossible travel sign-ins
# MAGIC 3. **entra_risky_signin** - High-risk sign-ins (anonymous IPs, leaked credentials)
# MAGIC 4. **entra_failed_mfa** - Failed MFA attempts
# MAGIC 5. **entra_privilege_escalation** - Privilege escalation via role assignments (Audit logs)
# MAGIC 6. **entra_guest_user_abuse** - Guest users accessing sensitive resources

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON Files (Entra ID Format)
# MAGIC
# MAGIC Export each table to a Volume as JSON files compatible with Azure Event Hubs / SIEM tools

# COMMAND ----------

# Base path for JSON exports
base_path = f"/Volumes/{catalog_name}/{schema_name}/entra_id/"

# Table configurations
tables = [
    (f"{catalog_name}.{schema_name}.entra_brute_force_signin", "brute_force_signin"),
    (f"{catalog_name}.{schema_name}.entra_impossible_travel", "impossible_travel"),
    (f"{catalog_name}.{schema_name}.entra_risky_signin", "risky_signin"),
    (f"{catalog_name}.{schema_name}.entra_failed_mfa", "failed_mfa"),
    (f"{catalog_name}.{schema_name}.entra_privilege_escalation", "privilege_escalation"),
    (f"{catalog_name}.{schema_name}.entra_guest_user_abuse", "guest_user_abuse")
]

print("Exporting Entra ID data to JSON files...")
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
print("✅ All Entra ID exports complete!")
print("\nJSON files available at:")
for _, file_name in tables:
    print(f"  - {base_path}{file_name}/")
print("\nThese JSON files can be used with:")
print("  • Azure Event Hubs")
print("  • Microsoft Sentinel")
print("  • Splunk (with Entra ID Add-on)")
print("  • SumoLogic")
print("  • ArcSight")
print("  • Any SIEM tool supporting Entra ID log format")



# COMMAND ----------

