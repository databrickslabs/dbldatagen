# Databricks notebook source
# MAGIC %md
# MAGIC # Microsoft Entra ID Security Scenario Synthetic Data
# MAGIC
# MAGIC This notebook uses **[dbldatagen](https://databrickslabs.github.io/dbldatagen/)** to simulate
# MAGIC Microsoft Entra ID (Azure AD) Sign-in and Audit logs containing common security scenarios.
# MAGIC The scenarios modelled here are:
# MAGIC
# MAGIC 1. **Brute-force sign-in attempts** - password spraying with a high failure rate.
# MAGIC 2. **Impossible travel** - the same user appearing in geographically distant locations.
# MAGIC 3. **Risky sign-ins** - anonymous IPs, malware-linked addresses, leaked credentials.
# MAGIC 4. **Failed MFA attempts** - authentication denied at the second factor.
# MAGIC 5. **Privilege escalation (Audit log)** - role assignments and admin operations.
# MAGIC 6. **Guest user abuse** - external `#EXT#` accounts touching sensitive resources.
# MAGIC
# MAGIC Each scenario is saved as a Delta table and optionally exported to JSON, which you can
# MAGIC replay into Microsoft Sentinel, Azure Event Hubs, or any SIEM that ingests Entra ID logs.

# COMMAND ----------

# MAGIC %pip install dbldatagen
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set the following parameters using widgets at the top of the notebook:
# MAGIC
# MAGIC | Widget         | What it controls                                                         |
# MAGIC | -------------- | ------------------------------------------------------------------------ |
# MAGIC | `catalog_name` | Catalog where Delta tables are written                                   |
# MAGIC | `schema_name`  | Schema where Delta tables are written (tables are named `entra_*`)       |
# MAGIC | `base_rows`    | Rows generated per scenario                                              |
# MAGIC | `partitions`   | Number of Spark partitions used when generating data                     |

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "datagen", "Schema Name")
dbutils.widgets.text("base_rows", "10000", "Rows per scenario")
dbutils.widgets.text("partitions", "4", "Spark partitions")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
base_rows = int(dbutils.widgets.get("base_rows"))
partitions = int(dbutils.widgets.get("partitions"))

print(f"catalog_name: {catalog_name}")
print(f"schema_name:  {schema_name}")
print(f"base_rows:    {base_rows}")
print(f"partitions:   {partitions}")

# COMMAND ----------

from dbldatagen import DataGenerator
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, expr, lit, struct, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1: Brute-force sign-in attempts
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Brute-force and password-spraying attacks churn through sign-in endpoints at high volume.
# MAGIC Failure rates run ~85% (the spray misses for most accounts), most of the traffic originates
# MAGIC from a small set of attacker IPs, and the attacker rotates through a list of usernames.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC 80% of the records come from five suspicious IPs and 20% from internal traffic.
# MAGIC `resultType = 'Failure'` ~85% of the time, with Entra error codes mirroring real outcomes
# MAGIC (`50126` invalid credentials, `50053` account locked, `50055` password expired).

# COMMAND ----------

brute_force_ips = ["203.0.113.42", "198.51.100.88", "45.33.32.156", "185.220.101.23", "91.108.56.99"]
internal_ips = ["192.168.1.10", "192.168.1.100", "192.168.1.200"]
brute_force_weights = [16] * len(brute_force_ips) + [7] * len(internal_ips)

brute_force_signin_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn(
        "time", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 43200), "
             "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")",
    )
    .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
    .withColumn("operationName", "string", values=["Sign-in activity"])
    .withColumn("category", "string", values=["SignInLogs"])
    .withColumn("resultType", "string", values=["Failure", "Success"], weights=[85, 15], random=True)
    .withColumn(
        "resultDescription", "string", baseColumn="resultType",
        expr="case resultType when 'Success' then 'Sign-in successful' else 'Invalid username or password' end",
    )
    .withColumn("durationMs", "integer", expr="cast(rand() * 2000 as int)")
    .withColumn(
        "callerIpAddress", "string",
        values=brute_force_ips + internal_ips,
        weights=brute_force_weights, random=True,
    )
    .withColumn("correlationId", "string", expr="uuid()")
    .withColumn("userPrincipalName", "string", expr="concat('user', cast(rand() * 500 as int), '@contoso.com')")
    .withColumn("userDisplayName", "string", expr="concat('User ', cast(rand() * 500 as int))")
    .withColumn("userId", "string", expr="uuid()")
    .withColumn(
        "appDisplayName", "string",
        values=["Microsoft Azure Portal", "Office 365", "Microsoft Teams"], random=True,
    )
    .withColumn("appId", "string", expr="uuid()")
    .withColumn(
        "clientAppUsed", "string",
        values=["Browser", "Mobile Apps and Desktop clients", "Exchange ActiveSync"], random=True,
    )
    .withColumn(
        "conditionalAccessStatus", "string", baseColumn="resultType",
        expr="case when resultType = 'Success' then 'success' "
             "when rand() < 0.5 then 'notApplied' else 'failure' end",
    )
    .withColumn(
        "deviceOS", "string",
        values=["Windows 10", "Windows 11", "MacOS", "iOS", "Android"], random=True,
    )
    .withColumn("browser", "string", values=["Chrome", "Edge", "Firefox", "Safari"], random=True)
    .withColumn("deviceId", "string", expr="uuid()")
    .withColumn(
        "riskDetail", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "element_at(array('userPassedMFADrivenByRiskBasedPolicy', 'adminGeneratedTemporaryPassword', 'none'), "
             "cast(rand() * 3 as int) + 1) else 'none' end",
    )
    .withColumn(
        "riskLevelAggregated", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' and rand() < 0.6 then 'high' "
             "when resultType = 'Failure' then 'medium' else 'low' end",
    )
    .withColumn(
        "riskState", "string", baseColumn="riskLevelAggregated",
        expr="case riskLevelAggregated "
             "when 'high' then 'atRisk' "
             "when 'medium' then 'confirmedCompromised' "
             "else 'none' end",
    )
    .withColumn(
        "errorCode", "integer", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "element_at(array(50126, 50053, 50055), cast(rand() * 3 as int) + 1) else 0 end",
    )
    .withColumn(
        "failureReason", "string", baseColumn="errorCode",
        expr="case errorCode "
             "when 50126 then 'Invalid username or password' "
             "when 50053 then 'Account is locked' "
             "when 50055 then 'Password expired' end",
    )
).build()

display(brute_force_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Impossible travel
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC "Impossible travel" describes a single user appearing in geographically distant locations
# MAGIC in a short window - e.g. New York at 09:00 UTC and Moscow at 09:15 UTC. Unless the user
# MAGIC boards a very fast plane, one of those sign-ins is fraudulent.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC Traffic spread across six IP ranges in four regions to create the travel-distance signal.
# MAGIC The user pool is deliberately small ("executive0..49") so the same names reappear across
# MAGIC regions. Risk indicators are skewed toward `high`, and 70% of attempts succeed - these
# MAGIC detections are usually downstream of the successful sign-in.
# MAGIC
# MAGIC Entra ID Sign-in logs include a `location` object with city, state, country, and
# MAGIC coordinates. Real deployments enrich events from Microsoft's built-in telemetry; here we
# MAGIC simulate that with a small static lookup keyed by the IP's /24 prefix. The locations are
# MAGIC fictitious - "Alpha District" is benign; "Beta District" and "Gamma District" are
# MAGIC flagged as suspicious. Scenarios 3 and 6 reuse the same lookup.

# COMMAND ----------

geo_locations = [
    # (ip_pattern,   city,            state,             countryOrRegion, latitude, longitude, risk_category, risk_level)
    # Normal - legitimate user locations
    ("192.168.1.*",  "Northport",     "Alpha District",  "ZZ",             37.7749, -122.4194, "normal",      "low"),
    ("10.0.0.*",     "Westbridge",    "Alpha District",  "ZZ",             47.6062, -122.3321, "normal",      "low"),
    ("172.16.0.*",   "Centralview",   "Alpha District",  "ZZ",             40.7128,  -74.0060, "normal",      "low"),
    ("198.51.100.*", "Eastshore",     "Alpha District",  "ZZ",             41.8781,  -87.6298, "normal",      "low"),
    # Suspicious - elevated-risk source ranges
    ("203.0.113.*",  "Shadowmere",    "Beta District",   "XX",             55.7558,   37.6173, "suspicious",  "high"),
    ("45.33.32.*",   "Darkwater",     "Beta District",   "XX",             39.9042,  116.4074, "suspicious",  "high"),
    ("185.220.101.*","Greystone",     "Beta District",   "XX",             35.6892,   51.3890, "suspicious",  "high"),
    ("91.108.56.*",  "Nebula City",   "Beta District",   "XX",             50.4501,   30.5234, "suspicious",  "medium"),
    ("104.244.42.*", "Voidhaven",     "Gamma District",  "YY",              6.5244,    3.3792, "suspicious",  "medium"),
    ("200.152.38.*", "Twilight Zone", "Gamma District",  "YY",            -23.5505,  -46.6333, "suspicious",  "medium"),
]

geo_df = spark.createDataFrame(
    geo_locations,
    ["ip_pattern", "city", "state", "countryOrRegion", "latitude", "longitude", "risk_category", "risk_level"],
)
geo_df.createOrReplaceTempView("entra_geolocation")

display(geo_df)

# COMMAND ----------

impossible_travel_signin_logs_raw = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn(
        "time", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 172800), "
             "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")",
    )
    .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
    .withColumn("operationName", "string", values=["Sign-in activity"])
    .withColumn("category", "string", values=["SignInLogs"])
    .withColumn("resultType", "string", values=["Success", "Failure"], weights=[70, 30], random=True)
    .withColumn(
        "resultDescription", "string", baseColumn="resultType",
        expr="case resultType when 'Success' then 'Sign-in successful' "
             "else 'Suspicious activity detected' end",
    )
    .withColumn("durationMs", "integer", expr="cast(rand() * 3000 as int)")
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "45.33.32.", "185.220.101.", "192.168.1.", "198.51.100.", "10.0.0."],
        random=True, omit=True,
    )
    .withColumn(
        "callerIpAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("correlationId", "string", expr="uuid()")
    .withColumn("userPrincipalName", "string", expr="concat('executive', cast(rand() * 50 as int), '@contoso.com')")
    .withColumn("userDisplayName", "string", expr="concat('Executive ', cast(rand() * 50 as int))")
    .withColumn("userId", "string", expr="uuid()")
    .withColumn(
        "appDisplayName", "string",
        values=["Microsoft Azure Portal", "Office 365", "Outlook", "OneDrive"], random=True,
    )
    .withColumn("appId", "string", expr="uuid()")
    .withColumn(
        "clientAppUsed", "string",
        values=["Browser", "Mobile Apps and Desktop clients"], random=True,
    )
    .withColumn("conditionalAccessStatus", "string", values=["success", "notApplied"], random=True)
    .withColumn(
        "deviceOS", "string",
        values=["Windows 10", "MacOS", "iOS", "Android"], random=True,
    )
    .withColumn("browser", "string", values=["Chrome", "Edge", "Safari", "Firefox"], random=True)
    .withColumn("deviceId", "string", expr="uuid()")
    .withColumn(
        "riskDetail", "string",
        values=["impossibleTravel", "anonymizedIPAddress", "unfamiliarFeatures"], random=True,
    )
    .withColumn("riskLevelAggregated", "string", values=["high", "medium"], weights=[80, 20], random=True)
    .withColumn("riskState", "string", values=["atRisk", "confirmedCompromised"], random=True)
    .withColumn(
        "errorCode", "integer", baseColumn="resultType",
        expr="case when resultType = 'Failure' then 50058 else 0 end",
    )
    .withColumn(
        "failureReason", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "'Suspicious activity detected - impossible travel' end",
    )
).build()

impossible_travel_signin_logs = (
    impossible_travel_signin_logs_raw
    .withColumn(
        "ip_prefix",
        expr(
            "concat(split(callerIpAddress, '\\\\.')[0], '.', "
            "split(callerIpAddress, '\\\\.')[1], '.', "
            "split(callerIpAddress, '\\\\.')[2], '.*')"
        ),
    )
    .join(spark.table("entra_geolocation"), col("ip_prefix") == col("ip_pattern"), "left")
    .drop("ip_prefix", "ip_pattern", "risk_category", "risk_level")
)

display(impossible_travel_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Risky sign-ins
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Microsoft's Identity Protection flags sign-ins that exhibit risk signals - connection from
# MAGIC a known Tor exit, a malware-infected host, credentials that appeared in a public breach,
# MAGIC or an address on a threat-intel feed. Conditional Access policies block most of them;
# MAGIC the rest get stepped up for MFA.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC All traffic from five suspicious /24 ranges. 60% of events are blocked (`resultType =
# MAGIC 'Failure'` with `conditionalAccessStatus = 'failure'`). Risk detail rotates across the
# MAGIC five canonical high-signal indicators.

# COMMAND ----------

risky_signin_logs_raw = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn(
        "time", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 86400), "
             "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")",
    )
    .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
    .withColumn("operationName", "string", values=["Sign-in activity"])
    .withColumn("category", "string", values=["SignInLogs"])
    .withColumn("resultType", "string", values=["Failure", "Success"], weights=[60, 40], random=True)
    .withColumn(
        "resultDescription", "string", baseColumn="resultType",
        expr="case resultType when 'Success' then 'Sign-in successful' "
             "else 'Blocked by Conditional Access policy' end",
    )
    .withColumn("durationMs", "integer", expr="cast(rand() * 1500 as int)")
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "45.33.32.", "185.220.101.", "91.108.56.", "104.244.42."],
        random=True, omit=True,
    )
    .withColumn(
        "callerIpAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("correlationId", "string", expr="uuid()")
    .withColumn("userPrincipalName", "string", expr="concat('user', cast(rand() * 200 as int), '@contoso.com')")
    .withColumn("userDisplayName", "string", expr="concat('User ', cast(rand() * 200 as int))")
    .withColumn("userId", "string", expr="uuid()")
    .withColumn(
        "appDisplayName", "string",
        values=["Microsoft Azure Portal", "Office 365", "SharePoint", "Exchange Online"], random=True,
    )
    .withColumn("appId", "string", expr="uuid()")
    .withColumn(
        "clientAppUsed", "string",
        values=["Browser", "Exchange ActiveSync", "Other clients"], random=True,
    )
    .withColumn(
        "conditionalAccessStatus", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' then 'failure' "
             "when rand() < 0.3 then 'success' else 'notApplied' end",
    )
    .withColumn("deviceOS", "string", values=["Windows 10", "Linux", "Unknown"], random=True)
    .withColumn("browser", "string", values=["Chrome", "Firefox", "Tor Browser", "Unknown"], random=True)
    .withColumn("deviceId", "string", expr="uuid()")
    .withColumn(
        "riskDetail", "string",
        values=[
            "anonymousIPAddress", "maliciousIPAddress", "leakedCredentials",
            "malwareInfectedIPAddress", "suspiciousIPAddress",
        ],
        random=True,
    )
    .withColumn("riskLevelAggregated", "string", values=["high", "medium"], weights=[85, 15], random=True)
    .withColumn("riskState", "string", values=["atRisk", "confirmedCompromised"], weights=[70, 30], random=True)
    .withColumn(
        "errorCode", "integer", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "element_at(array(53003, 50058, 50074), cast(rand() * 3 as int) + 1) else 0 end",
    )
    .withColumn(
        "failureReason", "string", baseColumn="errorCode",
        expr="case errorCode "
             "when 53003 then 'Blocked by Conditional Access policy - risky sign-in' "
             "when 50058 then 'Suspicious activity detected' "
             "when 50074 then 'Strong authentication required' end",
    )
).build()

risky_signin_logs = (
    risky_signin_logs_raw
    .withColumn(
        "ip_prefix",
        expr(
            "concat(split(callerIpAddress, '\\\\.')[0], '.', "
            "split(callerIpAddress, '\\\\.')[1], '.', "
            "split(callerIpAddress, '\\\\.')[2], '.*')"
        ),
    )
    .join(spark.table("entra_geolocation"), col("ip_prefix") == col("ip_pattern"), "left")
    .drop("ip_prefix", "ip_pattern", "risk_category", "risk_level")
)

display(risky_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: Failed MFA attempts
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC An attacker who has the password but not the second factor trips a distinctive pattern:
# MAGIC the first stage of sign-in succeeds, but MFA fails repeatedly. Even without brute-forcing
# MAGIC the OTP, "MFA fatigue" attacks (push-notification spam) can sometimes trick users into
# MAGIC approving the login.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~90% `resultType = 'Failure'` across four MFA methods (push, OTP, SMS, voice). Error codes
# MAGIC mirror real Entra outcomes for MFA-related failures (`50074`, `50076`, `500121`).

# COMMAND ----------

failed_mfa_signin_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn(
        "time", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 86400), "
             "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")",
    )
    .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
    .withColumn("operationName", "string", values=["Sign-in activity"])
    .withColumn("category", "string", values=["SignInLogs"])
    .withColumn("resultType", "string", values=["Failure", "Success"], weights=[90, 10], random=True)
    .withColumn(
        "resultDescription", "string", baseColumn="resultType",
        expr="case resultType when 'Success' then 'MFA completed successfully' else 'MFA denied' end",
    )
    .withColumn("durationMs", "integer", expr="cast(rand() * 5000 as int)")
    .withColumn(
        "ipPrefix", "string",
        values=["192.168.1.", "10.0.0.", "203.0.113.", "198.51.100."], random=True, omit=True,
    )
    .withColumn(
        "callerIpAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("correlationId", "string", expr="uuid()")
    .withColumn("userPrincipalName", "string", expr="concat('employee', cast(rand() * 300 as int), '@contoso.com')")
    .withColumn("userDisplayName", "string", expr="concat('Employee ', cast(rand() * 300 as int))")
    .withColumn("userId", "string", expr="uuid()")
    .withColumn(
        "appDisplayName", "string",
        values=["Microsoft Azure Portal", "Office 365", "Microsoft Teams"], random=True,
    )
    .withColumn("appId", "string", expr="uuid()")
    .withColumn(
        "clientAppUsed", "string",
        values=["Browser", "Mobile Apps and Desktop clients"], random=True,
    )
    .withColumn(
        "conditionalAccessStatus", "string", baseColumn="resultType",
        expr="case when resultType = 'Success' then 'success' else 'failure' end",
    )
    .withColumn(
        "deviceOS", "string",
        values=["Windows 10", "Windows 11", "MacOS", "iOS", "Android"], random=True,
    )
    .withColumn("browser", "string", values=["Chrome", "Edge", "Safari", "Firefox"], random=True)
    .withColumn("deviceId", "string", expr="uuid()")
    .withColumn(
        "mfaMethod", "string",
        values=["PhoneAppNotification", "PhoneAppOTP", "OneWaySMS", "VoiceCall"], random=True,
    )
    .withColumn(
        "riskDetail", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' then 'userPassedMFADrivenByRiskBasedPolicy' else 'none' end",
    )
    .withColumn(
        "riskLevelAggregated", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' and rand() < 0.5 then 'medium' "
             "when resultType = 'Failure' then 'high' else 'low' end",
    )
    .withColumn(
        "riskState", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' and rand() < 0.4 then 'atRisk' else 'none' end",
    )
    .withColumn(
        "errorCode", "integer", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "element_at(array(50074, 50076, 500121), cast(rand() * 3 as int) + 1) else 0 end",
    )
    .withColumn(
        "failureReason", "string", baseColumn="errorCode",
        expr="case errorCode "
             "when 50074 then 'Strong authentication is required' "
             "when 50076 then 'MFA required but user did not complete' "
             "when 500121 then 'Authentication failed during MFA request' end",
    )
).build()

display(failed_mfa_signin_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Privilege escalation (Audit log)
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Once inside, attackers try to escalate by adding themselves (or a compromised account) to
# MAGIC privileged roles, creating service principals, or altering application permissions. In
# MAGIC Entra ID these show up as **Audit log** entries - a different schema from Sign-in logs
# MAGIC with `initiatedBy` and `targetResources` objects.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC A set of role-management operations targeting high-value roles (Global Administrator,
# MAGIC Security Administrator, etc.). ~70% are blocked because the initiator lacks permission.

# COMMAND ----------

privilege_escalation_audit_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn(
        "time", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 86400), "
             "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")",
    )
    .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
    .withColumn(
        "operationName", "string",
        values=[
            "Add member to role", "Add user", "Update user", "Update role",
            "Add service principal", "Add owner to application",
            "Add app role assignment to service principal", "Update application",
        ],
        random=True,
    )
    .withColumn("category", "string", values=["AuditLogs"])
    .withColumn("resultType", "string", values=["Failure", "Success"], weights=[70, 30], random=True)
    .withColumn("correlationId", "string", expr="uuid()")
    # Initiator — typically a lower-privilege account attempting to escalate.
    .withColumn(
        "initiatorRole", "string",
        values=["contractor", "temp_user", "developer", "service_account", "backup_admin", "test_user"],
        random=True, omit=True,
    )
    .withColumn(
        "initiatorUserPrincipalName", "string", baseColumn="initiatorRole",
        expr="concat(initiatorRole, cast(rand() * 20 as int), '@contoso.com')",
    )
    .withColumn("initiatorDisplayName", "string", expr="concat('Initiator ', cast(rand() * 50 as int))")
    .withColumn("initiatorId", "string", expr="uuid()")
    .withColumn(
        "initiatorIpPrefix", "string",
        values=["203.0.113.", "192.168.1.", "198.51.100."], random=True, omit=True,
    )
    .withColumn(
        "initiatorIpAddress", "string", baseColumn="initiatorIpPrefix",
        expr="concat(initiatorIpPrefix, cast(rand() * 254 + 1 as int))",
    )
    # Target — a high-privilege account being modified.
    .withColumn(
        "targetRole", "string",
        values=["admin", "globaladmin", "privileged_user", "elevated_account"],
        random=True, omit=True,
    )
    .withColumn(
        "targetUserPrincipalName", "string", baseColumn="targetRole",
        expr="concat(targetRole, cast(rand() * 10 as int), '@contoso.com')",
    )
    .withColumn("targetDisplayName", "string", expr="concat('Target User ', cast(rand() * 30 as int))")
    .withColumn("targetId", "string", expr="uuid()")
    .withColumn(
        "roleName", "string",
        values=[
            "Global Administrator", "User Administrator", "Security Administrator",
            "Privileged Role Administrator", "Application Administrator",
            "Authentication Administrator", "Intune Administrator",
        ],
        random=True,
    )
    .withColumn("activityDisplayName", "string", baseColumn="operationName", expr="operationName")
    .withColumn("activityDateTime", "string", baseColumn="time", expr="time")
    .withColumn(
        "categoryAudit", "string",
        values=["UserManagement", "RoleManagement", "ApplicationManagement"], random=True,
    )
    .withColumn(
        "result", "string", baseColumn="resultType",
        expr="case when resultType = 'Success' then 'success' else 'failure' end",
    )
    .withColumn(
        "resultReason", "string", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "'User does not have permissions to perform this action' else '' end",
    )
).build()

display(privilege_escalation_audit_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6: Guest user abuse
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC External collaboration relies on guest accounts (identified by `#EXT#` in the UPN). When
# MAGIC one is compromised - or invited mistakenly - it can quietly read SharePoint, Teams, or
# MAGIC Power BI content. Warning signs include sign-ins from consumer mail providers, suspicious
# MAGIC geographies, and access to sensitive apps.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC UPNs shaped like `guestN_provider.com#EXT#@contoso.onmicrosoft.com`. 50% of traffic comes
# MAGIC from suspicious /24s, 80% of sign-ins succeed (the guest already got in), and about 30%
# MAGIC carry elevated risk indicators.

# COMMAND ----------

guest_user_abuse_logs_raw = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn(
        "time", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 172800), "
             "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")",
    )
    .withColumn("resourceId", "string", values=["/tenants/12345678-1234-1234-1234-123456789012"])
    .withColumn("operationName", "string", values=["Sign-in activity"])
    .withColumn("category", "string", values=["SignInLogs"])
    .withColumn("resultType", "string", values=["Success", "Failure"], weights=[80, 20], random=True)
    .withColumn(
        "resultDescription", "string", baseColumn="resultType",
        expr="case resultType when 'Success' then 'Sign-in successful' else 'Access denied' end",
    )
    .withColumn("durationMs", "integer", expr="cast(rand() * 2000 as int)")
    .withColumn(
        "ipPrefix", "string",
        values=[
            "203.0.113.", "45.33.32.", "185.220.101.",
            "192.168.1.", "10.0.0.", "104.244.42.",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "callerIpAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("correlationId", "string", expr="uuid()")
    .withColumn(
        "guestProvider", "string",
        values=["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"],
        random=True, omit=True,
    )
    # The #EXT# suffix is Entra's marker for a federated guest account.
    .withColumn(
        "userPrincipalName", "string", baseColumn="guestProvider",
        expr="concat('guest', cast(rand() * 100 as int), '_', guestProvider, '#EXT#@contoso.onmicrosoft.com')",
    )
    .withColumn("userDisplayName", "string", expr="concat('Guest User ', cast(rand() * 100 as int))")
    .withColumn("userId", "string", expr="uuid()")
    .withColumn(
        "appDisplayName", "string",
        values=[
            "SharePoint Online", "Microsoft Teams", "OneDrive for Business",
            "Microsoft Power BI", "Azure Portal", "Office 365",
        ],
        random=True,
    )
    .withColumn("appId", "string", expr="uuid()")
    .withColumn(
        "clientAppUsed", "string",
        values=["Browser", "Mobile Apps and Desktop clients"], random=True,
    )
    .withColumn(
        "conditionalAccessStatus", "string", baseColumn="resultType",
        expr="case when resultType = 'Success' and rand() < 0.7 then 'notApplied' "
             "when resultType = 'Success' then 'success' else 'failure' end",
    )
    .withColumn(
        "deviceOS", "string",
        values=["Windows 10", "MacOS", "iOS", "Android", "Linux"], random=True,
    )
    .withColumn("browser", "string", values=["Chrome", "Edge", "Safari", "Firefox"], random=True)
    .withColumn("deviceId", "string", expr="uuid()")
    .withColumn(
        "riskDetail", "string",
        expr="case when rand() < 0.4 then "
             "element_at(array('unfamiliarFeatures', 'anonymizedIPAddress', 'none'), cast(rand() * 3 as int) + 1) "
             "else 'none' end",
    )
    .withColumn(
        "riskLevelAggregated", "string",
        values=["high", "medium", "low"], weights=[30, 30, 40], random=True,
    )
    .withColumn(
        "riskState", "string", baseColumn="riskLevelAggregated",
        expr="case when riskLevelAggregated = 'high' then 'atRisk' else 'none' end",
    )
    .withColumn(
        "errorCode", "integer", baseColumn="resultType",
        expr="case when resultType = 'Failure' then "
             "element_at(array(50105, 50126), cast(rand() * 2 as int) + 1) else 0 end",
    )
    .withColumn(
        "failureReason", "string", baseColumn="errorCode",
        expr="case errorCode "
             "when 50105 then 'Guest user not allowed' "
             "when 50126 then 'Invalid credentials' end",
    )
).build()

guest_user_abuse_logs = (
    guest_user_abuse_logs_raw
    .withColumn(
        "ip_prefix",
        expr(
            "concat(split(callerIpAddress, '\\\\.')[0], '.', "
            "split(callerIpAddress, '\\\\.')[1], '.', "
            "split(callerIpAddress, '\\\\.')[2], '.*')"
        ),
    )
    .join(spark.table("entra_geolocation"), col("ip_prefix") == col("ip_pattern"), "left")
    .drop("ip_prefix", "ip_pattern", "risk_category", "risk_level")
)

display(guest_user_abuse_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap each scenario in the Entra ID envelope
# MAGIC
# MAGIC Entra ID Sign-in and Audit logs have distinct nested shapes. The two helpers below wrap
# MAGIC the flat generated columns into each log type - `add_entra_id_signin_structs` for Sign-in
# MAGIC events (scenarios 1-4, 6) and `add_entra_id_audit_structs` for Audit events (scenario 5).
# MAGIC The sign-in helper also tolerates optional `mfaMethod` / geo columns so it works for both
# MAGIC geo-enriched and plain scenarios.

# COMMAND ----------


def add_entra_id_signin_structs(df: DataFrame) -> DataFrame:
    """Wraps flat sign-in columns in the Entra ID Sign-in log nested shape."""
    has_geo = "city" in df.columns
    has_mfa = "mfaMethod" in df.columns

    if has_geo:
        location_struct = when(
            col("city").isNotNull(),
            struct(
                col("city"),
                col("state"),
                col("countryOrRegion"),
                struct(col("latitude"), col("longitude")).alias("geoCoordinates"),
            ),
        ).alias("location")
    else:
        location_struct = lit(None).cast(
            "struct<city:string,state:string,countryOrRegion:string,"
            "geoCoordinates:struct<latitude:double,longitude:double>>"
        ).alias("location")

    if has_mfa:
        mfa_struct = when(
            col("mfaMethod").isNotNull(),
            struct(col("mfaMethod").alias("authMethod"), col("resultType").alias("authDetail")),
        ).alias("mfaDetail")
    else:
        mfa_struct = lit(None).cast("struct<authMethod:string,authDetail:string>").alias("mfaDetail")

    properties_struct = struct(
        expr("uuid()").alias("id"),
        col("time").alias("createdDateTime"),
        col("userDisplayName"),
        col("userPrincipalName"),
        col("userId"),
        col("appId"),
        col("appDisplayName"),
        col("callerIpAddress").alias("ipAddress"),
        col("clientAppUsed"),
        col("conditionalAccessStatus"),
        struct(
            col("deviceId"),
            col("deviceOS").alias("operatingSystem"),
            col("browser"),
        ).alias("deviceDetail"),
        location_struct,
        struct(
            col("errorCode"),
            col("failureReason"),
            lit(None).cast("string").alias("additionalDetails"),
        ).alias("status"),
        col("riskDetail"),
        col("riskLevelAggregated"),
        col("riskState"),
        mfa_struct,
    ).alias("properties")

    result = df.select(
        col("time"),
        col("resourceId"),
        col("operationName"),
        col("category"),
        col("resultType"),
        col("resultDescription"),
        col("durationMs"),
        col("callerIpAddress"),
        col("correlationId"),
        expr("concat(userPrincipalName, '|', userDisplayName)").alias("identity"),
        properties_struct,
    )

    cols_to_drop = []
    if has_geo:
        cols_to_drop.extend(["city", "state", "countryOrRegion", "latitude", "longitude"])
    # The flat source columns that are now projected under `properties` are simply
    # dropped here — `select` above already kept only the columns we want.
    return result


def add_entra_id_audit_structs(df: DataFrame) -> DataFrame:
    """Wraps flat audit columns in the Entra ID Audit log nested shape."""
    return df.select(
        col("time"),
        col("resourceId"),
        col("operationName"),
        col("category"),
        col("resultType"),
        col("correlationId"),
        col("initiatorUserPrincipalName").alias("identity"),
        struct(
            expr("uuid()").alias("id"),
            col("activityDateTime"),
            col("activityDisplayName"),
            col("categoryAudit").alias("category"),
            col("result"),
            col("resultReason"),
            struct(
                struct(
                    col("initiatorDisplayName").alias("displayName"),
                    col("initiatorUserPrincipalName").alias("userPrincipalName"),
                    col("initiatorId").alias("id"),
                    col("initiatorIpAddress").alias("ipAddress"),
                ).alias("user"),
            ).alias("initiatedBy"),
            array(
                struct(
                    lit("User").alias("type"),
                    col("targetDisplayName").alias("displayName"),
                    col("targetUserPrincipalName").alias("userPrincipalName"),
                    col("targetId").alias("id"),
                    array(
                        struct(
                            lit("Role").alias("displayName"),
                            lit(None).cast("string").alias("oldValue"),
                            col("roleName").alias("newValue"),
                        ),
                    ).alias("modifiedProperties"),
                ),
            ).alias("targetResources"),
            array().cast("array<string>").alias("additionalDetails"),
        ).alias("properties"),
    )


# COMMAND ----------

brute_force_signin_with_structs = add_entra_id_signin_structs(brute_force_signin_logs)
impossible_travel_signin_with_structs = add_entra_id_signin_structs(impossible_travel_signin_logs)
risky_signin_with_structs = add_entra_id_signin_structs(risky_signin_logs)
failed_mfa_signin_with_structs = add_entra_id_signin_structs(failed_mfa_signin_logs)
privilege_escalation_audit_with_structs = add_entra_id_audit_structs(privilege_escalation_audit_logs)
guest_user_abuse_with_structs = add_entra_id_signin_structs(guest_user_abuse_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save each dataset to a Delta table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Scenario 1: brute-force sign-ins
(
    brute_force_signin_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.entra_brute_force_signin")
)

# COMMAND ----------

# Scenario 2: impossible travel
(
    impossible_travel_signin_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.entra_impossible_travel")
)

# COMMAND ----------

# Scenario 3: risky sign-ins
(
    risky_signin_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.entra_risky_signin")
)

# COMMAND ----------

# Scenario 4: failed MFA attempts
(
    failed_mfa_signin_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.entra_failed_mfa")
)

# COMMAND ----------

# Scenario 5: privilege escalation (Audit)
(
    privilege_escalation_audit_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.entra_privilege_escalation")
)

# COMMAND ----------

# Scenario 6: guest user abuse
(
    guest_user_abuse_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.entra_guest_user_abuse")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics
# MAGIC
# MAGIC One SQL roll-up per scenario so you can eyeball that the intended proportions
# MAGIC (85% failure rate, 70% impossible travel detected, 90% failed MFA, etc.) actually
# MAGIC showed up in the generated data.

# COMMAND ----------

# Scenario 1: brute-force sign-ins
display(spark.sql(f"""
  SELECT
    'Brute Force Sign-ins' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT callerIpAddress) AS unique_ips,
    SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) AS failed_attempts,
    ROUND(100.0 * SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
  FROM {catalog_name}.{schema_name}.entra_brute_force_signin
"""))

# COMMAND ----------

# Scenario 2: impossible travel
display(spark.sql(f"""
  SELECT
    'Impossible Travel' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT properties.location.countryOrRegion) AS unique_countries,
    SUM(CASE WHEN properties.riskLevelAggregated = 'high' THEN 1 ELSE 0 END) AS high_risk_events,
    ROUND(100.0 * SUM(CASE WHEN properties.riskLevelAggregated = 'high' THEN 1 ELSE 0 END) / COUNT(*), 2) AS high_risk_pct
  FROM {catalog_name}.{schema_name}.entra_impossible_travel
"""))

# COMMAND ----------

# Scenario 3: risky sign-ins
display(spark.sql(f"""
  SELECT
    'Risky Sign-ins' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT properties.riskDetail) AS unique_risk_types,
    SUM(CASE WHEN properties.conditionalAccessStatus = 'failure' THEN 1 ELSE 0 END) AS ca_blocked,
    ROUND(100.0 * SUM(CASE WHEN properties.conditionalAccessStatus = 'failure' THEN 1 ELSE 0 END) / COUNT(*), 2) AS ca_block_rate_pct
  FROM {catalog_name}.{schema_name}.entra_risky_signin
"""))

# COMMAND ----------

# Scenario 4: failed MFA attempts
display(spark.sql(f"""
  SELECT
    'Failed MFA' AS scenario,
    COUNT(*) AS total_records,
    SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) AS mfa_failures,
    ROUND(100.0 * SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) / COUNT(*), 2) AS mfa_failure_rate_pct
  FROM {catalog_name}.{schema_name}.entra_failed_mfa
"""))

# COMMAND ----------

# Scenario 5: privilege escalation
display(spark.sql(f"""
  SELECT
    'Privilege Escalation' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT operationName) AS unique_operations,
    SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) AS blocked_attempts,
    ROUND(100.0 * SUM(CASE WHEN resultType = 'Failure' THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.entra_privilege_escalation
"""))

# COMMAND ----------

# Scenario 6: guest user abuse
display(spark.sql(f"""
  SELECT
    'Guest User Abuse' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT properties.userPrincipalName) AS unique_guests,
    SUM(CASE WHEN resultType = 'Success' THEN 1 ELSE 0 END) AS successful_access,
    ROUND(100.0 * SUM(CASE WHEN resultType = 'Success' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct
  FROM {catalog_name}.{schema_name}.entra_guest_user_abuse
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Data
# MAGIC
# MAGIC Six tables have been written in the output location:
# MAGIC
# MAGIC | Table                             | Scenario                                      |
# MAGIC | --------------------------------- | --------------------------------------------- |
# MAGIC | `entra_brute_force_signin`        | Brute-force password spraying                 |
# MAGIC | `entra_impossible_travel`         | Impossible-travel sign-ins                    |
# MAGIC | `entra_risky_signin`              | High-risk sign-ins blocked by CA policy       |
# MAGIC | `entra_failed_mfa`                | Failed MFA attempts                           |
# MAGIC | `entra_privilege_escalation`      | Privilege escalation (Audit logs)             |
# MAGIC | `entra_guest_user_abuse`          | Guest accounts touching sensitive resources   |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON (Entra ID format)
# MAGIC
# MAGIC Writes each dataset to a Unity Catalog Volume as JSON, the format Entra ID ships to
# MAGIC Azure Event Hubs and on to downstream SIEMs.

# COMMAND ----------

base_path = f"/Volumes/{catalog_name}/{schema_name}/entra_id/"

tables = [
    (f"{catalog_name}.{schema_name}.entra_brute_force_signin", "brute_force_signin"),
    (f"{catalog_name}.{schema_name}.entra_impossible_travel", "impossible_travel"),
    (f"{catalog_name}.{schema_name}.entra_risky_signin", "risky_signin"),
    (f"{catalog_name}.{schema_name}.entra_failed_mfa", "failed_mfa"),
    (f"{catalog_name}.{schema_name}.entra_privilege_escalation", "privilege_escalation"),
    (f"{catalog_name}.{schema_name}.entra_guest_user_abuse", "guest_user_abuse"),
]

for table_name, file_name in tables:
    output_path = f"{base_path}{file_name}"
    print(f"Exporting {table_name} -> {output_path}")
    (
        spark
        .table(table_name)
        .write
        .format("json")
        .mode("overwrite")
        .save(output_path)
    )

print("\nAll exports complete. Files are at:")
for _, file_name in tables:
    print(f"  {base_path}{file_name}/")
