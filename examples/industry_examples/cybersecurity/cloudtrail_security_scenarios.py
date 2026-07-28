# Databricks notebook source
# MAGIC %md
# MAGIC # AWS CloudTrail Security Scenario Synthetic Data
# MAGIC
# MAGIC This notebook uses **[dbldatagen](https://databrickslabs.github.io/dbldatagen/)** to simulate
# MAGIC AWS CloudTrail logs containing common attack patterns. The scenarios modelled here are:
# MAGIC
# MAGIC 1. **High-frequency login attempts from the same IP** - brute-force password spraying.
# MAGIC 2. **Large number of failed logins** - credential stuffing across many usernames.
# MAGIC 3. **Unusual geographies** - sign-ins from suspicious regions, joined to a mock IP geolocation
# MAGIC    lookup table.
# MAGIC 4. **High rate of error codes** - an AWS environment under attack or misconfigured.
# MAGIC 5. **Privilege escalation / IAM abuse** - unauthorized attempts to create keys, attach
# MAGIC    policies, and assume roles.
# MAGIC
# MAGIC Each scenario is saved as a Delta table and optionally exported to JSON, which you can
# MAGIC replay into a SIEM or any tool that ingests CloudTrail logs.

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
# MAGIC | `schema_name`  | Schema where Delta tables are written (tables are named `ctrail_*`)      |
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
from pyspark.sql.functions import col, struct, when, lit, expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1: High-frequency login attempts from the same IP
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC In a brute-force / password-spraying attack, sign-in traffic concentrates on a small set of
# MAGIC source IPs (the attacker's infrastructure) and targets a short list of high-value usernames.
# MAGIC Most attempts fail with `AccessDenied`, but the volume is what gives the attack away.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC 80% of the records come from five attacker IPs (`weights` skewed in favor of them); the remaining
# MAGIC 20% are internal traffic. About 60% of events carry `errorCode = AccessDenied`, a number of
# MAGIC failed logins that would raise eyebrows in any real environment.

# COMMAND ----------

brute_force_ips = ["203.0.113.42", "198.51.100.88", "45.33.32.156", "185.220.101.23", "91.108.56.99"]
internal_ips = ["192.168.1.10", "192.168.1.100", "192.168.1.200"]
high_freq_login_weights = [16] * len(brute_force_ips) + [7] * len(internal_ips)

high_frequency_login_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("eventVersion", "string", values=["1.11"])
    .withColumn("awsRegion", "string", values=["us-east-1"])
    .withColumn("eventSource", "string", values=["signin.amazonaws.com"])
    .withColumn("eventName", "string", values=["ConsoleLogin"])
    .withColumn("eventType", "string", values=["AwsConsoleSignIn"])
    .withColumn("eventCategory", "string", values=["Management"])
    .withColumn("recipientAccountId", "string", values=["123456789012"])
    .withColumn("sessionCredentialFromConsole", "string", values=["true"])
    .withColumn("managementEvent", "boolean", values=[True])
    .withColumn("readOnly", "boolean", values=[False])
    .withColumn(
        "eventTime", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 43200), "
             "\"yyyy-MM-dd'T'HH:mm:ss'Z'\")",
    )
    .withColumn(
        "sourceIPAddress", "string",
        values=brute_force_ips + internal_ips,
        weights=high_freq_login_weights,
        random=True,
    )
    .withColumn("userAgent", "string", values=["Mozilla/5.0", "aws-cli/2.0.0"], random=True)
    .withColumn("errorCode", "string", values=["AccessDenied"], percentNulls=0.4, random=True)
    .withColumn(
        "errorMessage", "string", baseColumn="errorCode",
        expr="case when errorCode is not null then 'Invalid username or password' end",
    )
    .withColumn(
        "userName", "string",
        values=["admin", "root", "administrator", "user", "test"], random=True,
    )
    .withColumn(
        "principalId", "string",
        expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')",
    )
    .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
    .withColumn("eventID", "string", expr="substring(md5(cast(rand() as string)), 1, 36)")
).build()

display(high_frequency_login_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Large number of failed login attempts
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Credential stuffing — replaying leaked password dumps — shows up as a very high failure
# MAGIC rate (95%+), traffic from a rotating set of IP ranges, and a long tail of distinct usernames
# MAGIC (the attacker has a list).
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~95% of events return an `AccessDenied` or `UnauthorizedOperation` error. Usernames come
# MAGIC from `user0`…`user999` to simulate the long tail.

# COMMAND ----------

failed_login_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("eventVersion", "string", values=["1.11"])
    .withColumn("awsRegion", "string", values=["us-east-1"])
    .withColumn(
        "eventSource", "string",
        values=["signin.amazonaws.com", "iam.amazonaws.com"], random=True,
    )
    .withColumn(
        "eventName", "string", baseColumn="eventSource",
        expr="case eventSource when 'signin.amazonaws.com' then 'ConsoleLogin' else 'GetUser' end",
    )
    .withColumn("eventType", "string", values=["AwsApiCall", "AwsConsoleSignIn"], random=True)
    .withColumn("eventCategory", "string", values=["Management"])
    .withColumn("recipientAccountId", "string", values=["123456789012"])
    .withColumn("sessionCredentialFromConsole", "string", values=["true"])
    .withColumn("managementEvent", "boolean", values=[True])
    .withColumn("readOnly", "boolean", values=[False])
    .withColumn(
        "eventTime", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 86400), "
             "\"yyyy-MM-dd'T'HH:mm:ss'Z'\")",
    )
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "198.51.100.", "45.33.32."], random=True, omit=True,
    )
    .withColumn(
        "sourceIPAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn(
        "userAgent", "string",
        values=["Mozilla/5.0", "Python-urllib/3.8", "aws-cli/2.0.0"], random=True,
    )
    .withColumn(
        "errorCode", "string",
        values=["AccessDenied", "UnauthorizedOperation"], percentNulls=0.05, random=True,
    )
    .withColumn(
        "errorMessage", "string", baseColumn="errorCode",
        expr="case errorCode "
             "when 'AccessDenied' then 'User is not authorized to perform this action' "
             "when 'UnauthorizedOperation' then 'Invalid credentials' end",
    )
    .withColumn("userName", "string", expr="concat('user', cast(rand() * 1000 as int))")
    .withColumn(
        "principalId", "string",
        expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')",
    )
    .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
    .withColumn("eventID", "string", expr="substring(md5(cast(rand() as string)), 1, 36)")
).build()

display(failed_login_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Unusual geographies
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Access from regions that a workforce never logs in from is one of the highest-signal
# MAGIC indicators in cloud detections. Real environments pair CloudTrail events with a GeoIP
# MAGIC service; here we simulate that by joining to the `ip_geolocation` lookup table created above.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC 70% of traffic originates from suspicious IP ranges (Region Beta / Gamma); the rest looks
# MAGIC like normal internal traffic. After generation we attach city, coordinates, and a `risk_level`
# MAGIC flag from the lookup table.

# COMMAND ----------

ip_geo_data = [
    # (ip_pattern,         country,         region,            city,            latitude, longitude, risk_level)
    # Normal — internal / corporate networks
    ("192.168.1.*",        "Region Alpha",  "Alpha District",  "Northport",       37.7749, -122.4194, "normal"),
    ("10.0.0.*",           "Region Alpha",  "Alpha District",  "Westbridge",      47.6062, -122.3321, "normal"),
    ("172.16.0.*",         "Region Alpha",  "Alpha District",  "Centralview",     40.7128,  -74.0060, "normal"),
    # Suspicious — commonly-seen source ranges in real threat intel
    ("203.0.113.*",        "Region Beta",   "Beta District",   "Shadowmere",      55.7558,   37.6173, "suspicious"),
    ("198.51.100.*",       "Region Beta",   "Beta District",   "Darkwater",       39.9042,  116.4074, "suspicious"),
    ("45.33.32.*",         "Region Beta",   "Beta District",   "Greystone",       39.0392,  125.7625, "suspicious"),
    ("185.220.101.*",      "Region Beta",   "Beta District",   "Phantom Bay",     35.6892,   51.3890, "suspicious"),
    ("91.108.56.*",        "Region Beta",   "Beta District",   "Nebula City",     50.4501,   30.5234, "suspicious"),
    ("104.244.42.*",       "Region Gamma",  "Gamma District",  "Voidhaven",        6.5244,    3.3792, "suspicious"),
    ("200.152.38.*",       "Region Gamma",  "Gamma District",  "Twilight Zone",  -23.5505,  -46.6333, "suspicious"),
]

ip_geo_df = spark.createDataFrame(
    ip_geo_data,
    ["ip_pattern", "country", "region", "city", "latitude", "longitude", "risk_level"],
)
ip_geo_df.createOrReplaceTempView("ip_geolocation")

display(ip_geo_df)

# COMMAND ----------

suspicious_prefixes = ["203.0.113.", "198.51.100.", "45.33.32.", "185.220.101.", "104.244.42."]
normal_prefixes = ["192.168.1."]

unusual_geography_logs_raw = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("eventVersion", "string", values=["1.11"])
    .withColumn("awsRegion", "string", values=["us-east-1", "us-west-2", "eu-west-1"], random=True)
    .withColumn(
        "eventSource", "string",
        values=["s3.amazonaws.com", "ec2.amazonaws.com", "iam.amazonaws.com", "dynamodb.amazonaws.com"],
        random=True,
    )
    .withColumn("eventName", "string", baseColumn="eventSource",
                expr="""case eventSource 
                        when 's3.amazonaws.com' then element_at(array('GetObject', 'PutObject', 'ListBucket'), cast(rand() * 3 as int) + 1)
                        when 'ec2.amazonaws.com' then element_at(array('DescribeInstances', 'RunInstances'), cast(rand() * 2 as int) + 1)
                        when 'iam.amazonaws.com' then element_at(array('ListUsers', 'GetUser', 'CreateAccessKey'), cast(rand() * 3 as int) + 1) 
                        else 'Query' end"""
    )
    .withColumn(
        "ipPrefix", "string",
        values=suspicious_prefixes + normal_prefixes,
        weights=[7] * len(suspicious_prefixes) + [15] * len(normal_prefixes),
        random=True, omit=True,
    )
    .withColumn(
        "sourceIPAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn(
        "userAgent", "string",
        values=["aws-cli/2.0.0", "aws-sdk-python/1.26.0", "Mozilla/5.0", "curl/7.68.0"], random=True,
    )
    .withColumn(
        "errorCode", "string",
        values=["AccessDenied", "UnauthorizedOperation", "InvalidParameter"],
        percentNulls=0.6, random=True,
    )
    .withColumn(
        "errorMessage", "string", baseColumn="errorCode",
        expr="case when errorCode is not null then 'Operation failed' end",
    )
    .withColumn(
        "readOnly", "boolean", baseColumn="eventName",
        expr="eventName like 'Get%' or eventName like 'List%' or eventName like 'Describe%'",
    )
    .withColumn("eventType", "string", values=["AwsApiCall"])
    .withColumn("eventCategory", "string", values=["Management"])
    .withColumn("recipientAccountId", "string", values=["123456789012"])
    .withColumn("sessionCredentialFromConsole", "string", values=["true"])
    .withColumn("managementEvent", "boolean", values=[True])
    .withColumn(
        "eventTime", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 259200), "
             "\"yyyy-MM-dd'T'HH:mm:ss'Z'\")",
    )
    .withColumn("userName", "string", values=["Mary", "Paulo", "Alice", "Bob"], random=True)
    .withColumn(
        "principalId", "string",
        expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')",
    )
    .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
    .withColumn("eventID", "string", expr="substring(md5(cast(rand() as string)), 1, 36)")
).build()

unusual_geography_logs = (
    unusual_geography_logs_raw
    .withColumn(
        "ip_prefix",
        expr(
            "concat(split(sourceIPAddress, '\\\\.')[0], '.', "
            "split(sourceIPAddress, '\\\\.')[1], '.', "
            "split(sourceIPAddress, '\\\\.')[2], '.*')"
        ),
    )
    .join(spark.table("ip_geolocation"), col("ip_prefix") == col("ip_pattern"), "left")
    .drop("ip_prefix", "ip_pattern")
)

display(unusual_geography_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: High rate of error codes
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC A burst of errors across services can signal several things: an attacker enumerating
# MAGIC permissions (`AccessDenied`), scraping endpoints (`ThrottlingException`), or a misconfiguration
# MAGIC amplifying into cascading `ServiceUnavailable` responses. Defenders treat the spike in
# MAGIC error rate as the signal.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC A set of logs from five AWS services with an ~85% error rate across six error types.

# COMMAND ----------

high_error_rate_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("eventVersion", "string", values=["1.11"])
    .withColumn("awsRegion", "string", values=["us-east-1", "us-west-2", "eu-west-1"], random=True)
    .withColumn(
        "eventSource", "string",
        values=[
            "s3.amazonaws.com",
            "ec2.amazonaws.com",
            "iam.amazonaws.com",
            "lambda.amazonaws.com",
            "dynamodb.amazonaws.com",
        ],
        random=True,
    )
    .withColumn(
        "eventName", "string", baseColumn="eventSource",
        expr="""case eventSource
                when 's3.amazonaws.com' then element_at(array('GetObject', 'PutObject', 'DeleteObject', 'ListBucket'), cast(rand() * 4 as int) + 1)
                when 'ec2.amazonaws.com' then element_at(array('RunInstances', 'TerminateInstances', 'StopInstances'), cast(rand() * 3 as int) + 1)
                when 'iam.amazonaws.com' then element_at(array('CreateUser', 'DeleteUser', 'AttachUserPolicy'), cast(rand() * 3 as int) + 1)
                when 'lambda.amazonaws.com' then element_at(array('InvokeFunction', 'CreateFunction'), cast(rand() * 2 as int) + 1)
                else 'PutItem' end"""
    )
    .withColumn(
        "ipPrefix", "string",
        values=["192.168.1.", "203.0.113.", "198.51.100."], random=True, omit=True,
    )
    .withColumn(
        "sourceIPAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn(
        "userAgent", "string",
        values=["aws-cli/2.0.0", "aws-sdk-python/1.26.0", "boto3/1.20.0", "aws-sdk-java/1.12.0"],
        random=True,
    )
    .withColumn(
        "errorCode", "string",
        values=[
            "AccessDenied", "UnauthorizedOperation", "InvalidParameter",
            "ThrottlingException", "ResourceNotFoundException", "ServiceUnavailable",
        ],
        percentNulls=0.15, random=True,
    )
    .withColumn(
        "errorMessage", "string", baseColumn="errorCode",
        expr="""case errorCode
                when 'AccessDenied' then 'User is not authorized to perform this action'
                when 'UnauthorizedOperation' then 'You are not authorized to perform this operation'
                when 'InvalidParameter' then 'Invalid parameter value provided'
                when 'ThrottlingException' then 'Rate exceeded'
                when 'ResourceNotFoundException' then 'The specified resource does not exist'
                when 'ServiceUnavailable' then 'Service is temporarily unavailable' end""",
    )
    .withColumn(
        "readOnly", "boolean", baseColumn="eventName",
        expr="eventName like 'Get%' or eventName like 'List%' or eventName like 'Describe%'",
    )
    .withColumn("eventType", "string", values=["AwsApiCall"])
    .withColumn("eventCategory", "string", values=["Management"])
    .withColumn("recipientAccountId", "string", values=["123456789012"])
    .withColumn("sessionCredentialFromConsole", "string", values=["true"])
    .withColumn("managementEvent", "boolean", values=[True])
    .withColumn(
        "eventTime", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 172800), "
             "\"yyyy-MM-dd'T'HH:mm:ss'Z'\")",
    )
    .withColumn(
        "userName", "string",
        values=["Mary", "Paulo", "Alice", "Bob", "Charlie", "David"], random=True,
    )
    .withColumn(
        "principalId", "string",
        expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')",
    )
    .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
    .withColumn("eventID", "string", expr="substring(md5(cast(rand() as string)), 1, 36)")
).build()

display(high_error_rate_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Privilege escalation and IAM abuse
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC After initial access, attackers try to escalate: create new access keys, attach
# MAGIC admin policies, set themselves as the default version of a policy, or assume roles
# MAGIC they don't own. In a healthy environment, most of these attempts are blocked with
# MAGIC `AccessDenied`, but the attempt itself is a strong signal.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC All events target IAM or STS. The `eventName` is drawn from a list of escalation-oriented
# MAGIC operations. ~75% of attempts are blocked. The `errorMessage` for `AccessDenied`
# MAGIC includes the blocked user's ARN and the operation they tried, closely matching events
# MAGIC emitted by CloudTrail.

# COMMAND ----------

iam_escalation_operations = [
    "CreateAccessKey", "CreateUser", "AttachUserPolicy",
    "AttachRolePolicy", "PutUserPolicy", "PutRolePolicy",
    "CreatePolicyVersion", "SetDefaultPolicyVersion", "UpdateAssumeRolePolicy",
    "AddUserToGroup", "CreateRole",
]
sts_operations = ["AssumeRole", "GetSessionToken"]
iam_array_sql = "array(" + ", ".join(f"'{op}'" for op in iam_escalation_operations) + ")"
sts_array_sql = "array(" + ", ".join(f"'{op}'" for op in sts_operations) + ")"
iam_event_name = (
    f"""case eventSource
    when 'iam.amazonaws.com' then element_at({iam_array_sql}, cast(rand() * {len(iam_escalation_operations)} as int) + 1)
    else element_at({sts_array_sql}, cast(rand() * {len(sts_operations)} as int) + 1) end"""
)

privilege_escalation_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("eventVersion", "string", values=["1.11"])
    .withColumn("awsRegion", "string", values=["us-east-1"])
    .withColumn("eventSource", "string", values=["iam.amazonaws.com", "sts.amazonaws.com"], random=True)
    .withColumn("eventName", "string", baseColumn="eventSource", expr=iam_event_name)
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "198.51.100.", "192.168.1."], random=True, omit=True,
    )
    .withColumn(
        "sourceIPAddress", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn(
        "userAgent", "string",
        values=["aws-cli/2.0.0", "aws-sdk-python/1.26.0", "Boto3/1.20.0", "aws-cli/1.18.0"],
        random=True,
    )
    .withColumn(
        "userName", "string",
        values=[
            "developer1", "contractor", "temp_user", "service_account",
            "admin_backup", "test_user", "Mary", "Bob",
        ],
        random=True,
    )
    .withColumn(
        "errorCode", "string",
        values=["AccessDenied", "UnauthorizedOperation"], percentNulls=0.25, random=True,
    )
    .withColumn(
        "errorMessage", "string", baseColumn=["errorCode", "userName", "eventName"],
        expr="case errorCode "
             "when 'AccessDenied' then concat('User: arn:aws:iam::123456789012:user/', userName, "
             "' is not authorized to perform: iam:', eventName) "
             "when 'UnauthorizedOperation' then 'You are not authorized to perform this operation' end",
    )
    .withColumn("eventType", "string", values=["AwsApiCall"])
    .withColumn("eventCategory", "string", values=["Management"])
    .withColumn("recipientAccountId", "string", values=["123456789012"])
    .withColumn("sessionCredentialFromConsole", "string", values=["true"])
    .withColumn("managementEvent", "boolean", values=[True])
    .withColumn("readOnly", "boolean", values=[False])
    .withColumn(
        "eventTime", "string",
        expr="date_format(current_timestamp() - make_interval(0, 0, 0, 0, 0, 0, rand() * 86400), "
             "\"yyyy-MM-dd'T'HH:mm:ss'Z'\")",
    )
    .withColumn(
        "principalId", "string",
        expr="concat('AIDA6', upper(substring(md5(cast(rand() as string)), 1, 7)), 'EXAMPLE')",
    )
    .withColumn("requestID", "string", expr="upper(substring(md5(cast(rand() as string)), 1, 16))")
    .withColumn("eventID", "string", expr="substring(md5(cast(rand() as string)), 1, 36)")
).build()

display(privilege_escalation_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap each scenario in the CloudTrail envelope
# MAGIC
# MAGIC Real CloudTrail records nest several fields (e.g. `userIdentity` and `tlsDetails`) into
# MAGIC structs. The helper below adds the nested shape on top of the flat columns we generated
# MAGIC for the previous scenarios.

# COMMAND ----------


def add_cloudtrail_structs(df: DataFrame) -> DataFrame:
    """Wraps the flat generated columns in CloudTrail's nested struct layout."""
    return df.select(
        col("*"),
        struct(
            lit("IAMUser").alias("type"),
            col("principalId"),
            expr("concat('arn:aws:iam::123456789012:user/', userName)").alias("arn"),
            lit("123456789012").alias("accountId"),
            lit("EXAMPLEACCESSKEY123456").alias("accessKeyId"),
            col("userName"),
            struct(
                struct(
                    expr("date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss'Z'\")").alias("creationDate"),
                    lit("false").alias("mfaAuthenticated"),
                ).alias("attributes")
            ).alias("sessionContext"),
        ).alias("userIdentity"),
        struct(
            lit("TLSv1.2").alias("tlsVersion"),
            lit("ECDHE-RSA-AES128-GCM-SHA256").alias("cipherSuite"),
            when(col("eventSource").contains("s3"), lit("s3.amazonaws.com"))
            .when(col("eventSource").contains("iam"), lit("iam.amazonaws.com"))
            .when(col("eventSource").contains("ec2"), lit("ec2.amazonaws.com"))
            .when(col("eventSource").contains("signin"), lit("signin.amazonaws.com"))
            .otherwise(col("eventSource")).alias("clientProvidedHostHeader"),
        ).alias("tlsDetails"),
    ).drop("userName", "principalId")


# COMMAND ----------

high_frequency_login_with_structs = add_cloudtrail_structs(high_frequency_login_logs)
failed_login_with_structs = add_cloudtrail_structs(failed_login_logs)
unusual_geography_with_structs = add_cloudtrail_structs(unusual_geography_logs)
high_error_rate_with_structs = add_cloudtrail_structs(high_error_rate_logs)
privilege_escalation_with_structs = add_cloudtrail_structs(privilege_escalation_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save each dataset to a Delta table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Scenario 1: high-frequency login attempts
(
    high_frequency_login_with_structs
    .write
    .format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.ctrail_high_frequency_login")
)

# COMMAND ----------

# Scenario 2: failed login attempts
(
    failed_login_with_structs
    .write
    .format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.ctrail_failed_logins")
)

# COMMAND ----------

# Scenario 3: unusual geographies
(
    unusual_geography_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.ctrail_unusual_geo")
)

# COMMAND ----------

# Scenario 4: high error rate
(
    high_error_rate_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.ctrail_high_errors")
)

# COMMAND ----------

# Scenario 5: privilege escalation / IAM abuse
(
    privilege_escalation_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.ctrail_privilege_escalation")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics
# MAGIC
# MAGIC One SQL roll-up per scenario so you can eyeball that the intended proportions
# MAGIC (80/20 IP split, 60% failure rate, etc.) actually showed up in the generated data.

# COMMAND ----------

# Scenario 1: high-frequency login
display(spark.sql(f"""
  SELECT
    'High Frequency Login' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT sourceIPAddress) AS unique_ips,
    SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) AS failed_attempts,
    ROUND(100.0 * SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
  FROM {catalog_name}.{schema_name}.ctrail_high_frequency_login
"""))

# COMMAND ----------

# Scenario 2: failed logins
display(spark.sql(f"""
  SELECT
    'Failed Logins' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT userIdentity.userName) AS unique_users_tried,
    SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) AS failed_attempts,
    ROUND(100.0 * SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct
  FROM {catalog_name}.{schema_name}.ctrail_failed_logins
"""))

# COMMAND ----------

# Scenario 3: unusual geographies
display(spark.sql(f"""
  SELECT
    'Unusual Geography' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT country) AS unique_countries,
    SUM(CASE WHEN risk_level = 'suspicious' THEN 1 ELSE 0 END) AS suspicious_country_access,
    ROUND(100.0 * SUM(CASE WHEN risk_level = 'suspicious' THEN 1 ELSE 0 END) / COUNT(*), 2) AS suspicious_pct
  FROM {catalog_name}.{schema_name}.ctrail_unusual_geo
"""))

# COMMAND ----------

# Scenario 4: high error rate
display(spark.sql(f"""
  SELECT
    'High Error Rate' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT errorCode) AS unique_error_types,
    SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) AS total_errors,
    ROUND(100.0 * SUM(CASE WHEN errorCode IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate_pct
  FROM {catalog_name}.{schema_name}.ctrail_high_errors
"""))

# COMMAND ----------

# Scenario 5: privilege escalation
display(spark.sql(f"""
  SELECT
    'Privilege Escalation' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT eventName) AS unique_operations,
    SUM(CASE WHEN errorCode IN ('AccessDenied', 'UnauthorizedOperation') THEN 1 ELSE 0 END) AS blocked_attempts,
    ROUND(100.0 * SUM(CASE WHEN errorCode IN ('AccessDenied', 'UnauthorizedOperation') THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.ctrail_privilege_escalation
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Data
# MAGIC
# MAGIC Five tables have been written in the output location:
# MAGIC
# MAGIC | Table                              | Scenario                              |
# MAGIC | ---------------------------------- | ------------------------------------- |
# MAGIC | `ctrail_high_frequency_login`      | Brute-force login attempts            |
# MAGIC | `ctrail_failed_logins`             | Credential stuffing                   |
# MAGIC | `ctrail_unusual_geo`               | Suspicious geographies                |
# MAGIC | `ctrail_high_errors`               | High API error rate                   |
# MAGIC | `ctrail_privilege_escalation`      | IAM privilege escalation              |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON (CloudTrail format)
# MAGIC
# MAGIC Writes each dataset to a Unity Catalog Volume as JSON files the format CloudTrail
# MAGIC delivers to S3. Replay these files into your SIEM, analytics pipeline, or ML training set.

# COMMAND ----------

base_path = f"/Volumes/{catalog_name}/{schema_name}/cloudtrails/"

tables = [
    (f"{catalog_name}.{schema_name}.ctrail_high_frequency_login", "high_frequency_login"),
    (f"{catalog_name}.{schema_name}.ctrail_failed_logins", "failed_logins"),
    (f"{catalog_name}.{schema_name}.ctrail_unusual_geo", "unusual_geo"),
    (f"{catalog_name}.{schema_name}.ctrail_high_errors", "high_errors"),
    (f"{catalog_name}.{schema_name}.ctrail_privilege_escalation", "privilege_escalation"),
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
