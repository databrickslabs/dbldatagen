# Databricks notebook source
# MAGIC %md
# MAGIC # Akamai WAF Security Scenarios
# MAGIC
# MAGIC Generates records for each security scenario:
# MAGIC 1. SQL Injection attacks
# MAGIC 2. Cross-Site Scripting (XSS) attacks
# MAGIC 3. Remote File Inclusion (RFI) / Local File Inclusion (LFI)
# MAGIC 4. DDoS / Rate limiting events
# MAGIC 5. Bot detection and mitigation
# MAGIC 6. API abuse and anomalous requests
# MAGIC
# MAGIC Based on Akamai SIEM Integration API log format

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
# MAGIC - All Delta tables will be saved to: `{catalog_name}.{schema_name}.akamai_*`
# MAGIC - JSON exports will be saved to: `/Volumes/{catalog_name}/{schema_name}/akamai_waf/`
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
from pyspark.sql.functions import col, struct, when, lit, array, expr, base64, monotonically_increasing_id, split, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mock Geolocation Data for Akamai WAF

# COMMAND ----------

# Create geolocation data for attack sources
attack_geo_locations = [
    # Normal traffic (legitimate users)
    ("192.168.1.*", "11734", "Northport", "ZZ", "NP", "normal"),
    ("10.0.0.*", "7922", "Westbridge", "ZZ", "WB", "normal"),
    ("172.16.0.*", "20940", "Centralview", "ZZ", "CV", "normal"),
    # Suspicious/Attack sources (fictitious locations)
    ("203.0.113.*", "12876", "Shadowmere", "XX", "SM", "suspicious"),
    ("198.51.100.*", "4134", "Darkwater", "XX", "DW", "suspicious"),
    ("45.33.32.*", "24940", "Greystone", "XX", "GS", "suspicious"),
    ("185.220.101.*", "63023", "Phantom Bay", "XX", "PB", "suspicious"),  # Common VPN/Tor exit
    ("91.108.56.*", "15895", "Nebula City", "XX", "NC", "suspicious"),
    ("104.244.42.*", "29465", "Voidhaven", "XX", "VH", "suspicious"),
]

geo_df = spark.createDataFrame(attack_geo_locations, ["ip_pattern", "asn", "city", "country", "regionCode", "risk_category"])
geo_df.createOrReplaceTempView("akamai_geolocation")

display(geo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1: SQL Injection Attacks

# COMMAND ----------

def generate_sql_injection_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: SQL Injection attack attempts
    - 85% denied by WAF
    - Various SQL injection patterns
    - Multiple attack vectors
    """
    
    # SQL injection patterns - properly escaped for SQL
    sql_injection_patterns = [
        "\\' OR \\'1\\'=\\'1",
        "\\' OR 1=1--",
        "\\'; DROP TABLE users--",
        "\\' UNION SELECT NULL--",
        "admin\\'--",
        "\\' OR \\'1\\'=\\'1\\' /*"
    ]
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("type", "string", values=["akamai_siem"])
        .withColumn("format", "string", values=["json"])
        .withColumn("version", "string", values=["1.0"])
        
        # Attack data
        .withColumn("_clientIP", "string",
                   expr="concat(case cast(rand() * 5 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '198.51.100.' " +
                        "when 2 then '45.33.32.' " +
                        "when 3 then '185.220.101.' " +
                        "else '192.168.1.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("_configId", "string",
                   expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
        .withColumn("_policyId", "string",
                   expr="concat('sqli_policy_', cast(rand() * 100 as int))")
        
        # 85% denied
        .withColumn("_ruleActions", "string",
                   expr="case when rand() < 0.85 then 'deny' else 'alert' end")
        
        # SQL injection patterns - use values instead of expr to avoid escaping issues
        .withColumn("_sqlPattern", "string",
                   values=["' OR '1'='1", "' OR 1=1--", "'; DROP TABLE users--", 
                          "' UNION SELECT NULL--", "admin'--", "' OR '1'='1' /*"])
        
        .withColumn("_ruleData", "string",
                   expr="concat('SQL Injection detected in query parameter: ', _sqlPattern)")
        .withColumn("_ruleMessages", "string",
                   expr="concat('Attack detected - SQL Injection pattern: ', _sqlPattern)")
        .withColumn("_ruleTags", "string", values=["SQL_INJECTION/WEB_ATTACK/SQLI"])
        .withColumn("_rules", "string", values=["950901", "981242", "981243", "981244"])
        
        # HTTP request details
        .withColumn("_method", "string",
                   expr="case cast(rand() * 3 as int) when 0 then 'GET' when 1 then 'POST' else 'PUT' end")
        .withColumn("_host", "string",
                   expr="case cast(rand() * 4 as int) " +
                        "when 0 then 'api.example.com' " +
                        "when 1 then 'www.example.com' " +
                        "when 2 then 'shop.example.com' " +
                        "else 'admin.example.com' end")
        .withColumn("_path", "string",
                   expr="case cast(rand() * 5 as int) " +
                        "when 0 then '/search' " +
                        "when 1 then '/login' " +
                        "when 2 then '/api/users' " +
                        "when 3 then '/products' " +
                        "else '/admin/dashboard' end")
        .withColumn("_query", "string",
                   expr="concat('q=', _sqlPattern, '&page=1')")
        .withColumn("_protocol", "string", values=["HTTP/1.1", "HTTP/2"])
        .withColumn("_status", "integer",
                   expr="case when _ruleActions = 'deny' then 403 when rand() < 0.3 then 500 else 200 end")
        .withColumn("_bytes", "integer", expr="cast(rand() * 5000 + 500 as int)")
        .withColumn("_userAgent", "string",
                   values=["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", 
                          "Python-urllib/3.8",
                          "curl/7.68.0",
                          "sqlmap/1.5"])
        
        # Event type
        .withColumn("_eventTypeId", "string", values=["1001"])
        .withColumn("_eventTypeName", "string", values=["SQL Injection"])
        .withColumn("_eventName", "string", values=["SQL Injection Attack Detected"])
        
        .withColumn("_timestamp", "long",
                   expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 3600 as int)) * 1000 as long)")
    )
    
    df = data_gen.build()
    
    # Add geolocation using split to get first 3 octets
    # Split IP by '.', take first 3 parts, and add '.*' pattern
    ip_parts = split(col("_clientIP"), "\\.")
    df_with_geo = df.withColumn("ip_prefix", 
        concat(ip_parts[0], lit("."), ip_parts[1], lit("."), ip_parts[2], lit(".*")))
    
    df_with_geo = df_with_geo.join(
        spark.table("akamai_geolocation"),
        df_with_geo["ip_prefix"] == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern", "risk_category")
    
    return df_with_geo

# Generate SQL injection attack attempts (malicious SQL in HTTP requests)
sql_injection_attack_logs = generate_sql_injection_scenario(spark, base_rows, partitions)
display(sql_injection_attack_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Cross-Site Scripting (XSS) Attacks

# COMMAND ----------

def generate_xss_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: XSS attack attempts
    - 80% blocked
    - Various XSS payloads
    - Reflected and stored XSS patterns
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("type", "string", values=["akamai_siem"])
        .withColumn("format", "string", values=["json"])
        .withColumn("version", "string", values=["1.0"])
        
        .withColumn("_clientIP", "string",
                   expr="concat(case cast(rand() * 6 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '198.51.100.' " +
                        "when 2 then '45.33.32.' " +
                        "when 3 then '185.220.101.' " +
                        "when 4 then '91.108.56.' " +
                        "else '192.168.1.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("_configId", "string",
                   expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
        .withColumn("_policyId", "string",
                   expr="concat('xss_policy_', cast(rand() * 100 as int))")
        
        # 80% blocked
        .withColumn("_ruleActions", "string",
                   expr="case when rand() < 0.8 then 'deny' else 'alert' end")
        
        # XSS payloads - use values to avoid escaping issues
        .withColumn("_xssPayload", "string",
                   values=["<script>alert('XSS')</script>",
                          "<img src=x onerror=alert('XSS')>",
                          "<svg onload=alert('XSS')>",
                          "javascript:alert('XSS')",
                          "<iframe src=javascript:alert('XSS')>",
                          "<body onload=alert('XSS')>"])
        
        .withColumn("_ruleData", "string",
                   expr="concat('XSS attack detected in input: ', _xssPayload)")
        .withColumn("_ruleMessages", "string",
                   expr="concat('Cross-Site Scripting attempt blocked: ', _xssPayload)")
        .withColumn("_ruleTags", "string", values=["XSS/WEB_ATTACK/CROSS_SITE_SCRIPTING"])
        .withColumn("_rules", "string", values=["941100", "941110", "941120", "941130"])
        
        .withColumn("_method", "string",
                   expr="case cast(rand() * 3 as int) when 0 then 'GET' when 1 then 'POST' else 'PUT' end")
        .withColumn("_host", "string",
                   values=["www.example.com", "blog.example.com", "forum.example.com", "shop.example.com"])
        .withColumn("_path", "string",
                   expr="case cast(rand() * 5 as int) " +
                        "when 0 then '/comment' " +
                        "when 1 then '/search' " +
                        "when 2 then '/profile' " +
                        "when 3 then '/post' " +
                        "else '/feedback' end")
        .withColumn("_query", "string",
                   expr="concat('input=', replace(_xssPayload, ' ', '%20'))")
        .withColumn("_protocol", "string", values=["HTTP/1.1", "HTTP/2"])
        .withColumn("_status", "integer",
                   expr="case when _ruleActions = 'deny' then 403 else 200 end")
        .withColumn("_bytes", "integer", expr="cast(rand() * 3000 + 300 as int)")
        .withColumn("_userAgent", "string",
                   values=["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", 
                          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                          "curl/7.68.0"])
        
        .withColumn("_eventTypeId", "string", values=["1002"])
        .withColumn("_eventTypeName", "string", values=["Cross-Site Scripting"])
        .withColumn("_eventName", "string", values=["XSS Attack Detected"])
        
        .withColumn("_timestamp", "long",
                   expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 7200 as int)) * 1000 as long)")
    )
    
    df = data_gen.build()
    
    # Add geolocation using split to get first 3 octets
    # Split IP by '.', take first 3 parts, and add '.*' pattern
    ip_parts = split(col("_clientIP"), "\\.")
    df_with_geo = df.withColumn("ip_prefix", 
        concat(ip_parts[0], lit("."), ip_parts[1], lit("."), ip_parts[2], lit(".*")))
    
    df_with_geo = df_with_geo.join(
        spark.table("akamai_geolocation"),
        df_with_geo["ip_prefix"] == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern", "risk_category")
    
    return df_with_geo

# Generate cross-site scripting (XSS) attack attempts (malicious JavaScript)
xss_attack_logs = generate_xss_scenario(spark, base_rows, partitions)
display(xss_attack_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Remote/Local File Inclusion (RFI/LFI)

# COMMAND ----------

def generate_file_inclusion_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: RFI/LFI attack attempts
    - 90% blocked
    - Path traversal attempts
    - Remote file inclusion
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("type", "string", values=["akamai_siem"])
        .withColumn("format", "string", values=["json"])
        .withColumn("version", "string", values=["1.0"])
        
        .withColumn("_clientIP", "string",
                   expr="concat(case cast(rand() * 5 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '198.51.100.' " +
                        "when 2 then '45.33.32.' " +
                        "when 3 then '185.220.101.' " +
                        "else '104.244.42.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("_configId", "string",
                   expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
        .withColumn("_policyId", "string",
                   expr="concat('rfi_policy_', cast(rand() * 100 as int))")
        
        # 90% denied
        .withColumn("_ruleActions", "string",
                   expr="case when rand() < 0.9 then 'deny' else 'alert' end")
        
        # File inclusion patterns - use values to avoid escaping issues
        .withColumn("_filePattern", "string",
                   values=["../../../../etc/passwd",
                          "../../../../../../windows/system32/config/sam",
                          "http://evil.com/shell.txt",
                          "php://filter/convert.base64-encode/resource=index.php",
                          "file:///etc/passwd",
                          "....//....//....//etc/passwd"])
        
        .withColumn("_ruleData", "string",
                   expr="concat('File Inclusion attack detected: ', _filePattern)")
        .withColumn("_ruleMessages", "string",
                   expr="concat('RFI/LFI attempt blocked - path traversal: ', _filePattern)")
        .withColumn("_ruleTags", "string", values=["FILE_INCLUSION/RFI/LFI/PATH_TRAVERSAL"])
        .withColumn("_rules", "string", values=["930100", "930110", "930120", "930130"])
        
        .withColumn("_method", "string", values=["GET", "POST"])
        .withColumn("_host", "string",
                   values=["api.example.com", "www.example.com", "files.example.com"])
        .withColumn("_path", "string",
                   expr="case cast(rand() * 4 as int) " +
                        "when 0 then '/download' " +
                        "when 1 then '/include' " +
                        "when 2 then '/file' " +
                        "else '/page' end")
        .withColumn("_query", "string",
                   expr="concat('file=', replace(_filePattern, '/', '%2F'))")
        .withColumn("_protocol", "string", values=["HTTP/1.1"])
        .withColumn("_status", "integer",
                   expr="case when _ruleActions = 'deny' then 403 else 500 end")
        .withColumn("_bytes", "integer", expr="cast(rand() * 2000 + 200 as int)")
        .withColumn("_userAgent", "string",
                   values=["Python-urllib/3.8", "curl/7.68.0", "Wget/1.20.3"])
        
        .withColumn("_eventTypeId", "string", values=["1003"])
        .withColumn("_eventTypeName", "string", values=["File Inclusion"])
        .withColumn("_eventName", "string", values=["RFI/LFI Attack Detected"])
        
        .withColumn("_timestamp", "long",
                   expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 1800 as int)) * 1000 as long)")
    )
    
    df = data_gen.build()
    
    # Add geolocation using split to get first 3 octets
    # Split IP by '.', take first 3 parts, and add '.*' pattern
    ip_parts = split(col("_clientIP"), "\\.")
    df_with_geo = df.withColumn("ip_prefix", 
        concat(ip_parts[0], lit("."), ip_parts[1], lit("."), ip_parts[2], lit(".*")))
    
    df_with_geo = df_with_geo.join(
        spark.table("akamai_geolocation"),
        df_with_geo["ip_prefix"] == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern", "risk_category")
    
    return df_with_geo

# Generate file inclusion attacks (path traversal, remote file inclusion)
file_inclusion_attack_logs = generate_file_inclusion_scenario(spark, base_rows, partitions)
display(file_inclusion_attack_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: DDoS / Rate Limiting

# COMMAND ----------

def generate_ddos_rate_limiting_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: DDoS and rate limiting events
    - High request rate from few IPs
    - 95% rate limited
    - Burst traffic patterns
    """
    
    ddos_ips = ["203.0.113.10", "198.51.100.20", "45.33.32.30", "185.220.101.40"]
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("type", "string", values=["akamai_siem"])
        .withColumn("format", "string", values=["json"])
        .withColumn("version", "string", values=["1.0"])
        
        # 90% from attacking IPs
        .withColumn("_clientIP", "string",
                   expr="case when rand() < 0.9 then " +
                        f"case cast(rand() * 4 as int) " +
                        f"when 0 then '{ddos_ips[0]}' " +
                        f"when 1 then '{ddos_ips[1]}' " +
                        f"when 2 then '{ddos_ips[2]}' " +
                        f"else '{ddos_ips[3]}' end " +
                        "else concat('192.168.1.', cast(rand() * 254 + 1 as int)) end")
        
        .withColumn("_configId", "string",
                   expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
        .withColumn("_policyId", "string",
                   expr="concat('rate_limit_policy_', cast(rand() * 50 as int))")
        
        # 95% rate limited
        .withColumn("_ruleActions", "string",
                   expr="case when rand() < 0.95 then 'rate_limit' else 'allow' end")
        
        .withColumn("_ruleData", "string",
                   expr="concat('Rate limit exceeded - ', cast(rand() * 1000 + 1000 as int), ' requests/minute')")
        .withColumn("_ruleMessages", "string",
                   expr="concat('Client ', _clientIP, ' exceeded rate limit threshold')")
        .withColumn("_ruleTags", "string", values=["RATE_LIMIT/DDOS/BURST_TRAFFIC"])
        .withColumn("_rules", "string", values=["RATE-LIMIT-001", "RATE-LIMIT-002"])
        
        .withColumn("_method", "string",
                   expr="case cast(rand() * 4 as int) when 0 then 'GET' when 1 then 'POST' when 2 then 'HEAD' else 'OPTIONS' end")
        .withColumn("_host", "string", values=["api.example.com", "www.example.com"])
        .withColumn("_path", "string",
                   expr="case cast(rand() * 5 as int) " +
                        "when 0 then '/api/v1/users' " +
                        "when 1 then '/api/v1/products' " +
                        "when 2 then '/search' " +
                        "when 3 then '/' " +
                        "else '/api/v1/orders' end")
        .withColumn("_query", "string",
                   expr="concat('page=', cast(rand() * 100 as int))")
        .withColumn("_protocol", "string", values=["HTTP/1.1", "HTTP/2"])
        .withColumn("_status", "integer",
                   expr="case when _ruleActions = 'rate_limit' then 429 else 200 end")
        .withColumn("_bytes", "integer", expr="cast(rand() * 1000 + 100 as int)")
        .withColumn("_userAgent", "string",
                   values=["Mozilla/5.0 (compatible; bot/1.0)", 
                          "Python-requests/2.28.0",
                          "Apache-HttpClient/4.5.13",
                          "Go-http-client/1.1"])
        
        .withColumn("_eventTypeId", "string", values=["1004"])
        .withColumn("_eventTypeName", "string", values=["Rate Limiting"])
        .withColumn("_eventName", "string", values=["Rate Limit Exceeded"])
        
        .withColumn("_timestamp", "long",
                   expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 300 as int)) * 1000 as long)")
    )
    
    df = data_gen.build()
    
    # Add geolocation using split to get first 3 octets
    # Split IP by '.', take first 3 parts, and add '.*' pattern
    ip_parts = split(col("_clientIP"), "\\.")
    df_with_geo = df.withColumn("ip_prefix", 
        concat(ip_parts[0], lit("."), ip_parts[1], lit("."), ip_parts[2], lit(".*")))
    
    df_with_geo = df_with_geo.join(
        spark.table("akamai_geolocation"),
        df_with_geo["ip_prefix"] == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern", "risk_category")
    
    return df_with_geo

# Generate DDoS and rate limiting events (high volume traffic from single IPs)
ddos_rate_limiting_logs = generate_ddos_rate_limiting_scenario(spark, base_rows, partitions)
display(ddos_rate_limiting_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Bot Detection and Mitigation

# COMMAND ----------

def generate_bot_detection_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: Malicious bot activity
    - Web scraping bots
    - Automated attacks
    - 70% blocked
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("type", "string", values=["akamai_siem"])
        .withColumn("format", "string", values=["json"])
        .withColumn("version", "string", values=["1.0"])
        
        .withColumn("_clientIP", "string",
                   expr="concat(case cast(rand() * 5 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '198.51.100.' " +
                        "when 2 then '91.108.56.' " +
                        "when 3 then '185.220.101.' " +
                        "else '104.244.42.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("_configId", "string",
                   expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
        .withColumn("_policyId", "string",
                   expr="concat('bot_policy_', cast(rand() * 100 as int))")
        
        # 70% blocked
        .withColumn("_ruleActions", "string",
                   expr="case when rand() < 0.7 then 'deny' else 'challenge' end")
        
        # Bot user agents - use values to avoid escaping issues
        .withColumn("_botUserAgent", "string",
                   values=["scrapy-bot/1.0",
                          "python-requests/2.28.0",
                          "Selenium/4.0",
                          "PhantomJS/2.1.1",
                          "HeadlessChrome/96.0",
                          "curl/7.68.0"])
        
        .withColumn("_ruleData", "string",
                   expr="concat('Malicious bot detected - User-Agent: ', _botUserAgent)")
        .withColumn("_ruleMessages", "string",
                   expr="concat('Bot signature matched: ', _botUserAgent, ' - Blocking automated traffic')")
        .withColumn("_ruleTags", "string", values=["BOT_DETECTION/AUTOMATED_TRAFFIC/WEB_SCRAPING"])
        .withColumn("_rules", "string", values=["BOT-001", "BOT-002", "BOT-003"])
        
        .withColumn("_method", "string", values=["GET", "POST"])
        .withColumn("_host", "string",
                   values=["www.example.com", "shop.example.com", "api.example.com"])
        .withColumn("_path", "string",
                   expr="case cast(rand() * 6 as int) " +
                        "when 0 then '/products' " +
                        "when 1 then '/search' " +
                        "when 2 then '/api/catalog' " +
                        "when 3 then '/prices' " +
                        "when 4 then '/inventory' " +
                        "else '/sitemap.xml' end")
        .withColumn("_query", "string",
                   expr="concat('page=', cast(rand() * 1000 as int))")
        .withColumn("_protocol", "string", values=["HTTP/1.1", "HTTP/2"])
        .withColumn("_status", "integer",
                   expr="case when _ruleActions = 'deny' then 403 when _ruleActions = 'challenge' then 429 else 200 end")
        .withColumn("_bytes", "integer", expr="cast(rand() * 8000 + 500 as int)")
        .withColumn("_userAgent", "string", expr="_botUserAgent")
        
        .withColumn("_eventTypeId", "string", values=["1005"])
        .withColumn("_eventTypeName", "string", values=["Bot Detection"])
        .withColumn("_eventName", "string", values=["Malicious Bot Detected"])
        
        .withColumn("_timestamp", "long",
                   expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 900 as int)) * 1000 as long)")
    )
    
    df = data_gen.build()
    
    # Add geolocation using split to get first 3 octets
    # Split IP by '.', take first 3 parts, and add '.*' pattern
    ip_parts = split(col("_clientIP"), "\\.")
    df_with_geo = df.withColumn("ip_prefix", 
        concat(ip_parts[0], lit("."), ip_parts[1], lit("."), ip_parts[2], lit(".*")))
    
    df_with_geo = df_with_geo.join(
        spark.table("akamai_geolocation"),
        df_with_geo["ip_prefix"] == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern", "risk_category")
    
    return df_with_geo

# Generate bot detection events (automated traffic, web scrapers, malicious bots)
bot_detection_logs = generate_bot_detection_scenario(spark, base_rows, partitions)
display(bot_detection_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6: API Abuse and Anomalous Requests

# COMMAND ----------

def generate_api_abuse_scenario(spark, num_records=10000, partitions=4):
    """
    Scenario: API abuse and anomalous requests
    - Unusual API patterns
    - Credential stuffing on APIs
    - 75% blocked
    """
    
    data_gen = (
        DataGenerator(spark, rows=num_records, partitions=partitions)
        .withColumn("type", "string", values=["akamai_siem"])
        .withColumn("format", "string", values=["json"])
        .withColumn("version", "string", values=["1.0"])
        
        .withColumn("_clientIP", "string",
                   expr="concat(case cast(rand() * 6 as int) " +
                        "when 0 then '203.0.113.' " +
                        "when 1 then '198.51.100.' " +
                        "when 2 then '45.33.32.' " +
                        "when 3 then '91.108.56.' " +
                        "when 4 then '185.220.101.' " +
                        "else '192.168.1.' end, cast(rand() * 254 + 1 as int))")
        
        .withColumn("_configId", "string",
                   expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
        .withColumn("_policyId", "string",
                   expr="concat('api_abuse_policy_', cast(rand() * 50 as int))")
        
        # 75% blocked
        .withColumn("_ruleActions", "string",
                   expr="case when rand() < 0.75 then 'deny' else 'alert' end")
        
        .withColumn("_apiAbuse", "string",
                   expr="case cast(rand() * 5 as int) " +
                        "when 0 then 'Excessive API calls - credential enumeration' " +
                        "when 1 then 'Invalid API key usage pattern' " +
                        "when 2 then 'Suspicious parameter manipulation' " +
                        "when 3 then 'API endpoint scanning' " +
                        "else 'Unauthorized API access attempt' end")
        
        .withColumn("_ruleData", "string", expr="_apiAbuse")
        .withColumn("_ruleMessages", "string",
                   expr="concat('API abuse detected: ', _apiAbuse)")
        .withColumn("_ruleTags", "string", values=["API_ABUSE/CREDENTIAL_STUFFING/ENUMERATION"])
        .withColumn("_rules", "string", values=["API-001", "API-002", "API-003"])
        
        .withColumn("_method", "string",
                   expr="case cast(rand() * 4 as int) when 0 then 'POST' when 1 then 'GET' when 2 then 'PUT' else 'DELETE' end")
        .withColumn("_host", "string",
                   values=["api.example.com", "api-v2.example.com", "rest.example.com"])
        .withColumn("_path", "string",
                   expr="case cast(rand() * 7 as int) " +
                        "when 0 then '/api/v1/auth/login' " +
                        "when 1 then '/api/v1/users' " +
                        "when 2 then '/api/v1/accounts' " +
                        "when 3 then '/api/v2/payment' " +
                        "when 4 then '/api/v1/admin' " +
                        "when 5 then '/api/v1/tokens' " +
                        "else '/api/v1/data' end")
        .withColumn("_query", "string",
                   expr="concat('apikey=', substring(md5(cast(rand() as string)), 1, 16))")
        .withColumn("_protocol", "string", values=["HTTP/1.1", "HTTP/2"])
        .withColumn("_status", "integer",
                   expr="case when _ruleActions = 'deny' then case cast(rand() * 3 as int) when 0 then 401 when 1 then 403 else 429 end else 200 end")
        .withColumn("_bytes", "integer", expr="cast(rand() * 3000 + 200 as int)")
        .withColumn("_userAgent", "string",
                   values=["PostmanRuntime/7.29.0", 
                          "insomnia/2022.7.0",
                          "Python-requests/2.28.0",
                          "curl/7.68.0",
                          "axios/0.27.2"])
        
        .withColumn("_eventTypeId", "string", values=["1006"])
        .withColumn("_eventTypeName", "string", values=["API Abuse"])
        .withColumn("_eventName", "string", values=["API Abuse Pattern Detected"])
        
        .withColumn("_timestamp", "long",
                   expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 1200 as int)) * 1000 as long)")
    )
    
    df = data_gen.build()
    
    # Add geolocation using split to get first 3 octets
    # Split IP by '.', take first 3 parts, and add '.*' pattern
    ip_parts = split(col("_clientIP"), "\\.")
    df_with_geo = df.withColumn("ip_prefix", 
        concat(ip_parts[0], lit("."), ip_parts[1], lit("."), ip_parts[2], lit(".*")))
    
    df_with_geo = df_with_geo.join(
        spark.table("akamai_geolocation"),
        df_with_geo["ip_prefix"] == col("ip_pattern"),
        "left"
    ).drop("ip_prefix", "ip_pattern", "risk_category")
    
    return df_with_geo

# Generate API abuse events (credential enumeration, suspicious API patterns)
api_abuse_logs = generate_api_abuse_scenario(spark, base_rows, partitions)
display(api_abuse_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Akamai SIEM JSON Structures

# COMMAND ----------

def add_akamai_structs(df):
    """Add proper Akamai SIEM log nested structures"""
    
    df_final = df.select(
        col("type"),
        col("format"),
        col("version"),
        
        # attackData struct
        struct(
            col("_clientIP").alias("clientIP"),
            col("_configId").alias("configId"),
            col("_policyId").alias("policyId"),
            col("_ruleActions").alias("ruleActions"),
            col("_ruleData").alias("ruleData"),
            col("_ruleMessages").alias("ruleMessages"),
            col("_ruleTags").alias("ruleTags"),
            col("_rules").alias("rules")
        ).alias("attackData"),
        
        # geo struct
        when(col("asn").isNotNull(),
            struct(
                col("asn").alias("asn"),
                col("city").alias("city"),
                col("country").alias("country"),
                col("regionCode").alias("regionCode")
            )
        ).alias("geo"),
        
        # httpMessage struct
        struct(
            col("_method").alias("method"),
            col("_host").alias("host"),
            col("_path").alias("path"),
            col("_query").alias("query"),
            col("_protocol").alias("protocol"),
            col("_status").cast("string").alias("status"),
            col("_bytes").cast("string").alias("bytes"),
            col("_userAgent").alias("userAgent")
        ).alias("httpMessage"),
        
        # eventType struct
        struct(
            col("_eventTypeId").alias("eventTypeId"),
            col("_eventTypeName").alias("eventTypeName"),
            col("_eventName").alias("eventName")
        ).alias("eventType"),
        
        col("_timestamp").alias("timestamp")
        
    ).drop("_clientIP", "_configId", "_policyId", "_ruleActions", "_ruleData", 
           "_ruleMessages", "_ruleTags", "_rules", "_method", "_host", "_path",
           "_query", "_protocol", "_status", "_bytes", "_userAgent", "_eventTypeId",
           "_eventTypeName", "_eventName", "_sqlPattern", "_xssPayload", "_filePattern",
           "_botUserAgent", "_apiAbuse", "asn", "city", "country", "regionCode")
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save All Scenarios to Delta Tables

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Transform raw logs into proper Akamai SIEM schema with nested structures
sql_injection_with_structs = add_akamai_structs(sql_injection_attack_logs)
xss_attack_with_structs = add_akamai_structs(xss_attack_logs)
file_inclusion_with_structs = add_akamai_structs(file_inclusion_attack_logs)
ddos_rate_limiting_with_structs = add_akamai_structs(ddos_rate_limiting_logs)
bot_detection_with_structs = add_akamai_structs(bot_detection_logs)
api_abuse_with_structs = add_akamai_structs(api_abuse_logs)

# COMMAND ----------

# Save Scenario 1: SQL Injection Attacks
sql_injection_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.akamai_sql_injection")

# COMMAND ----------

# Save Scenario 2: XSS Attacks
xss_attack_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.akamai_xss")

# COMMAND ----------

# Save Scenario 3: File Inclusion Attacks
file_inclusion_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.akamai_file_inclusion")

# COMMAND ----------

# Save Scenario 4: DDoS / Rate Limiting
ddos_rate_limiting_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.akamai_ddos_rate_limit")

# COMMAND ----------

# Save Scenario 5: Bot Detection
bot_detection_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.akamai_bot_detection")

# COMMAND ----------

# Save Scenario 6: API Abuse
api_abuse_with_structs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.akamai_api_abuse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

## Scenario 1: SQL Injection
display(spark.sql(f"""SELECT 
  'SQL Injection' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT attackData.clientIP) as unique_ips,
  SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) as blocked,
  ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.akamai_sql_injection"""))

# COMMAND ----------

## Scenario 2: XSS
display(spark.sql(f"""SELECT 
  'Cross-Site Scripting' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT attackData.clientIP) as unique_ips,
  SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) as blocked,
  ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.akamai_xss"""))

# COMMAND ----------

## Scenario 3: File Inclusion
display(spark.sql(f"""SELECT 
  'File Inclusion (RFI/LFI)' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT attackData.clientIP) as unique_ips,
  SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) as blocked,
  ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.akamai_file_inclusion"""))

# COMMAND ----------

## Scenario 4: DDoS/Rate Limiting
display(spark.sql(f"""SELECT 
  'DDoS / Rate Limiting' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT attackData.clientIP) as unique_ips,
  SUM(CASE WHEN attackData.ruleActions = 'rate_limit' THEN 1 ELSE 0 END) as rate_limited,
  ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'rate_limit' THEN 1 ELSE 0 END) / COUNT(*), 2) as rate_limit_pct
FROM {catalog_name}.{schema_name}.akamai_ddos_rate_limit"""))

# COMMAND ----------

## Scenario 5: Bot Detection
display(spark.sql(f"""SELECT 
  'Bot Detection' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT attackData.clientIP) as unique_ips,
  SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) as blocked,
  ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.akamai_bot_detection"""))

# COMMAND ----------

## Scenario 6: API Abuse
display(spark.sql(f"""SELECT 
  'API Abuse' as scenario,
  COUNT(*) as total_records,
  COUNT(DISTINCT attackData.clientIP) as unique_ips,
  SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) as blocked,
  ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) as block_rate_pct
FROM {catalog_name}.{schema_name}.akamai_api_abuse"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Complete!
# MAGIC
# MAGIC Generated 6 Akamai WAF security scenarios records each:
# MAGIC
# MAGIC 1. **akamai_sql_injection** - SQL Injection attacks
# MAGIC 2. **akamai_xss_10k** - Cross-Site Scripting (XSS)
# MAGIC 3. **akamai_file_inclusion** - RFI/LFI attacks
# MAGIC 4. **akamai_ddos_rate_limit** - DDoS and rate limiting
# MAGIC 5. **akamai_bot_detection** - Malicious bot activity
# MAGIC 6. **akamai_api_abuse** - API abuse patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON Files (Akamai SIEM Format)
# MAGIC
# MAGIC Export each table to a Volume as JSON files compatible with Akamai SIEM Integration API

# COMMAND ----------

# Base path for JSON exports
base_path = f"/Volumes/{catalog_name}/{schema_name}/akamai_waf/"

# Table configurations
tables = [
    (f"{catalog_name}.{schema_name}.akamai_sql_injection", "sql_injection"),
    (f"{catalog_name}.{schema_name}.akamai_xss", "xss"),
    (f"{catalog_name}.{schema_name}.akamai_file_inclusion", "file_inclusion"),
    (f"{catalog_name}.{schema_name}.akamai_ddos_rate_limit", "ddos_rate_limit"),
    (f"{catalog_name}.{schema_name}.akamai_bot_detection", "bot_detection"),
    (f"{catalog_name}.{schema_name}.akamai_api_abuse", "api_abuse")
]

print("Exporting Akamai WAF data to JSON files...")
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
print("✅ All Akamai WAF exports complete!")
print("\nJSON files available at:")
for _, file_name in tables:
    print(f"  - {base_path}{file_name}/")
print("\nThese JSON files can be used with:")
print("  • Akamai SIEM Integration API")
print("  • Splunk (with Akamai Add-on)")
print("  • SumoLogic")
print("  • QRadar")
print("  • ArcSight")
print("  • Any SIEM tool supporting Akamai log format")



# COMMAND ----------

