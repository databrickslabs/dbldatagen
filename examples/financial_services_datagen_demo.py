# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Services Industry Demo — `dbldatagen`
# MAGIC
# MAGIC This notebook demonstrates how to use the **Databricks Labs Data Generator (`dbldatagen`)**
# MAGIC to synthesize realistic Financial Services data at scale.
# MAGIC
# MAGIC ### Covered Use Cases
# MAGIC | # | Dataset | Rows | Description |
# MAGIC |---|---------|------|-------------|
# MAGIC | 1 | **Customer Profiles** | 1 M | Retail banking customers with demographics & risk tiers |
# MAGIC | 2 | **Bank Accounts** | 2 M | Checking / savings / loan accounts linked to customers |
# MAGIC | 3 | **Transactions** | 50 M | Card & wire transactions with merchant / channel info |
# MAGIC | 4 | **Credit Card Applications** | 500 K | Underwriting data with FICO, DTI, income, approval flag |
# MAGIC | 5 | **Trade Orders** | 10 M | Equities & fixed-income orders (order management system) |
# MAGIC | 6 | **Fraud Labels** | derived | Rule-based fraud flags joined on the transaction table |
# MAGIC
# MAGIC > **Runtime**: Databricks 13.3 LTS or above (Unity Catalog supported).

# COMMAND ----------
# MAGIC %pip install dbldatagen
dbutils.library.restartPython()

# COMMAND ----------
import dbldatagen as dg
from dbldatagen import DataGenerator, fakerText
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Customer Profiles
# MAGIC
# MAGIC Simulates a retail bank's customer master table.  
# MAGIC Fields include demographics, contact info, KYC tier, credit score bucket, and onboarding channel.

# COMMAND ----------

CUSTOMER_COUNT = 1_000_000

customer_spec = (
    DataGenerator(spark, name="customers", rows=CUSTOMER_COUNT, partitions=16, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    # --- Identity ---
    .withColumnSpec("customer_id",      prefix="CUST-", numColumns=1, baseColumn="id")
    .withColumn("first_name",           "string", template=r"\w", values=["James","Maria","David","Sarah","Wei","Aisha","Carlos","Priya","Mohammed","Emily"])
    .withColumn("last_name",            "string", template=r"\w",
                values=["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Moore"])
    .withColumn("date_of_birth",        "date",
                begin="1950-01-01", end="2000-12-31", random=True)
    .withColumn("gender",               "string", values=["M","F","Non-binary","Prefer not to say"],
                weights=[45, 45, 5, 5])
    # --- Contact ---
    .withColumn("email",                "string",
                expr="concat(lower(first_name), '.', lower(last_name), id % 9999, '@example.com')")
    .withColumn("phone",                "string", template=r"ddd-ddd-dddd")
    .withColumn("state",                "string",
                values=["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI","NJ","WA"],
                weights=[14,10,9,8,5,5,5,4,4,4,4,4])
    .withColumn("zip_code",             "string", template=r"ddddd")
    # --- Banking profile ---
    .withColumn("customer_since",       "date",
                begin="2000-01-01", end="2024-01-01", random=True)
    .withColumn("kyc_tier",             "string",
                values=["Basic","Standard","Enhanced","Private Banking"],
                weights=[30, 50, 15, 5])
    .withColumn("credit_score",         "integer", minValue=300, maxValue=850, random=True,
                distribution=dg.distributions.Normal(mean=680, stddev=90))
    .withColumn("annual_income",        "double",  minValue=20_000, maxValue=500_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.000015))
    .withColumn("onboarding_channel",   "string",
                values=["Branch","Online","Mobile","Referral","Employer"],
                weights=[20, 35, 30, 10, 5])
    .withColumn("is_active",            "boolean", expr="credit_score > 400 and annual_income > 25000")
)

customers_df = customer_spec.build()
customers_df.createOrReplaceTempView("customers")
print(f"Customers generated: {customers_df.count():,}")
display(customers_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Bank Accounts
# MAGIC
# MAGIC Each customer can hold multiple accounts.  
# MAGIC Linked via `customer_id` (foreign key relationship).

# COMMAND ----------

ACCOUNT_COUNT = 2_000_000

account_spec = (
    DataGenerator(spark, name="accounts", rows=ACCOUNT_COUNT, partitions=16, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("account_id",       "string", prefix="ACCT-", baseColumn="id")
    # Link to customers table (fan-out: ~2 accounts per customer)
    .withColumn("customer_id",      "string",
                expr="concat('CUST-', cast(cast(rand() * 999999 as int) + 1 as string))")
    .withColumn("account_type",     "string",
                values=["Checking","Savings","Money Market","CD","Home Loan","Auto Loan","Personal Loan"],
                weights=[35, 25, 10, 5, 10, 8, 7])
    .withColumn("account_status",   "string",
                values=["Active","Dormant","Closed","Restricted"],
                weights=[80, 10, 8, 2])
    .withColumn("open_date",        "date",
                begin="2000-01-01", end="2024-06-01", random=True)
    .withColumn("balance",          "double",
                minValue=-500, maxValue=250_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.00002))
    .withColumn("interest_rate",    "double", minValue=0.0, maxValue=0.18, random=True,
                expr="round(rand() * 0.18, 4)")
    .withColumn("currency",         "string",
                values=["USD","EUR","GBP","CAD","MXN"],
                weights=[85, 6, 4, 3, 2])
    .withColumn("branch_id",        "string", template=r"BRdddd", random=True)
)

accounts_df = account_spec.build()
accounts_df.createOrReplaceTempView("accounts")
print(f"Accounts generated: {accounts_df.count():,}")
display(accounts_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — Transactions
# MAGIC
# MAGIC High-volume payments dataset (50 M rows).  
# MAGIC Covers card-present, card-not-present, ACH, wire, and ATM channels.  
# MAGIC Timestamps span the last 3 years; amounts follow a log-normal distribution.

# COMMAND ----------

TXN_COUNT = 50_000_000

txn_spec = (
    DataGenerator(spark, name="transactions", rows=TXN_COUNT, partitions=128, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("transaction_id",   "string", prefix="TXN-", baseColumn="id")
    .withColumn("account_id",       "string",
                expr="concat('ACCT-', cast(cast(rand() * 1999999 as int) + 1 as string))")
    .withColumn("txn_timestamp",    "timestamp",
                begin="2022-01-01 00:00:00", end="2025-01-01 00:00:00", random=True)
    .withColumn("amount",           "double",
                minValue=0.01, maxValue=50_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.0008))
    .withColumn("txn_type",         "string",
                values=["Purchase","Refund","ATM Withdrawal","ACH Debit","ACH Credit","Wire Transfer","Fee"],
                weights=[55, 10, 10, 10, 8, 5, 2])
    .withColumn("channel",          "string",
                values=["Card Present","Card Not Present","Mobile","ATM","Branch","ACH","Wire"],
                weights=[35, 25, 20, 8, 4, 5, 3])
    .withColumn("merchant_category", "string",
                values=["Grocery","Gas Station","Restaurant","Online Retail","Travel","Healthcare",
                        "Entertainment","Utilities","Education","Other"],
                weights=[18, 10, 14, 20, 8, 6, 8, 6, 4, 6])
    .withColumn("merchant_id",      "string", template=r"MCHddddddd", random=True)
    .withColumn("merchant_state",   "string",
                values=["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI"],
                weights=[14,10,9,8,5,5,5,4,4,4])
    .withColumn("pos_entry_mode",   "string",
                values=["Chip","Swipe","Contactless","Manual","Online","Unknown"],
                weights=[40, 15, 25, 3, 15, 2])
    .withColumn("response_code",    "string",
                values=["00","05","14","51","54","57","91"],
                weights=[92, 3, 1, 2, 0.5, 0.5, 1])
    .withColumn("currency",         "string",
                values=["USD","EUR","GBP","CAD","MXN"],
                weights=[85, 6, 4, 3, 2])
)

transactions_df = txn_spec.build()
transactions_df.createOrReplaceTempView("transactions")
print(f"Transactions generated: {transactions_df.count():,}")
display(transactions_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — Credit Card Applications
# MAGIC
# MAGIC Underwriting dataset used to train / evaluate credit risk models.  
# MAGIC Includes FICO score, debt-to-income ratio, employment tenure, requested credit limit, and approval outcome.

# COMMAND ----------

APP_COUNT = 500_000

app_spec = (
    DataGenerator(spark, name="credit_apps", rows=APP_COUNT, partitions=16, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("application_id",       "string", prefix="APP-", baseColumn="id")
    .withColumn("customer_id",          "string",
                expr="concat('CUST-', cast(cast(rand() * 999999 as int) + 1 as string))")
    .withColumn("application_date",     "date",
                begin="2020-01-01", end="2024-12-31", random=True)
    .withColumn("product_type",         "string",
                values=["Standard","Rewards","Secured","Business","Premium"],
                weights=[30, 30, 10, 15, 15])
    .withColumn("requested_credit_limit","double",
                minValue=500, maxValue=50_000, random=True,
                distribution=dg.distributions.Normal(mean=8000, stddev=5000))
    .withColumn("fico_score",           "integer",
                minValue=300, maxValue=850, random=True,
                distribution=dg.distributions.Normal(mean=690, stddev=80))
    .withColumn("annual_income",        "double",
                minValue=15_000, maxValue=500_000, random=True,
                distribution=dg.distributions.Normal(mean=65_000, stddev=40_000))
    .withColumn("debt_to_income_ratio", "double",
                minValue=0.0, maxValue=0.75, random=True,
                expr="round(rand() * 0.75, 4)")
    .withColumn("employment_status",    "string",
                values=["Employed","Self-Employed","Part-Time","Retired","Unemployed","Student"],
                weights=[55, 15, 8, 10, 7, 5])
    .withColumn("employment_tenure_years","integer",
                minValue=0, maxValue=40, random=True,
                distribution=dg.distributions.Normal(mean=8, stddev=6))
    .withColumn("num_existing_tradelines","integer",
                minValue=0, maxValue=30, random=True,
                distribution=dg.distributions.Normal(mean=7, stddev=4))
    .withColumn("num_derogatory_marks",  "integer",
                minValue=0, maxValue=10, random=True,
                distribution=dg.distributions.Exponential(rate=1.5))
    .withColumn("months_since_last_derog","integer",
                minValue=0, maxValue=84, random=True)
    # Approval is deterministic: good FICO + low DTI + employed → approve
    .withColumn("is_approved",          "boolean",
                expr="""
                    fico_score >= 660
                    AND debt_to_income_ratio <= 0.43
                    AND employment_status IN ('Employed','Self-Employed','Retired')
                    AND num_derogatory_marks <= 1
                """)
    .withColumn("approved_credit_limit", "double",
                expr="""
                    CASE WHEN is_approved
                         THEN least(requested_credit_limit, round(annual_income * 0.20, 2))
                         ELSE 0.0
                    END
                """)
)

apps_df = app_spec.build()
apps_df.createOrReplaceTempView("credit_applications")
print(f"Applications generated: {apps_df.count():,}")
approval_rate = apps_df.filter("is_approved").count() / apps_df.count()
print(f"Approval rate: {approval_rate:.1%}")
display(apps_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 — Trade Orders (Capital Markets)
# MAGIC
# MAGIC Order management system (OMS) data for equities and fixed income.  
# MAGIC Includes order type, execution venue, fill status, and notional value.

# COMMAND ----------

ORDER_COUNT = 10_000_000

# Representative universe of securities
equity_symbols   = ["AAPL","MSFT","GOOGL","AMZN","NVDA","TSLA","META","BRK.B","JPM","V",
                    "UNH","XOM","LLY","AVGO","MA","HD","CVX","MRK","PEP","ABBV"]
bond_cusips      = ["912828YB3","38141GXZ2","594918BG8","037833100","02079K107",
                    "084670702","191216100","254687106","369604103","46625H100"]

order_spec = (
    DataGenerator(spark, name="trade_orders", rows=ORDER_COUNT, partitions=64, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("order_id",         "string", prefix="ORD-", baseColumn="id")
    .withColumn("trader_id",        "string", template=r"TRDddddd", random=True)
    .withColumn("desk_id",          "string",
                values=["EQUIT-US","EQUIT-EU","FICC-IG","FICC-HY","FICC-EM","MACRO","STRATS"],
                weights=[25, 15, 20, 15, 10, 10, 5])
    .withColumn("asset_class",      "string",
                values=["Equity","Corporate Bond","Government Bond","FX","Commodity","Derivative"],
                weights=[40, 20, 15, 10, 8, 7])
    .withColumn("ticker",           "string", values=equity_symbols,  random=True)
    .withColumn("cusip",            "string", values=bond_cusips,      random=True)
    .withColumn("instrument_id",    "string",
                expr="CASE WHEN asset_class = 'Equity' THEN ticker ELSE cusip END")
    .withColumn("side",             "string",
                values=["Buy","Sell","Short Sell"],
                weights=[48, 45, 7])
    .withColumn("order_type",       "string",
                values=["Market","Limit","Stop","Stop Limit","VWAP","TWAP"],
                weights=[30, 40, 10, 8, 7, 5])
    .withColumn("quantity",         "integer",
                minValue=1, maxValue=100_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.0002))
    .withColumn("limit_price",      "double",
                minValue=1.0, maxValue=5000.0, random=True,
                distribution=dg.distributions.Exponential(rate=0.005))
    .withColumn("notional_value",   "double",
                expr="round(quantity * limit_price, 2)")
    .withColumn("venue",            "string",
                values=["NYSE","NASDAQ","BATS","IEX","Dark Pool","OTC"],
                weights=[25, 30, 15, 10, 15, 5])
    .withColumn("order_status",     "string",
                values=["Filled","Partial Fill","Cancelled","Rejected","Pending"],
                weights=[65, 15, 12, 5, 3])
    .withColumn("order_timestamp",  "timestamp",
                begin="2023-01-01 09:30:00", end="2024-12-31 16:00:00", random=True)
    .withColumn("settlement_date",  "date",
                expr="date_add(cast(order_timestamp as date), 2)")
)

orders_df = order_spec.build()
orders_df.createOrReplaceTempView("trade_orders")
print(f"Trade orders generated: {orders_df.count():,}")
display(orders_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 — Fraud Labels (Derived via SQL)
# MAGIC
# MAGIC Apply heuristic fraud rules to the transactions table to create a labelled dataset  
# MAGIC suitable for training a binary classifier (e.g. a fraud detection model).
# MAGIC
# MAGIC Rules applied:
# MAGIC - Transaction `amount` > \$3,000  
# MAGIC - Channel is `Card Not Present`  
# MAGIC - `merchant_category` is `Online Retail` or `Travel`  
# MAGIC - `response_code` ≠ `00` (declined) — suspicious repeat attempts  
# MAGIC - `pos_entry_mode` = `Manual`

# COMMAND ----------

fraud_df = spark.sql("""
    SELECT
        transaction_id,
        account_id,
        txn_timestamp,
        amount,
        txn_type,
        channel,
        merchant_category,
        pos_entry_mode,
        response_code,
        -- Rule-based fraud scoring
        CAST(
            (amount > 3000.0) AND
            (channel = 'Card Not Present') AND
            (merchant_category IN ('Online Retail', 'Travel')) AND
            (pos_entry_mode IN ('Online', 'Manual'))
        AS INT) AS is_fraud_rule_1,

        CAST(
            response_code != '00' AND
            channel IN ('Card Not Present', 'Mobile') AND
            amount > 500
        AS INT) AS is_fraud_rule_2,

        -- Combined fraud label
        CAST(
            (
                (amount > 3000.0 AND channel = 'Card Not Present'
                    AND merchant_category IN ('Online Retail','Travel')
                    AND pos_entry_mode IN ('Online','Manual'))
                OR
                (response_code != '00' AND channel IN ('Card Not Present','Mobile') AND amount > 500)
            )
        AS INT) AS is_fraud,

        -- Add a soft probability for model training
        ROUND(
            CASE
                WHEN response_code != '00' AND amount > 3000 AND channel = 'Card Not Present' THEN 0.90
                WHEN response_code != '00' AND amount > 1000 THEN 0.70
                WHEN amount > 3000 AND channel = 'Card Not Present' THEN 0.55
                WHEN response_code != '00' THEN 0.40
                ELSE rand() * 0.05
            END, 4
        ) AS fraud_probability
    FROM transactions
""")

fraud_df.createOrReplaceTempView("transactions_with_fraud")
total   = fraud_df.count()
frauds  = fraud_df.filter("is_fraud = 1").count()
print(f"Total transactions labelled : {total:,}")
print(f"Fraud transactions          : {frauds:,}  ({frauds/total:.2%})")
display(fraud_df.filter("is_fraud = 1").limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

spark.sql("""
SELECT
    'Customers'           AS dataset, COUNT(*) AS row_count FROM customers
UNION ALL SELECT 'Accounts',          COUNT(*) FROM accounts
UNION ALL SELECT 'Transactions',      COUNT(*) FROM transactions
UNION ALL SELECT 'Credit Apps',       COUNT(*) FROM credit_applications
UNION ALL SELECT 'Trade Orders',      COUNT(*) FROM trade_orders
UNION ALL SELECT 'Fraud Labels',      COUNT(*) FROM transactions_with_fraud
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optional — Persist to Delta Tables

# COMMAND ----------

# Uncomment to write all datasets to Delta (requires a catalog / schema to exist)
#
# TARGET_CATALOG = "main"
# TARGET_SCHEMA  = "finserv_synthetic"
#
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
#
# datasets = {
#     "customers":                customers_df,
#     "accounts":                 accounts_df,
#     "transactions":             transactions_df,
#     "credit_applications":      apps_df,
#     "trade_orders":             orders_df,
#     "transactions_with_fraud":  fraud_df,
# }
#
# for table_name, df in datasets.items():
#     full_name = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
#     df.write.format("delta").mode("overwrite").saveAsTable(full_name)
#     print(f"Saved {full_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### Notes
# MAGIC - All data is **fully synthetic** — no real PII, account numbers, or financial records are used or implied.
# MAGIC - Row counts above are representative; scale `CUSTOMER_COUNT`, `TXN_COUNT`, etc. to match your workload.
# MAGIC - For billion-row testing, increase `partitions` proportionally and use a multi-node cluster (≥ 8 workers).
# MAGIC - See the [dbldatagen documentation](https://databrickslabs.github.io/dbldatagen) for more on distributions, constraints, and CDC data generation.