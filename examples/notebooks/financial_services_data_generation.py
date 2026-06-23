# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Services Data Generation with dbldatagen
# MAGIC
# MAGIC This notebook demonstrates how to use the **Databricks Labs Data Generator (`dbldatagen`)**
# MAGIC to synthesize realistic Financial Services data at scale. All data is fully synthetic. No real
# MAGIC PII, account numbers, or financial records are used.
# MAGIC
# MAGIC ### Covered Use Cases
# MAGIC | # | Dataset | Rows | Description |
# MAGIC |---|---------|------|-------------|
# MAGIC | 1 | **Customer Profiles** | 100 K | Retail banking customers with demographics & risk tiers |
# MAGIC | 2 | **Bank Accounts** | 200 K | Checking / savings / loan accounts linked to customers |
# MAGIC | 3 | **Transactions** | 2 M | Card & wire transactions with merchant / channel info |
# MAGIC | 4 | **Credit Card Applications** | 100 K | Underwriting data with FICO, DTI, income, approval flag |
# MAGIC | 5 | **Trade Orders** | 1 M | Equities & fixed-income orders (order management system) |
# MAGIC | 6 | **Fraud Labels** | derived | Rule-based fraud flags joined on the transaction table |

# COMMAND ----------
# MAGIC %pip install dbldatagen
# MAGIC %restart_python

# COMMAND ----------
import dbldatagen as dg

# Let Adaptive Query Execution pick the shuffle partition count for us.
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customer Profiles
# MAGIC
# MAGIC Simulates a retail bank's customer master table.
# MAGIC Fields include demographics, contact info, KYC tier, credit score bucket, and onboarding channel.
# MAGIC
# MAGIC `customer_since` is derived from `date_of_birth` with an `expr` so a customer is never onboarded
# MAGIC before they were born.

# COMMAND ----------

CUSTOMER_COUNT = 100_000

customer_spec = (
    dg.DataGenerator(spark, name="customers", rows=CUSTOMER_COUNT, partitions=8,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    # --- Identity ---
    .withColumn("customer_id", "string", prefix="CUST", baseColumn="id")
    .withColumn("first_name", "string",
                values=["James", "Maria", "David", "Sarah", "Wei",
                        "Aisha", "Carlos", "Priya", "Mohammed", "Emily"],
                random=True)
    .withColumn("last_name", "string",
                values=["Smith", "Johnson", "Williams", "Brown", "Jones",
                        "Garcia", "Miller", "Davis", "Wilson", "Moore"],
                random=True)
    .withColumn("date_of_birth", "date",
                begin="1950-01-01", end="2000-12-31", random=True)
    .withColumn("gender", "string",
                values=["M", "F", "Non-binary", "Prefer not to say"],
                weights=[45, 45, 5, 5])
    # --- Contact ---
    .withColumn("email", "string", baseColumn=["first_name", "last_name"],
                expr="concat(lower(first_name), '.', lower(last_name), id % 9999, '@example.com')")
    .withColumn("phone", "string", template=r"ddd-ddd-dddd")
    .withColumn("state", "string",
                values=["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "NJ", "WA"],
                weights=[14, 10, 9, 8, 5, 5, 5, 4, 4, 4, 4, 4])
    .withColumn("zip_code", "string", template=r"ddddd")
    # --- Banking profile ---
    # Onboard on a random day between birth and the present, so it is always after birth.
    .withColumn("customer_since", "date", baseColumn="date_of_birth",
                expr="date_add(date_of_birth, cast(rand() * datediff(date'2024-01-01', date_of_birth) as int))")
    .withColumn("kyc_tier", "string",
                values=["Basic", "Standard", "Enhanced", "Private Banking"],
                weights=[30, 50, 15, 5])
    .withColumn("credit_score", "integer", minValue=300, maxValue=850, random=True,
                distribution=dg.distributions.Normal(mean=680, stddev=90))
    .withColumn("annual_income", "double", minValue=20_000, maxValue=500_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.000015))
    .withColumn("onboarding_channel", "string",
                values=["Branch", "Online", "Mobile", "Referral", "Employer"],
                weights=[20, 35, 30, 10, 5])
    .withColumn("is_active", "boolean", baseColumn=["credit_score", "annual_income"],
                expr="credit_score > 400 AND annual_income > 25000")
)

customers_df = customer_spec.build()
customers_df.createOrReplaceTempView("customers")

print(f"Customers generated: {customers_df.count():,}")
display(customers_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bank Accounts
# MAGIC
# MAGIC Each customer can hold multiple accounts, linked via the `customer_id` foreign key.
# MAGIC The key is built from `CUSTOMER_COUNT` (ids run `0 .. COUNT-1`), so every account points at a
# MAGIC customer that actually exists and the keys stay valid if you change the counts.

# COMMAND ----------

ACCOUNT_COUNT = 200_000

account_spec = (
    dg.DataGenerator(spark, name="accounts", rows=ACCOUNT_COUNT, partitions=8,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("account_id", "string", prefix="ACCT", baseColumn="id")
    # Link to the customers table (fan-out: ~2 accounts per customer).
    .withColumn("customer_id", "string",
                expr=f"concat('CUST_', cast(rand() * {CUSTOMER_COUNT} as int))")
    .withColumn("account_type", "string",
                values=["Checking", "Savings", "Money Market", "CD", "Home Loan", "Auto Loan", "Personal Loan"],
                weights=[35, 25, 10, 5, 10, 8, 7])
    .withColumn("account_status", "string",
                values=["Active", "Dormant", "Closed", "Restricted"],
                weights=[80, 10, 8, 2])
    .withColumn("open_date", "date",
                begin="2000-01-01", end="2024-06-01", random=True)
    .withColumn("balance", "double",
                minValue=-500, maxValue=250_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.00002))
    .withColumn("interest_rate", "double",
                expr="round(rand() * 0.18, 4)")
    .withColumn("currency", "string",
                values=["USD", "EUR", "GBP", "CAD", "MXN"],
                weights=[85, 6, 4, 3, 2])
    .withColumn("branch_id", "string", template=r"BRdddd", random=True)
)

accounts_df = account_spec.build()
accounts_df.createOrReplaceTempView("accounts")

print(f"Accounts generated: {accounts_df.count():,}")
display(accounts_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transactions
# MAGIC
# MAGIC Payments dataset covering card-present, card-not-present, ACH, wire, and ATM channels.
# MAGIC Timestamps span the last 3 years; amounts follow a long-tailed (exponential) distribution.
# MAGIC
# MAGIC `channel`, `merchant_category`, and `pos_entry_mode` are **correlated** with `txn_type` via
# MAGIC `CASE` expressions — an `ATM Withdrawal` lands on the `ATM` channel with no merchant category,
# MAGIC while card purchases fan out across the card channels and carry a (weighted) merchant category.

# COMMAND ----------

TXN_COUNT = 2_000_000

txn_spec = (
    dg.DataGenerator(spark, name="transactions", rows=TXN_COUNT, partitions=16,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("transaction_id", "string", prefix="TXN", baseColumn="id")
    .withColumn("account_id", "string",
                expr=f"concat('ACCT_', cast(rand() * {ACCOUNT_COUNT} as int))")
    .withColumn("txn_timestamp", "timestamp",
                begin="2022-01-01 00:00:00", end="2025-01-01 00:00:00", random=True)
    .withColumn("amount", "double",
                minValue=0.01, maxValue=50_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.0008))
    # txn_type is the driver; the columns below are derived from it.
    .withColumn("txn_type", "string",
                values=["Purchase", "Refund", "ATM Withdrawal", "ACH Debit", "ACH Credit", "Wire Transfer", "Fee"],
                weights=[55, 10, 10, 10, 8, 5, 2])
    # Channel follows from the transaction type; card purchases/refunds fan out across card channels.
    .withColumn("channel", "string", baseColumn="txn_type",
                expr="""
                    CASE txn_type
                        WHEN 'ATM Withdrawal' THEN 'ATM'
                        WHEN 'ACH Debit'      THEN 'ACH'
                        WHEN 'ACH Credit'     THEN 'ACH'
                        WHEN 'Wire Transfer'  THEN 'Wire'
                        WHEN 'Fee'            THEN 'Branch'
                        ELSE element_at(array('Card Present', 'Card Not Present', 'Mobile'),
                                        cast(rand() * 3 as int) + 1)
                    END
                """)
    # Weighted merchant category, kept only for card purchases/refunds (NULL otherwise).
    .withColumn("merchant_category_raw", "string",
                values=["Grocery", "Gas Station", "Restaurant", "Online Retail", "Travel",
                        "Healthcare", "Entertainment", "Utilities", "Education", "Other"],
                weights=[18, 10, 14, 20, 8, 6, 8, 6, 4, 6], omit=True)
    .withColumn("merchant_category", "string", baseColumn=["txn_type", "merchant_category_raw"],
                expr="CASE WHEN txn_type IN ('Purchase', 'Refund') THEN merchant_category_raw END")
    .withColumn("merchant_id", "string", template=r"MCHddddddd", random=True)
    .withColumn("merchant_state", "string",
                values=["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI"],
                weights=[14, 10, 9, 8, 5, 5, 5, 4, 4, 4])
    # Point-of-sale entry mode follows from the channel.
    .withColumn("pos_entry_mode", "string", baseColumn="channel",
                expr="""
                    CASE channel
                        WHEN 'Card Present'     THEN element_at(array('Chip', 'Swipe', 'Contactless'),
                                                                cast(rand() * 3 as int) + 1)
                        WHEN 'Card Not Present' THEN 'Online'
                        WHEN 'Mobile'           THEN 'Contactless'
                        ELSE 'Unknown'
                    END
                """)
    .withColumn("response_code", "string",
                values=["00", "05", "14", "51", "54", "57", "91"],
                weights=[92, 3, 1, 2, 0.5, 0.5, 1])
    .withColumn("currency", "string",
                values=["USD", "EUR", "GBP", "CAD", "MXN"],
                weights=[85, 6, 4, 3, 2])
)

transactions_df = txn_spec.build()
transactions_df.createOrReplaceTempView("transactions")

print(f"Transactions generated: {transactions_df.count():,}")
display(transactions_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Credit Card Applications
# MAGIC
# MAGIC Underwriting dataset used to train / evaluate credit risk models.
# MAGIC Includes FICO score, debt-to-income ratio, employment tenure, requested credit limit, and approval outcome.
# MAGIC `is_approved` and `approved_credit_limit` are derived from the application's own attributes.

# COMMAND ----------

APP_COUNT = 100_000

app_spec = (
    dg.DataGenerator(spark, name="credit_apps", rows=APP_COUNT, partitions=8,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("application_id", "string", prefix="APP", baseColumn="id")
    .withColumn("customer_id", "string",
                expr=f"concat('CUST_', cast(rand() * {CUSTOMER_COUNT} as int))")
    .withColumn("application_date", "date",
                begin="2020-01-01", end="2024-12-31", random=True)
    .withColumn("product_type", "string",
                values=["Standard", "Rewards", "Secured", "Business", "Premium"],
                weights=[30, 30, 10, 15, 15])
    .withColumn("requested_credit_limit", "double",
                minValue=500, maxValue=50_000, random=True,
                distribution=dg.distributions.Normal(mean=8000, stddev=5000))
    .withColumn("fico_score", "integer",
                minValue=300, maxValue=850, random=True,
                distribution=dg.distributions.Normal(mean=690, stddev=80))
    .withColumn("annual_income", "double",
                minValue=15_000, maxValue=500_000, random=True,
                distribution=dg.distributions.Normal(mean=65_000, stddev=40_000))
    .withColumn("debt_to_income_ratio", "double",
                expr="round(rand() * 0.75, 4)")
    .withColumn("employment_status", "string",
                values=["Employed", "Self-Employed", "Part-Time", "Retired", "Unemployed", "Student"],
                weights=[55, 15, 8, 10, 7, 5])
    .withColumn("employment_tenure_years", "integer",
                minValue=0, maxValue=40, random=True,
                distribution=dg.distributions.Normal(mean=8, stddev=6))
    .withColumn("num_existing_tradelines", "integer",
                minValue=0, maxValue=30, random=True,
                distribution=dg.distributions.Normal(mean=7, stddev=4))
    .withColumn("num_derogatory_marks", "integer",
                minValue=0, maxValue=10, random=True,
                distribution=dg.distributions.Exponential(rate=1.5))
    .withColumn("months_since_last_derog", "integer",
                minValue=0, maxValue=84, random=True)
    # Approval is deterministic: good FICO + low DTI + employed → approve.
    .withColumn("is_approved", "boolean",
                baseColumn=["fico_score", "debt_to_income_ratio",
                            "employment_status", "num_derogatory_marks"],
                expr="""
                    fico_score >= 660
                    AND debt_to_income_ratio <= 0.43
                    AND employment_status IN ('Employed', 'Self-Employed', 'Retired')
                    AND num_derogatory_marks <= 1
                """)
    .withColumn("approved_credit_limit", "double",
                baseColumn=["is_approved", "requested_credit_limit", "annual_income"],
                expr="""
                    CASE WHEN is_approved
                         THEN least(requested_credit_limit, round(annual_income * 0.20, 2))
                         ELSE 0.0
                    END
                """)
)

apps_df = app_spec.build()
apps_df.createOrReplaceTempView("credit_applications")

# Row count and approval count in a single pass.
app_stats = apps_df.selectExpr(
    "count(*) AS total",
    "count_if(is_approved) AS approved",
).first()
print(f"Applications generated: {app_stats.total:,}")
print(f"Approval rate: {app_stats.approved / app_stats.total:.1%}")
display(apps_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Trade Orders (Capital Markets)
# MAGIC
# MAGIC Order management system (OMS) data for equities and fixed income.
# MAGIC Includes order type, execution venue, fill status, and notional value.
# MAGIC `instrument_id` is selected from the `ticker` or `cusip` depending on the `asset_class`.

# COMMAND ----------

ORDER_COUNT = 1_000_000

# Representative universe of securities.
equity_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK.B", "JPM", "V",
                  "UNH", "XOM", "LLY", "AVGO", "MA", "HD", "CVX", "MRK", "PEP", "ABBV"]
bond_cusips = ["912828YB3", "38141GXZ2", "594918BG8", "037833100", "02079K107",
               "084670702", "191216100", "254687106", "369604103", "46625H100"]

order_spec = (
    dg.DataGenerator(spark, name="trade_orders", rows=ORDER_COUNT, partitions=16,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("order_id", "string", prefix="ORD", baseColumn="id")
    .withColumn("trader_id", "string", template=r"TRDddddd", random=True)
    .withColumn("desk_id", "string",
                values=["EQUIT-US", "EQUIT-EU", "FICC-IG", "FICC-HY", "FICC-EM", "MACRO", "STRATS"],
                weights=[25, 15, 20, 15, 10, 10, 5])
    .withColumn("asset_class", "string",
                values=["Equity", "Corporate Bond", "Government Bond", "FX", "Commodity", "Derivative"],
                weights=[40, 20, 15, 10, 8, 7])
    .withColumn("ticker", "string", values=equity_symbols, random=True)
    .withColumn("cusip", "string", values=bond_cusips, random=True)
    .withColumn("instrument_id", "string", baseColumn=["asset_class", "ticker", "cusip"],
                expr="CASE WHEN asset_class = 'Equity' THEN ticker ELSE cusip END")
    .withColumn("side", "string",
                values=["Buy", "Sell", "Short Sell"],
                weights=[48, 45, 7])
    .withColumn("order_type", "string",
                values=["Market", "Limit", "Stop", "Stop Limit", "VWAP", "TWAP"],
                weights=[30, 40, 10, 8, 7, 5])
    .withColumn("quantity", "integer",
                minValue=1, maxValue=100_000, random=True,
                distribution=dg.distributions.Exponential(rate=0.0002))
    .withColumn("limit_price", "double",
                minValue=1.0, maxValue=5000.0, random=True,
                distribution=dg.distributions.Exponential(rate=0.005))
    .withColumn("notional_value", "double", baseColumn=["quantity", "limit_price"],
                expr="round(quantity * limit_price, 2)")
    .withColumn("venue", "string",
                values=["NYSE", "NASDAQ", "BATS", "IEX", "Dark Pool", "OTC"],
                weights=[25, 30, 15, 10, 15, 5])
    .withColumn("order_status", "string",
                values=["Filled", "Partial Fill", "Cancelled", "Rejected", "Pending"],
                weights=[65, 15, 12, 5, 3])
    .withColumn("order_timestamp", "timestamp",
                begin="2023-01-01 09:30:00", end="2024-12-31 16:00:00", random=True)
    .withColumn("settlement_date", "date", baseColumn="order_timestamp",
                expr="date_add(cast(order_timestamp as date), 2)")
)

orders_df = order_spec.build()
orders_df.createOrReplaceTempView("trade_orders")

print(f"Trade orders generated: {orders_df.count():,}")
display(orders_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fraud Labels (Derived via SQL)
# MAGIC
# MAGIC Apply heuristic fraud rules to the transactions table to create a labelled dataset
# MAGIC suitable for training a binary classifier (e.g. a fraud detection model).
# MAGIC
# MAGIC Rules applied:
# MAGIC - Transaction `amount` > \$3,000
# MAGIC - Channel is `Card Not Present`
# MAGIC - `merchant_category` is `Online Retail` or `Travel`
# MAGIC - `response_code` ≠ `00` (declined) — suspicious repeat attempts
# MAGIC - `pos_entry_mode` = `Online` (card-not-present / keyed entry)

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
            (pos_entry_mode = 'Online')
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
                    AND merchant_category IN ('Online Retail', 'Travel')
                    AND pos_entry_mode = 'Online')
                OR
                (response_code != '00' AND channel IN ('Card Not Present', 'Mobile') AND amount > 500)
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

# Total and fraud counts in a single pass.
fraud_stats = fraud_df.selectExpr(
    "count(*) AS total",
    "count_if(is_fraud = 1) AS frauds",
).first()
print(f"Total transactions labelled : {fraud_stats.total:,}")
print(f"Fraud transactions          : {fraud_stats.frauds:,}  ({fraud_stats.frauds / fraud_stats.total:.2%})")
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
# MAGIC ---
# MAGIC ### Notes
# MAGIC - All data is **fully synthetic** — no real PII, account numbers, or financial records are used or implied.
# MAGIC - Row counts are kept modest so the notebook runs quickly. Scale `CUSTOMER_COUNT`, `TXN_COUNT`,
# MAGIC   etc. up to match your workload — the foreign keys follow automatically.
# MAGIC - For billion-row testing, increase `partitions` proportionally and use a multi-node cluster.
# MAGIC - See the [dbldatagen documentation](https://databrickslabs.github.io/dbldatagen) for more on
# MAGIC   distributions, constraints, and CDC data generation.
