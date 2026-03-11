# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Cleanup & Setup
# MAGIC
# MAGIC Drops all `dbldatagen_v1_*` tables and creates the shared summary table.

# COMMAND ----------

dbutils.widgets.text("catalog", "anup_kalburgi", "Catalog")
dbutils.widgets.text("schema", "datagen_demo", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PREFIX = "dbldatagen_v1_"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop all existing tables with our prefix

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
dropped = []
for row in tables:
    tbl = row["tableName"]
    if tbl.startswith(PREFIX):
        fqn = f"{CATALOG}.{SCHEMA}.{tbl}"
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        dropped.append(tbl)

print(f"Dropped {len(dropped)} tables: {dropped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create summary table

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{PREFIX}test_summary (
    test_name STRING,
    table_name STRING,
    check_name STRING,
    row_count LONG,
    expected_row_count LONG,
    passed BOOLEAN,
    details STRING,
    timestamp TIMESTAMP
)
"""
)

print(f"Summary table ready: {CATALOG}.{SCHEMA}.{PREFIX}test_summary")
