# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Test Batch Generation
# MAGIC
# MAGIC Generates all 5 Employee Management tables and verifies row counts,
# MAGIC PK uniqueness, and FK integrity.

# COMMAND ----------

# MAGIC %pip install /Volumes/anup_kalburgi/datagen_demo/dbldatagen/lib/dbldatagen-latest-py3-none-any.whl --quiet
# MAGIC %pip install faker --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
from datetime import datetime

from pyspark.sql import functions as F

from dbldatagen.v1 import (
    DataGenPlan,
    PrimaryKey,
    TableSpec,
    generate,
    fk,
    faker,
    integer,
    pattern,
    pk_auto,
    text,
    timestamp,
)
from dbldatagen.v1.schema import LogNormal

# COMMAND ----------

CATALOG = "anup_kalburgi"
SCHEMA = "datagen_demo"
PREFIX = "dbldatagen_v1_"
TEST_NAME = "batch_generation"
SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.{PREFIX}test_summary"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Employee Management Plan

# COMMAND ----------

def build_employee_plan():
    return DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="departments",
                rows=50,
                primary_key=PrimaryKey(columns=["dept_id"]),
                columns=[
                    pk_auto("dept_id"),
                    text("name", values=[
                        "Engineering", "Sales", "Marketing", "Finance", "HR",
                        "Legal", "Operations", "Support", "Research", "Product",
                    ]),
                    text("location", values=[
                        "New York", "San Francisco", "Chicago", "Austin", "Seattle",
                        "Boston", "Denver", "Miami", "Portland", "Atlanta",
                    ]),
                    integer("budget", min=100_000, max=10_000_000, distribution=LogNormal()),
                ],
            ),
            TableSpec(
                name="employees",
                rows=10_000,
                primary_key=PrimaryKey(columns=["emp_id"]),
                columns=[
                    pk_auto("emp_id"),
                    fk("dept_id", "departments.dept_id"),
                    faker("first_name", "first_name"),
                    faker("last_name", "last_name"),
                    pattern("email", "{alpha:8}@company.com"),
                    timestamp("hire_date", start="2015-01-01", end="2025-12-31"),
                    integer("salary", min=30_000, max=500_000, distribution=LogNormal()),
                    text("status", values=["active", "inactive", "on_leave", "terminated"]),
                ],
            ),
            TableSpec(
                name="projects",
                rows=500,
                primary_key=PrimaryKey(columns=["project_id"]),
                columns=[
                    pk_auto("project_id"),
                    fk("dept_id", "departments.dept_id"),
                    pattern("name", "PRJ-{digit:4}-{alpha:3}"),
                    timestamp("start_date", start="2020-01-01", end="2025-12-31"),
                    integer("budget", min=10_000, max=5_000_000, distribution=LogNormal()),
                    text("priority", values=["low", "medium", "high", "critical"]),
                ],
            ),
            TableSpec(
                name="assignments",
                rows=20_000,
                primary_key=PrimaryKey(columns=["assignment_id"]),
                columns=[
                    pk_auto("assignment_id"),
                    fk("emp_id", "employees.emp_id"),
                    fk("project_id", "projects.project_id"),
                    timestamp("assigned_date", start="2020-01-01", end="2025-12-31"),
                    text("role", values=[
                        "developer", "designer", "tester", "analyst", "lead",
                        "manager", "consultant", "reviewer",
                    ]),
                    integer("hours_allocated", min=10, max=500),
                ],
            ),
            TableSpec(
                name="reviews",
                rows=15_000,
                primary_key=PrimaryKey(columns=["review_id"]),
                columns=[
                    pk_auto("review_id"),
                    fk("emp_id", "employees.emp_id"),
                    timestamp("review_date", start="2020-01-01", end="2025-12-31"),
                    integer("rating", min=1, max=5),
                    text("comments", values=[
                        "Exceeds expectations", "Meets expectations",
                        "Needs improvement", "Outstanding performance",
                        "Satisfactory", "Below average",
                    ]),
                ],
            ),
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Write Tables

# COMMAND ----------

plan = build_employee_plan()
start = time.time()
results = generate(spark, plan)
gen_time = time.time() - start
print(f"Generation took {gen_time:.1f}s")

# COMMAND ----------

for table_name, df in results.items():
    fqn = f"{CATALOG}.{SCHEMA}.{PREFIX}{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
    print(f"Wrote {table_name} -> {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

EXPECTED_ROWS = {
    "departments": 50,
    "employees": 10_000,
    "projects": 500,
    "assignments": 20_000,
    "reviews": 15_000,
}

FK_CHECKS = [
    ("employees", "dept_id", "departments", "dept_id"),
    ("projects", "dept_id", "departments", "dept_id"),
    ("assignments", "emp_id", "employees", "emp_id"),
    ("assignments", "project_id", "projects", "project_id"),
    ("reviews", "emp_id", "employees", "emp_id"),
]

PK_COLS = {
    "departments": "dept_id",
    "employees": "emp_id",
    "projects": "project_id",
    "assignments": "assignment_id",
    "reviews": "review_id",
}

summary_rows = []

# COMMAND ----------

# Row count checks
for table_name, expected in EXPECTED_ROWS.items():
    fqn = f"{CATALOG}.{SCHEMA}.{PREFIX}{table_name}"
    actual = spark.table(fqn).count()
    passed = actual == expected
    summary_rows.append((
        TEST_NAME, table_name, "row_count", actual, expected, passed,
        json.dumps({"generation_time_s": round(gen_time, 2)}),
        datetime.now(),
    ))
    print(f"  {table_name}: row_count={actual}, expected={expected}, passed={passed}")

# COMMAND ----------

# PK uniqueness checks
for table_name, pk_col in PK_COLS.items():
    fqn = f"{CATALOG}.{SCHEMA}.{PREFIX}{table_name}"
    df = spark.table(fqn)
    dup_count = df.groupBy(pk_col).count().filter("count > 1").count()
    passed = dup_count == 0
    summary_rows.append((
        TEST_NAME, table_name, "pk_unique", df.count(), EXPECTED_ROWS[table_name], passed,
        json.dumps({"pk_col": pk_col, "duplicate_groups": dup_count}),
        datetime.now(),
    ))
    print(f"  {table_name}: pk_unique ({pk_col}), duplicates={dup_count}, passed={passed}")

# COMMAND ----------

# FK integrity checks
for child_table, fk_col, parent_table, pk_col in FK_CHECKS:
    child_fqn = f"{CATALOG}.{SCHEMA}.{PREFIX}{child_table}"
    parent_fqn = f"{CATALOG}.{SCHEMA}.{PREFIX}{parent_table}"
    child_df = spark.table(child_fqn)
    parent_df = spark.table(parent_fqn)
    orphans = child_df.select(fk_col).join(
        parent_df.select(pk_col),
        child_df[fk_col] == parent_df[pk_col],
        "left_anti",
    ).count()
    passed = orphans == 0
    summary_rows.append((
        TEST_NAME, child_table, "fk_integrity", child_df.count(),
        EXPECTED_ROWS[child_table], passed,
        json.dumps({
            "child": f"{child_table}.{fk_col}",
            "parent": f"{parent_table}.{pk_col}",
            "orphan_count": orphans,
        }),
        datetime.now(),
    ))
    print(f"  {child_table}.{fk_col} -> {parent_table}.{pk_col}: orphans={orphans}, passed={passed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Summary

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

summary_schema = StructType([
    StructField("test_name", StringType()),
    StructField("table_name", StringType()),
    StructField("check_name", StringType()),
    StructField("row_count", LongType()),
    StructField("expected_row_count", LongType()),
    StructField("passed", BooleanType()),
    StructField("details", StringType()),
    StructField("timestamp", TimestampType()),
])

summary_df = spark.createDataFrame(summary_rows, schema=summary_schema)
summary_df.write.format("delta").mode("append").saveAsTable(SUMMARY_TABLE)

# COMMAND ----------

# Assert all passed
failed = [r for r in summary_rows if not r[5]]
if failed:
    for r in failed:
        print(f"FAILED: {r[0]}/{r[1]}/{r[2]}: {r[6]}")
    raise AssertionError(f"{len(failed)} checks failed in {TEST_NAME}")

print(f"All {len(summary_rows)} checks passed for {TEST_NAME}")
