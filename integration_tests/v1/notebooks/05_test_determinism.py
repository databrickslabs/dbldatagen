# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Test Determinism
# MAGIC
# MAGIC Generates the Employee Management plan twice with the same seed and verifies
# MAGIC that both runs produce identical DataFrames (same rows in same order).

# COMMAND ----------

dbutils.widgets.text("wheel_volume_path", "anup_kalburgi/datagen_demo/dbldatagen/lib", "Wheel Volume Path")
dbutils.widgets.text("catalog", "anup_kalburgi", "Catalog")

# COMMAND ----------

# MAGIC %pip install /Volumes/${wheel_volume_path}/dbldatagen-latest-py3-none-any.whl --quiet
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

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "datagen_demo"
PREFIX = "dbldatagen_v1_"
TEST_NAME = "determinism"
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
                    text(
                        "name",
                        values=[
                            "Engineering",
                            "Sales",
                            "Marketing",
                            "Finance",
                            "HR",
                            "Legal",
                            "Operations",
                            "Support",
                            "Research",
                            "Product",
                        ],
                    ),
                    text(
                        "location",
                        values=[
                            "New York",
                            "San Francisco",
                            "Chicago",
                            "Austin",
                            "Seattle",
                            "Boston",
                            "Denver",
                            "Miami",
                            "Portland",
                            "Atlanta",
                        ],
                    ),
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
                    text(
                        "role",
                        values=[
                            "developer",
                            "designer",
                            "tester",
                            "analyst",
                            "lead",
                            "manager",
                            "consultant",
                            "reviewer",
                        ],
                    ),
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
                    text(
                        "comments",
                        values=[
                            "Exceeds expectations",
                            "Meets expectations",
                            "Needs improvement",
                            "Outstanding performance",
                            "Satisfactory",
                            "Below average",
                        ],
                    ),
                ],
            ),
        ],
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Run 1

# COMMAND ----------

plan1 = build_employee_plan()
start = time.time()
results1 = generate(spark, plan1)
run1_time = time.time() - start
print(f"Run 1 took {run1_time:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Run 2

# COMMAND ----------

plan2 = build_employee_plan()
start = time.time()
results2 = generate(spark, plan2)
run2_time = time.time() - start
print(f"Run 2 took {run2_time:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: Exact Match

# COMMAND ----------

EXPECTED_ROWS = {
    "departments": 50,
    "employees": 10_000,
    "projects": 500,
    "assignments": 20_000,
    "reviews": 15_000,
}

PK_COLS = {
    "departments": "dept_id",
    "employees": "emp_id",
    "projects": "project_id",
    "assignments": "assignment_id",
    "reviews": "review_id",
}

summary_rows = []

for table_name in EXPECTED_ROWS:
    df1 = results1[table_name]
    df2 = results2[table_name]

    # Row count match
    count1 = df1.count()
    count2 = df2.count()
    passed_count = count1 == count2 == EXPECTED_ROWS[table_name]
    summary_rows.append(
        (
            TEST_NAME,
            table_name,
            "row_count_match",
            count1,
            EXPECTED_ROWS[table_name],
            passed_count,
            json.dumps({"run1_count": count1, "run2_count": count2}),
            datetime.now(),
        )
    )

    # Full DataFrame match (exceptAll both directions)
    diff_1_minus_2 = df1.exceptAll(df2).count()
    diff_2_minus_1 = df2.exceptAll(df1).count()
    passed_exact = diff_1_minus_2 == 0 and diff_2_minus_1 == 0
    summary_rows.append(
        (
            TEST_NAME,
            table_name,
            "exact_match",
            count1,
            EXPECTED_ROWS[table_name],
            passed_exact,
            json.dumps(
                {
                    "rows_in_run1_not_run2": diff_1_minus_2,
                    "rows_in_run2_not_run1": diff_2_minus_1,
                }
            ),
            datetime.now(),
        )
    )

    # PK determinism: verify PK columns match exactly
    pk_col = PK_COLS[table_name]
    pk1 = df1.select(pk_col).orderBy(pk_col)
    pk2 = df2.select(pk_col).orderBy(pk_col)
    pk_diff_1 = pk1.exceptAll(pk2).count()
    pk_diff_2 = pk2.exceptAll(pk1).count()
    passed_pk = pk_diff_1 == 0 and pk_diff_2 == 0
    summary_rows.append(
        (
            TEST_NAME,
            table_name,
            "pk_determinism",
            count1,
            EXPECTED_ROWS[table_name],
            passed_pk,
            json.dumps(
                {
                    "pk_col": pk_col,
                    "pk_in_run1_not_run2": pk_diff_1,
                    "pk_in_run2_not_run1": pk_diff_2,
                }
            ),
            datetime.now(),
        )
    )

    print(f"  {table_name}: counts={count1}/{count2}, " f"exact_match={passed_exact}, pk_determinism={passed_pk}")

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

summary_schema = StructType(
    [
        StructField("test_name", StringType()),
        StructField("table_name", StringType()),
        StructField("check_name", StringType()),
        StructField("row_count", LongType()),
        StructField("expected_row_count", LongType()),
        StructField("passed", BooleanType()),
        StructField("details", StringType()),
        StructField("timestamp", TimestampType()),
    ]
)

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
