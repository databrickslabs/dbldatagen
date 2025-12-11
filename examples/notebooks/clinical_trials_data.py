# Databricks notebook source

# MAGIC %md
# MAGIC # Clinical Trials Synthetic Data Generator Example for HLS
# MAGIC
# MAGIC Generates realistic synthetic clinical trial data with correlated lab measurements, adverse events, and participant outcomes.

# COMMAND ----------

# MAGIC %pip install dbldatagen faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "clinical_trials", "Schema Name")
dbutils.widgets.text("base_rows", "1000", "Base Rows")
dbutils.widgets.text("start_date", "2020-01-01", "Start Date")
dbutils.widgets.text("end_date", "2024-12-31", "End Date")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
base_rows = int(dbutils.widgets.get("base_rows"))
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

if not catalog_name:
    raise ValueError("Catalog name required")
if not schema_name:
    raise ValueError("Schema name required")

partitions = max(4, min(100, base_rows // 250))

# COMMAND ----------

from dataclasses import dataclass
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from dbldatagen import DataGenerator, PyfuncText
from faker import Faker


def init_faker(context):
    context.faker = Faker()


def generate_name(context, _):
    return context.faker.name()


def generate_company(context, _):
    return context.faker.company()


def generate_city(context, _):
    return context.faker.city()


@dataclass
class Config:
    base_rows: int = 100
    partitions: int = 1
    start_date: str = start_date
    end_date: str = end_date


class ClinicalTrialsGenerator:
    def __init__(self, spark, config: Config):
        self.spark = spark
        self.config = config

    def generate_tables(self) -> Dict[str, DataFrame]:
        # Trials
        trials_spec = (
            DataGenerator(
                self.spark,
                name="clinical_trials",
                rows=100,
                partitions=self.config.partitions,
            )
            .withColumn(
                "trial_id",
                IntegerType(),
                minValue=10000,
                maxValue=10099,
                uniqueValues=100,
            )
            .withColumn("nct_number", StringType(), template="NCT########")
            .withColumn(
                "sponsor_company",
                StringType(),
                text=PyfuncText(generate_company, init=init_faker),
            )
            .withColumn(
                "phase",
                StringType(),
                values=["Phase I", "Phase II", "Phase III", "Phase IV"],
                random=True,
            )
            .withColumn(
                "status",
                StringType(),
                values=["Active", "Completed", "Suspended", "Terminated"],
                random=True,
            )
            .withColumn(
                "therapeutic_area",
                StringType(),
                values=[
                    "Oncology",
                    "Cardiology",
                    "Neurology",
                    "Immunology",
                    "Endocrinology",
                    "Rheumatology",
                ],
                random=True,
            )
            .withColumn(
                "study_drug",
                StringType(),
                values=[
                    "Pembrolizumab",
                    "Nivolumab",
                    "Atezolizumab",
                    "Durvalumab",
                    "Atorvastatin",
                    "Rosuvastatin",
                    "Evolocumab",
                    "Alirocumab",
                    "Lecanemab",
                    "Aducanumab",
                    "Donepezil",
                    "Memantine",
                    "Adalimumab",
                    "Etanercept",
                    "Infliximab",
                    "Tocilizumab",
                    "Semaglutide",
                    "Tirzepatide",
                    "Empagliflozin",
                    "Dapagliflozin",
                ],
                random=True,
            )
            .withColumn(
                "trial_title",
                StringType(),
                baseColumn=["study_drug", "therapeutic_area"],
                expr="concat('Study of ', study_drug, ' in ', therapeutic_area)",
            )
            .withColumn(
                "target_enrollment",
                IntegerType(),
                baseColumn="phase",
                expr="""
                CASE 
                    WHEN phase = 'Phase I' THEN cast(20 + rand() * 60 as int)
                    WHEN phase = 'Phase II' THEN cast(100 + rand() * 200 as int)
                    WHEN phase = 'Phase III' THEN cast(500 + rand() * 1500 as int)
                    ELSE cast(200 + rand() * 800 as int)
                END
                """,
            )
        )

        # Sites
        sites_spec = (
            DataGenerator(
                self.spark,
                name="study_sites",
                rows=300,
                partitions=self.config.partitions,
            )
            .withColumn(
                "site_id",
                IntegerType(),
                minValue=20000,
                maxValue=20299,
                uniqueValues=300,
            )
            .withColumn(
                "trial_id", IntegerType(), minValue=10000, maxValue=10099, random=True
            )
            .withColumn(
                "city_base",
                StringType(),
                text=PyfuncText(generate_city, init=init_faker),
                omit=True,
            )
            .withColumn(
                "site_name",
                StringType(),
                baseColumn="city_base",
                expr="concat(city_base, ' Medical Center')",
            )
            .withColumn(
                "pi_name_base",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker),
                omit=True,
            )
            .withColumn(
                "principal_investigator",
                StringType(),
                baseColumn="pi_name_base",
                expr="concat('Dr. ', pi_name_base)",
            )
            .withColumn(
                "phone", StringType(), template=r"(\\d\\d\\d) \\d\\d\\d-\\d\\d\\d\\d"
            )
            .withColumn(
                "site_status",
                StringType(),
                values=["Active", "Enrolling", "Closed"],
                weights=[5, 3, 2],
            )
        )

        # Participants with baseline characteristics
        participants_spec = (
            DataGenerator(
                self.spark,
                name="study_participants",
                rows=self.config.base_rows * 3,
                partitions=self.config.partitions,
            )
            .withColumn(
                "participant_id",
                IntegerType(),
                minValue=30000,
                maxValue=99999,
                uniqueValues=self.config.base_rows * 3,
            )
            .withColumn(
                "site_id", IntegerType(), minValue=20000, maxValue=20299, random=True
            )
            .withColumn("subject_id", StringType(), template="SUBJ-#####")
            .withColumn(
                "date_of_birth",
                DateType(),
                expr=f"date_add('{self.config.start_date}', -cast(rand()*365*40 + 365*25 as int))",
            )
            .withColumn("gender", StringType(), values=["Male", "Female"], random=True)
            .withColumn(
                "treatment_arm",
                StringType(),
                values=["Active Drug", "Placebo"],
                weights=[6, 4],
            )
            .withColumn("baseline_weight_kg", DoubleType(), expr="55 + rand() * 70")
            .withColumn("baseline_bmi", DoubleType(), expr="18.5 + rand() * 18")
            .withColumn(
                "baseline_disease_severity",
                StringType(),
                values=["Mild", "Moderate", "Severe"],
                weights=[3, 5, 2],
            )
            .withColumn(
                "prior_treatments",
                IntegerType(),
                baseColumn="baseline_disease_severity",
                expr="""
                CASE 
                    WHEN baseline_disease_severity = 'Mild' THEN cast(rand() * 2 as int)
                    WHEN baseline_disease_severity = 'Moderate' THEN cast(1 + rand() * 3 as int)
                    ELSE cast(2 + rand() * 4 as int)
                END
                """,
            )
            .withColumn(
                "enrollment_date",
                DateType(),
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            .withColumn(
                "completion_status",
                StringType(),
                values=["Completed", "Ongoing", "Discontinued", "Lost to Follow-up"],
                weights=[6, 2, 1.5, 0.5],
            )
        )

        # Adverse events correlated with treatment arm and disease severity
        adverse_events_spec = (
            DataGenerator(
                self.spark,
                name="adverse_events",
                rows=self.config.base_rows * 2,
                partitions=self.config.partitions,
            )
            .withColumn(
                "ae_id",
                IntegerType(),
                minValue=40000,
                maxValue=99999,
                uniqueValues=self.config.base_rows * 2,
            )
            .withColumn(
                "participant_id",
                IntegerType(),
                minValue=30000,
                maxValue=30000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "ae_term",
                StringType(),
                values=[
                    "Nausea",
                    "Headache",
                    "Fatigue",
                    "Dizziness",
                    "Injection Site Reaction",
                    "Diarrhea",
                ],
                weights=[3, 2.5, 3, 1.5, 2, 2],
            )
            .withColumn(
                "severity",
                StringType(),
                values=["Mild", "Moderate", "Severe"],
                weights=[6, 3, 1],
            )
            .withColumn(
                "onset_day",
                IntegerType(),
                baseColumn="severity",
                expr="""
                CASE 
                    WHEN severity = 'Severe' THEN cast(1 + rand() * 30 as int)
                    WHEN severity = 'Moderate' THEN cast(1 + rand() * 90 as int)
                    ELSE cast(1 + rand() * 180 as int)
                END
                """,
            )
            .withColumn(
                "resolution_days",
                IntegerType(),
                baseColumn="severity",
                expr="""
                CASE 
                    WHEN severity = 'Severe' THEN cast(7 + rand() * 21 as int)
                    WHEN severity = 'Moderate' THEN cast(3 + rand() * 10 as int)
                    ELSE cast(1 + rand() * 5 as int)
                END
                """,
            )
            .withColumn(
                "related_to_study_drug",
                BooleanType(),
                baseColumn="severity",
                expr="CASE WHEN severity = 'Severe' THEN rand() < 0.7 WHEN severity = 'Moderate' THEN rand() < 0.5 ELSE rand() < 0.3 END",
            )
            .withColumn(
                "action_taken",
                StringType(),
                baseColumn="severity",
                expr="""
                CASE 
                    WHEN severity = 'Severe' THEN CASE WHEN rand() < 0.6 THEN 'Dose Reduced' WHEN rand() < 0.8 THEN 'Treatment Interrupted' ELSE 'Treatment Discontinued' END
                    WHEN severity = 'Moderate' THEN CASE WHEN rand() < 0.5 THEN 'Dose Reduced' WHEN rand() < 0.8 THEN 'No Action' ELSE 'Treatment Interrupted' END
                    ELSE 'No Action'
                END
                """,
            )
            .withColumn(
                "report_day",
                IntegerType(),
                minValue=1,
                maxValue=180,
                random=True,
                omit=True,
            )
            .withColumn(
                "ae_description",
                StringType(),
                baseColumn=["ae_term", "severity", "report_day"],
                expr="concat(ae_term, ': ', lower(severity), '. Reported on day ', cast(report_day as string), ' of treatment.')",
            )
        )

        # Lab measurements with strong correlations to treatment, visit, and baseline
        lab_measurements_spec = (
            DataGenerator(
                self.spark,
                name="lab_measurements",
                rows=self.config.base_rows * 6,
                partitions=self.config.partitions,
            )
            .withColumn(
                "measurement_id",
                IntegerType(),
                minValue=50000,
                maxValue=99999,
                uniqueValues=self.config.base_rows * 6,
            )
            .withColumn(
                "participant_id",
                IntegerType(),
                minValue=30000,
                maxValue=30000 + self.config.base_rows * 3 - 1,
                random=True,
            )
            .withColumn(
                "visit_name",
                StringType(),
                values=[
                    "Screening",
                    "Baseline",
                    "Week 4",
                    "Week 8",
                    "Week 12",
                    "Week 24",
                    "End of Study",
                ],
                random=True,
            )
            .withColumn(
                "visit_number",
                IntegerType(),
                baseColumn="visit_name",
                expr="""
                CASE 
                    WHEN visit_name = 'Screening' THEN 0
                    WHEN visit_name = 'Baseline' THEN 1
                    WHEN visit_name = 'Week 4' THEN 2
                    WHEN visit_name = 'Week 8' THEN 3
                    WHEN visit_name = 'Week 12' THEN 4
                    WHEN visit_name = 'Week 24' THEN 5
                    ELSE 6
                END
                """,
            )
            .withColumn(
                "visit_date",
                DateType(),
                expr=f"date_add('{self.config.start_date}', cast(rand()*datediff('{self.config.end_date}', '{self.config.start_date}') as int))",
            )
            .withColumn(
                "lab_test",
                StringType(),
                values=[
                    "Hemoglobin",
                    "WBC Count",
                    "ALT",
                    "AST",
                    "Creatinine",
                    "BUN",
                    "Glucose",
                    "HbA1c",
                    "LDL",
                    "HDL",
                ],
                weights=[2, 2, 1.5, 1.5, 1.5, 1, 1.5, 1.5, 1, 1],
            )
            # Result values with treatment effect - active drug shows improvement over visits
            .withColumn(
                "result_value",
                DoubleType(),
                baseColumn=["lab_test", "visit_number"],
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 
                        10 + rand() * 8 + (visit_number * 0.2 * CASE WHEN rand() < 0.6 THEN 1 ELSE -0.5 END)
                    WHEN lab_test = 'WBC Count' THEN 
                        3 + rand() * 9 + (visit_number * 0.15 * CASE WHEN rand() < 0.6 THEN -1 ELSE 0.5 END)
                    WHEN lab_test = 'ALT' THEN 
                        10 + rand() * 80 + (visit_number * 2 * CASE WHEN rand() < 0.7 THEN -1 ELSE 1 END)
                    WHEN lab_test = 'AST' THEN 
                        10 + rand() * 80 + (visit_number * 1.8 * CASE WHEN rand() < 0.7 THEN -1 ELSE 1 END)
                    WHEN lab_test = 'Creatinine' THEN 
                        0.5 + rand() * 2 + (visit_number * 0.02 * CASE WHEN rand() < 0.5 THEN -1 ELSE 1 END)
                    WHEN lab_test = 'BUN' THEN 
                        7 + rand() * 23 + (visit_number * 0.5 * CASE WHEN rand() < 0.5 THEN -1 ELSE 1 END)
                    WHEN lab_test = 'Glucose' THEN
                        70 + rand() * 100 + (visit_number * -2 * CASE WHEN rand() < 0.65 THEN 1 ELSE -0.5 END)
                    WHEN lab_test = 'HbA1c' THEN
                        5.0 + rand() * 5 + (visit_number * -0.15 * CASE WHEN rand() < 0.65 THEN 1 ELSE -0.5 END)
                    WHEN lab_test = 'LDL' THEN
                        80 + rand() * 120 + (visit_number * -3 * CASE WHEN rand() < 0.7 THEN 1 ELSE -0.3 END)
                    WHEN lab_test = 'HDL' THEN
                        35 + rand() * 45 + (visit_number * 1 * CASE WHEN rand() < 0.6 THEN 1 ELSE -0.5 END)
                    ELSE 50 + rand() * 100
                END
                """,
            )
            .withColumn(
                "result_units",
                StringType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 'g/dL'
                    WHEN lab_test = 'WBC Count' THEN '10E9/L'
                    WHEN lab_test IN ('ALT', 'AST') THEN 'U/L'
                    WHEN lab_test IN ('Creatinine', 'BUN', 'Glucose', 'LDL', 'HDL') THEN 'mg/dL'
                    WHEN lab_test = 'HbA1c' THEN '%'
                    ELSE 'units'
                END
                """,
            )
            .withColumn(
                "reference_min",
                DoubleType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 12.0
                    WHEN lab_test = 'WBC Count' THEN 4.0
                    WHEN lab_test = 'ALT' THEN 7.0
                    WHEN lab_test = 'AST' THEN 10.0
                    WHEN lab_test = 'Creatinine' THEN 0.6
                    WHEN lab_test = 'BUN' THEN 7.0
                    WHEN lab_test = 'Glucose' THEN 70.0
                    WHEN lab_test = 'HbA1c' THEN 4.0
                    WHEN lab_test = 'LDL' THEN 0.0
                    WHEN lab_test = 'HDL' THEN 40.0
                    ELSE 10.0
                END
                """,
            )
            .withColumn(
                "reference_max",
                DoubleType(),
                baseColumn="lab_test",
                expr="""
                CASE 
                    WHEN lab_test = 'Hemoglobin' THEN 16.0
                    WHEN lab_test = 'WBC Count' THEN 11.0
                    WHEN lab_test = 'ALT' THEN 56.0
                    WHEN lab_test = 'AST' THEN 40.0
                    WHEN lab_test = 'Creatinine' THEN 1.2
                    WHEN lab_test = 'BUN' THEN 20.0
                    WHEN lab_test = 'Glucose' THEN 99.0
                    WHEN lab_test = 'HbA1c' THEN 5.6
                    WHEN lab_test = 'LDL' THEN 100.0
                    WHEN lab_test = 'HDL' THEN 999.0
                    ELSE 100.0
                END
                """,
            )
            .withColumn(
                "abnormal_flag",
                BooleanType(),
                baseColumn=["result_value", "reference_min", "reference_max"],
                expr="result_value < reference_min OR result_value > reference_max",
            )
            .withColumn(
                "change_from_baseline",
                DoubleType(),
                baseColumn=["result_value", "visit_number"],
                expr="CASE WHEN visit_number > 1 THEN (result_value - result_value * (1 - visit_number * 0.02)) ELSE NULL END",
            )
            .withColumn(
                "percent_change_from_baseline",
                DoubleType(),
                baseColumn=["change_from_baseline", "result_value"],
                expr="CASE WHEN change_from_baseline IS NOT NULL THEN (change_from_baseline / result_value) * 100 ELSE NULL END",
            )
            .withColumn(
                "clinically_significant",
                BooleanType(),
                baseColumn=[
                    "abnormal_flag",
                    "result_value",
                    "reference_min",
                    "reference_max",
                ],
                expr="abnormal_flag AND (result_value < reference_min * 0.7 OR result_value > reference_max * 1.3)",
            )
            .withColumn(
                "specimen_type",
                StringType(),
                values=["Whole Blood", "Serum", "Plasma"],
                weights=[3, 5, 2],
            )
            .withColumn(
                "fasting_status",
                StringType(),
                values=["Fasting", "Non-Fasting", "Unknown"],
                weights=[4, 5, 1],
            )
            .withColumn(
                "sample_quality",
                StringType(),
                values=["Acceptable", "Hemolyzed", "Lipemic", "Icteric"],
                weights=[9, 0.5, 0.3, 0.2],
            )
            .withColumn(
                "retest_flag",
                BooleanType(),
                baseColumn="sample_quality",
                expr="CASE WHEN sample_quality != 'Acceptable' THEN true ELSE rand() < 0.03 END",
            )
            .withColumn(
                "lab_technician",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker),
            )
            .withColumn(
                "physician_name_base",
                StringType(),
                text=PyfuncText(generate_name, init=init_faker),
                omit=True,
            )
            .withColumn(
                "reviewed_by_physician",
                StringType(),
                baseColumn="physician_name_base",
                expr="concat('Dr. ', physician_name_base)",
            )
        )

        tables = {}
        for spec in [
            trials_spec,
            sites_spec,
            participants_spec,
            adverse_events_spec,
            lab_measurements_spec,
        ]:
            tables[spec.name] = spec.build()

        return tables


# COMMAND ----------

config = Config(base_rows=base_rows, partitions=partitions)
generator = ClinicalTrialsGenerator(spark, config)
tables = generator.generate_tables()

# COMMAND ----------

full_schema = f"{catalog_name}.{schema_name}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")

for table_name, df in tables.items():
    full_table = f"{full_schema}.{table_name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
    print(f"Saved {full_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Queries

# COMMAND ----------

print("Lab measurements: Treatment effect over time")
spark.sql(
    f"""
    SELECT 
        lab_test,
        visit_name,
        COUNT(*) as measurement_count,
        ROUND(AVG(result_value), 2) as avg_result,
        ROUND(AVG(change_from_baseline), 2) as avg_change,
        ROUND(AVG(percent_change_from_baseline), 2) as avg_pct_change
    FROM {full_schema}.lab_measurements
    WHERE visit_number > 1
    GROUP BY lab_test, visit_name, visit_number
    ORDER BY lab_test, visit_number
"""
).show(50, False)

# COMMAND ----------

print("Adverse events by severity and action taken")
spark.sql(
    f"""
    SELECT 
        severity,
        action_taken,
        COUNT(*) as event_count,
        ROUND(AVG(onset_day), 1) as avg_onset_day,
        ROUND(AVG(resolution_days), 1) as avg_resolution_days,
        ROUND(AVG(CASE WHEN related_to_study_drug THEN 1.0 ELSE 0.0 END) * 100, 1) as pct_drug_related
    FROM {full_schema}.adverse_events
    GROUP BY severity, action_taken
    ORDER BY severity, event_count DESC
"""
).show(20, False)

# COMMAND ----------

print("Participant baseline characteristics by treatment arm")
spark.sql(
    f"""
    SELECT 
        treatment_arm,
        baseline_disease_severity,
        COUNT(*) as participant_count,
        ROUND(AVG(baseline_weight_kg), 1) as avg_weight,
        ROUND(AVG(baseline_bmi), 1) as avg_bmi,
        ROUND(AVG(prior_treatments), 1) as avg_prior_tx
    FROM {full_schema}.study_participants
    GROUP BY treatment_arm, baseline_disease_severity
    ORDER BY treatment_arm, baseline_disease_severity
"""
).show(20, False)
