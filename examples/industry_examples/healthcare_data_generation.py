# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Data Generation with dbldatagen
# MAGIC
# MAGIC This notebook demonstrates how to use the **Databricks Labs Data Generator (`dbldatagen`)**
# MAGIC to synthesize realistic Healthcare data at scale. All data is fully synthetic. No real patient
# MAGIC PII, PHI, or medical records are used.
# MAGIC
# MAGIC Healthcare is one of the most data-intensive industries, with strict privacy requirements
# MAGIC (HIPAA) making **synthetic data generation** especially valuable for development, testing,
# MAGIC and ML model training without exposing real patient information.
# MAGIC
# MAGIC ### Covered Use Cases
# MAGIC | # | Dataset | Rows | Description |
# MAGIC |---|---------|------|-------------|
# MAGIC | 1 | **Patients** | 100 K | Demographics, insurance, blood type, primary care assignment |
# MAGIC | 2 | **Providers** | 5 K | Physicians, nurses, specialists with NPI, specialty, facility |
# MAGIC | 3 | **Encounters** | 1 M | Hospital visits, ER admissions, telehealth — linked to patients & providers |
# MAGIC | 4 | **Diagnoses** | 2 M | ICD-10 coded diagnoses per encounter (multi-row, chronic & acute) |
# MAGIC | 5 | **Medications** | 1 M | Prescription orders with NDC codes, dosage, and adherence flags |
# MAGIC | 6 | **Lab Results** | 2 M | Lab test results with LOINC codes, reference ranges, and abnormal flags |
# MAGIC | 7 | **Insurance Claims** | 500 K | Claims derived from encounters with procedure codes, amounts, and status |

# COMMAND ----------
# MAGIC %pip install dbldatagen
# MAGIC %restart_python

# COMMAND ----------
from typing import Sequence
from pyspark.sql import DataFrame
import dbldatagen as dg

# Let Adaptive Query Execution pick the shuffle partition count for us.
spark.conf.set("spark.sql.shuffle.partitions", "auto")


def sql_array(values: Sequence[str | float | int]) -> str:
    """Renders a Python list as a Spark SQL ``array(...)`` literal.

    Used together with a hidden random index column and ``element_at`` so that paired
    lists — e.g. a code and its description — stay correlated: the same index selects
    the matching entry from each list.

    Args:
        values: Items to render. String items are quoted; numeric items are rendered
            as-is.

    Returns:
        A Spark SQL ``array(...)`` literal, e.g. ``array('A', 'B')`` or ``array(1, 2)``.
    """
    items = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
    return f"array({items})"


def add_encounter_columns(child_df: DataFrame,
                          columns: Sequence[str] = ("patient_id",)) -> DataFrame:
    """Joins a child table to ``encounters`` on ``encounter_id`` to copy parent columns.

    Args:
        child_df: A generated child table containing an ``encounter_id`` column.
        columns: Encounter columns to copy onto each child row.

    Returns:
        The child DataFrame with the requested encounter columns joined on.
    """
    return child_df.join(
        encounters_df.select("encounter_id", *columns),
        on="encounter_id",
        how="inner",
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Patients
# MAGIC
# MAGIC The patient master table — the central entity all other healthcare datasets link to.
# MAGIC Includes demographics, contact details, insurance coverage, and care assignment.
# MAGIC Blood type, smoking status, and BMI bucket are included to support clinical ML use cases.
# MAGIC
# MAGIC Note how `registration_date` is derived from `date_of_birth` with an `expr` so a patient is
# MAGIC never registered before they were born, and `is_active` is in turn derived from
# MAGIC `registration_date`.

# COMMAND ----------

PATIENT_COUNT = 100_000

patient_spec = (
    dg.DataGenerator(spark, name="patients", rows=PATIENT_COUNT, partitions=8,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    # --- Identity ---
    .withColumn("patient_id", "string", prefix="PAT", baseColumn="id")
    .withColumn("first_name", "string",
                values=["James", "Maria", "David", "Sarah", "Wei", "Aisha", "Carlos", "Priya",
                        "Mohammed", "Emily", "Robert", "Linda", "Michael", "Barbara", "William", "Patricia"],
                random=True)
    .withColumn("last_name", "string",
                values=["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                        "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris"],
                random=True)
    .withColumn("date_of_birth", "date",
                begin="1930-01-01", end="2024-01-01", random=True)
    .withColumn("gender", "string",
                values=["Male", "Female", "Non-binary", "Unknown"],
                weights=[48, 48, 3, 1])
    .withColumn("race", "string",
                values=["White", "Black or African American", "Asian", "Hispanic or Latino",
                        "American Indian or Alaska Native", "Native Hawaiian", "Two or More", "Unknown"],
                weights=[60, 13, 6, 18, 1, 0.5, 1.5, 0])
    .withColumn("ethnicity", "string",
                values=["Not Hispanic or Latino", "Hispanic or Latino", "Unknown"],
                weights=[77, 18, 5])
    # --- Contact ---
    .withColumn("address_state", "string",
                values=["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
                        "NJ", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI", "CO"],
                weights=[12, 9, 8, 7, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2])
    .withColumn("zip_code", "string", template=r"ddddd")
    .withColumn("phone", "string", template=r"(ddd) ddd-dddd")
    # --- Clinical profile ---
    .withColumn("blood_type", "string",
                values=["O+", "A+", "B+", "AB+", "O-", "A-", "B-", "AB-"],
                weights=[38, 34, 9, 3, 7, 6, 2, 1])
    .withColumn("smoking_status", "string",
                values=["Never", "Former", "Current", "Unknown"],
                weights=[55, 25, 15, 5])
    .withColumn("bmi_category", "string",
                values=["Underweight", "Normal", "Overweight", "Obese"],
                weights=[3, 32, 34, 31])
    .withColumn("primary_language", "string",
                values=["English", "Spanish", "Chinese", "Vietnamese", "Arabic", "French", "Other"],
                weights=[78, 13, 2, 1, 1, 1, 4])
    # --- Insurance ---
    .withColumn("insurance_type", "string",
                values=["Commercial", "Medicare", "Medicaid", "Self-Pay", "Military", "Other"],
                weights=[48, 20, 17, 8, 4, 3])
    .withColumn("insurance_plan_id", "string", template=r"INS-ddddddd", random=True)
    .withColumn("member_id", "string", template=r"MBR-ddddddddd", random=True)
    # --- Care assignment ---
    .withColumn("primary_care_npi", "string", template=r"dddddddddd", random=True)
    # Register the patient on a random day between their birth date and the present,
    # so registration is always after birth (demonstrates deriving one column from another).
    .withColumn("registration_date", "date", baseColumn="date_of_birth",
                expr="date_add(date_of_birth, cast(rand() * datediff(date'2024-06-01', date_of_birth) as int))")
    .withColumn("is_active", "boolean", baseColumn="registration_date",
                expr="registration_date >= date'2015-01-01'")
)

patients_df = patient_spec.build()
patients_df.createOrReplaceTempView("patients")

print(f"Patients generated: {patients_df.count():,}")
display(patients_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Providers
# MAGIC
# MAGIC Healthcare providers: physicians, nurses, specialists, and allied health professionals.
# MAGIC Each provider has a unique NPI (National Provider Identifier), specialty, and facility assignment.
# MAGIC This table is the "provider" dimension that encounters and prescriptions reference.
# MAGIC
# MAGIC `credential` and `sub_specialty` are derived from `specialty` with `CASE` expressions — a few
# MAGIC special cases plus a default keeps each provider internally consistent (no "Nurse Practitioner"
# MAGIC carrying an `MD` credential) without an exhaustive rule for every specialty.

# COMMAND ----------

PROVIDER_COUNT = 5_000

# A representative (deliberately short) list of specialties. Nurse Practitioner and
# Physician Assistant are kept as explicit special cases for the credential rule below.
specialties = [
    "Internal Medicine", "Family Medicine", "Pediatrics", "Cardiology", "Oncology",
    "Orthopedics", "Emergency Medicine", "Obstetrics & Gynecology", "General Surgery",
    "Psychiatry", "Nurse Practitioner", "Physician Assistant",
]

provider_spec = (
    dg.DataGenerator(spark, name="providers", rows=PROVIDER_COUNT, partitions=4,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("provider_id", "string", prefix="PRV", baseColumn="id")
    .withColumn("npi", "string", template=r"dddddddddd", random=True)
    .withColumn("first_name", "string",
                values=["James", "Maria", "David", "Sarah", "Wei", "Aisha", "Carlos",
                        "Robert", "Linda", "Michael", "William", "Patricia", "John", "Jennifer"],
                random=True)
    .withColumn("last_name", "string",
                values=["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                        "Davis", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson"],
                random=True)
    .withColumn("specialty", "string", values=specialties, random=True)
    # Credential follows from the specialty: NP/PA are explicit, everyone else is a
    # physician (mostly MD, some DO).
    .withColumn("credential", "string", baseColumn="specialty",
                expr="""
                    CASE
                        WHEN specialty = 'Nurse Practitioner'  THEN 'NP'
                        WHEN specialty = 'Physician Assistant' THEN 'PA'
                        WHEN rand() < 0.85                     THEN 'MD'
                        ELSE 'DO'
                    END
                """)
    # Sub-specialty: a handful of meaningful cases, everything else falls through to 'General'.
    .withColumn("sub_specialty", "string", baseColumn="specialty",
                expr="""
                    CASE specialty
                        WHEN 'Cardiology'  THEN 'Interventional Cardiology'
                        WHEN 'Orthopedics' THEN 'Sports Medicine'
                        WHEN 'Oncology'    THEN 'Medical Oncology'
                        WHEN 'Pediatrics'  THEN 'General Pediatrics'
                        ELSE 'General'
                    END
                """)
    .withColumn("facility_id", "string", template=r"FAC-ddddd", random=True)
    .withColumn("facility_type", "string",
                values=["Hospital", "Outpatient Clinic", "ASC", "Urgent Care",
                        "Telehealth", "Long-Term Care", "Rehab Center"],
                weights=[30, 35, 10, 10, 8, 4, 3])
    .withColumn("state", "string",
                values=["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
                        "NJ", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI", "CO"],
                random=True)
    .withColumn("accepting_patients", "boolean", expr="rand() < 0.7")
    .withColumn("years_experience", "integer",
                minValue=1, maxValue=40, random=True,
                distribution=dg.distributions.Normal(mean=15, stddev=8))
)

providers_df = provider_spec.build()
providers_df.createOrReplaceTempView("providers")

print(f"Providers generated: {providers_df.count():,}")
display(providers_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Encounters
# MAGIC
# MAGIC An encounter is any clinical interaction between a patient and the health system —
# MAGIC an ER visit, inpatient admission, outpatient appointment, or telehealth call.
# MAGIC This is the central fact table that diagnoses, medications, labs, and claims all reference.
# MAGIC
# MAGIC The `patient_id` / `provider_id` foreign keys are built from the **same count constants**
# MAGIC used to generate those tables (`PATIENT_COUNT`, `PROVIDER_COUNT`). Because `withIdOutput()`
# MAGIC numbers ids `0 .. COUNT-1`, `cast(rand() * COUNT as int)` covers exactly that range — every
# MAGIC generated key points at a row that actually exists, and the keys stay valid if you change the
# MAGIC counts above.

# COMMAND ----------

ENCOUNTER_COUNT = 1_000_000

encounter_spec = (
    dg.DataGenerator(spark, name="encounters", rows=ENCOUNTER_COUNT, partitions=16,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("encounter_id", "string", prefix="ENC", baseColumn="id")
    # Foreign keys into patients (0..PATIENT_COUNT-1) and providers (0..PROVIDER_COUNT-1).
    .withColumn("patient_id", "string",
                expr=f"concat('PAT_', cast(rand() * {PATIENT_COUNT} as int))")
    .withColumn("provider_id", "string",
                expr=f"concat('PRV_', cast(rand() * {PROVIDER_COUNT} as int))")
    .withColumn("facility_id", "string", template=r"FAC-ddddd", random=True)
    # Encounter metadata
    .withColumn("encounter_type", "string",
                values=["Inpatient", "Outpatient", "Emergency", "Telehealth",
                        "Urgent Care", "Observation", "Home Health", "Preventive"],
                weights=[15, 40, 12, 15, 8, 4, 3, 3])
    .withColumn("admit_date", "date",
                begin="2019-01-01", end="2024-12-31", random=True)
    .withColumn("length_of_stay_days", "integer",
                minValue=0, maxValue=60, random=True,
                distribution=dg.distributions.Exponential(rate=0.5))
    .withColumn("discharge_date", "date", baseColumn=["admit_date", "length_of_stay_days"],
                expr="date_add(admit_date, length_of_stay_days)")
    .withColumn("discharge_disposition", "string",
                values=["Home", "SNF", "Rehab", "Home with Services", "AMA", "Expired", "Transfer", "Hospice"],
                weights=[65, 10, 5, 10, 2, 1, 5, 2])
    .withColumn("chief_complaint", "string",
                values=["Chest pain", "Shortness of breath", "Abdominal pain", "Fever", "Headache",
                        "Back pain", "Dizziness", "Nausea/vomiting", "Follow-up", "Annual exam",
                        "Cough", "Fatigue", "Rash", "Injury", "Mental health", "Other"],
                weights=[8, 7, 8, 7, 6, 7, 5, 5, 12, 8, 5, 5, 3, 5, 4, 6])
    .withColumn("visit_type", "string",
                values=["Scheduled", "Unscheduled", "Follow-up", "Referral", "Walk-in", "Transfer"],
                weights=[35, 20, 25, 10, 7, 3])
    .withColumn("department", "string",
                values=["Internal Medicine", "Emergency", "Cardiology", "Orthopedics",
                        "Oncology", "Neurology", "OB/GYN", "Pediatrics", "Psychiatry", "Surgery"],
                weights=[20, 15, 12, 10, 8, 8, 7, 8, 6, 6])
    # Vitals snapshot — Normal distributions centred on clinically typical values.
    .withColumn("systolic_bp", "integer",
                minValue=80, maxValue=220, random=True,
                distribution=dg.distributions.Normal(mean=122, stddev=18))
    .withColumn("diastolic_bp", "integer",
                minValue=50, maxValue=130, random=True,
                distribution=dg.distributions.Normal(mean=79, stddev=12))
    .withColumn("heart_rate", "integer",
                minValue=40, maxValue=180, random=True,
                distribution=dg.distributions.Normal(mean=75, stddev=15))
    .withColumn("temperature_f", "double",
                minValue=96.0, maxValue=106.0, random=True,
                distribution=dg.distributions.Normal(mean=98.6, stddev=1.0))
    .withColumn("o2_saturation_pct", "integer",
                minValue=85, maxValue=100, random=True,
                distribution=dg.distributions.Normal(mean=97, stddev=2))
    # Financial — Exponential gives a long right tail (most charges small, a few very large).
    .withColumn("total_charge_usd", "double",
                minValue=50.0, maxValue=500_000.0, random=True,
                distribution=dg.distributions.Exponential(rate=0.00005))
)

encounters_df = encounter_spec.build()
encounters_df.createOrReplaceTempView("encounters")

print(f"Encounters generated: {encounters_df.count():,}")
display(encounters_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Diagnoses (ICD-10)
# MAGIC
# MAGIC Each encounter can have multiple diagnoses — a primary diagnosis plus secondary conditions.
# MAGIC ICD-10 codes are used globally for classifying diseases and health conditions.
# MAGIC This dataset is key for population health analytics, chronic disease management, and ML risk models.
# MAGIC
# MAGIC `icd10_code` and `icd10_description` must stay in sync. We generate a single hidden index
# MAGIC column (`code_index`, `omit=True`) and have **both** columns pick their entry from that same
# MAGIC index with `element_at` — so position *i* of the code list always pairs with position *i* of
# MAGIC the description list.

# COMMAND ----------

DIAGNOSIS_COUNT = 2_000_000

# Representative ICD-10 codes paired position-for-position with their descriptions.
icd10_codes = [
    "I10", "E11.9", "E11.65", "J06.9", "M54.5", "Z00.00", "K21.0", "F41.1", "I25.10",
    "E78.5", "J44.1", "N18.3", "F32.9", "M79.3", "Z12.11", "I50.9", "E13.9", "G47.33",
    "K57.30", "M17.11", "J45.909", "N39.0", "I48.91", "E11.40", "Z23", "R05.9", "M47.816",
    "I63.9", "G43.909", "F03.90"
]
icd10_descriptions = [
    "Essential (primary) hypertension",
    "Type 2 diabetes mellitus without complications",
    "Type 2 diabetes mellitus with hyperglycemia",
    "Acute upper respiratory infection, unspecified",
    "Low back pain",
    "Encounter for general adult medical examination",
    "Gastro-esophageal reflux disease with esophagitis",
    "Generalized anxiety disorder",
    "Atherosclerotic heart disease of native coronary artery",
    "Hyperlipidemia, unspecified",
    "Chronic obstructive pulmonary disease, acute exacerbation",
    "Chronic kidney disease, stage 3",
    "Major depressive disorder, single episode, unspecified",
    "Myalgia",
    "Encounter for screening for malignant neoplasm of colon",
    "Heart failure, unspecified",
    "Other specified diabetes mellitus without complications",
    "Obstructive sleep apnea",
    "Diverticulosis of large intestine without perforation",
    "Primary osteoarthritis, right knee",
    "Unspecified asthma, uncomplicated",
    "Urinary tract infection, site not specified",
    "Typical atrial flutter",
    "Type 2 diabetes mellitus with diabetic neuropathy",
    "Encounter for immunization",
    "Cough",
    "Spondylosis with radiculopathy, lumbar region",
    "Cerebral infarction, unspecified",
    "Migraine, unspecified, not intractable",
    "Dementia, unspecified"
]

diagnosis_spec = (
    dg.DataGenerator(spark, name="diagnoses", rows=DIAGNOSIS_COUNT, partitions=16,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("diagnosis_id", "string", prefix="DX", baseColumn="id")
    .withColumn("encounter_id", "string",
                expr=f"concat('ENC_', cast(rand() * {ENCOUNTER_COUNT} as int))")
    # Omitted index for matching entries across lists of values
    .withColumn("code_index", "integer",
                minValue=0, maxValue=len(icd10_codes) - 1, random=True, omit=True)
    .withColumn("icd10_code", "string", baseColumn="code_index",
                expr=f"element_at({sql_array(icd10_codes)}, code_index + 1)")
    .withColumn("icd10_description", "string", baseColumn="code_index",
                expr=f"element_at({sql_array(icd10_descriptions)}, code_index + 1)")
    .withColumn("diagnosis_type", "string",
                values=["Primary", "Secondary", "Tertiary", "Admitting", "Discharge"],
                weights=[35, 40, 10, 8, 7])
    .withColumn("diagnosis_date", "date",
                begin="2019-01-01", end="2024-12-31", random=True)
    .withColumn("is_chronic", "boolean", baseColumn="icd10_code",
                expr="""icd10_code IN (
                    'I10','E11.9','E11.65','I25.10','E78.5','J44.1','N18.3',
                    'F32.9','I50.9','G47.33','M17.11','J45.909','I48.91','E11.40','F03.90'
                )""")
    .withColumn("onset_type", "string",
                values=["Acute", "Chronic", "Acute-on-Chronic", "Unknown"],
                weights=[35, 40, 15, 10])
    .withColumn("severity", "string",
                values=["Mild", "Moderate", "Severe", "Critical", "Unspecified"],
                weights=[30, 35, 20, 5, 10])
    .withColumn("clinician_notes", "string",
                values=["Stable", "Worsening", "Improving", "New onset", "Under treatment",
                        "Resolved", "Monitoring", "Referred"],
                random=True)
)

diagnoses_df = diagnosis_spec.build()
diagnoses_df = add_encounter_columns(diagnoses_df, columns=("patient_id",))
diagnoses_df.createOrReplaceTempView("diagnoses")

print(f"Diagnoses generated: {diagnoses_df.count():,}")
display(diagnoses_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Medications & Prescriptions
# MAGIC
# MAGIC Prescription orders linked to encounters and patients.
# MAGIC Includes NDC (National Drug Code) labeler codes, dosage, route, frequency, and adherence signals.
# MAGIC Medication adherence (`is_adherent`) is derived from refill behaviour and is useful for
# MAGIC chronic disease management and population health models.
# MAGIC
# MAGIC As with diagnoses, `drug_name` and its `ndc_code` are kept correlated through a shared
# MAGIC hidden index.

# COMMAND ----------

MEDICATION_COUNT = 1_000_000

medications = [
    "Lisinopril", "Metformin", "Atorvastatin", "Amlodipine", "Metoprolol Succinate",
    "Omeprazole", "Levothyroxine", "Albuterol", "Gabapentin", "Hydrochlorothiazide",
    "Sertraline", "Losartan", "Montelukast", "Furosemide", "Pantoprazole",
    "Escitalopram", "Bupropion", "Duloxetine", "Rosuvastatin", "Empagliflozin"
]
# NDC labeler prefixes, paired position-for-position with the drugs above.
ndc_prefixes = [
    "00006", "00071", "00185", "00378", "00555", "00603", "00677", "00781", "00904", "16714",
    "43598", "45963", "50111", "51079", "55111", "60505", "62037", "65862", "68180", "72205"
]

medication_spec = (
    dg.DataGenerator(spark, name="medications", rows=MEDICATION_COUNT, partitions=16,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("prescription_id", "string", prefix="RX", baseColumn="id")
    .withColumn("encounter_id", "string",
                expr=f"concat('ENC_', cast(rand() * {ENCOUNTER_COUNT} as int))")
    # Omitted index for matching entries across lists of values
    .withColumn("drug_index", "integer",
                minValue=0, maxValue=len(medications) - 1, random=True, omit=True)
    .withColumn("drug_name", "string", baseColumn="drug_index",
                expr=f"element_at({sql_array(medications)}, drug_index + 1)")
    .withColumn("ndc_code", "string", baseColumn="drug_index",
                expr=f"element_at({sql_array(ndc_prefixes)}, drug_index + 1)")
    .withColumn("dose", "double",
                values=[2.5, 5.0, 10.0, 20.0, 25.0, 40.0, 50.0, 80.0, 100.0, 500.0],
                random=True)
    .withColumn("dose_unit", "string",
                values=["mg", "mcg", "mL", "units", "puffs"],
                weights=[70, 15, 8, 4, 3])
    .withColumn("route", "string",
                values=["Oral", "Intravenous", "Subcutaneous", "Topical", "Inhaled", "Sublingual", "Intramuscular"],
                weights=[70, 10, 8, 5, 4, 2, 1])
    .withColumn("frequency", "string",
                values=["Once daily", "Twice daily", "Three times daily", "Four times daily",
                        "Every 8 hours", "As needed", "Weekly", "Monthly"],
                weights=[40, 25, 10, 5, 8, 7, 3, 2])
    .withColumn("days_supply", "integer",
                values=[7, 14, 30, 60, 90],
                weights=[5, 5, 50, 20, 20])
    .withColumn("refills_authorized", "integer",
                minValue=0, maxValue=12, random=True,
                distribution=dg.distributions.Normal(mean=3, stddev=2))
    .withColumn("refills_dispensed", "integer",
                minValue=0, maxValue=12, random=True)
    .withColumn("prescribed_date", "date",
                begin="2019-01-01", end="2024-12-31", random=True)
    .withColumn("is_generic", "boolean",
                expr="rand() > 0.35")
    # Adherence: patient filled >=80% of authorized refills.
    .withColumn("is_adherent", "boolean", baseColumn=["refills_dispensed", "refills_authorized"],
                expr="refills_dispensed >= (refills_authorized * 0.8)")
    .withColumn("pharmacy_id", "string", template=r"PHR-ddddd", random=True)
    .withColumn("formulary_tier", "integer",
                minValue=1, maxValue=5, random=True,
                distribution=dg.distributions.Normal(mean=2, stddev=1))
)

medications_df = medication_spec.build()
medications_df = add_encounter_columns(medications_df, columns=("patient_id", "provider_id"))
medications_df.createOrReplaceTempView("medications")

# Compute the row count and non-adherent count in a single pass instead of scanning twice.
rx_stats = medications_df.selectExpr(
    "count(*) AS total_rx",
    "count_if(NOT is_adherent) AS non_adherent",
).first()
print(f"Prescriptions generated: {rx_stats.total_rx:,}")
print(f"Non-adherence rate: {rx_stats.non_adherent / rx_stats.total_rx:.1%}")
display(medications_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Results (LOINC)
# MAGIC
# MAGIC Laboratory test results are one of the richest signals in healthcare analytics.
# MAGIC LOINC (Logical Observation Identifiers Names and Codes) is the international standard for lab tests.
# MAGIC Each result includes the observed value, reference range, and an `is_abnormal` flag —
# MAGIC essential for building early-warning and sepsis prediction models.
# MAGIC
# MAGIC Here a single hidden index keeps **four** columns aligned: `loinc_code`, `test_name`,
# MAGIC `reference_range_low`, and `reference_range_high`. Because low/high come from the same row of
# MAGIC the reference table, `low` is never greater than `high`. `result_value` is then generated with
# MAGIC an `expr` *relative to that test's own range*, so the `is_abnormal` flag is meaningful (roughly
# MAGIC 15–20% abnormal) rather than random.

# COMMAND ----------

LAB_COUNT = 2_000_000

# Common LOINC codes with their names and typical reference ranges — paired by position.
loinc_codes = ["2345-7", "2160-0", "17861-6", "2093-3", "2085-9", "4548-4", "33914-3",
               "2823-3", "2951-2", "6768-6", "1742-6", "1920-8", "2532-0", "6298-4",
               "2028-9", "718-7", "777-3", "26515-7", "3094-0", "1975-2"]
loinc_names = ["Glucose", "Creatinine", "Calcium", "Cholesterol Total", "HDL Cholesterol",
               "Hemoglobin A1c", "eGFR", "Potassium", "Sodium", "Alkaline Phosphatase",
               "ALT", "AST", "LDH", "Potassium (plasma)", "CO2", "Hemoglobin",
               "Platelets", "Platelets (auto)", "BUN", "Bilirubin Total"]
ref_range_low = [70, 0.6, 8.5, 0, 40, 0, 60, 3.5, 135, 44, 7, 10, 140, 3.5, 22, 12.0, 150, 150, 7, 0.2]
ref_range_high = [100, 1.2, 10.5, 200, 60, 5.7, 999, 5.0, 145, 147, 56, 40, 280, 5.0, 29, 17.5, 400, 400, 20, 1.2]

lab_spec = (
    dg.DataGenerator(spark, name="lab_results", rows=LAB_COUNT, partitions=16,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("lab_result_id", "string", prefix="LAB", baseColumn="id")
    .withColumn("encounter_id", "string",
                expr=f"concat('ENC_', cast(rand() * {ENCOUNTER_COUNT} as int))")
    # Omitted index for matching entries across lists of values
    .withColumn("test_index", "integer",
                minValue=0, maxValue=len(loinc_codes) - 1, random=True, omit=True)
    .withColumn("loinc_code", "string", baseColumn="test_index",
                expr=f"element_at({sql_array(loinc_codes)}, test_index + 1)")
    .withColumn("test_name", "string", baseColumn="test_index",
                expr=f"element_at({sql_array(loinc_names)}, test_index + 1)")
    .withColumn("reference_range_low", "double", baseColumn="test_index",
                expr=f"element_at({sql_array(ref_range_low)}, test_index + 1)")
    .withColumn("reference_range_high", "double", baseColumn="test_index",
                expr=f"element_at({sql_array(ref_range_high)}, test_index + 1)")
    # Draw the result around this test's own reference band; the *1.2 spread with a -0.15
    # offset lets a realistic minority of results fall below low / above high.
    .withColumn("result_value", "double",
                baseColumn=["reference_range_low", "reference_range_high"],
                expr="greatest(round(reference_range_low + (rand() - 0.15) * 1.2 * "
                     "(reference_range_high - reference_range_low), 2), 0.0)")
    .withColumn("result_unit", "string",
                values=["mg/dL", "mmol/L", "g/dL", "%", "U/L", "mEq/L", "10^3/uL", "mL/min/1.73m2"],
                random=True)
    # Flag results outside the (correlated) reference range.
    .withColumn("is_abnormal", "boolean",
                baseColumn=["result_value", "reference_range_low", "reference_range_high"],
                expr="result_value < reference_range_low OR result_value > reference_range_high")
    .withColumn("abnormal_flag", "string",
                baseColumn=["result_value", "reference_range_low", "reference_range_high"],
                expr="""
                    CASE
                        WHEN result_value < reference_range_low  THEN 'L'
                        WHEN result_value > reference_range_high THEN 'H'
                        ELSE 'N'
                    END
                """)
    .withColumn("result_status", "string",
                values=["Final", "Preliminary", "Corrected", "Cancelled", "Entered in Error"],
                weights=[90, 5, 3, 1, 1])
    .withColumn("collection_datetime", "timestamp",
                begin="2019-01-01 00:00:00", end="2024-12-31 23:59:59", random=True)
    .withColumn("resulted_datetime", "timestamp", baseColumn="collection_datetime",
                expr="collection_datetime + interval 2 hours")
    .withColumn("performing_lab", "string",
                values=["Quest Diagnostics", "LabCorp", "Hospital Lab", "Point of Care", "Reference Lab"],
                weights=[30, 30, 25, 10, 5])
    .withColumn("critical_flag", "boolean", baseColumn="is_abnormal",
                expr="is_abnormal AND rand() < 0.05")
)

labs_df = lab_spec.build()
labs_df = add_encounter_columns(labs_df, columns=("patient_id",))
labs_df.createOrReplaceTempView("lab_results")

# One aggregation pass for the row count and both rates (instead of three full scans).
lab_stats = labs_df.selectExpr(
    "count(*) AS total",
    "count_if(is_abnormal) AS abnormal",
    "count_if(critical_flag) AS critical",
).first()
print(f"Lab results generated: {lab_stats.total:,}")
print(f"Abnormal rate: {lab_stats.abnormal / lab_stats.total:.1%}  |  "
      f"Critical rate: {lab_stats.critical / lab_stats.total:.1%}")
display(labs_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Insurance Claims
# MAGIC
# MAGIC Medical claims derived from encounters — the financial record of care delivered.
# MAGIC Includes CPT procedure codes, billed vs. allowed amounts, payer adjudication, and denial reasons.
# MAGIC Revenue cycle analytics, fraud detection, and payer-provider benchmarking all rely on this data.
# MAGIC
# MAGIC `cpt_code` and `cpt_description` are correlated via a shared index, and the financial columns
# MAGIC are layered with `expr` so `allowed_amount`, `patient_responsibility`, and `payer_paid_amount`
# MAGIC are all consistent fractions of the `billed_amount`.

# COMMAND ----------

CLAIM_COUNT = 500_000

# Common CPT (Current Procedural Terminology) codes, paired with descriptions.
cpt_codes = [
    "99213", "99214", "99215", "99232", "99291", "93000", "71046", "80053", "36415",
    "93306", "70553", "27447", "43239", "45378", "99285", "29881", "93510", "70450",
    "99283", "97110"
]
cpt_descriptions = [
    "Office visit, est. patient moderate complexity",
    "Office visit, est. patient moderate-high complexity",
    "Office visit, est. patient high complexity",
    "Subsequent hospital care, moderate complexity",
    "Critical care, first 30-74 min",
    "Electrocardiogram with interpretation",
    "Chest X-ray, 2 views",
    "Comprehensive metabolic panel",
    "Venipuncture",
    "Echocardiography with Doppler",
    "MRI brain with contrast",
    "Total knee replacement",
    "EGD with biopsy",
    "Colonoscopy, diagnostic",
    "Emergency dept visit, high severity",
    "Knee arthroscopy with meniscectomy",
    "Left heart catheterization",
    "CT head/brain without contrast",
    "Emergency dept visit, moderate severity",
    "Therapeutic exercise"
]

claim_spec = (
    dg.DataGenerator(spark, name="insurance_claims", rows=CLAIM_COUNT, partitions=8,
                     randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("claim_id", "string", prefix="CLM", baseColumn="id")
    .withColumn("encounter_id", "string",
                expr=f"concat('ENC_', cast(rand() * {ENCOUNTER_COUNT} as int))")
    .withColumn("payer_id", "string", template=r"PAY-ddddd", random=True)
    .withColumn("payer_name", "string",
                values=["UnitedHealth", "Anthem", "Aetna", "Cigna", "Humana",
                        "BCBS", "Medicare", "Medicaid", "Centene", "Molina"],
                weights=[14, 12, 11, 10, 9, 13, 10, 8, 7, 6])
    .withColumn("claim_type", "string",
                values=["Professional", "Institutional", "Dental", "Vision", "Pharmacy"],
                weights=[45, 30, 8, 5, 12])
    .withColumn("service_date", "date",
                begin="2019-01-01", end="2024-12-31", random=True)
    .withColumn("submission_date", "date", baseColumn="service_date",
                expr="date_add(service_date, cast(rand() * 14 as int))")
    # Omitted index for matching entries across lists of values
    .withColumn("cpt_index", "integer",
                minValue=0, maxValue=len(cpt_codes) - 1, random=True, omit=True)
    .withColumn("cpt_code", "string", baseColumn="cpt_index",
                expr=f"element_at({sql_array(cpt_codes)}, cpt_index + 1)")
    .withColumn("cpt_description", "string", baseColumn="cpt_index",
                expr=f"element_at({sql_array(cpt_descriptions)}, cpt_index + 1)")
    .withColumn("diagnosis_code", "string", values=icd10_codes, random=True)
    # Financials — each amount is a consistent fraction of the one above it.
    .withColumn("billed_amount", "double",
                minValue=50.0, maxValue=250_000.0, random=True,
                distribution=dg.distributions.Exponential(rate=0.00008))
    .withColumn("allowed_amount", "double", baseColumn="billed_amount",
                expr="round(billed_amount * (0.3 + rand() * 0.5), 2)")
    .withColumn("patient_responsibility", "double", baseColumn="allowed_amount",
                expr="round(allowed_amount * (0.1 + rand() * 0.25), 2)")
    .withColumn("payer_paid_amount", "double", baseColumn=["allowed_amount", "patient_responsibility"],
                expr="round(allowed_amount - patient_responsibility, 2)")
    # Adjudication
    .withColumn("claim_status", "string",
                values=["Paid", "Denied", "Pending", "Partially Paid", "Adjusted", "Voided"],
                weights=[65, 12, 8, 8, 5, 2])
    .withColumn("denial_reason", "string", baseColumn="claim_status",
                expr="""
                    CASE WHEN claim_status = 'Denied' THEN
                        CASE cast(rand() * 5 as int)
                            WHEN 0 THEN 'Not medically necessary'
                            WHEN 1 THEN 'Prior authorization required'
                            WHEN 2 THEN 'Duplicate claim'
                            WHEN 3 THEN 'Non-covered service'
                            ELSE 'Eligibility issue'
                        END
                    ELSE NULL
                    END
                """)
    .withColumn("place_of_service", "string",
                values=["11", "21", "22", "23", "24", "31", "32", "81"],
                weights=[35, 20, 10, 12, 8, 5, 5, 5])
    .withColumn("drg_code", "string",
                values=["470", "291", "292", "871", "392", "603", "065", "247", "313", "281"],
                random=True)
)

claims_df = claim_spec.build()
claims_df = add_encounter_columns(claims_df, columns=("patient_id", "provider_id"))
claims_df.createOrReplaceTempView("insurance_claims")

# Row count and denied count in one pass.
claim_stats = claims_df.selectExpr(
    "count(*) AS total",
    "count_if(claim_status = 'Denied') AS denied",
).first()
print(f"Claims generated: {claim_stats.total:,}")
print(f"Claim denial rate: {claim_stats.denied / claim_stats.total:.1%}")
display(claims_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

spark.sql("""
SELECT 'Patients'         AS dataset, COUNT(*) AS row_count FROM patients
UNION ALL SELECT 'Providers',        COUNT(*) FROM providers
UNION ALL SELECT 'Encounters',       COUNT(*) FROM encounters
UNION ALL SELECT 'Diagnoses',        COUNT(*) FROM diagnoses
UNION ALL SELECT 'Medications',      COUNT(*) FROM medications
UNION ALL SELECT 'Lab Results',      COUNT(*) FROM lab_results
UNION ALL SELECT 'Insurance Claims', COUNT(*) FROM insurance_claims
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Analytical Queries
# MAGIC
# MAGIC Example analytics that can be run on the generated data to validate realism
# MAGIC and demonstrate end-to-end utility.

# COMMAND ----------

# MAGIC %md ### Diagnoses by Volume

# COMMAND ----------

spark.sql("""
    SELECT icd10_description, icd10_code, COUNT(*) AS diagnosis_count,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
    FROM diagnoses
    GROUP BY icd10_description, icd10_code
    ORDER BY diagnosis_count DESC
    LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md ### Readmission Risk

# COMMAND ----------

spark.sql("""
    SELECT e.patient_id,
           COUNT(DISTINCT e.encounter_id) AS total_encounters,
           SUM(e.length_of_stay_days)     AS total_los_days,
           AVG(e.total_charge_usd)        AS avg_charge_usd,
           COUNT(DISTINCT d.icd10_code)   AS distinct_diagnoses
    FROM encounters e
    LEFT JOIN diagnoses d ON e.encounter_id = d.encounter_id
    GROUP BY e.patient_id
    HAVING total_encounters >= 3
    ORDER BY total_encounters DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md ### Medication Non-Adherence by Drug

# COMMAND ----------

spark.sql("""
    SELECT drug_name,
           COUNT(*)                                          AS total_prescriptions,
           SUM(CASE WHEN NOT is_adherent THEN 1 ELSE 0 END) AS non_adherent_count,
           ROUND(AVG(CASE WHEN NOT is_adherent THEN 1.0 ELSE 0.0 END) * 100, 1) AS non_adherence_pct
    FROM medications
    GROUP BY drug_name
    ORDER BY non_adherence_pct DESC
""").display()

# COMMAND ----------

# MAGIC %md ### Claim Denial Rate by Payer

# COMMAND ----------

spark.sql("""
    SELECT payer_name,
           COUNT(*)                                                      AS total_claims,
           SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END)     AS denied_claims,
           ROUND(AVG(CASE WHEN claim_status='Denied' THEN 1.0 ELSE 0.0 END)*100,1) AS denial_rate_pct,
           ROUND(SUM(billed_amount), 0)                                  AS total_billed_usd,
           ROUND(SUM(payer_paid_amount), 0)                              AS total_paid_usd
    FROM insurance_claims
    GROUP BY payer_name
    ORDER BY denial_rate_pct DESC
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### Notes & HIPAA Reminder
# MAGIC - All data is **fully synthetic** — no real patient PII, PHI, or medical records.
# MAGIC   Safe to use in any environment without HIPAA compliance considerations.
# MAGIC - ICD-10 codes, LOINC codes, CPT codes, and NDC prefixes used are real coding standards
# MAGIC   but the data values associated with them are randomly generated.
# MAGIC - Row counts are kept modest so the notebook runs quickly. Scale `PATIENT_COUNT`,
# MAGIC   `ENCOUNTER_COUNT`, etc. up to match your cluster size and workload — the foreign keys and
# MAGIC   `partitions` settings will follow. For billion-row testing, increase `partitions`
# MAGIC   proportionally.
# MAGIC - See the [dbldatagen documentation](https://databrickslabs.github.io/dbldatagen) for
# MAGIC   more on distributions, constraints, and change data capture (CDC) data generation.
