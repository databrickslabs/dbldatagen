# Databricks notebook source
# MAGIC %md
# MAGIC # Manufacturing Industry Demo — `dbldatagen`
# MAGIC
# MAGIC This notebook demonstrates how to use the **Databricks Labs Data Generator (`dbldatagen`)**
# MAGIC to synthesize realistic Manufacturing / IoT data at scale for a **wind-turbine predictive
# MAGIC maintenance** use case.
# MAGIC
# MAGIC It reproduces the data model behind the Databricks
# MAGIC [Lakehouse IoT Platform predictive-maintenance demo](https://notebooks.databricks.com/demos/lakehouse-iot-platform/index.html)
# MAGIC — wind turbines streaming vibration sensor data, a historical failure log used as ML labels,
# MAGIC a spare-parts inventory, and a derived feature/label set ready for model training.
# MAGIC
# MAGIC ### Covered Use Cases
# MAGIC | # | Dataset | Rows | Description |
# MAGIC |---|---------|------|-------------|
# MAGIC | 1 | **Turbines** | 1 K | Wind-turbine asset registry with location & model |
# MAGIC | 2 | **Historical Turbine Status** | 1 K | Failure log per turbine — the ML **label** (`abnormal_sensor`) |
# MAGIC | 3 | **Sensor Readings** | 10 M | High-volume vibration/energy time-series (sensors A–F) |
# MAGIC | 4 | **Parts Inventory** | 200 | Spare parts with sensor mapping & stock info |
# MAGIC | 5 | **Maintenance Work Orders** | derived | Faulty turbines joined to the relevant spare part |
# MAGIC | 6 | **Predictive-Maintenance Training Set** | derived | Hourly sensor features + label, ready for ML |
# MAGIC
# MAGIC > **Runtime**: Databricks 13.3 LTS or above (Unity Catalog supported).
# MAGIC >
# MAGIC > A faulty turbine shows **elevated variance on one specific sensor** (A–F), and that sensor is
# MAGIC > recorded in the status log — so the derived training set carries a genuinely learnable signal.

# COMMAND ----------
# MAGIC %pip install dbldatagen
dbutils.library.restartPython()

# COMMAND ----------
import dbldatagen as dg
from dbldatagen import DataGenerator
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# --- Scale knobs: tune these to match your workload ---------------------------
NUM_TURBINES          = 1_000
# Each turbine emits a fixed-cadence stream (mirrors the dbdemos generator's
# `sample_size` / `frequency_sec`), so every hourly bucket holds the same number of
# evenly-spaced readings — 3600 / READING_FREQ_SEC = 360 per turbine-hour here.
READINGS_PER_TURBINE  = 10_000
READING_FREQ_SEC      = 10       # seconds between consecutive readings for a turbine
SENSOR_START          = "2024-06-01 00:00:00"
SENSOR_READING_COUNT  = NUM_TURBINES * READINGS_PER_TURBINE   # = 10M, high-volume headline
PARTS_COUNT           = 200
# Fault model: ~10% of turbines are faulty; a faulty turbine degrades exactly one
# of its six sensors. Both the status log and the sensor stream derive the fault
# deterministically from `turbine_id`, so they stay consistent without a join.
FAULT_RATE_DENOM      = 10     # 1-in-10 turbines faulty
SENSOR_NAMES          = ["sensor_A", "sensor_B", "sensor_C", "sensor_D", "sensor_E", "sensor_F"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Turbines
# MAGIC
# MAGIC The wind-turbine asset registry: one row per physical turbine, with its model, nameplate
# MAGIC capacity, and geographic location. `turbine_id` is the foreign key every other table joins on.

# COMMAND ----------

turbine_spec = (
    DataGenerator(spark, name="turbines", rows=NUM_TURBINES, partitions=8, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    # Zero-padded, stable id used as the join key across every table
    .withColumn("turbine_id",     "string", expr="concat('TBN-', lpad(cast(id as string), 5, '0'))", baseColumn="id")
    .withColumn("model",          "string",
                values=["EpicWind", "StandardWind", "TurbineX", "WindMax"],
                weights=[40, 35, 15, 10])
    .withColumn("capacity_kw",    "integer",
                values=[1500, 2000, 2500, 3000, 4000],
                weights=[15, 30, 30, 15, 10])
    # One physical wind-farm site packs city, state, and a base lat/long together, so
    # turbines at the same site share a location instead of city/state/coordinates being
    # drawn independently. Omitted from the output; the components are split out below.
    .withColumn("site",           "string",
                values=["Greenpoint,TX,32.90,-97.30", "Eastfield,IA,41.60,-93.60",
                        "Westridge,OK,35.50,-97.50",  "Northgate,KS,38.50,-98.00",
                        "Southport,CA,35.30,-119.00", "Lakeside,IL,40.10,-89.00",
                        "Hillcrest,CO,39.00,-104.80", "Bayview,MN,46.70,-94.70",
                        "Fairwind,ND,47.50,-100.50",  "Crestline,OR,44.10,-120.50"],
                weights=[20, 14, 12, 10, 10, 8, 8, 6, 6, 6],
                random=True, omit=True)
    .withColumn("location",       "string", expr="split(site, ',')[0]", baseColumn="site")
    .withColumn("state",          "string", expr="split(site, ',')[1]", baseColumn="site")
    # Jitter ±0.25° (~25 km) around the site's base coordinates so turbines cluster
    # geographically while still being individually distinct.
    .withColumn("lat",            "double",
                expr="round(cast(split(site, ',')[2] as double) + (rand() - 0.5) * 0.5, 5)",
                baseColumn="site")
    .withColumn("long",           "double",
                expr="round(cast(split(site, ',')[3] as double) + (rand() - 0.5) * 0.5, 5)",
                baseColumn="site")
    .withColumn("country",        "string", values=["US"])
    .withColumn("install_date",   "date", begin="2010-01-01", end="2023-12-31", random=True)
)

turbines_df = turbine_spec.build()
turbines_df.createOrReplaceTempView("turbines")
print(f"Turbines generated: {turbines_df.count():,}")
display(turbines_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Historical Turbine Status (ML Label)
# MAGIC
# MAGIC One row per turbine describing an observed operating period and whether it was healthy.
# MAGIC `abnormal_sensor` is `'ok'` for a healthy turbine, otherwise the name of the degraded sensor.
# MAGIC This column is the **label** for the predictive-maintenance model.
# MAGIC
# MAGIC The fault is derived deterministically from `turbine_id` (`hash % 10 == 0` → faulty), so the
# MAGIC same turbine will show the matching anomaly in the sensor stream generated below.

# COMMAND ----------

# CASE expression shared with the sensor stream so label and signal agree.
ABNORMAL_SENSOR_EXPR = f"""
    CASE WHEN pmod(abs(hash(turbine_id)), {FAULT_RATE_DENOM}) = 0
         THEN element_at(array({", ".join(repr(s) for s in SENSOR_NAMES)}),
                         cast(pmod(abs(hash(turbine_id, 42)), {len(SENSOR_NAMES)}) + 1 as int))
         ELSE 'ok'
    END
"""

status_spec = (
    DataGenerator(spark, name="historical_turbine_status", rows=NUM_TURBINES, partitions=8,
                  randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("turbine_id",      "string", expr="concat('TBN-', lpad(cast(id as string), 5, '0'))", baseColumn="id")
    .withColumn("start_time",      "timestamp", begin="2024-01-01 00:00:00", end="2024-06-01 00:00:00", random=True)
    .withColumn("end_time",        "timestamp", begin="2024-06-02 00:00:00", end="2024-12-31 00:00:00", random=True)
    .withColumn("abnormal_sensor", "string", expr=ABNORMAL_SENSOR_EXPR, baseColumn="turbine_id")
)

status_df = status_spec.build()
status_df.createOrReplaceTempView("historical_turbine_status")
faulty = status_df.filter("abnormal_sensor != 'ok'").count()
print(f"Status rows: {status_df.count():,}  |  faulty turbines: {faulty:,} ({faulty/NUM_TURBINES:.1%})")
display(status_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — Sensor Readings
# MAGIC
# MAGIC High-volume time-series (10 M rows): six vibration sensors (A–F) plus energy produced. Each
# MAGIC turbine emits a fixed-cadence stream (`READINGS_PER_TURBINE` readings, one every
# MAGIC `READING_FREQ_SEC` s), so every hourly bucket holds the same number of evenly-spaced samples
# MAGIC — this keeps the downstream hourly `stddev` features dense and stable. A hidden `fault_idx`
# MAGIC (omitted from the output) is derived from `turbine_id` using the **same** rule as the status
# MAGIC log; the matching sensor then receives elevated variance so faulty turbines are statistically
# MAGIC distinguishable downstream.

# COMMAND ----------

# fault_idx: 0 = healthy, 1..6 = which sensor (A..F) is degraded — matches ABNORMAL_SENSOR_EXPR.
FAULT_IDX_EXPR = f"""
    CASE WHEN pmod(abs(hash(turbine_id)), {FAULT_RATE_DENOM}) = 0
         THEN pmod(abs(hash(turbine_id, 42)), {len(SENSOR_NAMES)}) + 1
         ELSE 0
    END
"""

def sensor_expr(idx, base_mean, base_std):
    # baseline normal noise + an extra positive-variance bump when this sensor is the faulty one
    return (f"round({base_mean} + randn() * {base_std} + "
            f"CASE WHEN fault_idx = {idx} THEN abs(randn()) * {base_std * 5:.2f} ELSE 0 END, 4)")

sensor_spec = (
    DataGenerator(spark, name="sensor_readings", rows=SENSOR_READING_COUNT, partitions=64,
                  randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("reading_id",  "string", expr="concat('R-', cast(id as string))", baseColumn="id")
    # Decompose the row id into (turbine, sequence) so each turbine gets an equal,
    # fixed-cadence stream rather than a random scatter. id 0..9,999 -> turbine 0,
    # 10,000..19,999 -> turbine 1, etc.; `seq` is the reading's position in that stream.
    .withColumn("turbine_idx", "long", expr=f"id div {READINGS_PER_TURBINE}", baseColumn="id", omit=True)
    .withColumn("seq",         "long", expr=f"id % {READINGS_PER_TURBINE}",  baseColumn="id", omit=True)
    .withColumn("turbine_id",  "string",
                expr="concat('TBN-', lpad(cast(turbine_idx as string), 5, '0'))", baseColumn="turbine_idx")
    # Regular grid: base time + seq * READING_FREQ_SEC — evenly spaced, so hourly stats
    # are computed over dense, uniformly-sampled buckets (360 readings / turbine-hour).
    .withColumn("timestamp",   "timestamp",
                expr=f"timestampadd(SECOND, seq * {READING_FREQ_SEC}, timestamp('{SENSOR_START}'))",
                baseColumn="seq")
    .withColumn("fault_idx",   "integer", expr=FAULT_IDX_EXPR, baseColumn="turbine_id", omit=True)
    .withColumn("sensor_A",    "double", expr=sensor_expr(1, 2.5, 0.30), baseColumn="fault_idx")
    .withColumn("sensor_B",    "double", expr=sensor_expr(2, 3.1, 0.35), baseColumn="fault_idx")
    .withColumn("sensor_C",    "double", expr=sensor_expr(3, 1.8, 0.25), baseColumn="fault_idx")
    .withColumn("sensor_D",    "double", expr=sensor_expr(4, 4.2, 0.40), baseColumn="fault_idx")
    .withColumn("sensor_E",    "double", expr=sensor_expr(5, 2.0, 0.28), baseColumn="fault_idx")
    .withColumn("sensor_F",    "double", expr=sensor_expr(6, 3.6, 0.33), baseColumn="fault_idx")
    # Energy dips for faulty turbines
    .withColumn("energy",      "double",
                expr="round(greatest(10.0, 350 + randn() * 45 - CASE WHEN fault_idx > 0 THEN 70 ELSE 0 END), 3)",
                baseColumn="fault_idx")
)

sensor_df = sensor_spec.build()
sensor_df.createOrReplaceTempView("sensor_readings")
print(f"Sensor readings generated: {sensor_df.count():,}")
display(sensor_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — Parts Inventory
# MAGIC
# MAGIC Spare-parts catalogue. Each part type maps to the sensor whose degradation it addresses
# MAGIC (mirrors the dbdemos `parts.sensors` array), and carries stock, lead-time, and dimension info.

# COMMAND ----------

parts_spec = (
    DataGenerator(spark, name="parts", rows=PARTS_COUNT, partitions=4, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("EAN",                          "string", template=r"dddddddd")
    .withColumn("type",                         "string",
                values=["blade", "gearbox", "pitch motor", "main bearing", "generator", "tower bolt"],
                weights=[20, 20, 18, 17, 15, 10])
    # Map each part type to the sensor it covers (array of one, like dbdemos)
    .withColumn("sensors",                      "array<string>",
                expr="""array(CASE type
                            WHEN 'blade'        THEN 'sensor_A'
                            WHEN 'gearbox'      THEN 'sensor_B'
                            WHEN 'pitch motor'  THEN 'sensor_C'
                            WHEN 'main bearing' THEN 'sensor_D'
                            WHEN 'generator'    THEN 'sensor_E'
                            ELSE 'sensor_F' END)""",
                baseColumn="type")
    .withColumn("stock_available",              "integer", minValue=0, maxValue=20, random=True)
    .withColumn("stock_location",               "string",
                values=["Pacific/Honolulu", "America/Detroit", "America/Chicago",
                        "America/Denver", "America/New_York", "America/Los_Angeles"],
                random=True)
    .withColumn("production_time",              "integer", minValue=1, maxValue=10, random=True)
    .withColumn("approvisioning_estimated_days", "integer", minValue=30, maxValue=365, random=True)
    .withColumn("height",                       "integer", minValue=100, maxValue=2000, random=True)
    .withColumn("width",                        "integer", minValue=100, maxValue=2000, random=True)
    .withColumn("weight",                       "integer", minValue=50,  maxValue=6000, random=True)
)

parts_df = parts_spec.build()
parts_df.createOrReplaceTempView("parts")
print(f"Parts generated: {parts_df.count():,}")
display(parts_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 — Maintenance Work Orders (Derived via SQL)
# MAGIC
# MAGIC Generate a work order for every faulty turbine by joining it to a spare part that covers its
# MAGIC degraded sensor. One part is selected per turbine; priority is driven by parts availability.

# COMMAND ----------

work_orders_df = spark.sql("""
    WITH faulty AS (
        SELECT turbine_id, abnormal_sensor, end_time
        FROM historical_turbine_status
        WHERE abnormal_sensor != 'ok'
    ),
    matched AS (
        SELECT
            f.turbine_id,
            f.abnormal_sensor,
            f.end_time,
            p.EAN              AS part_ean,
            p.type             AS part_type,
            p.stock_available,
            p.approvisioning_estimated_days,
            ROW_NUMBER() OVER (PARTITION BY f.turbine_id ORDER BY p.stock_available DESC) AS rn
        FROM faulty f
        JOIN parts p ON array_contains(p.sensors, f.abnormal_sensor)
    )
    SELECT
        concat('WO-', turbine_id, '-', date_format(end_time, 'yyyyMMdd')) AS work_order_id,
        turbine_id,
        abnormal_sensor,
        part_ean,
        part_type,
        stock_available,
        approvisioning_estimated_days,
        CASE
            WHEN stock_available = 0 THEN 'Critical'
            WHEN stock_available < 3 THEN 'High'
            ELSE 'Normal'
        END AS priority,
        CASE WHEN stock_available > 0 THEN 'Ready to Schedule' ELSE 'Awaiting Parts' END AS status,
        cast(end_time as date) AS created_date
    FROM matched
    WHERE rn = 1
""")

work_orders_df.createOrReplaceTempView("maintenance_work_orders")
print(f"Work orders generated: {work_orders_df.count():,}")
display(work_orders_df.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 — Predictive-Maintenance Training Set (Derived via SQL)
# MAGIC
# MAGIC Aggregate raw sensor readings into hourly statistical features (standard deviation per sensor,
# MAGIC average energy), enrich with turbine metadata, and attach the `abnormal_sensor` label from the
# MAGIC status log. The `sensor_vector` array is the model-ready feature column. This mirrors the
# MAGIC dbdemos `sensor_hourly` → `turbine_training_dataset` flow.

# COMMAND ----------

training_df = spark.sql("""
    WITH sensor_hourly AS (
        SELECT
            turbine_id,
            date_trunc('hour', timestamp)     AS hourly_timestamp,
            avg(energy)                        AS avg_energy,
            stddev_pop(sensor_A)               AS std_sensor_A,
            stddev_pop(sensor_B)               AS std_sensor_B,
            stddev_pop(sensor_C)               AS std_sensor_C,
            stddev_pop(sensor_D)               AS std_sensor_D,
            stddev_pop(sensor_E)               AS std_sensor_E,
            stddev_pop(sensor_F)               AS std_sensor_F
        FROM sensor_readings
        GROUP BY turbine_id, date_trunc('hour', timestamp)
    )
    SELECT
        concat(h.turbine_id, '-', date_format(h.hourly_timestamp, 'yyyyMMddHH')) AS composite_key,
        array(h.std_sensor_A, h.std_sensor_B, h.std_sensor_C,
              h.std_sensor_D, h.std_sensor_E, h.std_sensor_F) AS sensor_vector,
        h.*,
        t.model,
        t.location,
        t.state,
        s.abnormal_sensor
    FROM sensor_hourly h
    INNER JOIN turbines t                   USING (turbine_id)
    INNER JOIN historical_turbine_status s  USING (turbine_id)
""")

training_df.createOrReplaceTempView("turbine_training_dataset")
print(f"Training rows generated: {training_df.count():,}")
print("\nLabel distribution:")
training_df.groupBy("abnormal_sensor").count().orderBy(F.desc("count")).show()
display(training_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

spark.sql("""
SELECT 'Turbines'                AS dataset, COUNT(*) AS row_count FROM turbines
UNION ALL SELECT 'Historical Status',        COUNT(*) FROM historical_turbine_status
UNION ALL SELECT 'Sensor Readings',          COUNT(*) FROM sensor_readings
UNION ALL SELECT 'Parts',                    COUNT(*) FROM parts
UNION ALL SELECT 'Work Orders',              COUNT(*) FROM maintenance_work_orders
UNION ALL SELECT 'Training Set',             COUNT(*) FROM turbine_training_dataset
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### Notes
# MAGIC - All data is **fully synthetic** — no real turbine, sensor, or operational records are used or implied.
# MAGIC - Row counts above are representative; scale `NUM_TURBINES`, `READINGS_PER_TURBINE`, etc. to match your workload.
# MAGIC - For billion-row testing, increase `partitions` proportionally and use a multi-node cluster (≥ 8 workers).
# MAGIC - The fault signal is intentionally learnable: a faulty turbine shows elevated variance on exactly one sensor,
# MAGIC   and that sensor is recorded as the `abnormal_sensor` label — ideal for an AutoML predictive-maintenance demo.
# MAGIC - See the [dbldatagen documentation](https://databrickslabs.github.io/dbldatagen) for more on distributions,
# MAGIC   constraints, and CDC data generation.
