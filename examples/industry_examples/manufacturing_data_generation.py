# Databricks notebook source
# MAGIC %md
# MAGIC # Manufacturing Data Generation with dbldatagen
# MAGIC
# MAGIC This notebook demonstrates how to use the **Databricks Labs Data Generator (`dbldatagen`)**
# MAGIC to synthesize realistic Manufacturing IoT data at scale for a **factory-machine predictive
# MAGIC maintenance** use case.
# MAGIC
# MAGIC It reproduces the data model behind the Databricks
# MAGIC [Lakehouse IoT Platform predictive-maintenance demo](https://notebooks.databricks.com/demos/lakehouse-iot-platform/index.html)
# MAGIC — factory machines streaming vibration sensor data, a historical failure log used as ML labels,
# MAGIC a spare-parts inventory, and a derived feature/label set ready for model training. A faulty machine shows
# MAGIC **elevated variance on one specific sensor** (A–F), and that sensor is recorded in the status log — so the
# MAGIC derived training set carries a genuinely learnable signal.
# MAGIC
# MAGIC ### Covered Use Cases
# MAGIC | # | Dataset | Rows | Description |
# MAGIC |---|---------|------|-------------|
# MAGIC | 1 | **Machines** | 1 K | Factory-machine asset registry with location & model |
# MAGIC | 2 | **Historical Machine Status** | 1 K | Failure log per machine — the ML **label** (`abnormal_sensor`) |
# MAGIC | 3 | **Sensor Readings** | 10 M | High-volume vibration/energy time-series (sensors A–F) |
# MAGIC | 4 | **Parts Inventory** | 200 | Spare parts with sensor mapping & stock info |
# MAGIC | 5 | **Maintenance Work Orders** | derived | Faulty machines joined to the relevant spare part |
# MAGIC | 6 | **Predictive-Maintenance Training Set** | derived | Hourly sensor features + label, ready for ML |

# COMMAND ----------
# MAGIC %pip install dbldatagen
# MAGIC %restart_python

# COMMAND ----------
import dbldatagen as dg
from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# Scale knobs: tune these to match your workload
NUM_MACHINES            = 1_000
READINGS_PER_MACHINE    = 10_000
READING_FREQ_SECONDS    = 10
SENSOR_START_TS         = "2024-06-01 00:00:00"
NUM_SENSOR_READINGS     = NUM_MACHINES * READINGS_PER_MACHINE
NUM_PARTS               = 200
FAULT_RATE              = 0.1
SENSOR_NAMES            = ["sensor_A", "sensor_B", "sensor_C", "sensor_D", "sensor_E", "sensor_F"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Machines
# MAGIC
# MAGIC The factory-machine registry: one row per physical machine, with its model, nameplate, capacity, and geographic
# MAGIC location. The `machine_id` column is used to join to telemetry tables.

# COMMAND ----------

machine_spec = (
    dg.DataGenerator(spark, name="machines", rows=NUM_MACHINES, partitions=8, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("machine_id",     "string", expr="concat('MCH-', lpad(cast(id as string), 5, '0'))", baseColumn="id")
    .withColumn("model",          "string",
                values=["PrecisionCNC", "StandardMill", "RoboArm", "PressMax"],
                weights=[40, 35, 15, 10])
    .withColumn("capacity_kw",    "integer",
                values=[1500, 2000, 2500, 3000, 4000],
                weights=[15, 30, 30, 15, 10])
    # One physical factory site packs city, state, and a base lat/long together
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
    # Add some jitter ±0.25° (~25 km) around the base coordinates; Machines have distinct, clustered locations
    .withColumn("lat",            "double",
                expr="round(cast(split(site, ',')[2] as double) + (rand() - 0.5) * 0.5, 5)",
                baseColumn="site")
    .withColumn("long",           "double",
                expr="round(cast(split(site, ',')[3] as double) + (rand() - 0.5) * 0.5, 5)",
                baseColumn="site")
    .withColumn("country",        "string", values=["US"])
    .withColumn("install_date",   "date", begin="2010-01-01", end="2023-12-31", random=True)
)

machines_df = machine_spec.build()
machines_df.createOrReplaceTempView("machines")
print(f"Machines generated: {machines_df.count():,}")
display(machines_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Machine Status (ML Label)
# MAGIC
# MAGIC Labels machines for a specific operating period with a status indicating if the machine was healthy.
# MAGIC `abnormal_sensor` is `'ok'` for a healthy machine, otherwise the name of the degraded sensor.
# MAGIC This column is the **label** for the predictive-maintenance model.

# COMMAND ----------

# Common expression to track if a sensor is reporting abnormally; Shared across datasets for consistency
ABNORMAL_SENSOR_EXPR = f"""
    CASE WHEN pmod(abs(hash(machine_id)), {int(1/FAULT_RATE)}) = 0
         THEN element_at(array({", ".join(repr(s) for s in SENSOR_NAMES)}),
                         cast(pmod(abs(hash(machine_id, 42)), {len(SENSOR_NAMES)}) + 1 as int))
         ELSE 'ok'
    END
"""

status_spec = (
    dg.DataGenerator(spark, name="historical_machine_status", rows=NUM_MACHINES, partitions=8,
                  randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("machine_id",      "string", expr="concat('MCH-', lpad(cast(id as string), 5, '0'))", baseColumn="id")
    .withColumn("start_time",      "timestamp", begin="2024-01-01 00:00:00", end="2024-06-01 00:00:00", random=True)
    .withColumn("end_time",        "timestamp", begin="2024-06-02 00:00:00", end="2024-12-31 00:00:00", random=True)
    .withColumn("abnormal_sensor", "string", expr=ABNORMAL_SENSOR_EXPR, baseColumn="machine_id")
)

status_df = status_spec.build()
status_df.createOrReplaceTempView("historical_machine_status")
faulty = status_df.filter("abnormal_sensor != 'ok'").count()
print(f"Status rows: {status_df.count():,}  |  faulty machines: {faulty:,} ({faulty/NUM_MACHINES:.1%})")
display(status_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sensor Readings
# MAGIC
# MAGIC Readings from 6 IoT vibration sensors (A–F) and a throughput sensor. Each machine emits readings at
# MAGIC a fixed frequency. A `fault_idx` (omitted from the output) is derived from `machine_id` using the
# MAGIC same rule as the machine status log; faulty sensors emit readings with elevated variance.

# COMMAND ----------

FAULT_IDX_EXPR = f"""
    CASE WHEN pmod(abs(hash(machine_id)), {int(1/FAULT_RATE)}) = 0
         THEN pmod(abs(hash(machine_id, 42)), {len(SENSOR_NAMES)}) + 1
         ELSE 0
    END
"""

def sensor_expr(idx, base_mean, base_std):
    # baseline normal noise + an extra positive-variance bump when this sensor is the faulty one
    return (f"round({base_mean} + randn() * {base_std} + "
            f"CASE WHEN fault_idx = {idx} THEN abs(randn()) * {base_std * 5:.2f} ELSE 0 END, 4)")

sensor_spec = (
    dg.DataGenerator(spark, name="sensor_readings", rows=NUM_SENSOR_READINGS, partitions=64,
                  randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("reading_id",  "string", expr="concat('R-', cast(id as string))", baseColumn="id")
    # Decompose the row id into a pair of (machine_id, seq) that tracks the sequence id for each machine
    .withColumn("machine_idx", "long", expr=f"id div {READINGS_PER_MACHINE}", baseColumn="id", omit=True)
    .withColumn("seq",         "long", expr=f"id % {READINGS_PER_MACHINE}",  baseColumn="id", omit=True)
    .withColumn("machine_id",  "string",
                expr="concat('MCH-', lpad(cast(machine_idx as string), 5, '0'))", baseColumn="machine_idx")
    # Build a regularly-spaced grid: base time + seq * READING_FREQ_SECONDS
    .withColumn("timestamp",   "timestamp",
                expr=f"timestampadd(SECOND, seq * {READING_FREQ_SECONDS}, timestamp('{SENSOR_START_TS}'))",
                baseColumn="seq")
    .withColumn("fault_idx",   "integer", expr=FAULT_IDX_EXPR, baseColumn="machine_id", omit=True)
    .withColumn("sensor_A",    "double", expr=sensor_expr(1, 2.5, 0.30), baseColumn="fault_idx")
    .withColumn("sensor_B",    "double", expr=sensor_expr(2, 3.1, 0.35), baseColumn="fault_idx")
    .withColumn("sensor_C",    "double", expr=sensor_expr(3, 1.8, 0.25), baseColumn="fault_idx")
    .withColumn("sensor_D",    "double", expr=sensor_expr(4, 4.2, 0.40), baseColumn="fault_idx")
    .withColumn("sensor_E",    "double", expr=sensor_expr(5, 2.0, 0.28), baseColumn="fault_idx")
    .withColumn("sensor_F",    "double", expr=sensor_expr(6, 3.6, 0.33), baseColumn="fault_idx")
    # Throughput dips for faulty machines
    .withColumn("throughput",  "double",
                expr="round(greatest(10.0, 350 + randn() * 45 - CASE WHEN fault_idx > 0 THEN 70 ELSE 0 END), 3)",
                baseColumn="fault_idx")
)

sensor_df = sensor_spec.build()
sensor_df.createOrReplaceTempView("sensor_readings")
print(f"Sensor readings generated: {sensor_df.count():,}")
display(sensor_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parts Inventory
# MAGIC
# MAGIC A catalog of spare parts. Each part type maps to a specific sensor requiring repair.

# COMMAND ----------

parts_spec = (
    dg.DataGenerator(spark, name="parts", rows=NUM_PARTS, partitions=4, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("EAN",                          "string", template=r"dddddddd")
    .withColumn("type",                         "string",
                values=["spindle", "gearbox", "servo motor", "main bearing", "drive motor", "mounting bolt"],
                weights=[20, 20, 18, 17, 15, 10])
    # Map each part type to the sensor it covers
    .withColumn("sensors",                      "array<string>",
                expr="""array(CASE type
                            WHEN 'spindle'       THEN 'sensor_A'
                            WHEN 'gearbox'       THEN 'sensor_B'
                            WHEN 'servo motor'   THEN 'sensor_C'
                            WHEN 'main bearing'  THEN 'sensor_D'
                            WHEN 'drive motor'   THEN 'sensor_E'
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
# MAGIC ## Maintenance Work Orders
# MAGIC
# MAGIC Generate a work order for every faulty machine. Get the spare part that fixes its degraded
# MAGIC sensor. One part is selected per machine; priority is driven by parts availability.

# COMMAND ----------

work_orders_df = spark.sql("""
    WITH faulty AS (
        SELECT machine_id, abnormal_sensor, end_time
        FROM historical_machine_status
        WHERE abnormal_sensor != 'ok'
    ),
    matched AS (
        SELECT
            f.machine_id,
            f.abnormal_sensor,
            f.end_time,
            p.EAN              AS part_ean,
            p.type             AS part_type,
            p.stock_available,
            p.approvisioning_estimated_days,
            ROW_NUMBER() OVER (PARTITION BY f.machine_id ORDER BY p.stock_available DESC) AS rn
        FROM faulty f
        JOIN parts p ON array_contains(p.sensors, f.abnormal_sensor)
    )
    SELECT
        concat('WO-', machine_id, '-', date_format(end_time, 'yyyyMMdd')) AS work_order_id,
        machine_id,
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
# MAGIC ## Predictive Maintenance Training Set
# MAGIC
# MAGIC Aggregate raw sensor readings into hourly statistical features (standard deviation per sensor,
# MAGIC average throughput), enrich with machine metadata, and attach the `abnormal_sensor` label from the
# MAGIC status log. The `sensor_vector` array is a feature column for machine learning inference.

# COMMAND ----------

training_df = spark.sql("""
    WITH sensor_hourly AS (
        SELECT
            machine_id,
            date_trunc('hour', timestamp)     AS hourly_timestamp,
            avg(throughput)                    AS avg_throughput,
            stddev_pop(sensor_A)               AS std_sensor_A,
            stddev_pop(sensor_B)               AS std_sensor_B,
            stddev_pop(sensor_C)               AS std_sensor_C,
            stddev_pop(sensor_D)               AS std_sensor_D,
            stddev_pop(sensor_E)               AS std_sensor_E,
            stddev_pop(sensor_F)               AS std_sensor_F
        FROM sensor_readings
        GROUP BY machine_id, date_trunc('hour', timestamp)
    )
    SELECT
        concat(h.machine_id, '-', date_format(h.hourly_timestamp, 'yyyyMMddHH')) AS composite_key,
        array(h.std_sensor_A, h.std_sensor_B, h.std_sensor_C,
              h.std_sensor_D, h.std_sensor_E, h.std_sensor_F) AS sensor_vector,
        h.*,
        t.model,
        t.location,
        t.state,
        s.abnormal_sensor
    FROM sensor_hourly h
    INNER JOIN machines t                   USING (machine_id)
    INNER JOIN historical_machine_status s  USING (machine_id)
""")

training_df.createOrReplaceTempView("machine_training_dataset")
print(f"Training rows generated: {training_df.count():,}")
print("\nLabel distribution:")
training_df.groupBy("abnormal_sensor").count().orderBy(F.desc("count")).show()
display(training_df.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

spark.sql("""
SELECT 'Machines'                AS dataset, COUNT(*) AS row_count FROM machines
UNION ALL SELECT 'Historical Status',        COUNT(*) FROM historical_machine_status
UNION ALL SELECT 'Sensor Readings',          COUNT(*) FROM sensor_readings
UNION ALL SELECT 'Parts',                    COUNT(*) FROM parts
UNION ALL SELECT 'Work Orders',              COUNT(*) FROM maintenance_work_orders
UNION ALL SELECT 'Training Set',             COUNT(*) FROM machine_training_dataset
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### Notes
# MAGIC - All data is **fully synthetic** — no real machine, sensor, or operational records are used or implied.
# MAGIC - Row counts above are representative; scale `NUM_MACHINES`, `READINGS_PER_MACHINE`, etc. to match your workload.
# MAGIC - For billion-row testing, increase `partitions` proportionally and use a multi-node cluster (≥ 8 workers).
# MAGIC - The fault signal is intentionally learnable: a faulty machine shows elevated variance on exactly one sensor,
# MAGIC   and that sensor is recorded with the `abnormal_sensor` label. This can be used for an AutoML predictive-maintenance demo.
# MAGIC - See the [dbldatagen documentation](https://databrickslabs.github.io/dbldatagen) for more on distributions,
# MAGIC   constraints, and CDC data generation.
