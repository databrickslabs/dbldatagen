# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Oil & Gas Data Generation with DBLDATAGEN
# MAGIC
# MAGIC Welcome to this tutorial notebook! Here, you'll learn how to use the **DBLDATAGEN** library to generate realistic synthetic datasets for oil & gas analytics.  The Daily Production dataset is a fundamental analytical product for all upstream operators and this notebook walks through the creation of this dataset using DBLDATAGEN.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC - **Define Data Generators for well header, daily production, and type curve dataset based on ARPS decline curve parameters** for multiple formations
# MAGIC - **Generate well header, daily production, and type curve forecast data**
# MAGIC - **Visualize and analyze synthetic production data**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This notebook is designed for **petroleum engineers**, **data scientists**, and **analytics professionals** who want to quickly create and experiment with realistic E&P datasets in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC # Section: Install DBLDATAGEN
# MAGIC
# MAGIC This section ensures the **DBLDATAGEN** library is installed in your Databricks environment.
# MAGIC
# MAGIC > **DBLDATAGEN** is a powerful tool for generating large-scale synthetic datasets, ideal for testing, prototyping, and analytics development in oil & gas.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install DBLDATAGEN
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC # Section: Import Libraries
# MAGIC
# MAGIC This section imports all necessary Python libraries:
# MAGIC
# MAGIC - **PySpark**: Distributed data processing
# MAGIC - **DBLDATAGEN**: Synthetic data generation
# MAGIC - **Pandas**: Data manipulation
# MAGIC - **Matplotlib** & **Seaborn**: Data visualization
# MAGIC
# MAGIC These libraries are essential for data generation, manipulation, and visualization throughout the notebook.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import required packages
from pyspark.sql.functions import col
from datetime import date, timedelta
import dbldatagen as dg
import pandas as pd
import matplotlib.pyplot as plt
import random
import seaborn as sns


# COMMAND ----------

# MAGIC %md
# MAGIC # Section: Data Generation Functions
# MAGIC
# MAGIC This section defines **reusable functions** for generating synthetic well header data, daily production profiles, and type curve forecasts.
# MAGIC
# MAGIC - Each function is **modular** and **parameterized**, allowing you to easily adjust:
# MAGIC   - Number of wells
# MAGIC   - Decline curve parameters (ARPS methodology)
# MAGIC   - Other simulation settings
# MAGIC
# MAGIC > ⚙️ **Tip:** Adjust parameters to simulate different reservoir scenarios and production behaviors.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Well Header Data Generation Function
def generate_well_header_data(
    spark_session,
    formation,
    q_i,
    d,
    b_factor,
    num_assets: int = 1000,
) -> dg.DataGenerator:
    # Set the number of wells to generate and number of partitions for parallelism
    row_count = num_assets
    partitions_requested = 4
    randomSeed = int(random.uniform(20, 1000))  # Random seed for reproducibility

    # Define the synthetic well header data specification using DBLDATAGEN
    data_spec = (
        dg.DataGenerator(
            sparkSession=spark_session,
            name=formation,
            rows=row_count,
            partitions=partitions_requested,
            randomSeed=randomSeed,  # Set a new random seed each run
        )
        # Generate unique API numbers for each well
        .withColumn(
            "API_NUMBER",
            "bigInt",
            minValue=42000000000000,
            maxValue=42999999999999,
            random=True,
        )
        # Assign wells to one of several possible fields
        .withColumn(
            "FIELD_NAME",
            "string",
            values=["Field_1", "Field_2", "Field_3", "Field_4", "Field_5"],
            random=True,
        )
        # Generate random latitude within a specified range
        .withColumn(
            "LATITUDE",
            "float",
            minValue=31.00,
            maxValue=32.50,
            step=1e-6,
            random=True,
        )
        # Generate random longitude within a specified range
        .withColumn(
            "LONGITUDE",
            "float",
            minValue=-104.00,
            maxValue=-101.00,
            step=1e-6,
            random=True,
        )
        # Assign wells to a random county from a list
        .withColumn(
            "COUNTY",
            "string",
            values=["Reeves", "Midland", "Ector", "Loving", "Ward"],
            random=True,
        )
        # Set state and country (fixed values)
        .withColumn("STATE", "string", values=["Texas"])
        .withColumn("COUNTRY", "string", values=["USA"])
        # Set well type (Oil)
        .withColumn(
            "WELL_TYPE",
            "string",
            values=["Oil"],
            random=True,
        )
        # Set well orientation (Horizontal)
        .withColumn(
            "WELL_ORIENTATION",
            "string",
            values=["Horizontal"],
            random=True,
        )
        # Assign producing formation (fixed to input formation)
        .withColumn(
            "PRODUCING_FORMATION",
            "string",
            values=[formation],
            random=True,
        )
        # Assign current status with weighted probabilities
        .withColumn(
            "CURRENT_STATUS",
            "string",
            values=["Producing", "Shut-in", "Plugged and Abandoned", "Planned"],
            random=True,
            weights=[80, 10, 5, 5],
        )
        # Generate random total depth for each well
        .withColumn(
            "TOTAL_DEPTH", "integer", minValue=12000, maxValue=20000, random=True
        )
        # Generate random spud date within a specified range
        .withColumn(
            "SPUD_DATE", "date", begin="2020-01-01", end="2025-02-14", random=True
        )
        # Generate random completion date within a specified range
        .withColumn(
            "COMPLETION_DATE",
            "date",
            begin="2020-01-01",
            end="2025-02-14",
            random=True,
        )
        # Generate random surface casing depth
        .withColumn(
            "SURFACE_CASING_DEPTH",
            "integer",
            minValue=500,
            maxValue=800,
            random=True,
        )
        # Set operator name (fixed value)
        .withColumn("OPERATOR_NAME", "string", values=["OPERATOR_XYZ"])
        # Generate random permit date within a specified range
        .withColumn(
            "PERMIT_DATE", "date", begin="2019-01-01", end="2025-02-14", random=True
        )
        # Assign ARPS initial production rate (q_i) for each well
        .withColumn(
            "q_i",
            "double",
            values=[q_i]
        )
        # Assign ARPS initial decline rate (d) for each well
        .withColumn(
            "d",
            "double",
            values=[d]
        )
        # Assign ARPS b-factor for each well
        .withColumn(
            "b",
            "double",
            values=[b_factor]
        )
    )

    # Build and return the synthetic well header DataFrame
    return data_spec.build()

# COMMAND ----------

# DBTITLE 1,Daily Production Data Generation Function
def generate_daily_production(
    spark_session, well_num, q_i, d, b_factor, q_i_multiplier
):
    """Creates a data generation specification for daily production data.

    @param spark_session Current Spark session
    @param well_num Well number
    @param q_i Initial production rate
    @param d Initial decline rate
    @param q_i_multiplier Initial production rate multiplier to randomness
    @param b_factor b factor for ARPS decline
    @return Spark DataFrame with daily production data
    """
    # Randomly determine the number of days to generate for this well (between 100 and 700)
    days_to_generate = int(round(random.uniform(100, 700)))
    data_gen = (
        dg.DataGenerator(
            sparkSession=spark_session,
            name="type_curve",
            rows=days_to_generate,
            randomSeed=int(round(random.uniform(20, 1000), 0)),  # Random seed for reproducibility
        )
        # Assign the unique well number (API or identifier) to all rows
        .withColumn("well_num", "bigInt", values=[well_num])
        # Generate the day index from first production (1 to 1000, but only as many as days_to_generate)
        .withColumn("day_from_first_production", "integer", minValue=1, maxValue=1000)
        # Set the first production date as today minus the number of days generated
        .withColumn(
            "first_production_date",
            "date",
            values=[date.today() - timedelta(days=days_to_generate)],
        )
        # Calculate the actual date for each row by adding the day offset to the first production date
        .withColumn(
            "date",
            "date",
            expr="date_add(first_production_date, day_from_first_production)",
        )
        # Assign ARPS initial production rate (q_i) for this well
        .withColumn("q_i", "double", values=[q_i])
        # Assign ARPS initial decline rate (d) for this well
        .withColumn("d", "double", values=[d])
        # Assign ARPS b-factor for this well
        .withColumn("b", "double", values=[b_factor])
        # Introduce a multiplier to q_i to simulate rare production shut-ins (mostly 1.0, sometimes 0)
        .withColumn(
            "q_i_multiplier",
            "double",
            values=[q_i_multiplier, 0],
            weights=[97, 3],  # 97% chance of normal production, 3% chance of zero (shut-in)
            random=True,
        )
        # Add a small random variation to production to simulate measurement noise or operational variability
        .withColumn("variation", "double", expr="rand() * 0.1 + 0.95")
        # Calculate actual oil production (BOPD) using the ARPS decline curve formula with all modifiers
        .withColumn(
            "actuals_bopd",
            "double",
            baseColumn=["q_i","d","b","q_i_multiplier","variation"],
            expr="(q_i * q_i_multiplier) / power(1 + b * d * variation * day_from_first_production, 1/b)",
        )
    )
    # Build and return the synthetic daily production DataFrame
    return data_gen.build()

# COMMAND ----------

# DBTITLE 1,Type Curve Data Generation Function
def generate_type_curve_forecast(
    spark_session, formation, q_i, d, b_factor
):
    """Creates a data generation specification for type curve forecast data using ARPS decline.

    @param spark_session Current Spark session
    @param formation Formation name
    @param q_i Initial production rate (BOPD)
    @param d Initial decline rate
    @param b_factor ARPS b-factor
    @return Spark DataFrame with type curve forecast data
    """
    days_to_generate = 2000  # Number of days to forecast in the type curve

    # Define the synthetic type curve data specification using DBLDATAGEN
    data_gen = (
        dg.DataGenerator(
            sparkSession=spark_session,
            name="type_curve",
            rows=days_to_generate,
            randomSeed=int(round(random.uniform(20, 1000), 0)),  # Random seed for reproducibility
        )
        # Assign the formation name to all rows
        .withColumn("formation", "STRING", values=[formation])
        # Generate the day index from first production (1 to 1000)
        .withColumn("day_from_first_production", "integer", minValue=1, maxValue=1000)
        # Assign ARPS initial production rate (q_i) for the type curve
        .withColumn("q_i", "double", values=[q_i])
        # Assign ARPS initial decline rate (d) for the type curve
        .withColumn("d", "double", values=[d])
        # Assign ARPS b-factor for the type curve
        .withColumn("b", "double", values=[b_factor])
        # Add a small random variation to simulate operational/measurement noise
        .withColumn("variation", "double", expr="rand() * 0.1 + 0.95")
        # Calculate forecasted oil production (BOPD) using the ARPS decline curve formula
        .withColumn(
            "forecast_bopd",
            "double",
            baseColumn=["q_i","d","b","day_from_first_production"],
            expr="(q_i ) / power(1 + b * d  * day_from_first_production, 1/b)",
        )
    )
    # Build and return the synthetic type curve forecast DataFrame
    return data_gen.build()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section: Generate Synthetic Data
# MAGIC
# MAGIC This section brings together all previous components to generate synthetic well header data, daily production profiles, and type curve forecasts for multiple formations.
# MAGIC
# MAGIC **In this section, you will:**
# MAGIC - Define ARPS decline curve parameters for each formation
# MAGIC - Generate and display well header and production data
# MAGIC - Create type curve forecasts
# MAGIC
# MAGIC This is the core of the tutorial, demonstrating the full workflow from parameter definition to data generation and visualization.

# COMMAND ----------

# DBTITLE 1,Data Generation Implementation
# Define type curve parameters for each formation using ARPS decline curve methodology
type_curve_dict = {
    "FORMATION_A": {"q_i": 6000, "d": 0.01, "b_factor": 0.8},
    "FORMATION_B": {"q_i": 7000, "d": 0.011, "b_factor": 0.7},
    "FORMATION_C": {"q_i": 5500, "d": 0.009, "b_factor": 0.8},
    "FORMATION_D": {"q_i": 5750, "d": 0.011, "b_factor": 0.7},
}

# Generate well header data for each formation using the defined type curve parameters
# Each formation will have a random number of wells (between 10 and 20)
well_header_specs = [
   generate_well_header_data(
        spark,
        formation,
        params["q_i"],
        params["d"],
        params["b_factor"],
        random.randint(10, 20)  # Randomly select number of assets per formation
    )
    for formation, params in type_curve_dict.items()
]

# Union all Spark DataFrames for well headers
wells_df = well_header_specs[0]
for spec in well_header_specs[1:]:
    wells_df = wells_df.unionByName(spec)
print("WELLS DATAFRAME")
display(wells_df)

# Convert the well header DataFrame to a dictionary for easy access by column
wells_dict = wells_df.toPandas().to_dict()
print(wells_dict)

# Generate daily production data for each well using ARPS decline curve parameters
# Loop over each well and create a production profile
daily_prod_specs = [
   generate_daily_production(
        spark,
        wells_dict["API_NUMBER"][i],  # Use API number as unique well identifier
        wells_dict["q_i"][i],         # Initial production rate (BOPD)
        wells_dict["d"][i],           # Initial decline rate
        wells_dict["b"][i],           # ARPS b-factor
        1.0                           # Production rate multiplier (set to 1.0 for base case)
    )
    for i in range(len(next(iter(wells_dict.values()))))
]

# Union all Spark DataFrames for daily production
daily_production_df = daily_prod_specs[0]
for spec in daily_prod_specs[1:]:
    daily_production_df = daily_production_df.unionByName(spec)
print("DAILY PRODUCTION DATAFRAME")
display(daily_production_df)

# Generate type curve data for each formation using the defined type curve parameters
# Each formation will have a random number of wells (between 10 and 20)
type_curve_specs = [
   generate_type_curve_forecast(
        spark,
        formation,
        params["q_i"],
        params["d"],
        params["b_factor"],
    )
    for formation, params in type_curve_dict.items()
]

# Union all Spark DataFrames for type curves
type_curve_df = type_curve_specs[0]
for spec in type_curve_specs[1:]:
    type_curve_df = type_curve_df.unionByName(spec)
print("TYPE CURVE DATAFRAME")
display(type_curve_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Section: Visualize Synthetic Production Data
# MAGIC
# MAGIC This section demonstrates how to visualize the generated synthetic production data using Matplotlib and Seaborn.
# MAGIC
# MAGIC You will learn how to plot oil production decline curves for a sample of wells, using standard petroleum engineering units (BOPD) and SPE nomenclature.
# MAGIC
# MAGIC The second chart shows the visual for the forecasted type curves that are created and the basis of daily production values.  These tables can be merged together for additional analytical use cases comparing actual to forecasted values.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Daily Production Visualization
# Sample 5 wells and show all rows for those wells
sampled_wells = daily_production_df.select("well_num").dropDuplicates().limit(5)
sampled_df = daily_production_df.join(sampled_wells, on="well_num")

pdf = sampled_df.select("well_num", "date", "actuals_bopd").toPandas()

plt.figure(figsize=(10, 6))
for well_num, group in pdf.groupby("well_num"):
    plt.plot(group["date"], group["actuals_bopd"], label=str(well_num), linestyle='-')

plt.title("Oil Production BOPD (All Days for Sampled Wells)")
plt.xlabel("date")
plt.ylabel("Production Rate (barrels per day)")
plt.legend(title="Well Num")
plt.grid(True)
plt.show()

# COMMAND ----------

# DBTITLE 1,Type Curve Visualization
# Sample 5 wells and show all rows for those wells
sampled_wells = daily_production_df.select("well_num").dropDuplicates().limit(5)
sampled_df = daily_production_df.join(sampled_wells, on="well_num")

pdf = (
    type_curve_df
    .select("formation", "day_from_first_production", "forecast_bopd")
    .orderBy("formation", "day_from_first_production")
    .toPandas()
)

plt.figure(figsize=(10, 6))
for formation, group in pdf.groupby("formation"):
    plt.plot(group["day_from_first_production"], group["forecast_bopd"], label=str(formation), linestyle='-')

plt.title("Forecasted Production BOPD ")
plt.xlabel("Days from first Production")
plt.ylabel("Production Rate (barrels per day)")
plt.legend(title="Formation")
plt.grid(True)
plt.show()