# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API, include additional configurations and common functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename  and drop columns, and add new columns

# COMMAND ----------

results_column_mapping = {
    "resultId": "result_id",
    "raceId": "race_id",
    "driverId": "driver_id",
    "constructorId": "constructor_id",
    "positionText": "position_text",
    "positionOrder": "position_order",
    "fastestLap": "fastest_lap",
    "fastestLapTime": "fastest_lap_time",
    "fastestLapSpeed": "fastest_lap_speed"
}

# COMMAND ----------

columns_to_drop = ['statusId']

# COMMAND ----------

results_df = rename_columns(results_df, results_column_mapping)

# COMMAND ----------

results_df = drop_columns(results_df, columns_to_drop)

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write a data to a Datalake as a Parquet

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

dbutils.notebook.exit("Success")
