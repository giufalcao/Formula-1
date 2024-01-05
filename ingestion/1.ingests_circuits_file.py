# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# Taking data from raw and pasting it into processed layer

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader, include  additional configurations and common functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and drop columns, and add new columns

# COMMAND ----------

circuits_column_mapping = {
    "circuitId": "circuit_id",
    "circuitRef": "circuit_ref",
    "lat": "latitude",
    "lng": "longitude",
    "alt": "altitude"
}

# COMMAND ----------

columns_to_drop = ['url']

# COMMAND ----------

circuits_df = rename_columns(circuits_df, circuits_column_mapping)

# COMMAND ----------

circuits_df = drop_columns(circuits_df, columns_to_drop)

# COMMAND ----------

circuits_df = add_ingestion_date(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write data to a Datalake as a Parquet

# COMMAND ----------

circuits_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
