# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API, include  additional configurations and common functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and drop columns, and add new columns

# COMMAND ----------

from pyspark.sql.functions import concat

# COMMAND ----------

drives_column_mapping = {
    "driverId": "driver_id",
    "driverRef": "driver_ref"
}

# COMMAND ----------

columns_to_drop = ['url']

# COMMAND ----------

drives_df = rename_columns(drivers_df, drives_column_mapping)

# COMMAND ----------

drives_df = drop_columns(drives_df, columns_to_drop)

# COMMAND ----------

drives_df = add_ingestion_date(drives_df)

# COMMAND ----------

drives_df = drives_df.withColumn("name", concat(
        drives_df["name.forename"],
        lit(" "),
        drives_df["name.surname"]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write a data to a Datalake as a Parquet

# COMMAND ----------

drives_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drives")

# COMMAND ----------

dbutils.notebook.exit("Success")
