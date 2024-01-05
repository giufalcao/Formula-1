# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader, include  additional configurations and common functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename and drop columns, and add new columns

# COMMAND ----------

races_column_mapping = {
    "raceId": "race_id",
    "year": "race_year",
    "circuitId": "circuit_id"
}

# COMMAND ----------

columns_to_drop = ['url']

# COMMAND ----------

races_df = rename_columns(races_df, races_column_mapping)

# COMMAND ----------

races_df = drop_columns(races_df, columns_to_drop)

# COMMAND ----------

races_df = add_ingestion_date(races_df)

# COMMAND ----------

races_df = races_df.withColumn(
    "race_timestemp", 
    to_timestamp(
        concat(
            races_df["date"],
            lit(" "),
            races_df["time"]
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# MAGIC %md
# MAGIC ### Step 3 - Write a data to a Datalake as a Parquet

# COMMAND ----------

races_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
