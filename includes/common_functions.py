
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def create_spark_context(app_name: str, warehouse_location: str = None) -> SparkSession:
    """
    Create a Spark session with optional configuration for the Warehouse location.

    Parameters:
    - app_name (str): The name of the Spark application.
    - warehouse_location (str): The path to the existing Spark Warehouse.

    Returns:
    - SparkSession: The Spark session object.
    """
    spark = SparkSession.builder \
        .appName(app_name) 

    if warehouse_location:
        spark.config("spark.sql.warehouse.dir", warehouse_location)

    return spark.getOrCreate()

def add_ingestion_date(dataframe: DataFrame) -> DataFrame:
    """
    Adds an ingestion date column to a PySpark DataFrame.

    Parameters:
    - dataframe (DataFrame): The PySpark DataFrame to which the ingestion date column will be added.

    Returns:
    DataFrame: The PySpark DataFrame with the addition of an 'ingestion_date' column containing current timestamps.
    """

    return dataframe.withColumn("ingestion_date", current_timestamp())

def rename_columns(dataframe: DataFrame, column_mapping_names: dict) -> DataFrame:
    """
    Rename columns in a PySpark DataFrame based on the provided mapping.

    Parameters:
    - dataframe (DataFrame): The PySpark DataFrame to be modified.
    - column_mapping (dict): A dictionary mapping old column names to new column names.

    Returns:
    DataFrame: The PySpark DataFrame with columns renamed according to the provided mapping.
    """

    for old_column, new_column in column_mapping_names.items():
        dataframe = dataframe.withColumnRenamed(old_column, new_column)
    
    return dataframe

def drop_columns(dataframe: DataFrame, columns_to_drop: list) -> DataFrame:
    """
    Drops specified columns from a PySpark DataFrame.

    Parameters:
    - dataframe (DataFrame): The PySpark DataFrame from which columns will be dropped.
    - columns_to_drop (list): List of column names to be dropped.

    Returns:
    DataFrame: The PySpark DataFrame with specified columns dropped.
    """

    return dataframe.drop(*columns_to_drop)
