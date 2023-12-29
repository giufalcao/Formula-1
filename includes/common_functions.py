
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def add_ingestion_date(input_df: DataFrame) -> DataFrame:
    """
        This function is adding an ingestion date to a Dataframe

        Parameters:
            input_df (spark.DataFrame): The Spark Dataframe where the ingestion date is going to be added.

        Returns:
            The Spark Dataframe with ingestion data column.
    """
    
    return input_df.withColumn("ingestion_date", current_timestamp())
    