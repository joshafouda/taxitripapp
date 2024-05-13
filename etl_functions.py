import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, LongType, DoubleType, StringType
from pyspark.sql import DataFrame
import pandas as pd
from sqlalchemy import create_engine
import json

with open('config.json', 'r') as f:
    config = json.load(f)

db_user = config['db_user']
db_password = config['db_password']
db_host = config['db_host']
db_port = config['db_port']
db_name = config['db_name']

connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create database engine
engine = create_engine(connection_string)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

# Ajuster la configuration pour la représentation en chaîne des plans d'exécution
spark.conf.set("spark.sql.debug.maxToStringFields", "255")

def extract(file_path):
    """
    Extracts data from a Parquet file and returns a PySpark DataFrame.

    Parameters:
    file_path (str): The path to the Parquet file.

    Returns:
    pyspark.sql.DataFrame: DataFrame containing the data from the Parquet file.
    """
    try:
        # Initialize logging
        logging.basicConfig(filename='extract.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Reading Parquet file into DataFrame
        df = spark.read.parquet(file_path)

        logging.info("Parquet file extracted successfully.")

        return df
    except Exception as e:
        logging.error("An error occurred while extracting Parquet file: %s", str(e))
        return None


def transform(df):
    """
    Filters the DataFrame by removing observations with missing values or where trip_distance, passenger_count, or total_amount is less than or equal to 0.
    Merges the filtered DataFrame with zone information.
    Concatenates borough information for pickup and dropoff locations.
    
    Parameters:
    df (pyspark.sql.DataFrame): Input DataFrame.

    Returns:
    pyspark.sql.DataFrame: Transformed DataFrame.
    """
    try:
        # Initialize logging
        logging.basicConfig(filename='transform.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Renaming columns to lowercase
        for column_name in df.columns:
            df = df.withColumnRenamed(column_name, column_name.lower())

        # Dropping rows with missing values and filtering by specified conditions
        filtered_df = df.na.drop(subset=["passenger_count", "total_amount"]) \
            .filter((col("trip_distance") > 0) & (col("passenger_count") > 0) & (col("total_amount") > 0))

        # Read zone information from PostgreSQL
        df_zones = pd.read_sql("SELECT * FROM taxi_zone;", engine)

        # Select relevant columns from df_zones
        df_zones_subset = df_zones[['locationid', 'borough', 'zone', 'service_zone']]

        # Convert df_zones-subset to a Spark dataframe
        df_zones_subset = spark.createDataFrame(df_zones_subset)

        # Merge filtered DataFrame with zone information for pickup locations
        merged = filtered_df.join(df_zones_subset,
                           on=filtered_df.pulocationid == df_zones_subset.locationid,
                           how="left")
        
        # Renommage des colonnes
        merged = merged.withColumnRenamed("borough", "puborough") \
                                 .withColumnRenamed("zone", "puzone") \
                                 .withColumnRenamed("service_zone", "pu_service_zone") #\
                                 #.withColumnRenamed("locationid", "pulocationid")

        merged = merged.drop("locationid")

        # Merge filtered DataFrame with zone information for dropoff locations
        merged = merged.join(df_zones_subset,
                     on=merged.dolocationid == df_zones_subset.locationid,
                     how="left")
        
        # Renommage des colonnes
        merged = merged.withColumnRenamed("borough", "doborough") \
                                 .withColumnRenamed("zone", "dozone") \
                                 .withColumnRenamed("service_zone", "do_service_zone") #\
                                 #.withColumnRenamed("locationid", "dolocationid")
        
        merged = merged.drop("locationid")

        # Concatenate borough information for pickup and dropoff locations
        merged = merged.withColumn("Itineraire Arrondissement", concat_ws(" - ", merged["puborough"], merged["doborough"]))
        merged = merged.withColumn("Itineraire zone", concat_ws(" - ", merged["puzone"], merged["dozone"]))

        # Drop rows with null values
        merged = merged.dropna()

        logging.info("Data transformation completed successfully.")

        return merged
    
    except Exception as e:
        logging.error("An error occurred while transforming the data: %s", str(e))
        return None


def load(df: DataFrame, file_path: str):
    """
    Saves a PySpark DataFrame to Parquet format.

    Parameters:
    df (pyspark.sql.DataFrame): Input PySpark DataFrame.
    file_path (str): The path where the Parquet file will be saved.

    Returns:
    None
    """
    try:
        # Set up logging
        logging.basicConfig(filename='load.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Save DataFrame to Parquet
        df.write.parquet(file_path, mode="append")

        logging.info("Data loaded successfully into Parquet.")

    except Exception as e:
        logging.error("An error occurred while saving DataFrame to Parquet: %s", str(e))
