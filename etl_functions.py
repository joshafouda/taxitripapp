import datetime
import logging
import os
import pandas as pd
from sqlalchemy import create_engine

def extract(file_path):
    """
    Extracts data from a Parquet file and returns a pandas DataFrame.

    Parameters:
    file_path (str): The path to the Parquet file.

    Returns:
    pandas.DataFrame: DataFrame containing the data from the Parquet file.
    """
    try:
        # Set up logging
        logging.basicConfig(filename='extract.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Reading Parquet file into DataFrame
        df = pd.read_parquet(file_path)

        logging.info("Parquet file extracted successfully.")
        return df
    except Exception as e:
        logging.error("An error occurred while extracting Parquet file: %s", str(e))
        return None


def transform(df):
    """
    Filters the DataFrame by removing observations with missing values in 'passenger_count' and 'total_amount' columns.

    Parameters:
    df (pandas.DataFrame): Input DataFrame.

    Returns:
    pandas.DataFrame: Filtered DataFrame.
    """
    try:
        # Set up logging
        logging.basicConfig(filename='transform.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Dropping all rows with missing values
        filtered_df = df.dropna(axis = 0)

        # Correcting column name to lowercase
        filtered_df = filtered_df.rename(columns = str.lower)

        logging.info("Data transformation completed successfully.")
        return filtered_df
    except Exception as e:
        logging.error("An error occurred while transforming the data: %s", str(e))
        return None

    

def load(df, table_name, connection_string):
    """
    Loads data from a DataFrame into a PostgreSQL table.

    Parameters:
    df (pandas.DataFrame): Input DataFrame.
    table_name (str): Name of the PostgreSQL table to load the data into.
    connection_string (str): PostgreSQL connection string.

    Returns:
    bool: True if data loading is successful, False otherwise.
    """
    try:
        # Set up logging
        logging.basicConfig(filename='load.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Create database engine
        engine = create_engine(connection_string)

        # Load DataFrame into PostgreSQL table
        df.to_sql(table_name, engine, if_exists='append', index=False)

        # Close the connection
        engine.dispose()

        logging.info("Data loaded successfully into PostgreSQL table.")
        return True
    except Exception as e:
        logging.error("An error occurred while loading data into PostgreSQL: %s", str(e))
        return False
