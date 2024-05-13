import etl_functions
import logging
import os

# Configuration des logs
logging.basicConfig(filename='pyspark_etl_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths
#input_file_path = "histo_data_files"  # Adjust the path to your Parquet files
output_folder = "data_loaded"  # Dossier pour enregistrer les données tranformees

# Create output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Liste des fichiers Parquet dans le dossier
parquet_files = [f for f in os.listdir("histo_data_files") if f.endswith('.parquet')]

# Pour chaque fichier Parquet
for file_name in parquet_files:
    # Extract data
    logging.info("Starting data extraction...")
    input_df = etl_functions.extract("histo_data_files/" + file_name)
    logging.info("Data extraction completed.")

    # Transform data
    logging.info("Starting data transformation...")
    transformed_df = etl_functions.transform(input_df)
    logging.info("Data transformation completed.")

    # Load data
    if transformed_df is not None:
        logging.info("Starting data loading...")
        output_file_path = output_folder + "/transformed_taxi_data.parquet"
        etl_functions.load(transformed_df, output_file_path)
        logging.info("Data loading completed.")