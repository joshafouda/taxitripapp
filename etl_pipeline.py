import os
import logging
import etl_functions

# Set up logging
logging.basicConfig(filename='run_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Chemin du dossier contenant les données historiques
path_to_histo_data_folder = "chemin/vers/le/dossier"

# Chemin de la base de données PostgreSQL
connection_string = "postgresql://username:password@localhost:5432/database_name"

def run_pipeline(path_to_histo_data_folder, connection_string):
    """
    Runs the historical data ETL pipeline.

    Parameters:
    path_to_histo_data_folder (str): Path to the folder containing historical data Parquet files.
    connection_string (str): PostgreSQL connection string.

    Returns:
    None
    """
    try:
        # Liste des fichiers Parquet dans le dossier
        parquet_files = [f for f in os.listdir(path_to_histo_data_folder) if f.endswith('.parquet')]

        # Pour chaque fichier Parquet
        for file_name in parquet_files:
            # Chemin complet du fichier
            file_path = os.path.join(path_to_histo_data_folder, file_name)

            # Extraction des données du fichier Parquet
            logging.info(f"Extracting data from {file_name}...")
            df = etl_functions.extract(file_path)

            # Transformation des données
            logging.info(f"Transforming data from {file_name}...")
            transformed_df = etl_functions.transform(df)

            # Chargement des données transformées dans PostgreSQL
            if transformed_df is not None:
                table_name = "ytaxi_histo"
                success = etl_functions.load(transformed_df, table_name, connection_string)
                if success:
                    logging.info(f"Data from {file_name} loaded into PostgreSQL table successfully.")
                else:
                    logging.error(f"Failed to load data from {file_name} into PostgreSQL table.")
            else:
                logging.error(f"Failed to transform data from {file_name}. Skipping loading into PostgreSQL.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

# Exécution du pipeline ETL historique
path_to_histo_data_folder = "histo_data_files"

# Définir les informations de connexion à la base de données
db_user = 'postgres'
db_password = 'HairiaMonAmour_2018' # renseigner votre mot de passe
db_host = 'localhost'  # ou l'adresse IP du serveur de la base de données
db_port = '5432'  # Port par défaut pour PostgreSQL
db_name = 'nyc_yellow_taxi_record'

connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

run_pipeline(path_to_histo_data_folder, connection_string)