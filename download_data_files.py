# download_taxi_data.py
import os
import requests
import time
import hashlib
from datetime import datetime

def download_histo_data(path_to_histo_data_folder):
    """
    Downloads yellow taxi trip PARQUET files from 2019 to current year into the specified folder.

    Parameters:
    path_to_histo_data_folder (str): Path to the folder where files will be saved.

    Returns:
    None
    """
    try:
        # Create a folder to store downloaded files if it doesn't exist
        os.makedirs(path_to_histo_data_folder, exist_ok=True)

        # Current year
        current_year = datetime.now().year

        # Loop over years from current year to 2019
        for year in range(current_year, 2018, -1):
            for month in range(1, 13):
                # Construct download URL based on year and month
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
                file_name = f"{path_to_histo_data_folder}/yellow_tripdata_{year}-{month:02d}.parquet"

                # Check if the file already exists
                if os.path.exists(file_name):
                    print(f"The file {file_name} already exists, skipping to the next file...")
                    continue

                # Download the file with error handling
                try:
                    print(f"Downloading {file_name}...")
                    response = requests.get(download_url, stream=True)
                    if response.status_code == 200:
                        with open(file_name, "wb") as f:
                            for chunk in response.iter_content(chunk_size=1024):
                                f.write(chunk)
                        print(f"{file_name} downloaded successfully!")
                    else:
                        print(f"Failed to download {file_name}. HTTP status code: {response.status_code}")
                except Exception as e:
                    print(f"An error occurred while downloading {file_name}: {str(e)}")

                # Pause for 1 second between each download to avoid overloading the remote server
                time.sleep(1)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    print("Download completed!")

# Date of the day
print("Date of historical data download:", datetime.today())

# Path to the folder to save historical data
path_to_histo_data_folder = "histo_data_files"

# Call the function to download historical data
download_histo_data(path_to_histo_data_folder)