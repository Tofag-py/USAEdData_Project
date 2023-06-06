import os
import wget
import zipfile
#from airflow import DAG
from prefect import task, flow
import pandas as pd
#from google.cloud import bigquery


@task(log_prints=True)
def make_destination():
    # Create a folder named "files" in the current directory
    folder_path = os.path.join(os.getcwd(), "files")
    try:
        os.makedirs(folder_path, exist_ok=True)
        return folder_path
    except OSError as e:
        print(f"Failed to create destination folder: {folder_path}")
        return None

@task(log_prints=True)
def get_files(url, destination):
    if destination is None:
        print("Destination folder not available.")
        return

    try:
        # Check if the file already exists in the destination folder
        file_name = url.split("/")[-1]
        file_path = os.path.join(destination, file_name)
        print("Checking if file already exists...")
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
        else:
            # Download the file using wget
            print("File not found")
            print("File downloading...")
            file_path = wget.download(url, out=destination)
            print(f"File downloaded successfully: {file_path}")

        # Extract the file using zipfile
        print("Extracting file...")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(destination)
        print("File successfully extracted")

        # Delete the zip file
        os.remove(file_path)
        print("Zip file deleted")

        # Set the location of the CSV file
        csv_destination = os.path.join(destination, "Most-Recent-Cohorts-Institution.csv")
        return csv_destination

    except Exception as e:
        print(f"Failed to download file from: {url}")
        print(e)

@task(log_prints=True)
def transform(csv_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path, usecols=range(23), low_memory=False)

    # Define the desired data types for the columns
    dtypes = {
        'UNITID': int,
        'OPEID': float,
        'OPEID6': float,
        'INSTNM': str,
        'CITY': str,
        'STABBR': str,
        'ZIP': str,
        'ACCREDAGENCY': str,
        'INSTURL': str,
        'NPCURL': str,
        'SCH_DEG': float,
        'HCM2': int,
        'MAIN': int,
        'NUMBRANCH': int,
        'PREDDEG': int,
        'HIGHDEG': int,
        'CONTROL': int,
        'ST_FIPS': int,
        'REGION': int,
        'LOCALE': float,
        'LOCALE2': float,
        'LATITUDE': float,
        'LONGITUDE': float
    }

    # Convert the columns to the desired data types
    df = df.astype(dtypes)

    return df

# @task(logprints=True)
# def df_to_bigquery(df):
#     # Create a BigQuery client
#     client = bigquery.Client()

#     # Specify the BigQuery dataset and table to load the DataFrame
#     dataset_id = 'USAEdData_Project'
#     table_id = 'USAEdData_Project'

#     # Write the DataFrame to BigQuery
#     job = client.load_table_from_dataframe(df, f'{dataset_id}.{table_id}')
#     job.result()  # Wait for the job to complete


@flow(log_prints=True)
def download_to_bq():
    url = "https://ed-public-download.app.cloud.gov/downloads/Most-Recent-Cohorts-Institution_04192023.zip"
    
    destination = make_destination()
    csv_destination = get_files(url, destination)

    csv_path = csv_destination
    df = transform(csv_path)
    #df_to_bigquery(df)




if __name__ == '__main__':
    download_to_bq()