import pandas as pd
from google.cloud import bigquery
from Ingestfile import download_local

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

def df_to_bigquery(df):
    # Create a BigQuery client
    client = bigquery.Client()

    # Specify the BigQuery dataset and table to load the DataFrame
    dataset_id = 'USAEdData_Project'
    table_id = 'USAEdData_Project'

    # Write the DataFrame to BigQuery
    job = client.load_table_from_dataframe(df, f'{dataset_id}.{table_id}')
    job.result()  # Wait for the job to complete

csv_path = download_local()
df = transform(csv_path)
df_to_bigquery(df)
