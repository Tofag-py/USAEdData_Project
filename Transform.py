ls
cd files
ls

import pandas as pd
df = pd.read_csv("Most-Recent-Cohorts-Institution.csv")
df = pd.read_csv('Most-Recent-Cohorts-Institution.csv', usecols=range(23))
df = pd.read_csv('Most-Recent-Cohorts-Institution.csv', usecols=range(23), dtype={'column_9': str})
df = pd.read_csv('Most-Recent-Cohorts-Institution.csv', usecols=range(23), low_memory=False)
print(df.columns.tolist())  # Print the list of column headers
print(df.dtypes)  # Print the schema (data types) of the columns
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

# Display the updated dataframe with the new data types
print(df.dtypes)
jupyter nbconvert --to script Transform.ipynb
pwd

ls
import nbformat
from nbformat.v4 import output_from_msg
import pandas as pd

def notebook_to_script(notebook_file, script_file):
    notebook = nbformat.read(notebook_file, as_version=4)
    code_cells = [cell.source for cell in notebook.cells if cell.cell_type == 'code']
    script = '\n'.join(code_cells)

    with open(script_file, 'w') as file:
        file.write(script)

# Specify the paths of the notebook and script files
notebook_file = 'Transform.ipynb'
script_file = 'your_script.py'

# Convert the notebook to a script
notebook_to_script(notebook_file, script_file)
