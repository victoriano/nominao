import pandas as pd
import requests
from io import BytesIO

try:
    import xlrd
except ImportError:
    raise ImportError("xlrd is required to run this script. Install it with 'pip install xlrd'")

# URL of the Excel file
url = 'https://www.ine.es/daco/daco42/nombyapel/nombres_por_edad_media.xls'

# Fetch the content of the Excel file
response = requests.get(url)
if response.status_code == 200:
    excel_data = BytesIO(response.content)

    # Read the Excel file into DataFrames
    male_names_df = pd.read_excel(excel_data, sheet_name='Hombres', skiprows=6)
    female_names_df = pd.read_excel(excel_data, sheet_name='Mujeres', skiprows=6)

    # Add a 'Gender' column to each DataFrame
    male_names_df['Gender'] = 'Male'
    female_names_df['Gender'] = 'Female'

    # Concatenate the two DataFrames
    combined_df = pd.concat([male_names_df, female_names_df])

    # Select only the 'Nombre', 'Frecuencia', 'Edad Media', and 'Gender' columns
    final_df = combined_df[['Nombre', 'Frecuencia', 'Edad Media (*)', 'Gender']]

    # Write the DataFrame to a CSV file
    final_df.to_csv('names_frecuencia_edad_media.csv', index=False)
else:
    print(f"Failed to download the file. Status code: {response.status_code}")