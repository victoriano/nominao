import csv
import requests

# Function to get metadata for a given name
def get_name_metadata(name):
    response = requests.get(f'https://www.ine.es/tnombres/mapaWidget?nombre={name}&sexo=1')
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# Function to process the CSV file and get metadata for top N names
def process_names(file_path, top_n):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            if i >= top_n:
                break
            name = row['Nombre']
            metadata = get_name_metadata(name)
            print(f"Metadata for {name}: {metadata}")

# Process the top N names and print their metadata
top_n = 5  # Define the number of top names to process and print
process_names('output_data/names_frecuencia_edad_media.csv', top_n)