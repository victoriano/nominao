import pandas as pd
import nltk
from pathlib import Path

# Ensure you have the necessary NLTK data downloaded
nltk.download('punkt', quiet=True)

def count_syllables_spanish(name):
    # This is a placeholder function. You need to implement the syllable counting logic here.
    # The implementation will depend on the rules of Spanish phonetics.
    # For now, we'll just use a simple heuristic based on vowel groups.
    vowels = "aeiouáéíóúü"
    syllables = 0
    previous_char_was_vowel = False
    
    for char in name.lower():
        if char in vowels:
            if not previous_char_was_vowel:
                syllables += 1
                previous_char_was_vowel = True
        else:
            previous_char_was_vowel = False
    
    return syllables

def add_syllable_and_character_counts(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Calculate the number of characters for each name
    df['Character_Count'] = df['Nombre'].apply(lambda name: len(name) if isinstance(name, str) else 0)
    
    # Calculate the number of syllables for each name
    df['Syllable_Count'] = df['Nombre'].apply(lambda name: count_syllables_spanish(name) if isinstance(name, str) else 0)
    
    # Write the updated DataFrame back to a CSV file
    df.to_csv(csv_file_path, index=False)
    return df

def calculate_name_percentage(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Group by 'Gender' and calculate the sum of 'Frecuencia'
    gender_totals = df.groupby('Gender')['Frecuencia'].sum().reset_index()
    print(gender_totals)
    
    # Merge the totals back into the original DataFrame
    df = df.merge(gender_totals, on='Gender', suffixes=('', '_Total'))
    
    # Calculate the percentage for each name
    df['Percentage'] = (df['Frecuencia'] / df['Frecuencia_Total']) * 100
    
    # Drop the total frequency column as it's no longer needed
    df.drop('Frecuencia_Total', axis=1, inplace=True)
    
    # Write the updated DataFrame back to a CSV file
    df.to_csv(csv_file_path, index=False)
    return df

def add_name_popularity_rank(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Sort the DataFrame by 'Gender' and 'Frecuencia' in descending order
    df.sort_values(by=['Gender', 'Frecuencia'], ascending=[True, False], inplace=True)
    
    # Add a new column 'Popularity' which is the rank of the name within each gender
    df['Popularity'] = df.groupby('Gender').cumcount() + 1
    
    # Write the updated DataFrame back to a CSV file
    df.to_csv(csv_file_path, index=False)
    return df

def identify_compound_names(csv_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Identify compound names by checking if there is more than one word in the 'Name' column
    # and handle non-string values
    df['Is_Compound'] = df['Nombre'].apply(lambda name: True if isinstance(name, str) and len(name.split()) > 1 else False)
    
    # Write the updated DataFrame back to a CSV file
    df.to_csv(csv_file_path, index=False)
    return df

# Call the function with the path to your CSV file
script_dir = Path(__file__).parent
input_file = script_dir / 'output_data' / '1_data_download_INE_names' / 'names_frecuencia_edad_media.csv'
output_dir = script_dir / 'output_data' / '2_data_process_INE_names'
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / 'names_frecuencia_edad_media.csv'

# Start from the downloaded dataset to avoid mutating the original file
df = pd.read_csv(input_file)
df.to_csv(output_file, index=False)

compound_df = identify_compound_names(str(output_file))
simple_df = compound_df[
    (~compound_df['Is_Compound'])
    & (compound_df['Gender'].str.lower() == 'male')
].copy()
simple_df.to_csv(output_file, index=False)

calculate_name_percentage(str(output_file))
add_name_popularity_rank(str(output_file))
add_syllable_and_character_counts(str(output_file))