import csv
import os
import time
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv

class NameOriginEnricher:
    def __init__(self, api_key=None):
        """Initialize the enricher with Gemini API"""
        load_dotenv()
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable must be set")
        
        genai.configure(api_key=self.api_key)
        
        # Define the valid categories
        self.valid_categories = [
            'Hispano', 'Latino-bíblico', 'Germánico', 'Anglosajón', 
            'Árabe', 'Griego', 'Hebreo', 'Vasco', 'Celta', 'Otro'
        ]
        
        # Create model with structured output configuration
        self.generation_config = {
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "object",
                "properties": {
                    "origin": {
                        "type": "string",
                        "enum": self.valid_categories
                    }
                },
                "required": ["origin"]
            }
        }
        
        self.model = genai.GenerativeModel('gemini-2.5-flash')

    def classify_name_origin(self, name):
        """Classify a single name's origin using Gemini API with structured output"""
        try:
            # Create the prompt for classification
            prompt = f"""
            Analiza el siguiente nombre español y clasifícalo según su origen etimológico.
            
            Categorías:
            - Hispano: Nombres de origen español, ibérico o relacionados con la península ibérica
            - Latino-bíblico: Nombres de origen latino, romano o bíblico/cristiano
            - Germánico: Nombres de origen germánico, visigodo o de pueblos germánicos
            - Anglosajón: Nombres de origen inglés, anglosajón o de lenguas germánicas del norte
            - Árabe: Nombres de origen árabe o musulmán
            - Griego: Nombres de origen griego clásico
            - Hebreo: Nombres de origen hebreo (no bíblico)
            - Vasco: Nombres de origen vasco o euskera
            - Celta: Nombres de origen celta o gallego-celta
            - Otro: Para nombres que no encajan en las categorías anteriores
            
            Nombre a clasificar: {name}
            """
            
            # Generate content with structured output
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            # Parse JSON response
            import json
            result = json.loads(response.text)
            origin = result.get('origin', 'Otro')
            
            return origin
            
        except Exception as e:
            print(f"Error classifying name '{name}': {e}")
            return 'Otro'

    def enrich_names_file(self, input_file, output_file, max_names=None, delay=1):
        """
        Enrich names from CSV file with origin classification
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            max_names: Maximum number of names to process (None for all)
            delay: Delay between API calls in seconds
        """
        processed_count = 0
        
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            # Get the original fieldnames and add the new field
            fieldnames = reader.fieldnames + ['Family_Origin']
            
            with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in reader:
                    if max_names and processed_count >= max_names:
                        break
                    
                    name = row['Nombre']
                    
                    # Classify the name origin
                    origin = self.classify_name_origin(name)
                    
                    # Add the origin to the row
                    row['Family_Origin'] = origin
                    
                    # Write the enriched row
                    writer.writerow(row)
                    
                    processed_count += 1
                    print(f"Processed {processed_count}: {name} -> {origin}")
                    
                    # Add delay to respect API rate limits
                    if delay > 0:
                        time.sleep(delay)
        
        print(f"Completed processing {processed_count} names")
        print(f"Enriched data saved to: {output_file}")

def main():
    """Main function to run the name origin enrichment"""
    # Set up file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / 'output_data' / 'names_frecuencia_edad_media.csv'
    output_file = script_dir / 'output_data' / 'names_with_origin.csv'
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    try:
        # Initialize the enricher
        enricher = NameOriginEnricher()
        
        # Process the names (limit to first 10 for testing, remove limit for full processing)
        enricher.enrich_names_file(
            input_file=str(input_file),
            output_file=str(output_file),
            max_names=10,  # Remove this parameter to process all names
            delay=1  # 1 second delay between API calls
        )
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure to set your GEMINI_API_KEY environment variable:")
        print("export GEMINI_API_KEY='your_api_key_here'")

if __name__ == "__main__":
    main()