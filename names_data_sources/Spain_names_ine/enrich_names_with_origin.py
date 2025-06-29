import csv
import os
import time
import random
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
        
        # Define the valid categories - reduced from 200+ to 37 categories
        # Based on analysis of https://buscador-nombres-bebes.com/categorias/#por-origen
        # Filtered origins with <10 names and merged similar categories
        self.valid_categories = [
            'Africano',      # Includes: Africano, Akan, Hausa, Yoruba, Wolof, Mandinga, Swahili, etc.
            'Americano',     # Includes: Maya, Náhuatl, Quechua, Guaraní, Mapuche, Taíno, Indígena
            'Arameo',        # Includes: Arameo, Asirio, Babilónico
            'Armenio',       # Armenian names
            'Catalán',       # Catalan names
            'Celta',         # Includes: Celta, Gaélico, Galés, Irlandés, Escocés
            'Chino',         # Includes: Chino, Mandarín
            'Contemporáneo', # Includes: Contemporáneo, Neológico, Ficticio, Literario
            'Coreano',       # Korean names
            'Desconocido',   # Unknown origin
            'Egipcio',       # Egyptian names
            'Escandinavo',   # Includes: Escandinavo, Nórdico, Danés, Sueco, Noruego, Finlandés
            'Eslavo',        # Includes: Eslavo, Ruso, Ucraniano, Polaco, Checo, Búlgaro, Serbio, etc.
            'Español',       # Includes: Español, Castellano, Hispano, Hispánico
            'Francés',       # Includes: Francés, Bretón, Provenzal, Occitano
            'Gallego',       # Galician names
            'Georgiano',     # Georgian names
            'Germánico',     # Includes: Germánico, Alemán, Anglosajón, Gótico, Visigodo
            'Griego',        # Greek names
            'Guanche',       # Canary Islands indigenous names
            'Hawaiano',      # Includes: Hawaiano, Polinesio
            'Hebreo',        # Includes: Hebreo, Hebraico, Bíblico
            'Húngaro',       # Hungarian names
            'Indonesio',     # Includes: Indonesio, Javanés
            'Inglés',        # Includes: Inglés, Angloamericano, Anglófono
            'Italiano',      # Italian names
            'Japonés',       # Japanese names
            'Latino',        # Includes: Latino, Romano
            'Lituano',       # Includes: Lituano, Letón, Báltico
            'Persa',         # Includes: Persa, Iraní
            'Portugués',     # Includes: Portugués, Brasileño
            'Rumano',        # Romanian names
            'Sánscrito',     # Includes: Sánscrito, Hindú, Indio, Panyabí, Urdu
            'Turco',         # Includes: Turco, Túrquico
            'Vasco',         # Includes: Vasco, Euskera
            'Árabe',         # Includes: Árabe, Bereber, Amazigh
            'Otro'           # Other/uncategorized
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
            
            Para nombres compuestos (dos nombres unidos como "Maria Carmen"), analiza 
            el origen predominante o más relevante de los componentes.
            
            Usa estas categorías según el origen:
            
            - "Latino": Origen romano/latín clásico (incluye nombres romanos)
            - "Griego": Origen griego antiguo
            - "Hebreo": Origen hebreo, judaico o bíblico
            - "Germánico": Origen germánico (incluye visigodo, franco, alemán, anglosajón)
            - "Árabe": Origen árabe, bereber o del norte de África musulmán
            - "Español": Específicamente castellano o hispano (no catalán/gallego/vasco)
            - "Catalán": Origen catalán
            - "Gallego": Origen gallego
            - "Vasco": Origen vasco o euskera
            - "Francés": Origen francés, bretón, provenzal u occitano
            - "Italiano": Origen italiano
            - "Inglés": Origen inglés o angloamericano
            - "Celta": Origen celta, gaélico, irlandés, escocés o galés
            - "Eslavo": Origen eslavo (ruso, polaco, ucraniano, búlgaro, serbio, etc.)
            - "Escandinavo": Origen nórdico, danés, sueco, noruego o finlandés
            - "Sánscrito": Origen sánscrito, hindú o de la India
            - "Chino": Origen chino o mandarín
            - "Japonés": Origen japonés
            - "Americano": Origen indígena americano (maya, náhuatl, quechua, etc.)
            - "Africano": Origen africano subsahariano
            - "Turco": Origen turco o túrquico
            - "Persa": Origen persa o iraní
            - "Armenio": Origen armenio
            - "Georgiano": Origen georgiano
            - "Húngaro": Origen húngaro
            - "Egipcio": Origen egipcio antiguo
            - "Arameo": Origen arameo, asirio o babilónico
            - "Guanche": Origen guanche (indígena canario)
            - "Contemporáneo": Nombres inventados recientemente o literarios
            
            Si no estás seguro del origen, usa "Desconocido".
            Si no encaja en ninguna categoría, usa "Otro".
            
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

    def enrich_names_file(self, input_file, output_file, max_names=None, delay=1, random_sample=False):
        """
        Enrich names from CSV file with origin classification
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            max_names: Maximum number of names to process (None for all)
            delay: Delay between API calls in seconds
            random_sample: If True, randomly sample names instead of processing sequentially
        """
        processed_count = 0
        
        # First, read all rows if we need to do random sampling
        if random_sample and max_names:
            with open(input_file, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                all_rows = list(reader)
                fieldnames = list(reader.fieldnames) + ['Family_Origin'] if reader.fieldnames else ['Family_Origin']
                
            # Randomly sample rows
            sample_size = min(max_names, len(all_rows))
            selected_rows = random.sample(all_rows, sample_size)
            
            # Process the random sample
            with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in selected_rows:
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
        else:
            # Sequential processing (original behavior)
            with open(input_file, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                
                # Get the original fieldnames and add the new field
                fieldnames = list(reader.fieldnames) + ['Family_Origin'] if reader.fieldnames else ['Family_Origin']
                
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
        
        print(f"\nCompleted processing {processed_count} names")
        print(f"Enriched data saved to: {output_file}")
    
    def test_random_names(self, input_file, num_names=5):
        """
        Test the classifier with random names from the dataset
        
        Args:
            input_file: Path to input CSV file
            num_names: Number of random names to test (default: 5)
        """
        print(f"\nTesting with {num_names} random names from the dataset...")
        print("-" * 60)
        
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            all_rows = list(reader)
        
        # Randomly sample names
        sample_size = min(num_names, len(all_rows))
        selected_rows = random.sample(all_rows, sample_size)
        
        for i, row in enumerate(selected_rows, 1):
            name = row['Nombre']
            origin = self.classify_name_origin(name)
            print(f"{i}. {name} -> {origin}")
            time.sleep(1)  # Small delay between API calls
        
        print("-" * 60)

def main():
    """Main function to run the name origin enrichment"""
    import sys
    
    # Set up file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / 'output_data' / 'names_frecuencia_edad_media.csv'
    output_file = script_dir / 'output_data' / 'names_with_origin.csv'
    output_file_random = script_dir / 'output_data' / 'names_with_origin_random_sample.csv'
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    try:
        # Initialize the enricher
        enricher = NameOriginEnricher()
        
        # Check command line arguments
        if len(sys.argv) > 1 and sys.argv[1] == '--test-random':
            # Run quick test with random names
            num_names = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            enricher.test_random_names(
                input_file=str(input_file),
                num_names=num_names
            )
        elif len(sys.argv) > 1 and sys.argv[1] == '--random':
            # Process random sample and save to file
            num_names = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            enricher.enrich_names_file(
                input_file=str(input_file),
                output_file=str(output_file_random),
                max_names=num_names,
                delay=1,
                random_sample=True
            )
        else:
            # Default: Process sequentially (first N names)
            enricher.enrich_names_file(
                input_file=str(input_file),
                output_file=str(output_file),
                max_names=10,  # Remove this parameter to process all names
                delay=1  # 1 second delay between API calls
            )
            
            print("\nTip: You can also run:")
            print("  python enrich_names_with_origin.py --test-random [num_names]  # Quick test with random names")
            print("  python enrich_names_with_origin.py --random [num_names]       # Process random sample to file")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure to set your GEMINI_API_KEY environment variable:")
        print("export GEMINI_API_KEY='your_api_key_here'")

if __name__ == "__main__":
    main()