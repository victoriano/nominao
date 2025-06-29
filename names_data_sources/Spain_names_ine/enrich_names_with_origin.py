import csv
import os
import time
import random
import json
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple
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
        
        # Define the valid categories - 35 categories total
        # Based on analysis of https://buscador-nombres-bebes.com/categorias/#por-origen
        # Filtered origins with <10 names and merged similar categories
        # NOTE: Hebrew/Biblical and Latin names are now classified as Spanish due to cultural assimilation
        self.valid_categories = [
            'Africano',         # Includes: Africano, Akan, Hausa, Yoruba, Wolof, Mandinga, Swahili, etc.
            'Alemán',           # German names, Teutonic origin
            'Anglosajón',       # Anglo-Saxon, Celtic, Gaelic, Welsh, Irish, Scottish, English, Old English
            'Arameo',           # Includes: Arameo, Asirio, Babilónico
            'Armenio',          # Armenian names
            'Catalán',          # Catalan names
            'Chino',            # Includes: Chino, Mandarín
            'Contemporáneo',    # Includes: Contemporáneo, Neológico, Ficticio, Literario
            'Coreano',          # Korean names
            'Desconocido',      # Unknown origin
            'Egipcio',          # Egyptian names
            'Escandinavo',      # Includes: Escandinavo, Nórdico, Danés, Sueco, Noruego, Finlandés
            'Eslavo',           # Includes: Eslavo, Ruso, Ucraniano, Polaco, Checo, Búlgaro, Serbio, etc.
            'Español',          # Includes: Español, Castellano, Hispano, Hispánico, Latino, Hebreo/Bíblico
            'Francés',          # Includes: Francés, Bretón, Provenzal, Occitano
            'Gallego',          # Galician names
            'Georgiano',        # Georgian names
            'Griego',           # Greek names
            'Guanche',          # Canary Islands indigenous names
            'Hawaiano',         # Includes: Hawaiano, Polinesio
            'Húngaro',          # Hungarian names
            'Indonesio',        # Includes: Indonesio, Javanés
            'Italiano',         # Italian names
            'Japonés',          # Japanese names
            'Latinoamericano',  # Spanish-sounding names typical of Latin America, rare in Spain
            'Lituano',          # Includes: Lituano, Letón, Báltico
            'Nativo Americano', # Includes: Maya, Náhuatl, Quechua, Guaraní, Mapuche, Taíno, Indígena
            'Persa',            # Includes: Persa, Iraní
            'Portugués',        # Includes: Portugués, Brasileño
            'Rumano',           # Romanian names
            'Sánscrito',        # Includes: Sánscrito, Hindú, Indio, Panyabí, Urdu
            'Turco',            # Includes: Turco, Túrquico
            'Vasco',            # Includes: Vasco, Euskera
            'Árabe',            # Includes: Árabe, Bereber, Amazigh
            'Otro'              # Other/uncategorized
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

    def get_all_enrichments(self, name: str) -> Dict[str, str]:
        """
        Get all enrichments for a name. This is the main method that coordinates
        all enrichment functions and returns a dictionary with all new columns.
        
        Args:
            name: The name to enrich
            
        Returns:
            Dictionary with column names as keys and enriched values
        """
        enrichments = {}
        
        # Get origin classification
        origin = self._get_origin_classification(name)
        enrichments['Family_Origin'] = origin
        
        # Get name description
        description = self._get_name_description(name, origin)
        enrichments['Name_Description'] = description
        
        # Get pronunciation difficulty assessments
        pronunciation_data = self._get_pronunciation_difficulty(name, origin)
        enrichments['Pronunciation_Spanish'] = pronunciation_data['spanish']
        enrichments['Pronunciation_Foreign'] = pronunciation_data['foreign']
        enrichments['Pronunciation_Explanation'] = pronunciation_data['explanation']
        
        # Future enrichments can be added here:
        # enrichments['Name_Popularity'] = self._get_name_popularity(name)
        # enrichments['Name_Gender_Distribution'] = self._get_gender_distribution(name)
        # enrichments['Name_Geographic_Distribution'] = self._get_geographic_distribution(name)
        # enrichments['Name_Variants'] = self._get_name_variants(name)
        # enrichments['Name_Sentiment'] = self._get_name_sentiment(name)
        
        return enrichments
    
    def _get_origin_classification(self, name: str) -> str:
        """Classify a single name's origin using Gemini API with structured output"""
        try:
            # Create the prompt for classification
            prompt = f"""
            Analiza el siguiente nombre español y clasifícalo según su origen etimológico.
            
            Para nombres compuestos (dos nombres unidos como "Maria Carmen"), aplica estas reglas:
            1. Si contiene mezcla de anglosajón + español → "Latinoamericano"
               - "Brandon José" → "Latinoamericano"
               - "Jennifer María" → "Latinoamericano" 
               - "Brayan Antonio" → "Latinoamericano"
            2. Para otros casos, clasificar según el componente MÁS ALEJADO del español y latino:
               - "María Aitor" → "Vasco" (por Aitor, no por María)
               - "Juan Chen" → "Chino" (por Chen, no por Juan)
               - "Rosa Fatima" → "Árabe" (por Fatima, no por Rosa)
               - "Carmen Yuki" → "Japonés" (por Yuki, no por Carmen)
            
                         Usa estas categorías según el origen:
             
             - "Español": Nombres españoles, castellanos, hispanos, INCLUYENDO:
               * Los de origen latino/romano y hebreo/bíblico asimilados (María, José, Carmen, Antonio)
               * Nombres germanizados/castellanizados (Guillermo, Carlos, Francisco, Fernando, etc.)
               * Solo clasifica como otra categoría si el nombre mantiene su forma extranjera original
                          - "Griego": Origen griego antiguo, pero solo si mantiene forma griega (no españolizada)
             - "Alemán": Solo nombres en su forma alemana original (Wilhelm, nicht Guillermo; Karl, nicht Carlos)
             - "Anglosajón": Origen anglosajón, celta, gaélico, irlandés, escocés, galés, inglés o angloamericano
             - "Árabe": Origen árabe, bereber o del norte de África musulmán
             - "Catalán": Origen catalán
             - "Gallego": Origen gallego
             - "Vasco": Origen vasco o euskera
             - "Francés": Origen francés, bretón, provenzal u occitano
             - "Italiano": Origen italiano
             - "Eslavo": Origen eslavo (ruso, polaco, ucraniano, búlgaro, serbio, etc.)
             - "Escandinavo": Origen nórdico, danés, sueco, noruego o finlandés
             - "Sánscrito": Origen sánscrito, hindú o de la India
             - "Chino": Origen chino o mandarín
             - "Japonés": Origen japonés
             - "Nativo Americano": Origen indígena americano (maya, náhuatl, quechua, etc.)
             - "Latinoamericano": Nombres que suenan españoles pero son MUY típicos de Latinoamérica 
               y muy infrecuentes en España (Yeimy, Brayan, Jhon, Dayanna, etc.)
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
            result = json.loads(response.text)
            origin = result.get('origin', 'Otro')
            
            return origin
            
        except Exception as e:
            print(f"Error classifying name '{name}': {e}")
            return 'Otro'
    
    def _get_name_description(self, name: str, origin: str) -> str:
        """
        Generate a description of the name including its meaning, origin, and interesting facts
        
        Args:
            name: The name to describe
            origin: The already classified origin of the name
            
        Returns:
            A text description of the name (cleaned for CSV output)
        """
        try:
            prompt = f"""
            Genera una descripción breve pero interesante sobre el nombre "{name}" considerando que su origen es {origin}.
            
            La descripción debe incluir (cuando sea aplicable):
            1. Significado etimológico del nombre
            2. Historia o contexto cultural
            3. Personajes famosos o referencias culturales
            4. Variantes en otros idiomas
            5. Datos curiosos o interesantes
            
            Requisitos IMPORTANTES:
            - Máximo 150 palabras
            - Tono informativo pero ameno
            - NO uses formato markdown (nada de **negrita**, *cursiva*, etc.)
            - Escribe los nombres siempre en MAYÚSCULAS (ejemplo: MARÍA, JOSÉ, CARMEN)
            - Si es un nombre compuesto, menciona ambos componentes
            - Evita información no verificable o inventada
            - Si no tienes información segura sobre algún aspecto, no lo menciones
            - Usa solo texto plano, sin símbolos especiales
            
            Genera la descripción en español usando solo texto plano.
            """
            
            # Use a simple text generation without structured output for descriptions
            response = self.model.generate_content(prompt)
            
            # Clean and return the description
            description = response.text.strip()
            
            # Additional cleaning for CSV safety
            description = self._clean_description_for_csv(description, name)
            
            # Ensure it's not too long
            if len(description) > 500:
                description = description[:497] + "..."
            
            return description
            
        except Exception as e:
            print(f"Error generating description for '{name}': {e}")
            return f"Nombre de origen {origin}."
    
    def _clean_description_for_csv(self, description: str, original_name: str) -> str:
        """
        Clean the description text to be CSV-safe
        
        Args:
            description: The raw description text
            original_name: The original name being described
            
        Returns:
            Cleaned description safe for CSV
        """
        import re
        
        # Remove markdown formatting
        description = re.sub(r'\*\*([^*]+)\*\*', r'\1', description)  # **bold** -> text
        description = re.sub(r'\*([^*]+)\*', r'\1', description)      # *italic* -> text
        description = re.sub(r'__([^_]+)__', r'\1', description)      # __bold__ -> text
        description = re.sub(r'_([^_]+)_', r'\1', description)        # _italic_ -> text
        
        # Convert name references to ALL CAPS
        # Split compound names and make each part ALL CAPS
        name_parts = original_name.split()
        for part in name_parts:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(part.lower()) + r'\b'
            description = re.sub(pattern, part.upper(), description, flags=re.IGNORECASE)
            
            # Also handle the original name format
            pattern_orig = r'\b' + re.escape(part) + r'\b'
            description = re.sub(pattern_orig, part.upper(), description)
        
        # Also handle the full name
        full_name_pattern = r'\b' + re.escape(original_name.lower()) + r'\b'
        description = re.sub(full_name_pattern, original_name.upper(), description, flags=re.IGNORECASE)
        
        # Replace problematic characters for CSV
        description = description.replace('"', "'")  # Replace double quotes with single quotes
        description = description.replace('\n', ' ')  # Replace newlines with spaces
        description = description.replace('\r', ' ')  # Replace carriage returns with spaces
        description = description.replace('\t', ' ')  # Replace tabs with spaces
        
        # Clean up multiple spaces
        description = re.sub(r'\s+', ' ', description)
        
        # Remove any remaining special characters that might break CSV
        description = re.sub(r'[^\w\s\.\,\;\:\!\?\(\)\-\']', '', description)
        
        return description.strip()
    
    def _clean_text_for_csv(self, text: str) -> str:
        """
        Clean any text field to be CSV-safe
        
        Args:
            text: The text to clean
            
        Returns:
            Cleaned text safe for CSV
        """
        import re
        
        # Replace problematic characters for CSV
        text = text.replace('"', "'")  # Replace double quotes with single quotes
        text = text.replace('\n', ' ')  # Replace newlines with spaces
        text = text.replace('\r', ' ')  # Replace carriage returns with spaces
        text = text.replace('\t', ' ')  # Replace tabs with spaces
        
        # Clean up multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _get_pronunciation_difficulty(self, name: str, origin: str) -> Dict[str, str]:
        """
        Evaluate pronunciation difficulty for Spanish speakers and foreigners
        
        Args:
            name: The name to evaluate
            origin: The origin of the name (for context)
            
        Returns:
            Dictionary with 'spanish', 'foreign', and 'explanation' keys
        """
        try:
            # Define difficulty levels
            difficulty_levels = ["muy fácil", "fácil", "difícil", "muy difícil"]
            
            # Create structured output configuration for pronunciation assessment
            pronunciation_config = {
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "object",
                    "properties": {
                        "spanish": {
                            "type": "string",
                            "enum": difficulty_levels
                        },
                        "foreign": {
                            "type": "string",
                            "enum": difficulty_levels
                        },
                        "explanation": {
                            "type": "string"
                        }
                    },
                    "required": ["spanish", "foreign", "explanation"]
                }
            }
            
            prompt = f"""
            Evalúa la dificultad de pronunciación del nombre "{name}" (origen: {origin}).
            
            Considera para ESPAÑOLES:
            - Muy fácil: Solo fonemas españoles comunes (María, Carlos, Antonio)
            - Fácil: Fonemas españoles con alguna combinación menos común (Xavier, Ainhoa)
            - Difícil: Contiene fonemas no españoles pero adaptables (Jennifer, Kevin)
            - Muy difícil: Fonemas muy ajenos al español (Txomin, Nguyen, Siobhan)
            
            Considera para EXTRANJEROS (hablantes de inglés principalmente):
            - Muy fácil: Nombres internacionales o con fonética simple (Ana, David, Laura)
            - Fácil: Pronunciación clara con pocas peculiaridades españolas (Carmen, Pablo)
            - Difícil: Contiene sonidos específicos del español (rr, ñ, j española)
            - Muy difícil: Múltiples sonidos difíciles o estructura compleja (Guillermo, Enrique)
            
            En la explicación (máximo 100 palabras):
            - Identifica los sonidos problemáticos específicos
            - Menciona si hay letras mudas o pronunciaciones no intuitivas
            - Explica las diferencias entre la dificultad para españoles vs extranjeros
            - Si es un nombre compuesto, evalúa ambas partes
            
            Responde con un JSON con las claves "spanish", "foreign" y "explanation".
            """
            
            # Generate content with structured output
            response = self.model.generate_content(
                prompt,
                generation_config=pronunciation_config
            )
            
            # Parse JSON response
            result = json.loads(response.text)
            
            # Ensure all required fields are present and clean for CSV
            explanation = result.get('explanation', 'Sin información de pronunciación disponible.')
            explanation = self._clean_text_for_csv(explanation)
            
            return {
                'spanish': result.get('spanish', 'fácil'),
                'foreign': result.get('foreign', 'difícil'),
                'explanation': explanation
            }
            
        except Exception as e:
            print(f"Error evaluating pronunciation for '{name}': {e}")
            return {
                'spanish': 'fácil',
                'foreign': 'difícil',
                'explanation': 'No se pudo evaluar la pronunciación de este nombre.'
            }

    def enrich_names_file(self, input_file, output_file, max_names=None, delay=1, random_sample=False):
        """
        Enrich names from CSV file with multiple enrichment columns
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            max_names: Maximum number of names to process (None for all)
            delay: Delay between API calls in seconds
            random_sample: If True, randomly sample names instead of processing sequentially
        """
        processed_count = 0
        
        # Define the new columns we'll be adding
        new_columns = ['Family_Origin', 'Name_Description', 'Pronunciation_Spanish', 
                      'Pronunciation_Foreign', 'Pronunciation_Explanation']
        
        # First, read all rows if we need to do random sampling
        if random_sample and max_names:
            with open(input_file, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                all_rows = list(reader)
                fieldnames = list(reader.fieldnames) + new_columns if reader.fieldnames else new_columns
                
            # Randomly sample rows
            sample_size = min(max_names, len(all_rows))
            selected_rows = random.sample(all_rows, sample_size)
            
            # Process the random sample
            with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                
                for row in selected_rows:
                    name = row['Nombre']
                    
                    # Get all enrichments for this name
                    enrichments = self.get_all_enrichments(name)
                    
                    # Add all enrichments to the row
                    row.update(enrichments)
                    
                    # Write the enriched row
                    writer.writerow(row)
                    
                    processed_count += 1
                    print(f"Processed {processed_count}: {name}")
                    print(f"  - Origin: {enrichments['Family_Origin']}")
                    print(f"  - Pronunciation: Spanish={enrichments['Pronunciation_Spanish']}, Foreign={enrichments['Pronunciation_Foreign']}")
                    print(f"  - Description: {enrichments['Name_Description'][:80]}...")
                    
                    # Add delay to respect API rate limits
                    if delay > 0:
                        time.sleep(delay)
        else:
            # Sequential processing (original behavior)
            with open(input_file, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                
                # Get the original fieldnames and add the new columns
                fieldnames = list(reader.fieldnames) + new_columns if reader.fieldnames else new_columns
                
                with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                    writer.writeheader()
                    
                    for row in reader:
                        if max_names and processed_count >= max_names:
                            break
                        
                        name = row['Nombre']
                        
                        # Get all enrichments for this name
                        enrichments = self.get_all_enrichments(name)
                        
                        # Add all enrichments to the row
                        row.update(enrichments)
                        
                        # Write the enriched row
                        writer.writerow(row)
                        
                        processed_count += 1
                        print(f"Processed {processed_count}: {name}")
                        print(f"  - Origin: {enrichments['Family_Origin']}")
                        print(f"  - Pronunciation: Spanish={enrichments['Pronunciation_Spanish']}, Foreign={enrichments['Pronunciation_Foreign']}")
                        print(f"  - Description: {enrichments['Name_Description'][:80]}...")
                        
                        # Add delay to respect API rate limits
                        if delay > 0:
                            time.sleep(delay)
        
        print(f"\nCompleted processing {processed_count} names")
        print(f"Enriched data saved to: {output_file}")
        print(f"Added columns: {', '.join(new_columns)}")
    
    def test_random_names(self, input_file, num_names=5):
        """
        Test the enrichment system with random names from the dataset
        
        Args:
            input_file: Path to input CSV file
            num_names: Number of random names to test (default: 5)
        """
        print(f"\nTesting with {num_names} random names from the dataset...")
        print("=" * 80)
        
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            all_rows = list(reader)
        
        # Randomly sample names
        sample_size = min(num_names, len(all_rows))
        selected_rows = random.sample(all_rows, sample_size)
        
        for i, row in enumerate(selected_rows, 1):
            name = row['Nombre']
            print(f"\n{i}. Nombre: {name}")
            print("-" * 40)
            
            # Get all enrichments
            enrichments = self.get_all_enrichments(name)
            
            # Display results
            print(f"   Origen: {enrichments['Family_Origin']}")
            print(f"   Pronunciación para españoles: {enrichments['Pronunciation_Spanish']}")
            print(f"   Pronunciación para extranjeros: {enrichments['Pronunciation_Foreign']}")
            print(f"   Explicación pronunciación: {enrichments['Pronunciation_Explanation']}")
            print(f"   Descripción:")
            
            # Word wrap the description for better display
            description = enrichments['Name_Description']
            words = description.split()
            current_line = "   "
            for word in words:
                if len(current_line) + len(word) + 1 > 80:
                    print(current_line)
                    current_line = "   " + word
                else:
                    current_line += " " + word
            if current_line.strip():
                print(current_line)
            
            time.sleep(1)  # Small delay between API calls
        
        print("\n" + "=" * 80)

def main():
    """Main function to run the name origin enrichment"""
    import sys
    import argparse
    
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Enrich Spanish names with etymological origin classification',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python enrich_names_with_origin.py                    # Process first 10 names sequentially
  python enrich_names_with_origin.py --num 50          # Process first 50 names sequentially  
  python enrich_names_with_origin.py --random 25       # Process 25 random names and save to file
  python enrich_names_with_origin.py --test-random 5   # Quick test with 5 random names (no file)
  python enrich_names_with_origin.py --all             # Process ALL names (may take hours!)
  
  # Custom input/output files:
  python enrich_names_with_origin.py --num 100 --output-file my_enriched_names.csv
  python enrich_names_with_origin.py --input-file custom_data.csv --random 50
        """
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--num', type=int, default=10,
                      help='Number of names to process sequentially from the beginning (default: 10)')
    group.add_argument('--random', type=int, metavar='N',
                      help='Process N random names and save to file')
    group.add_argument('--test-random', type=int, metavar='N', nargs='?', const=5,
                      help='Quick test with N random names (no file output, default: 5)')
    group.add_argument('--all', action='store_true',
                      help='Process ALL names in the dataset (WARNING: may take hours!)')
    
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between API calls in seconds (default: 1.0)')
    parser.add_argument('--input-file', type=str,
                       help='Custom input file path (default: names_frecuencia_edad_media.csv)')
    parser.add_argument('--output-file', type=str,
                       help='Custom output file path (default: auto-generated based on mode)')
    
    args = parser.parse_args()
    
    # Set up file paths
    script_dir = Path(__file__).parent
    
    # Input file - use custom path if provided, otherwise default
    if args.input_file:
        input_file = Path(args.input_file)
        if not input_file.is_absolute():
            input_file = script_dir / args.input_file
    else:
        input_file = script_dir / 'output_data' / 'names_frecuencia_edad_media.csv'
    
    # Set default output files first
    output_file = script_dir / 'output_data' / 'names_with_origin.csv'
    output_file_random = script_dir / 'output_data' / 'names_with_origin_random_sample.csv'
    
    # Override with custom output file if provided
    if args.output_file:
        custom_output = Path(args.output_file)
        if not custom_output.is_absolute():
            custom_output = script_dir / args.output_file
        
        if args.random is not None:
            output_file_random = custom_output
        else:
            output_file = custom_output
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    try:
        # Initialize the enricher
        enricher = NameOriginEnricher()
        
        # Process based on arguments
        if args.test_random is not None:
            # Quick test with random names (no file output)
            print(f"Running quick test with {args.test_random} random names...")
            enricher.test_random_names(
                input_file=str(input_file),
                num_names=args.test_random
            )
            
        elif args.random is not None:
            # Process random sample and save to file
            print(f"Processing {args.random} random names and saving to file...")
            enricher.enrich_names_file(
                input_file=str(input_file),
                output_file=str(output_file_random),
                max_names=args.random,
                delay=args.delay,
                random_sample=True
            )
            
        elif args.all:
            # Process ALL names
            print("Processing ALL names in the dataset...")
            print("WARNING: This may take several hours!")
            confirm = input("Are you sure you want to continue? (y/N): ")
            if confirm.lower() != 'y':
                print("Operation cancelled.")
                return
                
            enricher.enrich_names_file(
                input_file=str(input_file),
                output_file=str(output_file),
                max_names=None,  # Process all names
                delay=args.delay
            )
            
        else:
            # Default: Process first N names sequentially
            print(f"Processing first {args.num} names sequentially...")
            enricher.enrich_names_file(
                input_file=str(input_file),
                output_file=str(output_file),
                max_names=args.num,
                delay=args.delay
            )
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure to set your GEMINI_API_KEY environment variable:")
        print("export GEMINI_API_KEY='your_api_key_here'")

if __name__ == "__main__":
    main()