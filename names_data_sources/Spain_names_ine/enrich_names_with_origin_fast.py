#!/usr/bin/env python3
"""
Fast parallel version of name enrichment - truly concurrent API calls
Uses exact same prompts and structured output as original
"""

import os
import asyncio
import json
import csv
import time
from pathlib import Path
from typing import Dict, List
import argparse
import httpx
from concurrent.futures import ThreadPoolExecutor
import google.generativeai as genai

class FastNameEnricher:
    def __init__(self, api_key: str = None, model_name: str = 'gemini-2.5-flash', 
                 tier: str = 'level1'):
        """Initialize with API key and model"""
        self.api_key = api_key or os.environ.get('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(model_name)
        
        # Rate limits based on tier
        self.max_concurrent = 100 if tier == 'level1' else 5
        self.tier = tier
        
        print(f"Fast enricher initialized:")
        print(f"  Model: {model_name}")
        print(f"  Tier: {tier}")
        print(f"  Max concurrent requests: {self.max_concurrent}")
        
        # Exact same configs from original
        self.generation_config = {
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "object",
                "properties": {
                    "origin": {
                        "type": "string",
                        "enum": [
                            "Latino", "Griego", "Hebreo", "Germánico", "Árabe", "Español", 
                            "Catalán", "Gallego", "Vasco", "Francés", "Italiano", "Portugués",
                            "Inglés", "Alemán", "Anglosajón", "Celta", "Eslavo", "Rumano", 
                            "Escandinavo", "Chino", "Japonés", "Coreano", "Turco", "Persa", 
                            "Sánscrito", "Africano", "Nativo Americano", "Quechua", "Arameo", 
                            "Egipcio", "Armenio", "Georgiano", "Húngaro", "Guanche", 
                            "Latinoamericano", "Contemporáneo", "Desconocido", "Otro"
                        ]
                    }
                },
                "required": ["origin"]
            }
        }
        
        self.pronunciation_config = {
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "object",
                "properties": {
                    "spanish": {
                        "type": "string",
                        "enum": ["muy fácil", "fácil", "difícil", "muy difícil"]
                    },
                    "foreign": {
                        "type": "string",
                        "enum": ["muy fácil", "fácil", "difícil", "muy difícil"]
                    },
                    "explanation": {
                        "type": "string"
                    }
                },
                "required": ["spanish", "foreign", "explanation"]
            }
        }
    
    def _clean_text(self, text: str, name: str) -> str:
        """Clean text for CSV - same as original"""
        import re
        
        # Remove markdown
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        text = re.sub(r'__([^_]+)__', r'\1', text)
        text = re.sub(r'_([^_]+)_', r'\1', text)
        
        # Capitalize name references
        name_parts = name.split()
        for part in name_parts:
            pattern = r'\b' + re.escape(part.lower()) + r'\b'
            text = re.sub(pattern, part.title(), text, flags=re.IGNORECASE)
        
        # Clean for CSV
        text = text.replace('"', "'").replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s\.\,\;\:\!\?\(\)\-\']', '', text)
        
        return text.strip()
    
    async def _api_call_wrapper(self, name: str, call_type: str, origin: str = None):
        """Wrapper to make API calls in thread pool"""
        loop = asyncio.get_event_loop()
        
        if call_type == 'origin':
            # Exact prompt from original
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
            
            response = await loop.run_in_executor(
                None, 
                lambda: self.model.generate_content(prompt, generation_config=self.generation_config)
            )
            result = json.loads(response.text)
            return result.get('origin', 'Otro')
            
        elif call_type == 'description':
            # Exact prompt from original
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
            - Escribe los nombres siempre con la primera letra en mayúscula (ejemplo: María, José, Carmen)
            - Si es un nombre compuesto, menciona ambos componentes
            - Evita información no verificable o inventada
            - Si no tienes información segura sobre algún aspecto, no lo menciones
            - Usa solo texto plano, sin símbolos especiales
            
            Genera la descripción en español usando solo texto plano.
            """
            
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(prompt)
            )
            description = response.text.strip()
            if len(description) > 500:
                description = description[:497] + "..."
            return self._clean_text(description, name)
            
        elif call_type == 'pronunciation':
            # Exact prompt from original
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
            
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(prompt, generation_config=self.pronunciation_config)
            )
            result = json.loads(response.text)
            explanation = self._clean_text(result.get('explanation', ''), name)
            
            return {
                'spanish': result.get('spanish', 'fácil'),
                'foreign': result.get('foreign', 'difícil'),
                'explanation': explanation
            }
    
    async def enrich_name(self, name: str) -> Dict[str, str]:
        """Enrich a single name - all 3 API calls in parallel"""
        try:
            # First get origin
            origin = await self._api_call_wrapper(name, 'origin')
            
            # Then get description and pronunciation in parallel
            desc_task = self._api_call_wrapper(name, 'description', origin)
            pron_task = self._api_call_wrapper(name, 'pronunciation', origin)
            
            description, pronunciation = await asyncio.gather(desc_task, pron_task)
            
            return {
                'Family_Origin': origin,
                'Name_Description': description,
                'Pronunciation_Spanish': pronunciation['spanish'],
                'Pronunciation_Foreign': pronunciation['foreign'],
                'Pronunciation_Explanation': pronunciation['explanation']
            }
        except Exception as e:
            print(f"Error with {name}: {e}")
            return {
                'Family_Origin': 'Otro',
                'Name_Description': f'Nombre de origen desconocido.',
                'Pronunciation_Spanish': 'fácil',
                'Pronunciation_Foreign': 'difícil',
                'Pronunciation_Explanation': 'Sin información disponible.'
            }
    
    async def enrich_names_batch(self, names: List[str]) -> List[Dict[str, str]]:
        """Enrich multiple names truly in parallel"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def limited_enrich(name):
            async with semaphore:
                return await self.enrich_name(name)
        
        tasks = [limited_enrich(name) for name in names]
        return await asyncio.gather(*tasks)

async def process_file_fast(input_file: str, output_file: str, max_names: int = None,
                           tier: str = 'level1'):
    """Process CSV file with true parallel enrichment"""
    # Initialize enricher
    enricher = FastNameEnricher(tier=tier)
    
    # Read input
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames) + [
            'Family_Origin', 'Name_Description', 'Pronunciation_Spanish',
            'Pronunciation_Foreign', 'Pronunciation_Explanation'
        ]
    
    # Limit if needed
    if max_names:
        rows = rows[:max_names]
    
    names = [row['Nombre'] for row in rows]
    total = len(names)
    
    print(f"\nProcessing {total} names in parallel...")
    start_time = time.time()
    
    # Process all names in parallel
    enrichments = await enricher.enrich_names_batch(names)
    
    # Write results
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        
        for i, (row, enrichment) in enumerate(zip(rows, enrichments)):
            row.update(enrichment)
            writer.writerow(row)
            print(f"[{i+1}/{total}] {row['Nombre']}: {enrichment['Family_Origin']}")
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.1f} seconds")
    print(f"Average: {elapsed/total:.2f} seconds per name")
    print(f"Effective RPM: {(total * 3 / elapsed * 60):.0f}")
    print(f"Output saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Fast parallel name enrichment',
        epilog="""
Examples:
  # Fast processing with Level 1
  python enrich_names_with_origin_fast.py --num 50 --tier level1
  
  # Free tier (slower)
  python enrich_names_with_origin_fast.py --num 10 --tier free
        """
    )
    
    parser.add_argument('--num', type=int, default=50,
                       help='Number of names to process')
    parser.add_argument('--tier', choices=['free', 'level1'], default='level1',
                       help='API tier (default: level1)')
    parser.add_argument('--input-file', type=str,
                       help='Input CSV file')
    parser.add_argument('--output-file', type=str,
                       help='Output CSV file')
    
    args = parser.parse_args()
    
    # File paths
    script_dir = Path(__file__).parent
    
    if args.input_file:
        input_file = Path(args.input_file)
        if not input_file.is_absolute():
            input_file = script_dir / args.input_file
    else:
        input_file = script_dir / 'output_data' / 'names_frecuencia_edad_media.csv'
    
    if args.output_file:
        output_file = Path(args.output_file)
        if not output_file.is_absolute():
            output_file = script_dir / args.output_file
    else:
        output_file = script_dir / 'output_data' / f'names_enriched_fast_{args.tier}.csv'
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    # Run async processing
    asyncio.run(process_file_fast(
        str(input_file),
        str(output_file),
        max_names=args.num,
        tier=args.tier
    ))

if __name__ == "__main__":
    main() 