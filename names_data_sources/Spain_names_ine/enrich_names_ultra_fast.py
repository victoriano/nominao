#!/usr/bin/env python3
"""
Ultra-fast parallel enrichment - processes all names simultaneously
"""

import os
import asyncio
import json
import csv
import time
from pathlib import Path
from typing import Dict, List
import argparse
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor

class UltraFastEnricher:
    def __init__(self, api_key: str = None, tier: str = 'level1'):
        """Initialize with API key"""
        self.api_key = api_key or os.environ.get('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Concurrent limit based on tier
        self.max_concurrent = 300 if tier == 'level1' else 10
        
        print(f"Ultra-fast enricher initialized:")
        print(f"  Tier: {tier}")
        print(f"  Max concurrent: {self.max_concurrent}")
        
        # Exact configs from original
        self.origin_config = {
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "object",
                "properties": {
                    "origin": {
                        "type": "string",
                        "enum": ["Latino", "Griego", "Hebreo", "Germánico", "Árabe", "Español", 
                                "Catalán", "Gallego", "Vasco", "Francés", "Italiano", "Portugués",
                                "Inglés", "Alemán", "Anglosajón", "Celta", "Eslavo", "Rumano", 
                                "Escandinavo", "Chino", "Japonés", "Coreano", "Turco", "Persa", 
                                "Sánscrito", "Africano", "Nativo Americano", "Quechua", "Arameo", 
                                "Egipcio", "Armenio", "Georgiano", "Húngaro", "Guanche", 
                                "Latinoamericano", "Contemporáneo", "Desconocido", "Otro"]
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
                    "spanish": {"type": "string", "enum": ["muy fácil", "fácil", "difícil", "muy difícil"]},
                    "foreign": {"type": "string", "enum": ["muy fácil", "fácil", "difícil", "muy difícil"]},
                    "explanation": {"type": "string"}
                },
                "required": ["spanish", "foreign", "explanation"]
            }
        }
    
    def _clean_text(self, text: str, name: str) -> str:
        """Clean text for CSV"""
        import re
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        
        name_parts = name.split()
        for part in name_parts:
            pattern = r'\b' + re.escape(part.lower()) + r'\b'
            text = re.sub(pattern, part.title(), text, flags=re.IGNORECASE)
        
        text = text.replace('"', "'").replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def get_origin_prompt(self, name: str) -> str:
        """Get origin classification prompt - exact from original"""
        return f"""
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
    
    def get_description_prompt(self, name: str, origin: str) -> str:
        """Get description prompt - exact from original"""
        return f"""
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
    
    def get_pronunciation_prompt(self, name: str, origin: str) -> str:
        """Get pronunciation prompt - exact from original"""
        return f"""
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
    
    async def process_all_names(self, names: List[str]) -> List[Dict[str, str]]:
        """Process all names completely in parallel"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        executor = ThreadPoolExecutor(max_workers=self.max_concurrent)
        
        async def call_api(prompt, config=None):
            async with semaphore:
                loop = asyncio.get_event_loop()
                try:
                    if config:
                        response = await loop.run_in_executor(
                            executor, 
                            lambda: self.model.generate_content(prompt, generation_config=config)
                        )
                    else:
                        response = await loop.run_in_executor(
                            executor,
                            lambda: self.model.generate_content(prompt)
                        )
                    return response.text
                except Exception as e:
                    print(f"API error: {e}")
                    return None
        
        # Create all tasks at once
        all_tasks = []
        
        # First, all origin calls
        origin_tasks = []
        for name in names:
            task = call_api(self.get_origin_prompt(name), self.origin_config)
            origin_tasks.append(task)
        
        # Wait for all origins
        origin_results = await asyncio.gather(*origin_tasks)
        
        # Parse origins
        origins = []
        for name, result in zip(names, origin_results):
            if result:
                try:
                    origin = json.loads(result).get('origin', 'Otro')
                except:
                    origin = 'Otro'
            else:
                origin = 'Otro'
            origins.append(origin)
        
        # Now create all description and pronunciation tasks
        desc_tasks = []
        pron_tasks = []
        
        for name, origin in zip(names, origins):
            desc_tasks.append(call_api(self.get_description_prompt(name, origin)))
            pron_tasks.append(call_api(self.get_pronunciation_prompt(name, origin), self.pronunciation_config))
        
        # Wait for all
        desc_results = await asyncio.gather(*desc_tasks)
        pron_results = await asyncio.gather(*pron_tasks)
        
        # Compile results
        enrichments = []
        for i, name in enumerate(names):
            # Description
            desc = desc_results[i] or f"Nombre de origen {origins[i]}."
            if len(desc) > 500:
                desc = desc[:497] + "..."
            desc = self._clean_text(desc, name)
            
            # Pronunciation
            pron_data = {'spanish': 'fácil', 'foreign': 'difícil', 'explanation': 'Sin información.'}
            if pron_results[i]:
                try:
                    pron_data = json.loads(pron_results[i])
                    pron_data['explanation'] = self._clean_text(pron_data.get('explanation', ''), name)
                except:
                    pass
            
            enrichments.append({
                'Family_Origin': origins[i],
                'Name_Description': desc,
                'Pronunciation_Spanish': pron_data['spanish'],
                'Pronunciation_Foreign': pron_data['foreign'],
                'Pronunciation_Explanation': pron_data['explanation']
            })
        
        executor.shutdown(wait=False)
        return enrichments

async def process_file_ultra_fast(input_file: str, output_file: str, max_names: int = None, tier: str = 'level1'):
    """Process CSV file ultra fast"""
    enricher = UltraFastEnricher(tier=tier)
    
    # Read input
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames) + [
            'Family_Origin', 'Name_Description', 'Pronunciation_Spanish',
            'Pronunciation_Foreign', 'Pronunciation_Explanation'
        ]
    
    if max_names:
        rows = rows[:max_names]
    
    names = [row['Nombre'] for row in rows]
    total = len(names)
    
    print(f"\nProcessing {total} names ultra-fast...")
    start_time = time.time()
    
    # Process ALL names at once
    enrichments = await enricher.process_all_names(names)
    
    # Write results
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        
        for i, (row, enrichment) in enumerate(zip(rows, enrichments)):
            row.update(enrichment)
            writer.writerow(row)
            print(f"[{i+1}/{total}] {row['Nombre']}: {enrichment['Family_Origin']}")
    
    elapsed = time.time() - start_time
    print(f"\n✨ Completed in {elapsed:.1f} seconds!")
    print(f"⚡ Speed: {elapsed/total:.2f} seconds per name")
    print(f"🚀 Effective RPM: {(total * 3 / elapsed * 60):.0f}")
    print(f"📁 Output: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Ultra-fast parallel enrichment')
    parser.add_argument('--num', type=int, default=50, help='Number of names')
    parser.add_argument('--tier', choices=['free', 'level1'], default='level1')
    parser.add_argument('--input-file', type=str)
    parser.add_argument('--output-file', type=str)
    
    args = parser.parse_args()
    
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
        output_file = script_dir / 'output_data' / f'names_ultra_fast_{args.tier}.csv'
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    asyncio.run(process_file_ultra_fast(
        str(input_file),
        str(output_file),
        max_names=args.num,
        tier=args.tier
    ))

if __name__ == "__main__":
    main() 