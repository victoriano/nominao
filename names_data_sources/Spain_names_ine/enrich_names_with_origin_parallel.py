#!/usr/bin/env python3
"""
Parallel version of name origin enrichment using Gemini API with rate limiting
Supports both free tier and paid tier (Level 1) rate limits
"""

import os
import asyncio
import aiohttp
import json
import csv
import time
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import argparse
from dataclasses import dataclass
from collections import deque
import google.generativeai as genai

@dataclass
class RateLimits:
    """Rate limit configuration for different API tiers"""
    rpm: int  # Requests per minute
    tpm: int  # Tokens per minute
    rpd: int  # Requests per day
    
# Define rate limits based on Gemini API documentation
RATE_LIMITS = {
    'free': {
        'gemini-2.5-flash': RateLimits(rpm=10, tpm=250_000, rpd=250),
        'gemini-2.5-pro': RateLimits(rpm=5, tpm=250_000, rpd=100)
    },
    'level1': {
        'gemini-2.5-flash': RateLimits(rpm=1000, tpm=1_000_000, rpd=10_000),
        'gemini-2.5-pro': RateLimits(rpm=150, tpm=2_000_000, rpd=1_000)
    }
}

class RateLimiter:
    """Token bucket rate limiter for API calls"""
    def __init__(self, rpm: int):
        self.rpm = rpm
        self.tokens = rpm
        self.max_tokens = rpm
        self.refill_rate = rpm / 60.0  # tokens per second
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
        
    async def acquire(self):
        """Wait until a token is available and consume it"""
        async with self.lock:
            # Refill tokens based on time elapsed
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
            self.last_refill = now
            
            # Wait if no tokens available
            while self.tokens < 1:
                wait_time = (1 - self.tokens) / self.refill_rate
                await asyncio.sleep(wait_time)
                # Refill again after waiting
                now = time.time()
                elapsed = now - self.last_refill
                self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
                self.last_refill = now
            
            # Consume a token
            self.tokens -= 1

class NameOriginEnricherParallel:
    """Parallel name origin enricher using Gemini API with configurable rate limits"""
    
    def __init__(self, api_key: str = None, model_name: str = 'gemini-2.5-flash', 
                 tier: str = 'level1', max_workers: int = None):
        """
        Initialize the enricher with parallel processing capabilities
        
        Args:
            api_key: Gemini API key (or set GEMINI_API_KEY env var)
            model_name: Model to use ('gemini-2.5-flash' or 'gemini-2.5-pro')
            tier: API tier ('free' or 'level1')
            max_workers: Maximum concurrent workers (None = auto based on RPM)
        """
        # Get API key
        self.api_key = api_key or os.environ.get('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found. Set environment variable or pass api_key parameter.")
        
        # Configure Gemini
        genai.configure(api_key=self.api_key)
        
        # Model configuration
        self.model_name = model_name
        self.tier = tier
        self.model = genai.GenerativeModel(model_name)
        
        # Get rate limits for this model and tier
        if model_name not in RATE_LIMITS[tier]:
            raise ValueError(f"Model {model_name} not supported for tier {tier}")
        
        self.limits = RATE_LIMITS[tier][model_name]
        self.rate_limiter = RateLimiter(self.limits.rpm)
        
        # Set max workers based on RPM if not specified
        if max_workers is None:
            # Use a reasonable fraction of RPM to avoid overwhelming the API
            self.max_workers = min(50, max(5, self.limits.rpm // 20))
        else:
            self.max_workers = max_workers
            
        print(f"Initialized parallel enricher:")
        print(f"  Model: {model_name}")
        print(f"  Tier: {tier}")
        print(f"  RPM limit: {self.limits.rpm}")
        print(f"  Max workers: {self.max_workers}")
        
        # Generation configs for structured output
        self.origin_config = {
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
        
        # Request counters for monitoring
        self.requests_made = 0
        self.requests_failed = 0
        self.start_time = None
        
    async def _make_api_call(self, prompt: str, config: dict = None) -> Optional[str]:
        """Make a rate-limited API call"""
        await self.rate_limiter.acquire()
        
        try:
            if config:
                response = await asyncio.to_thread(
                    self.model.generate_content,
                    prompt,
                    generation_config=config
                )
            else:
                response = await asyncio.to_thread(
                    self.model.generate_content,
                    prompt
                )
            
            self.requests_made += 1
            return response.text
            
        except Exception as e:
            self.requests_failed += 1
            print(f"API call failed: {e}")
            return None
    
    async def get_origin_classification(self, name: str) -> str:
        """Classify a name's origin asynchronously"""
        prompt = f"""
        Analiza el siguiente nombre español y clasifícalo según su origen etimológico.
        
        Para nombres compuestos (dos nombres unidos como "Maria Carmen"), aplica estas reglas:
        1. Si contiene mezcla de anglosajón + español → "Latinoamericano"
        2. Para otros casos, clasificar según el componente MÁS ALEJADO del español y latino
        
        Usa las categorías proporcionadas según el origen real del nombre.
        
        Nombre a clasificar: {name}
        """
        
        response = await self._make_api_call(prompt, self.origin_config)
        
        if response:
            try:
                result = json.loads(response)
                return result.get('origin', 'Otro')
            except:
                return 'Otro'
        return 'Otro'
    
    async def get_name_description(self, name: str, origin: str) -> str:
        """Generate name description asynchronously"""
        prompt = f"""
        Genera una descripción breve pero interesante sobre el nombre "{name}" considerando que su origen es {origin}.
        
        La descripción debe incluir (cuando sea aplicable):
        1. Significado etimológico del nombre
        2. Historia o contexto cultural
        3. Personajes famosos o referencias culturales
        4. Variantes en otros idiomas
        
        Requisitos IMPORTANTES:
        - Máximo 150 palabras
        - NO uses formato markdown
        - Escribe los nombres con la primera letra en mayúscula
        - Usa solo texto plano
        
        Genera la descripción en español.
        """
        
        response = await self._make_api_call(prompt)
        
        if response:
            return self._clean_description_for_csv(response.strip(), name)
        return f"Nombre de origen {origin}."
    
    async def get_pronunciation_difficulty(self, name: str, origin: str) -> Dict[str, str]:
        """Evaluate pronunciation difficulty asynchronously"""
        prompt = f"""
        Evalúa la dificultad de pronunciación del nombre "{name}" (origen: {origin}).
        
        Considera para ESPAÑOLES:
        - Muy fácil: Solo fonemas españoles comunes
        - Fácil: Fonemas españoles con alguna combinación menos común
        - Difícil: Contiene fonemas no españoles pero adaptables
        - Muy difícil: Fonemas muy ajenos al español
        
        Considera para EXTRANJEROS (hablantes de inglés):
        - Muy fácil: Nombres internacionales o con fonética simple
        - Fácil: Pronunciación clara con pocas peculiaridades españolas
        - Difícil: Contiene sonidos específicos del español (rr, ñ, j)
        - Muy difícil: Múltiples sonidos difíciles o estructura compleja
        
        En la explicación (máximo 100 palabras), identifica los sonidos problemáticos.
        """
        
        response = await self._make_api_call(prompt, self.pronunciation_config)
        
        if response:
            try:
                result = json.loads(response)
                explanation = result.get('explanation', '')
                explanation = self._clean_description_for_csv(explanation, name)
                
                return {
                    'spanish': result.get('spanish', 'fácil'),
                    'foreign': result.get('foreign', 'difícil'),
                    'explanation': explanation
                }
            except:
                pass
                
        return {
            'spanish': 'fácil',
            'foreign': 'difícil',
            'explanation': 'No se pudo evaluar la pronunciación.'
        }
    
    async def enrich_single_name(self, name: str) -> Dict[str, str]:
        """Enrich a single name with all data asynchronously"""
        # Get origin first
        origin = await self.get_origin_classification(name)
        
        # Get description and pronunciation in parallel
        description_task = self.get_name_description(name, origin)
        pronunciation_task = self.get_pronunciation_difficulty(name, origin)
        
        description, pronunciation = await asyncio.gather(
            description_task, pronunciation_task
        )
        
        return {
            'Family_Origin': origin,
            'Name_Description': description,
            'Pronunciation_Spanish': pronunciation['spanish'],
            'Pronunciation_Foreign': pronunciation['foreign'],
            'Pronunciation_Explanation': pronunciation['explanation']
        }
    
    async def enrich_batch(self, rows: List[Dict], progress_callback=None) -> List[Dict]:
        """Enrich a batch of names in parallel"""
        tasks = []
        for i, row in enumerate(rows):
            name = row['Nombre']
            task = self.enrich_single_name(name)
            tasks.append((i, row, task))
        
        results = []
        for i, row, task in tasks:
            try:
                enrichments = await task
                row.update(enrichments)
                
                if progress_callback:
                    progress_callback(row, i + 1, len(rows))
                    
            except Exception as e:
                print(f"Error enriching {row['Nombre']}: {e}")
                # Add default values on error
                row.update({
                    'Family_Origin': 'Otro',
                    'Name_Description': 'Error al procesar',
                    'Pronunciation_Spanish': 'fácil',
                    'Pronunciation_Foreign': 'difícil',
                    'Pronunciation_Explanation': 'Error al procesar'
                })
            
            results.append(row)
        
        return results
    
    def _clean_description_for_csv(self, text: str, name: str) -> str:
        """Clean text for CSV output"""
        import re
        
        # Remove markdown
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        
        # Capitalize name references
        name_parts = name.split()
        for part in name_parts:
            pattern = r'\b' + re.escape(part.lower()) + r'\b'
            text = re.sub(pattern, part.title(), text, flags=re.IGNORECASE)
        
        # Clean for CSV
        text = text.replace('"', "'")
        text = text.replace('\n', ' ')
        text = text.replace('\r', ' ')
        text = text.replace('\t', ' ')
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def print_stats(self):
        """Print processing statistics"""
        if self.start_time:
            elapsed = time.time() - self.start_time
            rpm = (self.requests_made / elapsed) * 60 if elapsed > 0 else 0
            
            print(f"\nProcessing Statistics:")
            print(f"  Total requests: {self.requests_made}")
            print(f"  Failed requests: {self.requests_failed}")
            print(f"  Elapsed time: {elapsed:.1f}s")
            print(f"  Actual RPM: {rpm:.1f}")
            print(f"  Success rate: {((self.requests_made - self.requests_failed) / self.requests_made * 100):.1f}%")

async def process_file_parallel(enricher, input_file: str, output_file: str, 
                               max_names: int = None, batch_size: int = None):
    """Process CSV file with parallel enrichment"""
    
    # Auto-calculate batch size based on workers
    if batch_size is None:
        batch_size = enricher.max_workers
    
    # Read input file
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
        fieldnames = list(reader.fieldnames) + [
            'Family_Origin', 'Name_Description', 'Pronunciation_Spanish',
            'Pronunciation_Foreign', 'Pronunciation_Explanation'
        ]
    
    # Limit rows if specified
    if max_names:
        all_rows = all_rows[:max_names]
    
    total_rows = len(all_rows)
    print(f"\nProcessing {total_rows} names in batches of {batch_size}")
    print(f"Estimated time: {(total_rows / enricher.limits.rpm * 60):.1f} seconds")
    
    enricher.start_time = time.time()
    
    # Process in batches
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        
        processed = 0
        for i in range(0, total_rows, batch_size):
            batch = all_rows[i:i + batch_size]
            
            # Progress callback
            def progress(row, batch_idx, batch_total):
                nonlocal processed
                processed += 1
                print(f"[{processed}/{total_rows}] {row['Nombre']}: {row['Family_Origin']}")
            
            # Process batch in parallel
            enriched_batch = await enricher.enrich_batch(batch, progress)
            
            # Write results
            writer.writerows(enriched_batch)
            f.flush()  # Ensure data is written
    
    enricher.print_stats()
    print(f"\nEnriched data saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Parallel name enrichment with Gemini API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Level 1 (paid) - Fast processing with high concurrency
  python enrich_names_with_origin_parallel.py --tier level1 --num 100
  
  # Free tier - Slower but free
  python enrich_names_with_origin_parallel.py --tier free --num 50
  
  # Custom workers and batch size
  python enrich_names_with_origin_parallel.py --workers 20 --batch 10
  
  # Use Pro model for better quality (but slower)
  python enrich_names_with_origin_parallel.py --model gemini-2.5-pro --tier level1
        """
    )
    
    parser.add_argument('--num', type=int, default=50,
                       help='Number of names to process')
    parser.add_argument('--tier', choices=['free', 'level1'], default='level1',
                       help='API tier to use (default: level1)')
    parser.add_argument('--model', choices=['gemini-2.5-flash', 'gemini-2.5-pro'],
                       default='gemini-2.5-flash',
                       help='Gemini model to use (default: gemini-2.5-flash)')
    parser.add_argument('--workers', type=int,
                       help='Maximum concurrent workers (default: auto based on RPM)')
    parser.add_argument('--batch', type=int,
                       help='Batch size for processing (default: same as workers)')
    parser.add_argument('--input-file', type=str,
                       help='Input CSV file')
    parser.add_argument('--output-file', type=str,
                       help='Output CSV file')
    
    args = parser.parse_args()
    
    # Set up file paths
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
        output_file = script_dir / 'output_data' / f'names_enriched_parallel_{args.tier}.csv'
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    try:
        # Initialize enricher
        enricher = NameOriginEnricherParallel(
            model_name=args.model,
            tier=args.tier,
            max_workers=args.workers
        )
        
        # Run async processing
        asyncio.run(process_file_parallel(
            enricher,
            str(input_file),
            str(output_file),
            max_names=args.num,
            batch_size=args.batch
        ))
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 