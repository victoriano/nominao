#!/usr/bin/env python3
"""
Ultra-fast enrichment script for Spanish INE names.
Supports Gemini and OpenAI providers with highly parallel requests.
"""

import os
import asyncio
import json
import csv
import time
import random
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional

import google.generativeai as genai
from dotenv import load_dotenv

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None


class UltraFastEnricher:
    def __init__(
        self,
        api_key: Optional[str] = None,
        tier: str = "level1",
        model_name: str = "gemini-2.5-flash",
        provider: str = "gemini",
        max_concurrent: Optional[int] = None,
    ) -> None:
        """Initialise the enricher with the desired provider."""
        load_dotenv()
        self.provider = provider
        self.model_name = model_name
        self.tier = tier
        self.api_key = api_key

        if self.provider == "gemini":
            self.api_key = self.api_key or os.environ.get("GEMINI_API_KEY")
            if not self.api_key:
                raise ValueError("GEMINI_API_KEY not found")
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(self.model_name)
            self.openai_client = None
        elif self.provider == "openai":
            self.api_key = self.api_key or os.environ.get("OPENAI_API_KEY")
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY not found")
            if OpenAI is None:  # pragma: no cover - sanity guard
                raise ImportError("openai package is required. Install it via requirements.txt")
            self.openai_client = OpenAI()
            self.model = None
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

        if max_concurrent is not None:
            self.max_concurrent = max_concurrent
        else:
            if self.provider == "gemini":
                self.max_concurrent = 300 if tier == "level1" else 10
            else:
                # Tune this to your account limits. For now favour high throughput.
                self.max_concurrent = 100 if tier == "level1" else 20

        self.origin_config = {
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "object",
                "properties": {
                    "origin": {
                        "type": "string",
                        "enum": [
                            "Africano",
                            "Alemán",
                            "Anglosajón",
                            "Arameo",
                            "Armenio",
                            "Catalán",
                            "Chino",
                            "Contemporáneo",
                            "Coreano",
                            "Desconocido",
                            "Egipcio",
                            "Escandinavo",
                            "Eslavo",
                            "Español",
                            "Francés",
                            "Gallego",
                            "Georgiano",
                            "Griego",
                            "Guanche",
                            "Hawaiano",
                            "Húngaro",
                            "Indonesio",
                            "Italiano",
                            "Japonés",
                            "Latinoamericano",
                            "Lituano",
                            "Nativo Americano",
                            "Persa",
                            "Portugués",
                            "Rumano",
                            "Sánscrito",
                            "Turco",
                            "Vasco",
                            "Árabe",
                            "Otro",
                        ],
                    }
                },
                "required": ["origin"],
            },
        }

        self.pronunciation_config = {
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "object",
                "properties": {
                    "spanish": {
                        "type": "string",
                        "enum": ["muy fácil", "fácil", "difícil", "muy difícil"],
                    },
                    "foreign": {
                        "type": "string",
                        "enum": ["muy fácil", "fácil", "difícil", "muy difícil"],
                    },
                    "explanation": {"type": "string"},
                },
                "required": ["spanish", "foreign", "explanation"],
            },
        }

    # ------------------------------------------------------------------
    # Prompt helpers
    # ------------------------------------------------------------------
    def get_origin_prompt(self, name: str) -> str:
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
            
            Usa estas categorías según el origen actual en español:
            - "Español": Incluye nombres latinos y hebreos/bíblicos asimilados (María, José, Carmen, Antonio) y
              germanizados/castellanizados (Guillermo, Carlos, Francisco, Fernando). Solo usa otra categoría si el
              nombre mantiene su forma extranjera original.
            - Resto de categorías disponibles: Africano, Alemán, Anglosajón, Arameo, Armenio, Catalán, Chino,
              Contemporáneo, Coreano, Desconocido, Egipcio, Escandinavo, Eslavo, Francés, Gallego, Georgiano,
              Griego, Guanche, Hawaiano, Húngaro, Indonesio, Italiano, Japonés, Latinoamericano, Lituano,
              Nativo Americano, Persa, Portugués, Rumano, Sánscrito, Turco, Vasco, Árabe, Otro.
            
            Si no estás seguro del origen, usa "Desconocido".
            Si no encaja en ninguna categoría, usa "Otro".
            
            Nombre a clasificar: {name}
            """

    def get_description_prompt(self, name: str, origin: str) -> str:
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
            - Escribe los nombres siempre con la primera letra en mayúscula
            - Si es un nombre compuesto, menciona ambos componentes
            - Evita información no verificable o inventada
            - Si no tienes información segura sobre algún aspecto, no lo menciones
            - Usa solo texto plano, sin símbolos especiales
            
            Genera la descripción en español usando solo texto plano.
            """

    def get_pronunciation_prompt(self, name: str, origin: str) -> str:
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _clean_text(text: str, name: str) -> str:
        import re

        text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
        text = re.sub(r"\*([^*]+)\*", r"\1", text)
        text = re.sub(r"__([^_]+)__", r"\1", text)
        text = re.sub(r"_([^_]+)_", r"\1", text)

        name_parts = name.split()
        for part in name_parts:
            pattern = r"\b" + re.escape(part.lower()) + r"\b"
            text = re.sub(pattern, part.title(), text, flags=re.IGNORECASE)

        text = text.replace('"', "'")
        text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[^\w\s\.\,\;\:\!\?\(\)\-']", "", text)
        return text.strip()

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------
    async def process_all_names(self, names: List[str]) -> List[Dict[str, str]]:
        semaphore = asyncio.Semaphore(self.max_concurrent)
        executor = ThreadPoolExecutor(max_workers=self.max_concurrent)

        async def call_api(prompt: str, config: Optional[dict] = None) -> Optional[str]:
            async with semaphore:
                loop = asyncio.get_event_loop()
                try:
                    if self.provider == "gemini":
                        if config:
                            response = await loop.run_in_executor(
                                executor,
                                lambda: self.model.generate_content(prompt, generation_config=config),
                            )
                        else:
                            response = await loop.run_in_executor(
                                executor,
                                lambda: self.model.generate_content(prompt),
                            )
                        return response.text

                    def _openai_call() -> str:
                        messages = [
                            {
                                "role": "system",
                                "content": (
                                    "Eres un asistente que responde exactamente según las instrucciones; "
                                    "si se pide JSON, devuelve un JSON válido"
                                ),
                            },
                            {"role": "user", "content": prompt},
                        ]
                        if config:
                            response = self.openai_client.chat.completions.create(
                                model=self.model_name,
                                messages=messages,
                                response_format={"type": "json_object"},
                            )
                        else:
                            response = self.openai_client.chat.completions.create(
                                model=self.model_name,
                                messages=messages,
                            )
                        return response.choices[0].message.content

                    return await loop.run_in_executor(executor, _openai_call)
                except Exception as exc:  # pragma: no cover - debugging helper
                    print(f"API error: {exc}")
                    return None

        # Origins first
        origin_tasks = [call_api(self.get_origin_prompt(name), self.origin_config) for name in names]
        origin_results = await asyncio.gather(*origin_tasks)

        origins: List[str] = []
        for name, result in zip(names, origin_results):
            origin = "Otro"
            if result:
                try:
                    origin = json.loads(result).get("origin", "Otro")
                except Exception:
                    origin = "Otro"
            origins.append(origin)

        # Descriptions & pronunciation
        desc_tasks = [call_api(self.get_description_prompt(name, origin)) for name, origin in zip(names, origins)]
        pron_tasks = [
            call_api(self.get_pronunciation_prompt(name, origin), self.pronunciation_config)
            for name, origin in zip(names, origins)
        ]

        desc_results = await asyncio.gather(*desc_tasks)
        pron_results = await asyncio.gather(*pron_tasks)

        enrichments: List[Dict[str, str]] = []
        for name, origin, desc_text, pron_text in zip(names, origins, desc_results, pron_results):
            description = desc_text or f"Nombre de origen {origin}."
            if len(description) > 500:
                description = description[:497] + "..."
            description = self._clean_text(description, name)

            pron_data = {"spanish": "fácil", "foreign": "difícil", "explanation": "Sin información."}
            if pron_text:
                try:
                    pron_json = json.loads(pron_text)
                    pron_data = {
                        "spanish": pron_json.get("spanish", "fácil"),
                        "foreign": pron_json.get("foreign", "difícil"),
                        "explanation": self._clean_text(pron_json.get("explanation", ""), name),
                    }
                except Exception:
                    pass

            enrichments.append(
                {
                    "Family_Origin": origin,
                    "Name_Description": description,
                    "Pronunciation_Spanish": pron_data["spanish"],
                    "Pronunciation_Foreign": pron_data["foreign"],
                    "Pronunciation_Explanation": pron_data["explanation"],
                }
            )

        executor.shutdown(wait=False)
        return enrichments


async def process_file_ultra_fast(
    input_file: str,
    output_file: str,
    max_names: Optional[int] = None,
    tier: str = "level1",
    model_name: str = "gemini-2.5-flash",
    provider: str = "gemini",
    max_concurrent: Optional[int] = None,
    mode: str = "sequential",
    seed: Optional[int] = None,
) -> None:
    """Load names, enrich them and save the result."""
    enricher = UltraFastEnricher(
        tier=tier,
        model_name=model_name,
        provider=provider,
        max_concurrent=max_concurrent,
    )

    with open(input_file, "r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        fieldnames = list(reader.fieldnames) + [
            "Family_Origin",
            "Name_Description",
            "Pronunciation_Spanish",
            "Pronunciation_Foreign",
            "Pronunciation_Explanation",
        ]

    if not rows:
        print("No rows found in input file. Nothing to do.")
        return

    if seed is not None:
        random.seed(seed)

    if mode == "random":
        if max_names is None or max_names >= len(rows):
            random.shuffle(rows)
        else:
            rows = random.sample(rows, max_names)
    else:
        if max_names is not None and max_names > 0:
            rows = rows[:max_names]

    names = [row["Nombre"] for row in rows]
    total = len(names)

    print(f"\nProcessing {total} names ultra-fast ({mode})...")
    start_time = time.time()

    enrichments = await enricher.process_all_names(names)

    with open(output_file, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for idx, (row, enrichment) in enumerate(zip(rows, enrichments), start=1):
            row.update(enrichment)
            writer.writerow(row)
            print(f"[{idx}/{total}] {row['Nombre']}: {enrichment['Family_Origin']}")

    elapsed = time.time() - start_time
    avg = elapsed / total if total else 0
    effective_rpm = (total * 3 / elapsed * 60) if elapsed else 0

    print(f"\n✨ Completed in {elapsed:.1f} seconds!")
    print(f"⚡ Speed: {avg:.2f} seconds per name")
    print(f"🚀 Effective RPM: {effective_rpm:.0f}")
    print(f"📁 Output: {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultra-fast parallel enrichment")
    parser.add_argument("--num", type=int, default=50, help="Number of names (ignored with --all)")
    parser.add_argument("--all", action="store_true", help="Process all names in the dataset")
    parser.add_argument("--mode", choices=["sequential", "random"], default="sequential", help="Processing mode")
    parser.add_argument("--seed", type=int, help="Random seed when using random mode")
    parser.add_argument("--tier", choices=["free", "level1"], default="level1", help="Provider tier preset")
    parser.add_argument("--provider", choices=["gemini", "openai"], default="gemini", help="LLM provider")
    parser.add_argument("--model", type=str, default="gemini-2.5-flash", help="Provider model to use")
    parser.add_argument("--max-concurrent", type=int, help="Override max concurrent requests")
    parser.add_argument("--input-file", type=str, help="Input CSV file")
    parser.add_argument("--output-file", type=str, help="Output CSV file")

    args = parser.parse_args()
    script_dir = Path(__file__).parent

    if args.input_file:
        input_file = Path(args.input_file)
        if not input_file.is_absolute():
            input_file = script_dir / args.input_file
    else:
        input_file = script_dir / "output_data" / "names_frecuencia_edad_media.csv"

    if args.output_file:
        output_file = Path(args.output_file)
        if not output_file.is_absolute():
            output_file = script_dir / args.output_file
    else:
        default_name = f"names_ultra_fast_{args.provider}_{args.tier}.csv"
        output_file = script_dir / "output_data" / default_name

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    max_names = None if args.all else args.num

    asyncio.run(
        process_file_ultra_fast(
            input_file=str(input_file),
            output_file=str(output_file),
            max_names=max_names,
            tier=args.tier,
            model_name=args.model,
            provider=args.provider,
            max_concurrent=args.max_concurrent,
            mode=args.mode,
            seed=args.seed,
        )
    )


if __name__ == "__main__":
    main() 
