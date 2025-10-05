# NameSeek

A data collection and processing tool for gathering name statistics from various national databases.

## Current Data Sources

- **USA Social Security Administration (SSA) Names Database** - National and state-level baby name statistics
- **Spain National Statistics Institute (INE) Names Database** - Spanish names with frequency, age, and regional data

## Project Structure

```
.
├── names_data_sources/
│   ├── USA_names_ssa/      # US Social Security Administration name data
│   │   ├── main.py         # Main pipeline script
│   │   ├── download_SSA_names.py
│   │   ├── convert_to_parquet.py
│   │   ├── downloaded_data/    # Raw data (auto-generated, not tracked)
│   │   └── output_data/    # Processed parquet files (tracked)
│   └── Spain_names_ine/    # Spanish INE name data
│       ├── main.py         # Main pipeline script
│       ├── download_INE_names.py
│       ├── process_INE_names.py
│       ├── enrich_names.py              # Ultra-fast AI enrichment (Gemini/OpenAI)
│       └── output_data/    # Processed CSV files (tracked)
│           └── names_frecuencia_edad_media.csv
└── requirements.txt        # Project dependencies
```

## Setup

1. Create a virtual environment using uv:
```bash
uv venv
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate     # On Windows
```

2. Install dependencies:
```bash
uv pip install -r requirements.txt
```

3. (Optional) Set up Gemini API key for Spanish names origin classification:
```bash
export GEMINI_API_KEY='your_api_key_here'  # Unix/macOS
# or
set GEMINI_API_KEY=your_api_key_here       # Windows
```

Get your API key from: https://makersuite.google.com/app/apikey

## Recent Updates

### Version 2.0 - AI-Powered Name Enrichment
- **NEW**: Added rich name descriptions with etymology and cultural context
- **NEW**: Pronunciation difficulty analysis for Spanish and foreign speakers
- **NEW**: Modular architecture for easy addition of new enrichment columns
- **IMPROVED**: Refactored code for better maintainability and extensibility
- **ENHANCED**: More detailed output with comprehensive name analysis including pronunciation challenges

## Quick Start (Recommended)

Run these commands from the project root for the fastest setup:

```bash
# Spanish pipeline (skip API enrichment for speed)
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich

# Spanish pipeline with AI origin classification (requires Gemini API key)
export GEMINI_API_KEY='your_key_here'
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-mode random --origin-count 50

# USA pipeline (convert existing data to parquet)
uv run names_data_sources/USA_names_ssa/main.py --convert-only
```

## Detailed Usage

Each data source has its own automated pipeline. Simply run the main script for the desired data source:

### USA SSA Names Data

**Option 1: Using uv from project root (recommended):**
```bash
uv run names_data_sources/USA_names_ssa/main.py              # Complete pipeline
uv run names_data_sources/USA_names_ssa/main.py --skip-download     # Skip download, use existing data
uv run names_data_sources/USA_names_ssa/main.py --convert-only      # Only convert to parquet
uv run names_data_sources/USA_names_ssa/main.py --download-only     # Only download data
```

**Option 2: From USA directory:**
```bash
cd names_data_sources/USA_names_ssa
uv run main.py                    # Complete pipeline
uv run main.py --skip-download    # Skip download, use existing data
```

**What it does:**
- Downloads national baby names data (1880-2023) - 144 years!
- Downloads state-level baby names data - 51 states + DC
- Converts all data to efficient Parquet format with zstd compression
- **Options:** Use `--skip-download` if download fails due to SSA website restrictions
- Outputs: `output_data/names_database.parquet` and `output_data/state_names_database.parquet`

### Spanish INE Names Data

#### Basic Data Processing

### Spanish INE Names Data

Run the entire pipeline (download → process → AI enrichment) in a single command:

```bash
# Sequential (first N names)
uv run names_data_sources/Spain_names_ine/main.py --origin-mode sequential --origin-count 200

# Random sample (requires OPENAI/Gemini environment vars as needed)
uv run names_data_sources/Spain_names_ine/main.py --origin-mode random --origin-count 500 --origin-provider openai --origin-model gpt-4o-mini

# Full dataset (sequential, may take hours)
uv run names_data_sources/Spain_names_ine/main.py --origin-mode all --origin-provider gemini --origin-model gemini-2.5-flash --origin-tier level1
```

Key flags:
- `--origin-mode`: `sequential`, `random`, or `all`
- `--origin-count`: number of names (ignored when `--origin-mode all`)
- `--origin-provider`: `gemini` or `openai`
- `--origin-model`: provider-specific model (e.g. `gemini-2.5-flash`, `gpt-4o-mini`)
- `--origin-output`: optional custom CSV path
- `--origin-max-concurrent`: override concurrency if you need to tune rate limits

Environment variables:
- `GEMINI_API_KEY` (required when `--origin-provider gemini`)
- `OPENAI_API_KEY` (required when `--origin-provider openai`)

Outputs:
- `output_data/names_frecuencia_edad_media.csv`: base dataset with metrics
- Ultra-fast enrichment CSV (default name: `names_ultra_fast_<provider>_<tier>.csv` or custom via `--origin-output`).

Each enriched file includes:
- `Family_Origin`
- `Name_Description`
- `Pronunciation_Spanish`
- `Pronunciation_Foreign`
- `Pronunciation_Explanation`

**Main Categories:**
- **Español**: Spanish names, including castellanized and culturally assimilated names (Hebrew/Biblical, Latin)
- **Anglosajón**: Anglo-Saxon, Celtic, English, Irish, Scottish, Welsh origins  
- **Alemán**: German names in their original form
- **Francés**: French, Breton, Provençal, Occitan origins
- **Italiano**: Italian names
- **Árabe**: Arabic, Berber, North African origins
- **Eslavo**: Slavic origins (Russian, Polish, Ukrainian, etc.)
- **Catalán**: Catalan names
- **Gallego**: Galician names  
- **Vasco**: Basque/Euskera names

**Other Origins:**
- Escandinavo, Griego, Portugués, Sánscrito, Chino, Japonés, Coreano
- Armenio, Georgiano, Húngaro, Turco, Persa, Egipcio, Arameo
- Nativo Americano, Latinoamericano, Africano, Hawaiano
- Contemporáneo, Guanche, and more

#### Smart Classification Features

**Cultural Classification:**
Names are classified by their **current Spanish form**, not historical etymology:
- "Guillermo" → Español (not Alemán)
- "Carlos" → Español (not Alemán)  
- "Wilhelm" → Alemán
- "Karl" → Alemán

**Compound Name Rules:**
1. **Anglo-Spanish Mix** → Latinoamericano
   - "Brandon José" → Latinoamericano
   - "Jennifer María" → Latinoamericano

2. **Other Compounds** → Most distant from Spanish/Latin
   - "María Aitor" → Vasco (by Aitor)
   - "Juan Chen" → Chino (by Chen)

**Hebrew/Biblical and Latin Assimilation:**
Names of Hebrew/Biblical and Latin origin are classified as Spanish due to cultural assimilation:
- "María", "José", "Carmen", "Antonio" → Español

#### Enrichment Features

**NEW in v2.0:**
- **Name Descriptions**: Rich textual descriptions for each name including:
  - Etymological meaning and linguistic roots
  - Cultural and historical context
  - Famous bearers and cultural references
  - Variants in other languages
  - Interesting facts and curiosities
  - Maximum 150 words per description
- **Pronunciation Analysis**: Three columns evaluating pronunciation difficulty:
  - `Pronunciation_Spanish`: Difficulty for Spanish speakers (muy fácil, fácil, difícil, muy difícil)
  - `Pronunciation_Foreign`: Difficulty for foreign speakers (primarily English speakers)
  - `Pronunciation_Explanation`: Detailed explanation of specific phonetic challenges

**Original Features:**
- **35 origin categories**: Comprehensive classification system
- **Cultural classification**: Names classified by current Spanish form
- **Smart compound handling**: Special rules for Anglo-Spanish mixes
- **Structured output**: Guaranteed valid responses using Gemini AI

#### Output Files

**Sequential Processing:**
- `output_data/names_with_origin.csv`: Names processed sequentially from the beginning

**Random Sampling:**
- `output_data/names_with_origin_random_sample.csv`: Randomly sampled names

**File Format:**
Each output file contains all original columns plus:
- `Family_Origin`: The classified etymological origin
- `Name_Description`: Detailed description with etymology, meaning, and cultural context
- `Pronunciation_Spanish`: Difficulty for Spanish speakers
- `Pronunciation_Foreign`: Difficulty for foreign speakers (primarily English)
- `Pronunciation_Explanation`: Detailed phonetic analysis

#### Extensible Architecture

The enrichment system is designed to be modular and easily extensible. New enrichment columns can be added by:

1. **Create a new enrichment method** in the `NameOriginEnricher` class:
   ```python
   def _get_name_popularity(self, name: str) -> str:
       # Your enrichment logic here
       return popularity_info
   ```

2. **Add the call** to `get_all_enrichments()`:
   ```python
   enrichments['Name_Popularity'] = self._get_name_popularity(name)
   ```

3. **Update the columns list** in `enrich_names_file()`:
   ```python
   new_columns = ['Family_Origin', 'Name_Description', 'Pronunciation_Spanish', 
                  'Pronunciation_Foreign', 'Pronunciation_Explanation', 'Name_Popularity']
   ```

**Future enrichment ideas already scaffolded:**
- **Name_Popularity**: Historical popularity trends
- **Name_Gender_Distribution**: Gender usage statistics
- **Name_Geographic_Distribution**: Regional popularity patterns
- **Name_Variants**: International variations and nicknames
- **Name_Sentiment**: Sentiment analysis and perception

#### Classification Examples

```bash
# Quick examples of how the AI classifies names:

# Cultural classification (current form vs etymology)
"Guillermo" → Español    # (not Alemán - castellanized)
"Wilhelm" → Alemán       # (original German form)

# Compound name handling  
"Brandon José" → Latinoamericano    # (Anglo + Spanish mix)
"María Aitor" → Vasco              # (most distant from Spanish)

# Biblical/Latin assimilation
"José" → Español         # (culturally Spanish despite Hebrew origin)
"Antonio" → Español      # (culturally Spanish despite Latin origin)
```

### Pipeline Features

Both pipelines include:
- ✅ **Error handling** - Graceful failure recovery
- ✅ **Progress tracking** - Clear status messages
- ✅ **Modular design** - Individual scripts can be run separately
- ✅ **Data validation** - Ensures data integrity

### Performance & Limitations

**Spanish Names AI Enrichment:**
- **Processing Speed**: ~1 name/second (with 1s API delay)
- **API Limits**: Subject to Gemini API quotas
- **Recommended Batch Size**: 100-1000 names for testing
- **Full Dataset**: ~107,000 names would take ~30 hours
- **Cost**: Free tier usually sufficient for small batches

**Tips for Large-Scale Processing:**
- Use `--random` mode to process representative samples
- Process in batches with custom output files
- Monitor API usage in Google AI Studio
- Consider parallel processing for production use

## Dependencies

Key dependencies include:
- `polars` - High-performance data processing
- `pandas` - Data manipulation and analysis
- `requests` - HTTP requests for data download
- `xlrd` - Excel file reading (for Spanish INE data)
- `nltk` - Natural language processing (for Spanish analysis)
- `beautifulsoup4` - HTML parsing
- `tqdm` - Progress bars
- `google-generativeai` - Gemini AI for origin classification (Spanish names)
- `python-dotenv` - Environment variable management

## Data Sources Attribution

- **USA Names**: Data obtained from the U.S. Social Security Administration (www.ssa.gov)
- **Spain Names**: Data obtained from Instituto Nacional de Estadística (www.ine.es) 