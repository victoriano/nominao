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
│       ├── enrich_INE_names.py
│       ├── enrich_names_with_origin.py  # AI origin classification
│       └── output_data/    # Processed CSV files (tracked)
│           ├── names_frecuencia_edad_media.csv
│           ├── names_with_origin.csv
│           └── names_with_origin_random_sample.csv
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

## Quick Start (Recommended)

Run these commands from the project root for the fastest setup:

```bash
# Spanish pipeline (skip API enrichment for speed)
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich

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

**Option 1: Using uv from project root (recommended):**
```bash
uv run names_data_sources/Spain_names_ine/main.py              # Complete pipeline
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich # Skip API enrichment (faster)
```

**Option 2: From Spanish directory:**
```bash
cd names_data_sources/Spain_names_ine
source ../../.venv/bin/activate
python main.py                    # Runs complete pipeline (including enrichment)
python main.py --skip-enrich      # Skip API enrichment (faster)
```

**What it does:**
- Downloads names data from INE (Instituto Nacional de Estadística)
- Processes data to add analysis columns:
  - Character and syllable counts
  - Popularity rankings by gender
  - Compound name identification
  - Name percentage calculations
- **By default:** Enriches data with regional distribution via INE API
- **Options:** Use `--skip-enrich` to skip API calls for faster execution
- Outputs: `output_data/names_frecuencia_edad_media.csv` with comprehensive name statistics

#### AI-Powered Origin Classification

**NEW**: Advanced etymological origin classification for Spanish names using Google Gemini AI.

**Setup Requirements:**
```bash
# Set your Gemini API key
export GEMINI_API_KEY='your_api_key_here'

# Run basic pipeline first to generate base data
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich
```

**Usage Options:**

**Quick Test (No File Output):**
```bash
cd names_data_sources/Spain_names_ine

# Test with 5 random names (default)
uv run python enrich_names_with_origin.py --test-random

# Test with specific number of random names
uv run python enrich_names_with_origin.py --test-random 10
```

**Sequential Processing (First N Names):**
```bash
# Process first 10 names (default)
uv run python enrich_names_with_origin.py

# Process first 50 names
uv run python enrich_names_with_origin.py --num 50

# Process first 100 names
uv run python enrich_names_with_origin.py --num 100
```

**Random Sampling (With File Output):**
```bash
# Process 25 random names and save to file
uv run python enrich_names_with_origin.py --random 25

# Process 100 random names and save to file
uv run python enrich_names_with_origin.py --random 100
```

**Complete Processing:**
```bash
# Process ALL names (requires confirmation - may take hours!)
uv run python enrich_names_with_origin.py --all
```

**Speed Control:**
```bash
# Use custom delay between API calls (default: 1.0 second)
uv run python enrich_names_with_origin.py --num 20 --delay 0.5
```

**Help and Options:**
```bash
# View all available options
uv run python enrich_names_with_origin.py --help
```

#### Origin Categories (35 Total)

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

#### Output Files

**Sequential Processing:**
- `output_data/names_with_origin.csv`: Names processed sequentially from the beginning

**Random Sampling:**
- `output_data/names_with_origin_random_sample.csv`: Randomly sampled names

**File Format:**
Each output file contains all original columns plus:
- `Family_Origin`: The classified etymological origin

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