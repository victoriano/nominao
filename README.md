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

#### Integrated Pipeline with AI Origin Classification

**NEW**: Run the complete pipeline including AI-powered origin classification in one command!

##### Quick Examples

```bash
# Basic usage with origin classification (100 names by default)
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins

# Random sample of 50 names with classification
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-mode random --origin-count 50

# Sequential processing with custom output
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-count 200 --origin-output my_classified_names.csv

# Full pipeline with all processing stages (including INE enrichment and origin classification)
export GEMINI_API_KEY='your_key_here'
uv run names_data_sources/Spain_names_ine/main.py --classify-origins --origin-mode random --origin-count 100
```

##### Pipeline Options

| Option | Description | Default |
|--------|-------------|---------|
| `--classify-origins` | Enable AI origin classification after base processing | False |
| `--origin-mode` | Classification mode: `sequential`, `random`, or `all` | sequential |
| `--origin-count` | Number of names to classify (ignored if mode is `all`) | 100 |
| `--origin-output` | Custom output file path for classification results | Auto-generated |
| `--gemini-key` | Gemini API key (alternative to GEMINI_API_KEY env var) | None |

##### Common Use Cases

**1. Quick Testing (5-10 random names):**
```bash
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-mode random --origin-count 5
```

**2. Statistical Sample (1000 random names):**
```bash
export GEMINI_API_KEY='your_key_here'
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-mode random --origin-count 1000 --origin-output statistical_sample.csv
```

**3. Production Processing (First 10,000 names):**
```bash
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-count 10000 --origin-output production_batch.csv
```

**4. Complete Dataset (WARNING: May take hours!):**
```bash
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-mode all
```

##### Output Files Generated

The integrated pipeline generates multiple files:

1. **Base Processing Output:**
   - `output_data/names_frecuencia_edad_media.csv` - Enriched with all metadata columns

2. **Origin Classification & Enrichment Output:**
   - Sequential mode: `output_data/names_with_origin.csv`
   - Random mode: `output_data/names_with_origin_random_sample.csv`
   - Custom: Your specified filename via `--origin-output`

Each enriched file contains all original columns PLUS:
- `Family_Origin`: The AI-classified etymological origin (35 categories)
- `Name_Description`: Rich text description including etymology, cultural significance, and interesting facts
- `Pronunciation_Spanish`: Pronunciation difficulty for Spanish speakers (muy fácil, fácil, difícil, muy difícil)
- `Pronunciation_Foreign`: Pronunciation difficulty for foreign speakers (muy fácil, fácil, difícil, muy difícil)
- `Pronunciation_Explanation`: Detailed explanation of pronunciation challenges

**File Size Expectations:**
- Base file: ~3.4 MB for complete dataset
- Enriched file: Adds ~500-1000 bytes per name for descriptions
- Example: 10 names = ~6 KB additional data

**Example Output (Real Data):**
```csv
Nombre: ANGELA PIEDAD
Frecuencia: 27
Edad Media: 46.8
Gender: Female
Family_Origin: Español
Name_Description: "ANGELA PIEDAD es un nombre compuesto de origen español que fusiona 
dos conceptos de gran belleza y significado espiritual. Angela proviene del griego 
'ángelos' (mensajero), evocando la imagen de los ángeles como mensajeros divinos. 
Piedad, del latín 'pietas', representa devoción, compasión y amor filial..."
```

##### Pipeline Integration Benefits

**Traditional Approach (Multiple Commands):**
```bash
# Step 1: Download and process
cd names_data_sources/Spain_names_ine
uv run python download_INE_names.py
uv run python process_INE_names.py

# Step 2: Optional enrichment
uv run python enrich_INE_names.py  # or skip this

# Step 3: Origin classification
export GEMINI_API_KEY='your_key'
uv run python enrich_names_with_origin.py --random 100
```

**Integrated Pipeline (Single Command):**
```bash
# All steps in one command!
export GEMINI_API_KEY='your_key'
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich --classify-origins --origin-mode random --origin-count 100
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

#### AI-Powered Origin Classification & Enrichment (Standalone)

**NEW**: Advanced AI-powered enrichment for Spanish names using Google Gemini, including etymological origin classification and detailed descriptions.

> **Note**: This section covers standalone usage. For integrated pipeline usage, see [Integrated Pipeline with AI Origin Classification](#integrated-pipeline-with-ai-origin-classification) below.

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