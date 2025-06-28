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

## Usage

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

## Data Sources Attribution

- **USA Names**: Data obtained from the U.S. Social Security Administration (www.ssa.gov)
- **Spain Names**: Data obtained from Instituto Nacional de Estadística (www.ine.es) 