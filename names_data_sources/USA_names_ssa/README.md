# USA Names Database from SSA

This project downloads and processes U.S. Social Security Administration baby names data, converting it from text files to Parquet files for efficient processing.

## Data Sources

The data is automatically downloaded from the SSA website:
- National level data: https://www.ssa.gov/oact/babynames/names.zip
- State level data: https://www.ssa.gov/oact/babynames/state/namesbystate.zip

## Data Format

The project processes two types of data:

### National Level Data
Located in `downloaded_data/names/`:
- Yearly files named `yobYYYY.txt` (e.g., `yob1880.txt`)
- Each line contains: name,sex,count
- Sex is coded as 'M' (male) or 'F' (female)
- Names are 2-15 characters long
- Only names with 5 or more occurrences are included
- Files are sorted by sex and then by number of occurrences in descending order

### State Level Data
Located in `downloaded_data/namesbystate/`:
- One file per state named `XX.TXT` (where XX is the state's postal code)
- Each line contains: state,sex,year,name,count
- State is the 2-letter postal code
- Sex is coded as 'M' (male) or 'F' (female)
- Year ranges from 1910 onwards
- Names are 2-15 characters long
- Only names with 5 or more occurrences are included
- Files are sorted by sex, then year, then by number of occurrences in descending order

## Setup

1. Create a virtual environment using uv:
```bash
uv venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
uv pip install -r requirements.txt
```

## Usage

The process is fully automated. Simply run:

```bash
uv run main.py
```

This will:
1. Download the zip files from SSA website
2. Extract them to the `downloaded_data` directory
3. Convert the data to Parquet format

The final output will be two Parquet files in the `output_data` directory:
- `names_database.parquet`: Contains the national-level names data
- `state_names_database.parquet`: Contains the state-level names data

## Directory Structure

```
USA_names_ssa/
├── main.py                 # Main orchestration script
├── download_SSA_names.py   # Downloads and extracts SSA data
├── convert_to_parquet.py   # Converts text files to parquet format
├── downloaded_data/        # Contains extracted text files
│   ├── names/             # National level data
│   └── namesbystate/      # State level data
└── output_data/           # Contains generated parquet files
``` 