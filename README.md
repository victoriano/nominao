# NameSeek

A data collection and processing tool for gathering name statistics from various national databases.

## Current Data Sources

- USA Social Security Administration (SSA) Names Database
- Spain National Statistics Institute (INE) Names Database

## Project Structure

```
.
├── names_data_sources/
│   ├── USA_names_ssa/      # US Social Security Administration name data
│   └── Spain_names_ine/    # Spanish INE name data
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

Each data source has its own set of scripts for downloading and processing data. Navigate to the specific data source directory and run the main script:

```bash
cd names_data_sources/USA_names_ssa
uv run main.py
```

## Data Sources Attribution

- USA Names: Data obtained from the U.S. Social Security Administration (www.ssa.gov)
- Spain Names: Data obtained from Instituto Nacional de Estadística (www.ine.es) 