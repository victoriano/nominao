# Quick Setup Guide

If you encounter dependency errors, follow these steps:

## 1. Verify Virtual Environment
```bash
# From project root
ls -la .venv/  # Should exist here, not in subdirectories
```

## 2. Reinstall Dependencies (if needed)
```bash
# From project root
uv pip install -r requirements.txt
```

## 3. Test Installation
```bash
# Test key imports
source .venv/bin/activate
python -c "import pandas, xlrd, polars; print('✅ All dependencies working!')"
```

## 4. Run Pipelines

### Spanish INE (from project root):
```bash
uv run names_data_sources/Spain_names_ine/main.py --skip-enrich
```

### USA SSA (from project root):
```bash
uv run names_data_sources/USA_names_ssa/main.py --convert-only
```

## Common Issues:
- ❌ **Don't run `uv run` from subdirectories** 
- ❌ **Don't create multiple .venv folders**
- ✅ **Always run from project root with `uv run path/to/script.py`**
- ✅ **Or activate venv first: `source .venv/bin/activate`** 