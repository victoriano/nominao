import os
import subprocess
import sys
from pathlib import Path

def run_script(script_path):
    """Run a Python script and check for successful execution."""
    print(f"\nExecuting {script_path}...")
    try:
        # Using sys.executable ensures we use the same Python interpreter (with uv)
        result = subprocess.run([sys.executable, script_path], check=True)
        print(f"Successfully completed {script_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        return False

def main():
    # Get the absolute path to the current directory
    current_dir = Path(__file__).parent
    
    # Define paths to scripts
    download_script = current_dir / "download_SSA_names.py"
    convert_script = current_dir / "convert_to_parquet.py"
    
    # First, run the download script
    if not run_script(download_script):
        print("Failed to download data. Stopping execution.")
        sys.exit(1)
    
    # Then, run the conversion script
    if not run_script(convert_script):
        print("Failed to convert data to parquet. Stopping execution.")
        sys.exit(1)
    
    print("\nAll operations completed successfully!")

if __name__ == "__main__":
    main() 