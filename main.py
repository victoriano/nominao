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
    
    # Define paths to scripts in the order they should be executed
    download_script = current_dir / "download_INE_names.py"
    process_script = current_dir / "process_INE_names.py"
    enrich_script = current_dir / "enrich_INE_names.py"
    
    print("Starting Spanish INE names data processing pipeline...")
    print("=" * 60)
    
    # First, run the download script
    if not run_script(download_script):
        print("Failed to download INE data. Stopping execution.")
        sys.exit(1)
    
    # Then, run the processing script to add analysis columns
    if not run_script(process_script):
        print("Failed to process INE data. Stopping execution.")
        sys.exit(1)
    
    # Finally, run the enrichment script to add metadata
    print(f"\nNote: The enrichment script ({enrich_script.name}) may take a while")
    print("as it makes API calls to INE for additional metadata.")
    user_input = input("Do you want to run the enrichment script? (y/n): ").lower().strip()
    
    if user_input in ['y', 'yes']:
        if not run_script(enrich_script):
            print("Failed to enrich INE data, but core processing is complete.")
            print("You can run the enrichment script separately later if needed.")
        else:
            print("Data enrichment completed successfully!")
    else:
        print("Skipping enrichment script. You can run it separately later if needed.")
    
    print("\n" + "=" * 60)
    print("Spanish INE names data processing pipeline completed!")
    print(f"Output file: {current_dir / 'names_frecuencia_edad_media.csv'}")

if __name__ == "__main__":
    main() 