import os
import subprocess
import sys
import argparse
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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Spanish INE names data processing pipeline')
    parser.add_argument('--skip-enrich', action='store_true', 
                       help='Skip the enrichment script (API calls)')
    parser.add_argument('--enrich', action='store_true', 
                       help='Automatically run the enrichment script without prompting')
    args = parser.parse_args()
    
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
    
    # Handle enrichment script based on arguments
    should_run_enrich = False
    
    if args.skip_enrich:
        print(f"\nSkipping enrichment script as requested (--skip-enrich flag).")
    elif args.enrich:
        print(f"\nRunning enrichment script automatically (--enrich flag).")
        should_run_enrich = True
    else:
        # Default behavior: run enrichment without prompting
        print(f"\nRunning enrichment script (use --skip-enrich to skip).")
        should_run_enrich = True
    
    if should_run_enrich:
        print(f"Note: The enrichment script ({enrich_script.name}) may take a while")
        print("as it makes API calls to INE for additional metadata.")
        if not run_script(enrich_script):
            print("Failed to enrich INE data, but core processing is complete.")
            print("You can run the enrichment script separately later if needed.")
        else:
            print("Data enrichment completed successfully!")
    
    print("\n" + "=" * 60)
    print("Spanish INE names data processing pipeline completed!")
    print(f"Output file: {current_dir / 'output_data' / 'names_frecuencia_edad_media.csv'}")

if __name__ == "__main__":
    main() 