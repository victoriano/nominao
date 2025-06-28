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
    parser = argparse.ArgumentParser(description='USA SSA names data processing pipeline')
    parser.add_argument('--skip-download', action='store_true', 
                       help='Skip the download step (use existing data)')
    parser.add_argument('--download-only', action='store_true', 
                       help='Only download data, skip conversion')
    parser.add_argument('--convert-only', action='store_true', 
                       help='Only convert existing data to parquet')
    args = parser.parse_args()
    
    # Get the absolute path to the current directory
    current_dir = Path(__file__).parent
    
    # Define paths to scripts
    download_script = current_dir / "download_SSA_names.py"
    convert_script = current_dir / "convert_to_parquet.py"
    
    print("Starting USA SSA names data processing pipeline...")
    print("=" * 60)
    
    # Handle different execution modes
    should_download = not args.skip_download and not args.convert_only
    should_convert = not args.download_only
    
    if args.convert_only:
        print("Running conversion only (--convert-only flag).")
    elif args.skip_download:
        print("Skipping download (--skip-download flag).")
    elif args.download_only:
        print("Running download only (--download-only flag).")
    else:
        print("Running complete pipeline (download + conversion).")
    
    # Run download script if needed
    if should_download:
        print(f"\nNote: Download may fail due to SSA website restrictions.")
        print("Use --skip-download if you already have the data.")
        if not run_script(download_script):
            print("Failed to download data.")
            if not args.download_only:
                print("Continuing with conversion using existing data...")
                should_convert = True
            else:
                sys.exit(1)
    
    # Run conversion script if needed
    if should_convert:
        # Check if downloaded data exists
        downloaded_data = current_dir / "downloaded_data"
        if not downloaded_data.exists():
            print(f"Error: No downloaded data found at {downloaded_data}")
            print("Please run with download first or check the downloaded_data directory.")
            sys.exit(1)
            
        if not run_script(convert_script):
            print("Failed to convert data to parquet.")
            sys.exit(1)
    
    print("\n" + "=" * 60)
    print("USA SSA names data processing pipeline completed!")
    
    # Show output files
    output_dir = current_dir / "output_data"
    if output_dir.exists():
        print(f"Output files:")
        for file in output_dir.glob("*.parquet"):
            print(f"  - {file}")

if __name__ == "__main__":
    main() 