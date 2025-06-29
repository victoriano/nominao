import os
import subprocess
import sys
import argparse
from pathlib import Path

def run_script(script_path, args=None):
    """Run a Python script and check for successful execution."""
    print(f"\nExecuting {script_path}...")
    try:
        # Build command with arguments if provided
        cmd = [sys.executable, script_path]
        if args:
            cmd.extend(args)
        
        # Using sys.executable ensures we use the same Python interpreter (with uv)
        result = subprocess.run(cmd, check=True)
        print(f"Successfully completed {script_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        return False

def build_classification_args(args):
    """Build command line arguments for the origin classification script."""
    cmd_args = []
    
    # Determine the mode and count
    if args.origin_mode == 'all':
        cmd_args.append('--all')
    elif args.origin_mode == 'random':
        cmd_args.extend(['--random', str(args.origin_count)])
    else:  # sequential (default)
        cmd_args.extend(['--num', str(args.origin_count)])
    
    # Add custom output file if specified
    if args.origin_output:
        cmd_args.extend(['--output-file', args.origin_output])
    
    return cmd_args

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Spanish INE names data processing pipeline')
    parser.add_argument('--skip-enrich', action='store_true', 
                       help='Skip the INE enrichment script (API calls)')
    parser.add_argument('--enrich', action='store_true', 
                       help='Automatically run the INE enrichment script without prompting')
    
    # Origin classification options
    parser.add_argument('--classify-origins', action='store_true',
                       help='Run AI-powered origin classification after processing')
    parser.add_argument('--origin-mode', choices=['sequential', 'random', 'all'], default='sequential',
                       help='Mode for origin classification: sequential (default), random, or all')
    parser.add_argument('--origin-count', type=int, default=100,
                       help='Number of names to classify (default: 100, ignored if mode is "all")')
    parser.add_argument('--origin-output', type=str,
                       help='Custom output file for origin classification')
    parser.add_argument('--gemini-key', type=str,
                       help='Gemini API key (can also be set via GEMINI_API_KEY env var)')
    
    args = parser.parse_args()
    
    # Set Gemini API key if provided via command line
    if args.gemini_key:
        os.environ['GEMINI_API_KEY'] = args.gemini_key
    
    # Get the absolute path to the current directory
    current_dir = Path(__file__).parent
    
    # Define paths to scripts in the order they should be executed
    download_script = current_dir / "download_INE_names.py"
    process_script = current_dir / "process_INE_names.py"
    enrich_script = current_dir / "enrich_INE_names.py"
    classify_script = current_dir / "enrich_names_with_origin.py"
    
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
    
    # Run origin classification if requested
    if args.classify_origins:
        print("\n" + "=" * 60)
        print("Starting AI-powered origin classification...")
        
        # Check if GEMINI_API_KEY is set
        if not os.environ.get('GEMINI_API_KEY'):
            print("\nError: GEMINI_API_KEY environment variable is not set.")
            print("Please set it using --gemini-key or export GEMINI_API_KEY='your_key'")
            print("Skipping origin classification.")
        else:
            # Build arguments for classification script
            classification_args = build_classification_args(args)
            
            print(f"\nClassification mode: {args.origin_mode}")
            if args.origin_mode != 'all':
                print(f"Number of names to classify: {args.origin_count}")
            if args.origin_output:
                print(f"Output file: {args.origin_output}")
            
            # Run the classification script
            if run_script(classify_script, classification_args):
                print("\nOrigin classification completed successfully!")
                
                # Determine output file location
                if args.origin_output:
                    output_file = current_dir / args.origin_output
                else:
                    if args.origin_mode == 'random':
                        output_file = current_dir / 'output_data' / 'names_with_origin_random_sample.csv'
                    else:
                        output_file = current_dir / 'output_data' / 'names_with_origin.csv'
                
                print(f"Classification results saved to: {output_file}")
            else:
                print("\nFailed to complete origin classification.")
                print("You can run the classification script separately later if needed.")
    
    print("\n" + "=" * 60)
    print("Spanish INE names data processing pipeline completed!")
    
    # Show all output files
    print("\nOutput files:")
    print(f"- Base data: {current_dir / 'output_data' / 'names_frecuencia_edad_media.csv'}")
    
    if args.classify_origins and os.environ.get('GEMINI_API_KEY'):
        if args.origin_output:
            print(f"- Origin data: {current_dir / args.origin_output}")
        else:
            if args.origin_mode == 'random':
                print(f"- Origin data: {current_dir / 'output_data' / 'names_with_origin_random_sample.csv'}")
            else:
                print(f"- Origin data: {current_dir / 'output_data' / 'names_with_origin.csv'}")

if __name__ == "__main__":
    main() 