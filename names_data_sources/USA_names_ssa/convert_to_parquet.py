import polars as pl
import glob
from pathlib import Path

def extract_year_from_filename(filename):
    """Extract year from filename like 'yob1880.txt'"""
    return int(Path(filename).stem[3:])

def process_names_data():
    """Process national-level names data from yob files."""
    # Get the absolute path to the script's directory
    script_dir = Path(__file__).parent
    
    # Get all txt files in the national directory using absolute path
    input_files = glob.glob(str(script_dir / 'downloaded_data' / 'national' / 'yob*.txt'))
    
    print(f"Found {len(input_files)} input files")
    
    # Create a list to store all dataframes
    dfs = []
    
    # Process each file
    for file_path in sorted(input_files):
        year = extract_year_from_filename(file_path)
        print(f"Processing year {year}...")
        
        # Read the CSV file
        df = pl.read_csv(
            file_path,
            has_header=False,
            new_columns=['name', 'sex', 'count'],
            schema={
                'name': pl.Utf8,
                'sex': pl.Utf8,
                'count': pl.Int32
            }
        )
        
        # Add year column
        df = df.with_columns(pl.lit(year).alias('year'))
        
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No data files were found or processed")
    
    # Concatenate all dataframes
    combined_df = pl.concat(dfs)
    
    # Create output directory if it doesn't exist
    output_dir = script_dir / 'output_data'
    output_dir.mkdir(exist_ok=True)
    
    # Save as parquet file
    output_path = output_dir / 'names_database.parquet'
    print(f"Saving data to {output_path}...")
    combined_df.write_parquet(
        output_path,
        compression='zstd'  # Using zstd compression for good compression ratio and speed
    )

def process_state_names_data():
    """Process state-level names data."""
    # Get the absolute path to the script's directory
    script_dir = Path(__file__).parent
    
    # Get all txt files in the state directory
    input_files = glob.glob(str(script_dir / 'downloaded_data' / 'state' / '*.TXT'))
    
    print(f"Found {len(input_files)} state files")
    
    # Create a list to store all dataframes
    dfs = []
    
    # Process each state file
    for file_path in sorted(input_files):
        state = Path(file_path).stem
        print(f"Processing state {state}...")
        
        # Read the CSV file
        df = pl.read_csv(
            file_path,
            has_header=False,
            new_columns=['state', 'sex', 'year', 'name', 'count'],
            schema={
                'state': pl.Utf8,
                'sex': pl.Utf8,
                'year': pl.Int32,
                'name': pl.Utf8,
                'count': pl.Int32
            }
        )
        
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No state data files were found or processed")
    
    # Concatenate all dataframes
    combined_df = pl.concat(dfs)
    
    # Create output directory if it doesn't exist
    output_dir = script_dir / 'output_data'
    output_dir.mkdir(exist_ok=True)
    
    # Save as parquet file
    output_path = output_dir / 'state_names_database.parquet'
    print(f"Saving data to {output_path}...")
    combined_df.write_parquet(
        output_path,
        compression='zstd'
    )

if __name__ == '__main__':
    print("Processing national-level names data...")
    process_names_data()
    print("Processing state-level names data...")
    process_state_names_data()
    print("Done! Files have been saved to the output_data directory.") 