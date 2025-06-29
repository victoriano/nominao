#!/usr/bin/env python3
"""
Script to filter Spanish names dataset for young, popular names
- Filter: Average age < 40 years
- Sort by: Frequency (descending)
- Take: Top 50 most frequent
"""

import pandas as pd
import sys
from pathlib import Path

def filter_young_popular_names(input_file, output_file, max_age=40, top_n=50):
    """
    Filter names dataset for young, popular names
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        max_age: Maximum average age to include (default: 40)
        top_n: Number of top names to keep (default: 50)
    """
    print(f"Loading dataset from: {input_file}")
    
    # Load the dataset
    df = pd.read_csv(input_file)
    
    print(f"Original dataset size: {len(df)} names")
    print(f"Age range in dataset: {df['Edad Media (*)'].min():.1f} - {df['Edad Media (*)'].max():.1f} years")
    print(f"Frequency range: {df['Frecuencia'].min()} - {df['Frecuencia'].max()}")
    
    # Filter by age
    young_names = df[df['Edad Media (*)'] < max_age].copy()
    print(f"\nAfter filtering age < {max_age}: {len(young_names)} names")
    
    if len(young_names) == 0:
        print("No names found with the specified age criteria!")
        return
    
    # Sort by frequency (descending) and take top N
    young_names_sorted = young_names.sort_values(by='Frecuencia', ascending=False)
    top_names = young_names_sorted.head(top_n)
    
    print(f"Selected top {len(top_names)} most frequent young names")
    print(f"\nTop 10 preview:")
    print("=" * 60)
    for i, (_, row) in enumerate(top_names.head(10).iterrows(), 1):
        print(f"{i:2d}. {row['Nombre']:<20} - Freq: {row['Frecuencia']:>6,} - Age: {row['Edad Media (*)']:>5.1f}")
    
    # Save filtered dataset
    top_names.to_csv(output_file, index=False)
    print(f"\nFiltered dataset saved to: {output_file}")
    
    # Show some statistics
    print(f"\nStatistics for selected names:")
    print(f"- Age range: {top_names['Edad Media (*)'].min():.1f} - {top_names['Edad Media (*)'].max():.1f} years")
    print(f"- Frequency range: {top_names['Frecuencia'].min():,} - {top_names['Frecuencia'].max():,}")
    print(f"- Gender distribution: {top_names['Gender'].value_counts().to_dict()}")
    print(f"- Compound names: {top_names['Is_Compound'].sum()} out of {len(top_names)}")
    
    return output_file

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Filter Spanish names for young, popular names',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python filter_young_popular_names.py                           # Default: age<40, top 50
  python filter_young_popular_names.py --max-age 35 --top-n 100  # Custom filters
  python filter_young_popular_names.py --top-n 25               # Just top 25
        """
    )
    
    parser.add_argument('--max-age', type=int, default=40,
                       help='Maximum average age to include (default: 40)')
    parser.add_argument('--top-n', type=int, default=50,
                       help='Number of top names to keep (default: 50)')
    parser.add_argument('--input-file', type=str,
                       help='Custom input file path (default: names_frecuencia_edad_media.csv)')
    parser.add_argument('--output-file', type=str,
                       help='Custom output file path (default: auto-generated)')
    
    args = parser.parse_args()
    
    # Set up file paths
    script_dir = Path(__file__).parent
    
    # Input file
    if args.input_file:
        input_file = Path(args.input_file)
        if not input_file.is_absolute():
            input_file = script_dir / args.input_file
    else:
        input_file = script_dir / 'output_data' / 'names_frecuencia_edad_media.csv'
    
    # Output file
    if args.output_file:
        output_file = Path(args.output_file)
        if not output_file.is_absolute():
            output_file = script_dir / args.output_file
    else:
        output_file = script_dir / 'output_data' / f'young_popular_names_age{args.max_age}_top{args.top_n}.csv'
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        # Filter the dataset
        result_file = filter_young_popular_names(
            input_file=str(input_file),
            output_file=str(output_file),
            max_age=args.max_age,
            top_n=args.top_n
        )
        
        print(f"\n🎉 Success! Filtered dataset ready for enrichment:")
        print(f"📁 File: {result_file}")
        print(f"\n💡 Next step - Enrich with origin classification:")
        print(f"cd {script_dir}")
        print(f"uv run enrich_names_with_origin.py --input-file {output_file.name} --all")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 