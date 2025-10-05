import os
import subprocess
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv


def run_script(script_path, args=None):
    """Run a Python script with optional arguments."""
    print(f"\nExecuting {script_path}...")
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
    try:
        subprocess.run(cmd, check=True)
        print(f"Successfully completed {script_path}")
        return True
    except subprocess.CalledProcessError as exc:
        print(f"Error executing {script_path}: {exc}")
        return False


def build_ultrafast_args(args, base_file: Path):
    uf_args = [
        "--input-file", str(base_file),
        "--provider", args.origin_provider,
        "--model", args.origin_model,
        "--tier", args.origin_tier,
        "--mode", "random" if args.origin_mode == "random" else "sequential",
    ]

    if args.origin_mode == "all":
        uf_args.append("--all")
    else:
        uf_args.extend(["--num", str(args.origin_count)])

    if args.origin_seed is not None:
        uf_args.extend(["--seed", str(args.origin_seed)])

    if args.origin_max_concurrent:
        uf_args.extend(["--max-concurrent", str(args.origin_max_concurrent)])

    if args.origin_output:
        uf_args.extend(["--output-file", args.origin_output])

    return uf_args


def main():
    parser = argparse.ArgumentParser(description="Spanish INE names pipeline with ultra-fast enrichment")
    parser.add_argument("--origin-mode", choices=["sequential", "random", "all"], default="sequential",
                        help="Mode for origin classification")
    parser.add_argument("--origin-count", type=int, default=100,
                        help="Number of names to classify (ignored if mode is 'all')")
    parser.add_argument("--origin-output", type=str, help="Custom output file for origin classification")
    parser.add_argument("--origin-tier", choices=["free", "level1"], default="level1",
                        help="API tier preset for provider")
    parser.add_argument("--origin-provider", choices=["gemini", "openai"], default="gemini",
                        help="LLM provider to use")
    parser.add_argument("--origin-model", type=str, default="gemini-2.5-flash",
                        help="Model for ultra-fast enrichment (e.g., gemini-2.5-flash or gpt-4o-mini)")
    parser.add_argument("--origin-max-concurrent", type=int,
                        help="Override max concurrent requests for ultra-fast enrichment")
    parser.add_argument("--origin-seed", type=int,
                        help="Random seed when using random mode")
    parser.add_argument("--gemini-key", type=str,
                        help="Optional Gemini API key override")

    args = parser.parse_args()

    load_dotenv()
    if args.gemini_key:
        os.environ["GEMINI_API_KEY"] = args.gemini_key

    current_dir = Path(__file__).parent
    download_script = current_dir / "download_INE_names.py"
    process_script = current_dir / "process_INE_names.py"
    enrich_script = current_dir / "enrich_names.py"

    print("Starting Spanish INE names data processing pipeline...")
    print("=" * 60)

    if not run_script(download_script):
        sys.exit(1)

    if not run_script(process_script):
        sys.exit(1)

    base_file = current_dir / "output_data" / "names_frecuencia_edad_media.csv"
    if not base_file.exists():
        print(f"Error: Base file not found: {base_file}")
        sys.exit(1)

    ul_args = build_ultrafast_args(args, base_file)

    if run_script(enrich_script, ul_args):
        print("\nUltra-fast origin classification completed successfully!")
    else:
        print("\nFailed to complete ultra-fast origin classification.")

    print("\n" + "=" * 60)
    print("Spanish INE names data processing pipeline completed!")
    print("\nOutput files:")
    print(f"- Base data: {base_file}")
    if args.origin_output:
        print(f"- Origin data: {current_dir / args.origin_output}")
    else:
        default_name = f"names_ultra_fast_{args.origin_provider}_{args.origin_tier}.csv"
        print(f"- Origin data: {current_dir / 'output_data' / default_name}")


if __name__ == "__main__":
    main() 