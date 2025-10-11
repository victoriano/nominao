#!/usr/bin/env python3
"""Quick driver to exercise the Spain INE pipeline on a small sample.

Examples
--------
All phases with small sample:
    # Phases 1→5: download base data, process metrics, fetch details, enrich via LLM, and filter results.
    uv run python run_pipeline_sample.py --phase all --top 10 --sample-size 5

Run only phase 1 (download):
    # Phase 1: download the base INE dataset (frequencies by gender).
    uv run python run_pipeline_sample.py --phase 1

Run only phase 2 (processing):
    # Phase 2: compute metrics, rankings, and filter to simple male names.
    uv run python run_pipeline_sample.py --phase 2

Run only phase 3 (details for top 8 femenine names):
    # Phase 3: fetch per-decade, municipality, and province details for selected names.
    uv run python run_pipeline_sample.py --phase 3 --top 3 --gender Male

Run only phase 4 (enrichment for 5 names with Gemini):
    # Phase 4: enrich names with origin, description, and pronunciation via LLMs.
    uv run python run_pipeline_sample.py --phase 4 --sample-size 5 --provider gemini --model gemini-2.5-flash

Run only phase 5 (filter young popular names):
    # Phase 5: filter to young, popular names for the final sample.
    uv run python run_pipeline_sample.py --phase 5 --max-age 30 --top-filter 25
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INE_DIR = ROOT
OUTPUT_DIR = INE_DIR / "output_data"


def run(command: list[str]) -> None:
    print(f"\n→ Running: {' '.join(command)}")
    subprocess.run(command, check=True)


def phase1_download() -> None:
    run(["uv", "run", "python", str(INE_DIR / "1_download_INE_names.py")])


def phase2_process() -> None:
    run(["uv", "run", "python", str(INE_DIR / "2_process_INE_names.py")])


def phase3_details(top_n: int, gender: str) -> None:
    base_csv = OUTPUT_DIR / "2_data_process_INE_names" / "names_frecuencia_edad_media.csv"
    details_dir = OUTPUT_DIR / "3_data_download_INE_names_details"
    cmd = [
        "uv",
        "run",
        "python",
        str(INE_DIR / "3_download_INE_names_details.py"),
        "--base-csv",
        str(base_csv),
        "--output-dir",
        str(details_dir),
        "--top",
        str(top_n),
        "--gender",
        gender,
    ]
    run(cmd)


def phase4_enrich(sample_size: int, provider: str, model: str, tier: str, api_key: str | None) -> None:
    enrich_script = INE_DIR / "4_enrich_names.py"
    output_file = OUTPUT_DIR / "4_data_enrich_names" / f"names_ultra_fast_{provider}_{tier}_sample.csv"
    cmd = [
        "uv",
        "run",
        "python",
        str(enrich_script),
        "--num",
        str(sample_size),
        "--mode",
        "sequential",
        "--provider",
        provider,
        "--model",
        model,
        "--tier",
        tier,
        "--output-file",
        str(output_file),
    ]
    if api_key:
        if provider == "gemini":
            cmd.extend(["--gemini-key", api_key])
        elif provider == "openai":
            os.environ.setdefault("OPENAI_API_KEY", api_key)
    run(cmd)


def phase5_filter(max_age: int, top_n: int) -> None:
    filter_script = INE_DIR / "5_filter_young_popular_names.py"
    output_file = OUTPUT_DIR / "5_data_filter_young_popular_names" / f"young_popular_names_age{max_age}_top{top_n}_sample.csv"
    cmd = [
        "uv",
        "run",
        "python",
        str(filter_script),
        "--max-age",
        str(max_age),
        "--top-n",
        str(top_n),
        "--output-file",
        str(output_file),
    ]
    run(cmd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run pipeline steps for a small sample.")
    parser.add_argument("--phase", choices=["all", "1", "2", "3", "4", "5"], default="all")
    parser.add_argument("--top", type=int, default=10, help="Top N names for details (phase 3)")
    parser.add_argument("--gender", choices=["Male", "Female"], default="Male" )
    parser.add_argument("--sample-size", type=int, default=10, help="Names to enrich (phase 4)")
    parser.add_argument("--provider", choices=["gemini", "openai"], default="gemini")
    parser.add_argument("--tier", choices=["free", "level1"], default="level1")
    parser.add_argument("--api-key", dest="api_key", help="API key override for the chosen provider (optional)")
    parser.add_argument("--model", default="gemini-2.5-flash")
    parser.add_argument("--max-age", type=int, default=35)
    parser.add_argument("--top-filter", type=int, default=20)
    args = parser.parse_args()

    try:
        if args.phase in {"all", "1"}:
            phase1_download()
        if args.phase in {"all", "2"}:
            phase2_process()
        if args.phase in {"all", "3"}:
            phase3_details(args.top, args.gender)
        if args.phase in {"all", "4"}:
            phase4_enrich(args.sample_size, args.provider, args.model, args.tier, args.api_key)
        if args.phase in {"all", "5"}:
            phase5_filter(args.max_age, args.top_filter)
    except subprocess.CalledProcessError as exc:
        print(f"Command failed with exit code {exc.returncode}")
        sys.exit(exc.returncode)


if __name__ == "__main__":
    main()
