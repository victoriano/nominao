from __future__ import annotations

import re
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Sequence, Tuple

import pandas as pd

from utils.ine_client import INEClient
from utils.ine_fetchers import fetch_decade_records, fetch_region_records
import argparse
import sys

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "output_data" / "3_data_download_INE_names_details"


def _load_base_dataframe(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["Nombre"] = df["Nombre"].astype(str).str.upper()
    return df


def _normalize_requested_names(
    names: Sequence[str | Tuple[str, str]]
) -> list[Tuple[str, str | None]]:
    normalized: list[Tuple[str, str | None]] = []
    for item in names:
        if isinstance(item, tuple):
            name, gender = item
            normalized.append((str(name).upper(), gender))
        else:
            normalized.append((str(item).upper(), None))
    return normalized


def _sanitize_identifier(nombre: str, gender: str) -> str:
    safe_name = re.sub(r"[^A-Za-z0-9]+", "_", nombre.upper()).strip("_")
    gender_suffix = gender.lower()[0] if gender else "x"
    return f"{safe_name}_{gender_suffix}"


def _iter_target_rows(
    df: pd.DataFrame,
    names: Sequence[str | Tuple[str, str]] | None,
    limit: int | None,
) -> Iterable[pd.Series]:
    if names is None:
        subset = df if limit is None else df.head(limit)
        for _, row in subset.iterrows():
            yield row
        return

    normalized = _normalize_requested_names(names)
    seen: set[Tuple[str, str]] = set()
    collected = 0

    for nombre, gender in normalized:
        matches = df[df["Nombre"] == nombre]
        if gender is not None:
            matches = matches[matches["Gender"].str.lower() == str(gender).lower()]

        if matches.empty:
            print(f"Skipping {nombre}/{gender or 'any'}: not found in base dataset")
            continue

        for _, row in matches.iterrows():
            key = (row["Nombre"], row["Gender"])
            if key in seen:
                continue
            seen.add(key)
            yield row
            collected += 1
            if limit is not None and collected >= limit:
                return


def download_name_details(
    base_csv_path: Path,
    *,
    names: Sequence[str | Tuple[str, str]] | None = None,
    limit: int | None = None,
    output_dir: Path | None = None,
    file_prefix: str = "details",
) -> None:
    """Download detailed INE data (decades/municipios/provincias) for names.

    Args:
        base_csv_path: Path to the base INE CSV with name frequencies.
        names: Optional sequence of names to download. Accepts either strings
            (process all genders present in the dataset) or ``(name, gender)``
            tuples to target a specific gender.
        limit: Optional maximum number of name/gender rows to process. When
            ``names`` is provided, the limit is applied after filtering.
        output_dir: Optional directory where the detail files will be written.
            Defaults to ``output_data/details`` next to this package.
        file_prefix: Prefix used for the generated CSV filenames.
    """

    df = _load_base_dataframe(base_csv_path)
    if df.empty:
        print(f"Base dataset {base_csv_path} is empty; nothing to download.")
        return

    details_dir = (output_dir or DEFAULT_OUTPUT_DIR) / "details"
    details_dir.mkdir(parents=True, exist_ok=True)

    all_decade_records: list[DecadeRecord] = []
    all_municipality_records: list[RegionRecord] = []
    all_province_records: list[RegionRecord] = []

    with INEClient.create() as client:
        for row in _iter_target_rows(df, names, limit):
            nombre = row["Nombre"]
            gender = row["Gender"]
            frecuencia = int(row["Frecuencia"])

            identifier = _sanitize_identifier(nombre, gender)

            decade_records = fetch_decade_records(
                client,
                nombre=nombre,
                gender=gender,
                total_frequency=frecuencia,
            )
            all_decade_records.extend(decade_records)

            municipality_records = fetch_region_records(
                client,
                nombre=nombre,
                gender=gender,
                total_frequency=frecuencia,
                vista="muni",
            )
            all_municipality_records.extend(municipality_records)

            province_records = fetch_region_records(
                client,
                nombre=nombre,
                gender=gender,
                total_frequency=frecuencia,
                vista="prov",
            )
            all_province_records.extend(province_records)

    _write_records(all_decade_records, details_dir / f"{file_prefix}_decades.csv")
    _write_records(all_municipality_records, details_dir / f"{file_prefix}_municipios.csv")
    _write_records(all_province_records, details_dir / f"{file_prefix}_provincias.csv")


def _write_records(records: Sequence[object], output_path: Path) -> None:
    if not records:
        return

    rows = [asdict(record) for record in records]
    df = pd.DataFrame(rows)
    if "nombre_id" in df.columns:
        df = df.drop(columns=["nombre_id"])
    df.to_csv(output_path, index=False)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download detailed INE data for given names.")
    parser.add_argument("--base-csv", type=Path, required=True, help="Path to processed base CSV (phase 2 output).")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory to write detail CSVs.")
    parser.add_argument("--names", nargs="*", help="Explicit list of names to download (ignores --top).")
    parser.add_argument("--gender", choices=["Male", "Female"], help="Restrict to a specific gender when selecting top names.")
    parser.add_argument("--top", type=int, help="Number of top names by frequency to download (default if names not provided).")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to process (after filters).")
    parser.add_argument("--file-prefix", default="details", help="Prefix for generated detail files.")
    return parser.parse_args(argv)


def _select_names(df: pd.DataFrame, *, names: Sequence[str] | None, gender: str | None, top: int | None) -> list[Tuple[str, str]]:
    if names:
        norm = [name.upper() for name in names]
        result: list[Tuple[str, str]] = []
        for nombre in norm:
            subset = df[df["Nombre"] == nombre]
            if gender:
                subset = subset[subset["Gender"].str.lower() == gender.lower()]
            if subset.empty:
                print(f"Skipping {nombre}: not found in dataset", file=sys.stderr)
                continue
            for _, row in subset.iterrows():
                result.append((row["Nombre"], row["Gender"]))
        return result

    subset = df
    if gender:
        subset = subset[subset["Gender"].str.lower() == gender.lower()]

    subset = subset.sort_values(by=["Gender", "Frecuencia"], ascending=[True, False])
    if top:
        subset = subset.head(top)

    return [(row["Nombre"], row["Gender"]) for _, row in subset.iterrows()]


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)

    if not args.base_csv.exists():
        print(f"Error: base CSV not found: {args.base_csv}", file=sys.stderr)
        sys.exit(1)

    df = _load_base_dataframe(args.base_csv)
    targets = _select_names(df, names=args.names, gender=args.gender, top=args.top)

    if not targets:
        print("No names selected for details download.", file=sys.stderr)
        sys.exit(0)

    download_name_details(
        args.base_csv,
        names=targets,
        limit=args.limit,
        output_dir=args.output_dir,
        file_prefix=args.file_prefix,
    )


if __name__ == "__main__":
    main()


