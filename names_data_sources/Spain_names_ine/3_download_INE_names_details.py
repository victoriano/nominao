from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Sequence, Tuple

import pandas as pd

from .ine_client import INEClient
from .ine_fetchers import fetch_decade_records, fetch_region_records
from .output_writers import write_dataclass_csv

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
            write_dataclass_csv(
                decade_records,
                details_dir / f"{file_prefix}_decades_{identifier}.csv",
            )

            municipality_records = fetch_region_records(
                client,
                nombre=nombre,
                gender=gender,
                total_frequency=frecuencia,
                vista="muni",
            )
            write_dataclass_csv(
                municipality_records,
                details_dir / f"{file_prefix}_municipios_{identifier}.csv",
            )

            province_records = fetch_region_records(
                client,
                nombre=nombre,
                gender=gender,
                total_frequency=frecuencia,
                vista="prov",
            )
            write_dataclass_csv(
                province_records,
                details_dir / f"{file_prefix}_provincias_{identifier}.csv",
            )


