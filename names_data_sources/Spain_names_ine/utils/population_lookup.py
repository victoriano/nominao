from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

import polars as pl
import unicodedata
import re


DATA_PATH = Path(__file__).resolve().parent / "raw_data" / "poblacion_municipios_provincias.csv"


PROVINCE_REPLACEMENTS = {
    "RIOJA, LA": "LA RIOJA",
    "CORUÑA, A": "A CORUÑA",
    "RIOJA": "LA RIOJA",
}


def _parse_code(name: str) -> tuple[int, str]:
    code_str, label = name.split(" ", 1)
    return int(code_str), label


def _clean_label(label: str) -> str:
    label = label.strip().rstrip(",")
    if ", " in label:
        parts = label.split(", ")
        if len(parts) == 2:
            label = f"{parts[1]} {parts[0]}"
    upper = label.upper()
    if upper in PROVINCE_REPLACEMENTS:
        return PROVINCE_REPLACEMENTS[upper].title()
    return label


def _normalize(label: Optional[str]) -> Optional[str]:
    if label is None:
        return None
    label = label.strip()
    if not label:
        return None
    normalized = unicodedata.normalize("NFKD", label)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.replace("’", "'")
    normalized = normalized.upper()
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


@lru_cache(maxsize=1)
def _load_population_table() -> pl.DataFrame:
    df = pl.read_csv(
        DATA_PATH,
        separator=";",
        new_columns=[
            "ambito",
            "province_raw",
            "municipality_raw",
            "gender",
            "nationality",
            "period",
            "population",
        ],
    )

    df = (
        df.with_columns(pl.col("population").cast(pl.Utf8).alias("population_str"))
        .with_columns(
            pl.when(pl.col("population_str").is_not_null() & (pl.col("population_str") != ""))
            .then(pl.col("population_str").str.replace_all(r"[. ]", ""))
            .otherwise(None)
            .alias("population_clean")
        )
        .with_columns(
            pl.when(pl.col("population_clean").is_not_null() & (pl.col("population_clean") != ""))
            .then(pl.col("population_clean").cast(pl.Float64))
            .otherwise(None)
            .alias("population_float")
        )
        .with_columns(
            pl.col("population_float").round(0).cast(pl.Int64).alias("population")
        )
        .with_columns(pl.col("period").cast(pl.Utf8), pl.col("nationality").cast(pl.Utf8))
        .filter(pl.col("nationality") == "Total")
        .filter(pl.col("period") == "2024")
    )

    df = df.with_columns(
        pl.when(pl.col("province_raw").cast(pl.Utf8).str.len_chars() > 0)
        .then(pl.col("province_raw").cast(pl.Utf8).str.split_exact(" ", 1).struct.field("field_0").cast(pl.Int64))
        .otherwise(None)
        .alias("province_id"),
        pl.when(pl.col("province_raw").cast(pl.Utf8).str.len_chars() > 0)
        .then(
            pl.col("province_raw")
            .cast(pl.Utf8)
            .str.split_exact(" ", 1)
            .struct.field("field_1")
            .map_elements(_clean_label, return_dtype=pl.Utf8)
        )
        .otherwise(None)
        .alias("province_name"),
        pl.when(pl.col("municipality_raw").cast(pl.Utf8).str.len_chars() > 0)
        .then(pl.col("municipality_raw").cast(pl.Utf8).str.split_exact(" ", 1).struct.field("field_0").cast(pl.Int64))
        .otherwise(None)
        .alias("municipality_id"),
        pl.when(pl.col("municipality_raw").cast(pl.Utf8).str.len_chars() > 0)
        .then(pl.col("municipality_raw").cast(pl.Utf8).str.split_exact(" ", 1).struct.field("field_1"))
        .otherwise(None)
        .alias("municipality_name"),
    )

    return df


def _gender_aliases(gender: str) -> Iterable[str]:
    gender = gender.lower()
    if gender.startswith("m"):
        yield from ("Hombres", "Total")
    elif gender.startswith("f"):
        yield from ("Mujeres", "Total")
    else:
        yield "Total"


@lru_cache(maxsize=1)
def _build_population_maps() -> tuple[
    Dict[tuple[str, str], int],
    Dict[tuple[str, str, str], int],
    Dict[str, set[str]],
    Dict[str, str],
]:
    df = _load_population_table()

    province_map: Dict[tuple[str, str], int] = {}
    municipality_map: Dict[tuple[str, str, str], int] = {}
    municipality_candidates: Dict[str, set[str]] = {}
    province_display: Dict[str, str] = {}

    for row in df.iter_rows(named=True):
        population = row["population"]
        if population is None:
            continue

        gender = row["gender"]
        province_name = row["province_name"]
        province_norm = _normalize(province_name)

        municipality_name = row["municipality_name"]
        if municipality_name is None:
            if province_norm:
                province_map[(province_norm, gender)] = population
                if gender == "Total":
                    province_display.setdefault(province_norm, province_name)
            continue

        municipality_norm = _normalize(municipality_name)
        if not province_norm or not municipality_norm:
            continue

        municipality_map[(province_norm, municipality_norm, gender)] = population
        municipality_candidates.setdefault(municipality_norm, set()).add(province_norm)

    return province_map, municipality_map, municipality_candidates, province_display


def get_municipality_province_candidates(municipality_name: str) -> list[str]:
    _, _, municipality_candidates, province_display = _build_population_maps()
    municipality_norm = _normalize(municipality_name)
    if municipality_norm is None:
        return []
    province_norms = municipality_candidates.get(municipality_norm, set())
    return [province_display.get(norm, None) or norm for norm in province_norms]


def get_population_by_name(
    *,
    gender: str,
    province_name: Optional[str] = None,
    municipality_name: Optional[str] = None,
) -> Optional[int]:
    province_map, municipality_map, municipality_candidates, _ = _build_population_maps()

    gender_options: Sequence[str] = list(_gender_aliases(gender))

    province_norm = _normalize(province_name) if province_name else None

    if municipality_name:
        municipality_norm = _normalize(municipality_name)
        if municipality_norm is None:
            return None

        candidate_provinces = municipality_candidates.get(municipality_norm, set())
        if province_norm:
            candidate_provinces = {province_norm} if province_norm in candidate_provinces else set()
        elif len(candidate_provinces) == 1:
            province_norm = next(iter(candidate_provinces))
        else:
            return None

        if not candidate_provinces or province_norm not in candidate_provinces:
            return None

        for gender_key in gender_options:
            population = municipality_map.get((province_norm, municipality_norm, gender_key))
            if population is not None:
                return population
        return None

    if province_norm is None:
        return None

    for gender_key in gender_options:
        population = province_map.get((province_norm, gender_key))
        if population is not None:
            return population

    return None

