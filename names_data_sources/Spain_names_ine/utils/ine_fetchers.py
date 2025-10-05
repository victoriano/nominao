from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .ine_client import INEClient
from .population_lookup import (
    get_population_by_name,
    get_municipality_province_candidates,
)
from .svg_maps import get_municipality_map, get_province_map


GENDER_TO_SEX_PARAM: Dict[str, str] = {
    "Male": "1",
    "Female": "6",
}


@dataclass(slots=True)
class DecadeRecord:
    nombre_id: str
    nombre: str
    gender: str
    decade: str
    persons: int


@dataclass(slots=True)
class RegionRecord:
    nombre_id: str
    nombre: str
    gender: str
    region_id: int
    region_name: str
    percentage: float
    unidad: str
    persons: Optional[int]
    region_type: str
    parent_region_name: Optional[str] = None


def _percentage_to_absolute(population: int, value: float, unidad: Optional[str]) -> int:
    unidad = unidad or "‰"
    if unidad == "‰":
        factor = 1000.0
    elif unidad == "%":
        factor = 100.0
    else:
        return int(round(value))
    return int(round(population * (value / factor)))


def build_nombre_id(nombre: str, gender: str) -> str:
    slug = nombre.strip().upper().replace(" ", "_")
    gender_key = gender.lower()[0]
    return f"{slug}-{gender_key}"


def fetch_decade_records(
    client: INEClient,
    *,
    nombre: str,
    gender: str,
    total_frequency: int,
) -> List[DecadeRecord]:
    sexo = GENDER_TO_SEX_PARAM.get(gender)
    if sexo is None:
        raise ValueError(f"Unsupported gender value: {gender}")

    payload = client.grafico_widget(nombre=nombre, sexo=sexo)
    values = payload.get("values", [])
    if not values:
        return []

    value_series = values[0]
    ticks = payload.get("ticks", [])
    nombre_id = build_nombre_id(nombre, gender)

    records: List[DecadeRecord] = []

    for tick, amount in zip(ticks, value_series):
        if amount is None:
            continue
        persons = round(float(amount))
        records.append(
            DecadeRecord(
                nombre_id=nombre_id,
                nombre=nombre,
                gender=gender,
                decade=tick,
                persons=int(persons),
            )
        )

    return records


def _percentage_to_absolute(total: int, value: float, unidad: str) -> int:
    if unidad == "‰":
        factor = 1000.0
    elif unidad == "%":
        factor = 100.0
    else:
        return round(value)

    return int(round(total * (value / factor)))


def fetch_region_records(
    client: INEClient,
    *,
    nombre: str,
    gender: str,
    total_frequency: int,
    vista: str,
) -> List[RegionRecord]:
    sexo = GENDER_TO_SEX_PARAM.get(gender)
    if sexo is None:
        raise ValueError(f"Unsupported gender value: {gender}")

    payload = client.mapa_widget(nombre=nombre, sexo=sexo, vista=vista)
    regiones = payload.get("regiones", [])
    unidad = payload.get("unidad", "‰")
    nombre_id = build_nombre_id(nombre, gender)

    if vista == "muni":
        id_map = get_municipality_map()
        region_type = "municipio"
    elif vista == "prov":
        id_map = get_province_map()
        region_type = "provincia"
    else:
        raise ValueError(f"Unsupported vista value: {vista}")

    records: List[RegionRecord] = []

    for region in regiones:
        region_id = region.get("id")
        value = region.get("val")
        if region_id is None or value is None:
            continue

        try:
            region_id_int = int(region_id)
        except (TypeError, ValueError):
            continue

        try:
            value_float = float(str(value).replace(",", "."))
        except ValueError:
            continue

        region_name = id_map.get(region_id_int, "Desconocido")
        records.append(
            RegionRecord(
                nombre_id=nombre_id,
                nombre=nombre,
                gender=gender,
                region_id=region_id_int,
                region_name=region_name,
                percentage=value_float,
                unidad=unidad,
                persons=None,
                region_type=region_type,
            )
        )

    for record in records:
        population: Optional[int] = None
        parent_region: Optional[str] = None

        if record.region_type == "provincia":
            parent_region = record.region_name
            population = get_population_by_name(
                province_name=record.region_name,
                municipality_name=None,
                gender=record.gender,
            )
        elif record.region_type == "municipio":
            candidates = get_municipality_province_candidates(record.region_name)
            province_name: Optional[str] = None
            if len(candidates) == 1:
                province_name = candidates[0]
            elif candidates:
                province_name = None
            if province_name:
                population = get_population_by_name(
                    province_name=province_name,
                    municipality_name=record.region_name,
                    gender=record.gender,
                )
                parent_region = province_name
            else:
                parent_region = ", ".join(candidates) if candidates else None

        if population is not None:
            record.persons = _percentage_to_absolute(population, record.percentage, record.unidad)
        else:
            record.persons = None
        record.parent_region_name = parent_region

    records.sort(key=lambda record: (record.region_type, record.region_id))

    return records


