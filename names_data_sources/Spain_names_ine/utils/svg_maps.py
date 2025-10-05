"""Utility helpers to parse SVG map files provided by INE."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict
import xml.etree.ElementTree as ET


RAW_DATA_DIR = Path(__file__).resolve().parent / "raw_data"
SVG_NAMESPACE = "http://www.w3.org/2000/svg"


def _parse_svg_ids(svg_path: Path) -> Dict[int, str]:
    if not svg_path.exists():
        raise FileNotFoundError(f"SVG file not found: {svg_path}")

    tree = ET.parse(svg_path)
    root = tree.getroot()

    id_map: Dict[int, str] = {}

    for element in root.findall(f".//{{{SVG_NAMESPACE}}}path"):
        class_attr = element.attrib.get("class", "")
        title = element.attrib.get("title")

        if not class_attr or not title:
            continue

        identifier: str | None = None
        for css_class in class_attr.split():
            if css_class.startswith("id_"):
                identifier = css_class.split("_", 1)[1]
                break

        if identifier is None:
            continue

        try:
            identifier_int = int(identifier)
        except ValueError:
            continue

        label = title.strip()
        id_map[identifier_int] = label

    return id_map


@lru_cache(maxsize=1)
def get_municipality_map(raw_data_dir: Path | None = None) -> Dict[int, str]:
    base_dir = raw_data_dir or RAW_DATA_DIR
    return _parse_svg_ids(base_dir / "municipios.svg")


@lru_cache(maxsize=1)
def get_province_map(raw_data_dir: Path | None = None) -> Dict[int, str]:
    base_dir = raw_data_dir or RAW_DATA_DIR
    return _parse_svg_ids(base_dir / "provincias.svg")


