"""Utility helpers for INE data ingestion."""

from .ine_client import INEClient, batched  # noqa: F401
from .ine_fetchers import (  # noqa: F401
    DecadeRecord,
    RegionRecord,
    build_nombre_id,
    fetch_decade_records,
    fetch_region_records,
)
from .output_writers import write_dataclass_csv  # noqa: F401
from .svg_maps import get_municipality_map, get_province_map  # noqa: F401
from .population_lookup import (  # noqa: F401
    get_population_by_name,
    get_municipality_province_candidates,
)

__all__ = [
    "INEClient",
    "batched",
    "DecadeRecord",
    "RegionRecord",
    "build_nombre_id",
    "fetch_decade_records",
    "fetch_region_records",
    "write_dataclass_csv",
    "get_municipality_map",
    "get_province_map",
    "get_population_by_name",
    "get_municipality_province_candidates",
]


