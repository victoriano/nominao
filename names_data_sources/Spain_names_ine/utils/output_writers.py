from __future__ import annotations

import csv
from dataclasses import fields
from pathlib import Path
from typing import Iterable


def write_dataclass_csv(records: Iterable[object], output_path: Path) -> None:
    records = list(records)
    if not records:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample = records[0]
    column_names = [field.name for field in fields(sample)]

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()
        for record in records:
            writer.writerow({name: getattr(record, name) for name in column_names})


