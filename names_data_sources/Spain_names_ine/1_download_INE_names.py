from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

try:
    import xlrd
except ImportError as exc:
    raise ImportError("xlrd is required to run this script. Install it with 'pip install xlrd'") from exc

BASE_URL = "https://www.ine.es/daco/daco42/nombyapel/nombres_por_edad_media.xls"


def download_base_names_dataset(
    output_dir: Path | None = None,
    *,
    url: str = BASE_URL,
    session: requests.Session | None = None,
    max_retries: int = 3,
) -> Path:
    """Download the INE base names dataset and persist it as CSV.

    Args:
        output_dir: Directory where the CSV output will be written. If ``None``,
            a folder named ``output_data`` next to this script is used.
        url: Optional override for the INE Excel URL (useful for testing).
        session: Optional pre-configured ``requests.Session`` to reuse connections
            or inject custom behaviour in tests.

    Returns:
        Path to the generated CSV file.

    Raises:
        requests.HTTPError: If the download request fails.
    """

    output_path = Path(output_dir) if output_dir else Path(__file__).parent / "output_data"
    output_path.mkdir(parents=True, exist_ok=True)

    http = session or requests.Session()

    last_error: requests.HTTPError | None = None
    for attempt in range(1, max_retries + 1):
        response = http.get(url, timeout=30)
        try:
            response.raise_for_status()
            break
        except requests.HTTPError as exc:  # pragma: no cover - simple log
            last_error = exc
            if attempt == max_retries:
                raise
            print(f"Attempt {attempt} failed with {exc}. Retrying...")
    else:  # pragma: no cover - defensive branch
        if last_error:
            raise last_error

    excel_data = BytesIO(response.content)

    excel_data = BytesIO(response.content)

    male_names_df = pd.read_excel(excel_data, sheet_name="Hombres", skiprows=6)
    female_names_df = pd.read_excel(excel_data, sheet_name="Mujeres", skiprows=6)

    male_names_df["Gender"] = "Male"
    female_names_df["Gender"] = "Female"

    combined_df = pd.concat([male_names_df, female_names_df])
    final_df = combined_df[["Nombre", "Frecuencia", "Edad Media (*)", "Gender"]]

    output_file = output_path / "names_frecuencia_edad_media.csv"
    final_df.to_csv(output_file, index=False)

    return output_file


def download_names_with_details(
    names: list[str] | list[tuple[str, str]] | None = None,
    *,
    limit: int | None = None,
    output_dir: Path | None = None,
    file_prefix: str = "names",
) -> None:
    """Download the base dataset, then fetch detailed stats for selected names."""

    base_csv = download_base_names_dataset()

    # Deferred import to avoid circular references when utils imports this module.
    from .utils import download_name_details

    download_name_details(
        base_csv,
        names=names,
        limit=limit,
        output_dir=output_dir,
        file_prefix=file_prefix,
    )


def main() -> None:
    try:
        output_file = download_base_names_dataset()
        print(f"INE names dataset saved to {output_file}")
    except requests.HTTPError as exc:
        print(f"Failed to download the file: {exc}")
        raise


if __name__ == "__main__":
    main()