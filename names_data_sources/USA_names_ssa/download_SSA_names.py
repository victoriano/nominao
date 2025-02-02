import os
import urllib.request
import zipfile
from pathlib import Path


def download_and_extract(url, extract_folder):
    # Create the extract folder if it doesn't exist
    Path(extract_folder).mkdir(exist_ok=True)
    
    # Derive the local filename based on the URL
    zip_filename = os.path.join(extract_folder, os.path.basename(url))
    print(f"Downloading {url} to {zip_filename}...")
    urllib.request.urlretrieve(url, zip_filename)
    print(f"Extracting {zip_filename} to {extract_folder}...")
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    os.remove(zip_filename)
    print(f"Done extracting {url}")


def main():
    # Get the current script's directory and set up the downloaded_data directory
    script_dir = Path(__file__).parent
    download_dir = script_dir / "downloaded_data"
    download_dir.mkdir(exist_ok=True)
    
    # Create separate directories for national and state data
    national_dir = download_dir / "national"
    state_dir = download_dir / "state"
    
    # URLs to download
    url_names = "https://www.ssa.gov/oact/babynames/names.zip"
    url_namesbystate = "https://www.ssa.gov/oact/babynames/state/namesbystate.zip"
    
    print("Starting download and extraction process.")
    
    # Download and extract national data
    print("\nProcessing national data...")
    download_and_extract(url_names, str(national_dir))
    
    # Download and extract state data
    print("\nProcessing state data...")
    download_and_extract(url_namesbystate, str(state_dir))
    
    # Move README files to the root download directory
    for file in national_dir.glob("*ReadMe.pdf"):
        file.rename(download_dir / file.name)
    for file in state_dir.glob("*ReadMe.pdf"):
        file.rename(download_dir / file.name)
    
    print("\nAll files downloaded and extracted successfully.")


if __name__ == '__main__':
    main() 