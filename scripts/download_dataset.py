"""
Download the Olist Brazilian E-Commerce dataset from Kaggle.

Requirements:
  - Kaggle account at https://www.kaggle.com
  - API token at ~/.kaggle/kaggle.json  (Windows: %USERPROFILE%\\.kaggle\\kaggle.json)

Usage:
  python scripts/download_dataset.py
"""

import os
import sys
from pathlib import Path


def check_kaggle_credentials():
    """Check that Kaggle credentials exist (JSON token, access_token file, or env var)."""
    # New-style: environment variable
    if os.environ.get("KAGGLE_API_TOKEN"):
        print("Using KAGGLE_API_TOKEN environment variable.")
        return

    home = Path.home()
    # New-style: access_token file
    access_token = home / ".kaggle" / "access_token"
    if access_token.exists():
        print(f"Using Kaggle access token: {access_token}")
        return

    # Old-style: kaggle.json
    kaggle_json = home / ".kaggle" / "kaggle.json"
    if kaggle_json.exists():
        print(f"Using Kaggle JSON credentials: {kaggle_json}")
        return

    print("ERROR: Kaggle API credentials not found.")
    print(f"Expected one of:")
    print(f"  - Environment variable KAGGLE_API_TOKEN")
    print(f"  - {access_token}")
    print(f"  - {kaggle_json}")
    print("\nTo fix:")
    print("  1. Go to https://www.kaggle.com/settings")
    print("  2. Scroll to 'API' and create a token")
    print(f"  3. Save the token to: {access_token}")
    sys.exit(1)


def download_dataset():
    check_kaggle_credentials()

    try:
        import kaggle
    except ImportError:
        print("Installing kaggle package...")
        os.system(f"{sys.executable} -m pip install kaggle")
        import kaggle

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = "olistbr/brazilian-ecommerce"
    print(f"Downloading dataset: {dataset}")
    print(f"Output directory:    {output_dir.absolute()}\n")

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        dataset,
        path=str(output_dir),
        unzip=True,
        quiet=False,
    )

    print("\nOK: Download complete. Files:")
    csv_files = sorted(output_dir.glob("*.csv"))
    if not csv_files:
        print("  WARNING: No CSV files found; check the output directory.")
    for f in csv_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name:<55} {size_mb:6.1f} MB")


if __name__ == "__main__":
    download_dataset()
