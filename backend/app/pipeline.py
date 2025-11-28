# backend/app/pipeline.py

from pathlib import Path

from app.core.scrape_purchases import run_scrape
from app.core.clean_data import run_clean
from app.core.load_db import run_load


def run_full_pipeline() -> None:
    """
    1) Scrape (Playwright with saved state) → cryptopay_allData.json
    2) Clean raw JSON → cryptopay_cleaned.json + cryptopay_cleaned_delta.json
    3) Load cleaned data into SQLite (full or delta)
    """
    print("Step 1/3: Scraping purchases...")
    raw_path: Path = run_scrape()
    print(f"Raw data saved to: {raw_path}")

    print("Step 2/3: Cleaning data...")
    delta_path: Path = run_clean()
    print(f"Cleaned delta saved to: {delta_path}")

    print("Step 3/3: Loading data into SQLite...")
    run_load()
    print("✅ Pipeline completed successfully!")


if __name__ == "__main__":
    run_full_pipeline()
