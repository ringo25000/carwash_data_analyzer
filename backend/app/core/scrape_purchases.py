# backend/app/core/scrape_purchases.py

from pathlib import Path
import json

from scripts import cryptopay_scrape_data as scraper


def run_scrape() -> Path:
    """
    Wrapper around scripts/cryptopay_scrape_data.py

    - If no OUTPUT_FILE exists: full initial scrape (scrape_all_data)
    - Else: incremental_update
    - Writes JSON to OUTPUT_FILE
    - Returns the Path to OUTPUT_FILE
    """
    output_path = Path(scraper.OUTPUT_FILE)

    # Decide full vs incremental based on whether the file exists,
    # exactly like your current __main__ block.
    if not output_path.exists():
        print("No existing data file found – doing full initial scrape...")
        data = scraper.scrape_all_data()
    else:
        print("Existing data file found – doing incremental update...")
        data = scraper.incremental_update()

    # Make sure parent dir exists (should already, but safe):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} purchases to {output_path}")
    return output_path
