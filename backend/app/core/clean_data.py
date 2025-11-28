# backend/app/core/clean_data.py

from pathlib import Path
from scripts import cryptopay_clean_data as cleaner


def run_clean() -> Path:
    """
    Wrapper around scripts/cryptopay_clean_data.py

    - Reads cryptopay_allData.json
    - If no cleaned file yet: clean_all() and delta = everything
    - Else: incremental_clean() and delta = only new records
    - Writes:
        CLEAN_JSON_PATH  (full history)
        DELTA_JSON_PATH  (only new records)
    - Returns DELTA_JSON_PATH
    """
    cleaner.main()  # uses your existing clean_all / incremental_clean logic
    return cleaner.DELTA_JSON_PATH
