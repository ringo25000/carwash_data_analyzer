# backend/app/core/load_db.py

from scripts import load_transactions as loader


def run_load() -> None:
    """
    Wrapper around scripts/load_transactions.py

    - Checks current Purchase row count.
    - If DB empty: loads full CLEAN_JSON_PATH.
    - Else: loads DELTA_JSON_PATH (if non-empty).
    """
    loader.main()
