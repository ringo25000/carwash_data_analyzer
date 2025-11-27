# backend/scripts/load_transactions.py

import json
from pathlib import Path

# From the __init__ package we made for app/ db.py is a module named db
from app.db import get_connection, init_db

# Paths
ROOT_DIR = Path(__file__).resolve().parents[1]   # .../backend
DATA_DIR = ROOT_DIR / "data"
CLEAN_JSON_PATH = DATA_DIR / "cryptopay_cleaned.json"
DELTA_JSON_PATH = DATA_DIR / "cryptopay_cleaned_delta.json"  # <-- NEW


def load_json(path: Path):
    """Read a JSON file (full or delta) and return list of purchases."""
    if not path.exists():
        raise FileNotFoundError(f"{path} not found.")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"Expected top-level JSON list in {path.name}")

    return data


def get_purchase_count() -> int:
    """
    Return how many rows exist in the Purchase table.
    If the DB file is new, init_db() will create tables first.
    """
    init_db()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM Purchase")
        (count,) = cur.fetchone()
    return int(count)


def build_rows(purchases):
    """
    Convert list of purchase dicts into rows for executemany.

    JSON shape:

    {
      "transaction_id": 2085361712,
      "purchase_date": "2025-11-26",
      "purchase_time": "22:31:00",
      "cardholder_name": "EMV-TAP",
      "cardholder_last4": "0420",
      "total_amount": 3.75,
      "purchase_type": "W" or "V",
      "vacuum_number": null or int,
      "wash_bay_purchases": [
        { "bay_number": 5, "wash_purchase_total": 3.75 },
        ...
      ]
    }
    """

    purchase_rows = []
    vacuum_rows = []
    wash_bay_rows = []

    for p in purchases:
        transaction_id = int(p["transaction_id"])
        purchase_date = p["purchase_date"]          # already 'YYYY-MM-DD'
        purchase_time = p["purchase_time"]          # already 'HH:MM:SS'
        cardholder_name = p.get("cardholder_name")
        cardholder_last4 = p.get("cardholder_last4")
        total_amount = float(p["total_amount"])
        purchase_type = p["purchase_type"]          # 'V' or 'W'

        # ---- Parent row for Purchase table ----
        purchase_rows.append(
            (
                transaction_id,
                purchase_date,
                purchase_time,
                cardholder_name,
                cardholder_last4,
                total_amount,
                purchase_type,
            )
        )

        # ---- Child rows based on purchase_type ----
        if purchase_type == "V":
            # Vacuum purchase: one row in VacuumPurchase
            vacuum_number = p["vacuum_number"]
            if vacuum_number is None:
                # Safety – DB column is NOT NULL
                raise ValueError(
                    f"Vacuum purchase with null vacuum_number (tx {transaction_id})"
                )
            vacuum_rows.append((transaction_id, int(vacuum_number)))

        elif purchase_type == "W":
            # Wash purchase: 0..N rows in WashBayPurchase
            for line in p.get("wash_bay_purchases", []):
                bay_number = int(line["bay_number"])
                wash_total = float(line["wash_purchase_total"])
                wash_bay_rows.append(
                    (transaction_id, bay_number, wash_total)
                )

        else:
            raise ValueError(f"Unknown purchase_type: {purchase_type!r}")

    return purchase_rows, vacuum_rows, wash_bay_rows


def insert_all(purchase_rows, vacuum_rows, wash_bay_rows):
    """Insert all rows into the database using executemany."""
    # Make sure tables exist
    init_db()

    with get_connection() as conn:
        cur = conn.cursor()

        # --- Purchase table ---
        if purchase_rows:
            cur.executemany(
                """
                INSERT INTO Purchase (
                    transaction_id,
                    purchase_date,
                    purchase_time,
                    cardholder_name,
                    cardholder_last4,
                    total_amount,
                    purchase_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                purchase_rows,
            )

        # --- VacuumPurchase table ---
        if vacuum_rows:
            cur.executemany(
                """
                INSERT INTO VacuumPurchase (
                    transaction_id,
                    vacuum_number
                ) VALUES (?, ?)
                """,
                vacuum_rows,
            )

        # --- WashBayPurchase table ---
        if wash_bay_rows:
            cur.executemany(
                """
                INSERT INTO WashBayPurchase (
                    transaction_id,
                    bay_number,
                    wash_purchase_total
                ) VALUES (?, ?, ?)
                """,
                wash_bay_rows,
            )

        conn.commit()


def main():
    # 1) Check if DB has any data in Purchase
    purchase_count = get_purchase_count()
    print(f"Purchase table currently has {purchase_count} rows.")

    # 2) Decide whether to use full cleaned file or delta
    if purchase_count == 0:
        # Initial load / reinitialized DB: use full cleaned history
        print("Database is empty – loading full cleaned history.")
        purchases = load_json(CLEAN_JSON_PATH)
    else:
        # Incremental update: use delta only
        if not DELTA_JSON_PATH.exists():
            print(f"No delta file found at {DELTA_JSON_PATH}; nothing to load.")
            return

        purchases = load_json(DELTA_JSON_PATH)
        if not purchases:
            print("Delta file is empty – no new cleaned records to load into DB.")
            return

        print(f"Loading {len(purchases)} new cleaned records from delta file.")

    # 3) Build rows and insert
    purchase_rows, vacuum_rows, wash_bay_rows = build_rows(purchases)

    print(f"Prepared {len(purchase_rows)} Purchase rows")
    print(f"Prepared {len(vacuum_rows)} VacuumPurchase rows")
    print(f"Prepared {len(wash_bay_rows)} WashBayPurchase rows")

    insert_all(purchase_rows, vacuum_rows, wash_bay_rows)
    print("Database update complete.")


if __name__ == "__main__":
    main()
