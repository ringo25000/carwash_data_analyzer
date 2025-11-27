# backend/scripts/load_transactions.py

import json
from pathlib import Path

from app.db import get_connection, init_db

ROOT_DIR = Path(__file__).resolve().parents[1]   # .../backend
DATA_DIR = ROOT_DIR / "data"
CLEAN_JSON_PATH = DATA_DIR / "cryptopay_cleaned.json"
DELTA_JSON_PATH = DATA_DIR / "cryptopay_cleaned_delta.json"


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"{path} not found.")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected a list in {path.name}")
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


def build_rows(records):
    """
    Build Purchase, VacuumPurchase, and WashBayPurchase rows
    from a list of cleaned records (either full history or delta).
    """
    purchase_rows = []
    vacuum_rows = []
    wash_bay_rows = []

    for rec in records:
        txid = int(rec["transaction_id"])
        purchase_date = rec["purchase_date"]
        purchase_time = rec["purchase_time"]
        cardholder_name = rec.get("cardholder_name")
        cardholder_last4 = rec.get("cardholder_last4")
        total_amount = float(rec["total_amount"])
        purchase_type = rec["purchase_type"]   # 'V' or 'W'

        purchase_rows.append(
            (
                txid,
                purchase_date,
                purchase_time,
                cardholder_name,
                cardholder_last4,
                total_amount,
                purchase_type,
            )
        )

        if purchase_type == "V":
            vacuum_number = rec["vacuum_number"]
            if vacuum_number is None:
                raise ValueError(f"Vacuum purchase with null vacuum_number (tx {txid})")
            vacuum_rows.append((txid, int(vacuum_number)))

        elif purchase_type == "W":
            for line in rec.get("wash_bay_purchases", []):
                bay_number = int(line["bay_number"])
                wash_total = float(line["wash_purchase_total"])
                wash_bay_rows.append((txid, bay_number, wash_total))

        else:
            raise ValueError(f"Unknown purchase_type: {purchase_type!r}")

    return purchase_rows, vacuum_rows, wash_bay_rows


def insert_rows(purchase_rows, vacuum_rows, wash_bay_rows):
    """
    Insert the new rows into the DB.
    We assume the caller only passes truly new transactions
    (i.e., no duplicate transaction_ids).
    """
    init_db()

    with get_connection() as conn:
        cur = conn.cursor()

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
        records = load_json(CLEAN_JSON_PATH)
    else:
        # Incremental update: use delta only
        if not DELTA_JSON_PATH.exists():
            print(f"No delta file found at {DELTA_JSON_PATH}; nothing to load.")
            return

        records = load_json(DELTA_JSON_PATH)
        if not records:
            print("Delta file is empty – no new cleaned records to load into DB.")
            return

        print(f"Loading {len(records)} new cleaned records from delta file.")

    # 3) Build rows and insert
    purchase_rows, vacuum_rows, wash_bay_rows = build_rows(records)

    print("Prepared rows for insertion:")
    print(f"  Purchase:       {len(purchase_rows)}")
    print(f"  VacuumPurchase: {len(vacuum_rows)}")
    print(f"  WashBayPurchase:{len(wash_bay_rows)}")

    insert_rows(purchase_rows, vacuum_rows, wash_bay_rows)

    print("Database update complete.")


if __name__ == "__main__":
    main()
