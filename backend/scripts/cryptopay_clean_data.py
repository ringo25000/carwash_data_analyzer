# backend/scripts/cryptopay_clean_data.py

import json
import re
from datetime import datetime
from pathlib import Path

# --- Paths ---

BASE_DIR = Path(__file__).resolve().parents[1]   # backend/
DATA_DIR = BASE_DIR / "data"
RAW_JSON_PATH = DATA_DIR / "cryptopay_allData.json"
CLEAN_JSON_PATH = DATA_DIR / "cryptopay_cleaned.json"
DELTA_JSON_PATH = DATA_DIR / "cryptopay_cleaned_delta.json"  # <-- NEW


# --- Helper functions (cleaning only) ---

def parse_datetime(dt_str: str) -> tuple[str, str]:
    """
    '11/26/2025, 9:32 PM' -> ('2025-11-26', '21:32:00')
    """
    dt = datetime.strptime(dt_str, "%m/%d/%Y, %I:%M %p")
    purchase_date = dt.date().isoformat()                      # 'YYYY-MM-DD'
    purchase_time = dt.time().replace(microsecond=0).isoformat()  # 'HH:MM:SS'
    return purchase_date, purchase_time


def parse_cardholder(cardholder: str) -> tuple[str, str]:
    """
    Assumes format: 'NAME (1234)' -> ('NAME', '1234')
    Keeps last4 as a string to preserve leading zeros (e.g. '0420').
    """
    name_part, last4_part = cardholder.split("(", 1)
    cardholder_name = name_part.strip()
    cardholder_last4 = last4_part.strip().strip(")")
    return cardholder_name, cardholder_last4


def parse_money(money_str: str) -> float:
    """
    '$1.50' -> 1.50
    '$3,200.75' -> 3200.75

    Returned as float suitable for NUMERIC(4,2)-style storage in SQLite.
    """
    cleaned = money_str.replace("$", "").replace(",", "").strip()
    value = float(cleaned)
    return round(value, 2)


def parse_details_text(details_text: str) -> dict:
    """
    Decide if this is a Vacuum or Wash Bay purchase from details_text,
    and extract either:
      - purchase_type = 'V', vacuum_number
      - purchase_type = 'W', wash_bay_purchases (list of bay_number + wash_purchase_total)
    """
    lines = [line.strip() for line in details_text.splitlines() if line.strip()]

    vac_lines = [ln for ln in lines if ln.startswith("Vac")]
    bay_lines = [ln for ln in lines if ln.startswith("Wash Bay")]

    # Vacuum purchase
    if vac_lines:
        # Example: 'Vac\t(vacuum 3)\t$1.50'
        line = vac_lines[0]
        parts = [p for p in line.split("\t") if p.strip()]

        # e.g. '(vacuum 3)'
        vacuum_part = next(p for p in parts if "vacuum" in p.lower())
        vacuum_number = int(re.search(r"(\d+)", vacuum_part).group(1))

        return {
            "purchase_type": "V",
            "vacuum_number": vacuum_number,
        }

    # Wash Bay purchase (can be 1 or more lines)
    wash_bay_purchases: list[dict] = []

    for line in bay_lines:
        # Example: 'Wash Bay\t(bay 5)\t$3.75'
        parts = [p for p in line.split("\t") if p.strip()]

        # 1) Get bay_number from the whole line
        m = re.search(r"bay[^0-9]*([0-9]+)", line, re.IGNORECASE)
        bay_number = int(m.group(1))

        # 2) Get the money part from the split pieces
        total_part = next(p for p in parts if "$" in p)
        wash_purchase_total = parse_money(total_part)

        wash_bay_purchases.append(
            {
                "bay_number": bay_number,
                "wash_purchase_total": wash_purchase_total,
            }
        )

    return {
        "purchase_type": "W",
        "wash_bay_purchases": wash_bay_purchases,
    }


def compute_total_amount(raw_total_str: str, details: dict) -> float:
    """
    Compute the overall total_amount for the Purchase record.

    - For 'V' (vacuum): use the raw JSON total (parsed).
    - For 'W' (Wash Bay): sum all wash_purchase_total values.
    """
    purchase_type = details["purchase_type"]

    if purchase_type == "V":
        return parse_money(raw_total_str)

    wash_bay_purchases = details["wash_bay_purchases"]
    total = sum(entry["wash_purchase_total"] for entry in wash_bay_purchases)
    return round(total, 2)


def clean_record(raw: dict) -> dict:
    """
    Take one raw JSON entry from cryptopay_allData.json and
    return a normalized dict ready for DB loading, with names
    aligned to the schema.
    """
    transaction_id = int(raw["transaction_id"])
    purchase_date, purchase_time = parse_datetime(raw["datetime"])
    cardholder_name, cardholder_last4 = parse_cardholder(raw["cardholder"])

    details = parse_details_text(raw["details_text"])
    purchase_type = details["purchase_type"]

    total_amount = compute_total_amount(raw["total"], details)

    if purchase_type == "V":
        vacuum_number = details["vacuum_number"]
        wash_bay_purchases: list[dict] = []
    else:  # 'W'
        vacuum_number = None
        wash_bay_purchases = details["wash_bay_purchases"]

    return {
        "transaction_id": transaction_id,
        "purchase_date": purchase_date,
        "purchase_time": purchase_time,
        "cardholder_name": cardholder_name,
        "cardholder_last4": cardholder_last4,         # string, preserves leading zeros
        "total_amount": total_amount,
        "purchase_type": purchase_type,               # 'V' or 'W'
        "vacuum_number": vacuum_number,               # int or None
        "wash_bay_purchases": wash_bay_purchases      # list of {bay_number, wash_purchase_total}
    }


# --- Incremental cleaning helpers ---

def load_raw_records():
    if not RAW_JSON_PATH.exists():
        raise FileNotFoundError(f"{RAW_JSON_PATH} not found. Run cryptopay_scrape_data.py first.")
    with RAW_JSON_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_existing_cleaned():
    if not CLEAN_JSON_PATH.exists():
        return []
    with CLEAN_JSON_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def clean_all():
    """
    First-time mode: clean all raw records and treat them all as "new".
    Writes both:
      - full cleaned history
      - delta file containing all cleaned records
    """
    raw_records = load_raw_records()
    print(f"Loaded {len(raw_records)} raw records. Cleaning all...")

    cleaned_records = [clean_record(rec) for rec in raw_records]

    # Full history
    with CLEAN_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(cleaned_records, f, indent=2)

    # Delta = everything (first run)
    with DELTA_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(cleaned_records, f, indent=2)

    print(f"Saved {len(cleaned_records)} cleaned records to {CLEAN_JSON_PATH}")
    print(f"Saved {len(cleaned_records)} new cleaned records to {DELTA_JSON_PATH}")


def incremental_clean():
    """
    Advanced mode: only clean *new* raw records that are not yet present
    in the cleaned JSON, and write:
      - updated full cleaned file
      - delta file with only newly cleaned records

    Assumptions (true given your scraper logic):
      - RAW_JSON_PATH (cryptopay_allData.json) is sorted newest -> oldest.
      - CLEAN_JSON_PATH (cryptopay_cleaned.json) is also newest -> oldest.
      - transaction_id is globally unique.
      - The cleaner has previously processed all older transactions.
    """
    raw_records = load_raw_records()
    existing_cleaned = load_existing_cleaned()

    if not existing_cleaned:
        print("Cleaned file exists but has no records; cleaning all raw records.")
        return clean_all()

    latest_cleaned_txid = int(existing_cleaned[0]["transaction_id"])
    known_txids = {int(rec["transaction_id"]) for rec in existing_cleaned}
    new_cleaned: list[dict] = []

    print(f"Loaded {len(raw_records)} raw records.")
    print(f"Loaded {len(existing_cleaned)} existing cleaned records.")
    print(f"Latest cleaned transaction_id: {latest_cleaned_txid}")
    print(f"{len(known_txids)} distinct transaction_ids already cleaned.")

    for raw in raw_records:
        txid = int(raw["transaction_id"])

        # As soon as we hit the latest already-cleaned txid,
        # we know everything after this is older and already processed.
        if txid == latest_cleaned_txid:
            print("Hit latest cleaned transaction_id – stopping incremental clean.")
            break

        # Safety net: if for some reason this txid is already in cleaned set, skip it.
        if txid in known_txids:
            continue

        cleaned = clean_record(raw)
        new_cleaned.append(cleaned)
        known_txids.add(txid)

    if not new_cleaned:
        print("No new records to clean.")
        # still write an empty delta so loader knows there's nothing to do
        with DELTA_JSON_PATH.open("w", encoding="utf-8") as f:
            json.dump([], f)
        return

    # Newest first: new cleaned records + existing cleaned history
    all_cleaned = new_cleaned + existing_cleaned

    # Full history
    with CLEAN_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(all_cleaned, f, indent=2)

    # Delta file = only the new cleaned records from this run
    with DELTA_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(new_cleaned, f, indent=2)

    print(f"Cleaned {len(new_cleaned)} new records.")
    print(f"Saved {len(all_cleaned)} total cleaned records to {CLEAN_JSON_PATH}")
    print(f"Saved {len(new_cleaned)} new cleaned records to {DELTA_JSON_PATH}")


def main():
    if not RAW_JSON_PATH.exists():
        raise FileNotFoundError(f"{RAW_JSON_PATH} not found. Run cryptopay_scrape_data.py first.")

    if not CLEAN_JSON_PATH.exists():
        # First time: clean everything, delta = all
        clean_all()
    else:
        # Subsequent runs: only clean new stuff, delta = new
        incremental_clean()


if __name__ == "__main__":
    main()
