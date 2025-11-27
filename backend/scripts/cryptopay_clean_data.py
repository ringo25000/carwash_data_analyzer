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

    Returns:
      {
        "purchase_type": "V",
        "vacuum_number": 3
      }

    or:

      {
        "purchase_type": "W",
        "wash_bay_purchases": [
          {"bay_number": 5, "wash_purchase_total": 3.75},
          ...
        ]
      }
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
        #    Matches 'bay 5', '(bay 5)', 'bay   5', etc.
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

    # For Wash Bay, sum all wash_purchase_total entries
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


def main():
    if not RAW_JSON_PATH.exists():
        raise FileNotFoundError(f"{RAW_JSON_PATH} not found. Run cryptopay_scrape_data.py first.")

    with RAW_JSON_PATH.open("r", encoding="utf-8") as f:
        raw_records = json.load(f)

    print(f"Loaded {len(raw_records)} raw records. Cleaning...")

    cleaned_records = [clean_record(rec) for rec in raw_records]

    with CLEAN_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(cleaned_records, f, indent=2)

    print(f"Saved {len(cleaned_records)} cleaned records to {CLEAN_JSON_PATH}")


if __name__ == "__main__":
    main()
