# THIS IS FOR TESTING PURPOSES ONLY.
# DO NOT USE IN PRODUCTION.
# 
# Actually found an error in the credit card data i scraped. That data is supposed to have unique transaction_id values,
# but some rows were duplicated. This script helps find those duplicates.

# This is on the company im scraping off of. Not good for them, bad for my data :(


import json
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
DATA_FILE = BASE_DIR / "data" / "cryptopay_cleaned.json"

def main():
    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}")
        return

    with DATA_FILE.open("r", encoding="utf-8") as f:
        records = json.load(f)

    # adjust this if your key name is different
    ids = []
    for row in records:
        tid = row.get("transaction_id")
        if tid is not None:
            ids.append(tid)

    counter = Counter(ids)
    dupes = [tid for tid, count in counter.items() if count > 1]

    print(f"Total records: {len(records)}")
    print(f"Unique transaction_ids: {len(counter)}")
    print(f"Duplicate transaction_ids count: {len(dupes)}")

    if dupes:
        print("\nSample duplicate IDs:")
        for tid in dupes[:20]:
            print(" -", tid)

        # optional: show all rows with the first duplicate
        print("\nRows for first duplicate ID:")
        first = dupes[0]
        for row in records:
            if row.get("transaction_id") == first:
                print(json.dumps(row, indent=2))
    else:
        print("No duplicate transaction_id values found.")

if __name__ == "__main__":
    main()
