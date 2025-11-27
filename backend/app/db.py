# backend/app/db.py

from pathlib import Path
import sqlite3

# Folder containing this file (backend/app)
APP_DIR = Path(__file__).resolve().parent

# We'll store the SQLite file right next to db.py
DB_PATH = APP_DIR / "cryptopay.sqlite"

# Your schema file (the one in your screenshot)
SCHEMA_PATH = APP_DIR / "schema.sql"


def get_connection() -> sqlite3.Connection:
    """
    Open a connection to the SQLite DB.
    Row factory is set to sqlite3.Row so you can get dict-like rows later.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """
    Create tables if they don't exist, using schema.sql.
    Safe to call multiple times.
    """
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with get_connection() as conn:
        conn.executescript(schema_sql)
        conn.commit()


if __name__ == "__main__":
    init_db()
    print(f"Initialized database at {DB_PATH}")
