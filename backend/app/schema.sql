-- backend/app/models.sql
-- Follows SQLite syntax

CREATE TABLE IF NOT EXISTS Purchase (
    transaction_id      INTEGER PRIMARY KEY,
    purchase_date       TEXT NOT NULL,          -- 'YYYY-MM-DD'
    purchase_time       TEXT NOT NULL,          -- 'HH:MM:SS'
    cardholder_name     TEXT NOT NULL,
    cardholder_last4    TEXT NOT NULL,       -- last 4 digits as text to preserve leading zeros
    total_amount        NUMERIC(4, 2) NOT NULL, -- behaves like DECIMAL(4,2) in SQLite
    purchase_type       TEXT NOT NULL CHECK (purchase_type IN ('V', 'W'))
);

CREATE TABLE IF NOT EXISTS VacuumPurchase (
    transaction_id  INTEGER PRIMARY KEY,
    vacuum_number   INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES Purchase(transaction_id)
);

CREATE TABLE IF NOT EXISTS WashBayPurchase (
    line_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id  INTEGER NOT NULL,
    bay_number      INTEGER NOT NULL,
    wash_purchase_total      NUMERIC(4, 2) NOT NULL,     -- per-wash dollar amount
    FOREIGN KEY (transaction_id) REFERENCES Purchase(transaction_id)
);
