import sqlite3
import subprocess
from datetime import date, datetime
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------
# Setup
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]          # backend/
DB_PATH = BASE_DIR / "app" / "cryptopay.sqlite"

st.set_page_config(
    page_title="Carwash Control Panel",
    layout="wide",
)

# Reduce top padding so the title is closer to the top
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 0.75rem;
        }
        .stCaption p {
            color: #ffffff !important;
            font-size: 1.05rem;
        }
        .bay-vac-line {
            color: #ffffff;
            font-size: 1.2rem;
            font-weight: 600;
            margin: 0.2rem 0;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Carwash Control Panel")
st.markdown("---")

# Two main columns: left = buttons, right = metrics
left_col, right_col = st.columns([1, 3])

# ---------------------------------------------------------
# DB helpers
# ---------------------------------------------------------

@st.cache_resource
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def get_last_update_time() -> str:
    """Last modified time of the SQLite DB file."""
    try:
        mtime = DB_PATH.stat().st_mtime
    except FileNotFoundError:
        return "No database file found"
    dt_obj = datetime.fromtimestamp(mtime)
    return dt_obj.strftime("%Y-%m-%d %I:%M %p")


@st.cache_data(show_spinner=False)
def get_daily_metrics(target: date):
    """
    Compute daily metrics using SQL:
      - wash_plus_vac: sum of all totals
      - wash_total: all wash (W) purchases
      - vac_total: all vacuum (V) purchases
      - bay_totals: per-bay totals from WashBayPurchase
      - vacuum_totals: per-vac totals from VacuumPurchase
    Falls back to latest date in DB if target has no data.
    """
    conn = get_connection()
    cur = conn.cursor()

    # If target date has no data, fallback to latest purchase_date
    target_str = target.isoformat()

    cur.execute(
        "SELECT COUNT(*) AS c FROM Purchase WHERE purchase_date = ?;",
        (target_str,),
    )
    row = cur.fetchone()
    if row["c"] == 0:
        cur.execute("SELECT MAX(purchase_date) AS max_d FROM Purchase;")
        row2 = cur.fetchone()
        if not row2 or row2["max_d"] is None:
            # No data at all
            return {
                "date": target_str,
                "wash_plus_vac": 0.0,
                "wash_total": 0.0,
                "vac_total": 0.0,
                "bay_totals": {i: 0.0 for i in range(1, 8)},
                "vacuum_totals": {i: 0.0 for i in range(1, 7)},
            }
        target_str = row2["max_d"]

    # Grand + wash + vac totals from Purchase (Purchase.purchase_type ∈ {'W', 'V'})
    cur.execute(
        """
        SELECT
            COALESCE(SUM(total_amount), 0.0) AS grand_total,
            COALESCE(SUM(CASE WHEN purchase_type = 'W' THEN total_amount ELSE 0 END), 0.0) AS wash_total,
            COALESCE(SUM(CASE WHEN purchase_type = 'V' THEN total_amount ELSE 0 END), 0.0) AS vac_total
        FROM Purchase
        WHERE purchase_date = ?
        """,
        (target_str,),
    )
    row = cur.fetchone()
    grand_total = float(row["grand_total"])
    wash_total = float(row["wash_total"])
    vac_total = float(row["vac_total"])

    # Per-bay totals derived from WashBayPurchase rows
    cur.execute(
        """
        SELECT
            w.bay_number AS bay_number,
            COALESCE(SUM(w.wash_purchase_total), 0.0) AS total
        FROM WashBayPurchase w
        JOIN Purchase p ON p.transaction_id = w.transaction_id
        WHERE p.purchase_date = ?
        GROUP BY w.bay_number
        """,
        (target_str,),
    )
    bay_rows = cur.fetchall()
    bay_totals = {i: 0.0 for i in range(1, 8)}
    for r in bay_rows:
        bay_num = r["bay_number"]
        if bay_num in bay_totals:
            bay_totals[bay_num] = float(r["total"])

    # Per-vac totals derived from VacuumPurchase rows
    cur.execute(
        """
        SELECT
            v.vacuum_number AS vacuum_number,
            COALESCE(SUM(p.total_amount), 0.0) AS total
        FROM VacuumPurchase v
        JOIN Purchase p ON p.transaction_id = v.transaction_id
        WHERE p.purchase_date = ?
          AND p.purchase_type = 'V'
        GROUP BY v.vacuum_number
        """,
        (target_str,),
    )
    vac_rows = cur.fetchall()
    vacuum_totals = {i: 0.0 for i in range(1, 7)}
    for r in vac_rows:
        vac_num = r["vacuum_number"]
        if vac_num in vacuum_totals:
            vacuum_totals[vac_num] = float(r["total"])

    return {
        "date": target_str,
        "wash_plus_vac": grand_total,
        "wash_total": wash_total,
        "vac_total": vac_total,
        "bay_totals": bay_totals,
        "vacuum_totals": vacuum_totals,
    }


def format_currency(amount: float) -> str:
    return f"${amount:,.2f}"


# ---------------------------------------------------------
# LEFT COLUMN: Buttons
# ---------------------------------------------------------

with left_col:
    if st.button("Update Login"):
        with st.spinner("Running login script (python -m scripts.cryptopay_login)..."):
            result = subprocess.run(
                ["python", "-m", "scripts.cryptopay_login"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
            )

        if result.returncode == 0:
            st.success("Login updated successfully ✅")
            if result.stdout:
                with st.expander("Show login script output"):
                    st.code(result.stdout)
        else:
            st.error("Login update FAILED ❌")
            with st.expander("Show error output"):
                st.code(result.stderr or result.stdout)

    st.write("")

    if st.button("Update Data"):
        with st.spinner("Running ETL pipeline (python -m app.pipeline)..."):
            result = subprocess.run(
                ["python", "-m", "app.pipeline"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
            )

        if result.returncode == 0:
            st.success("Data updated successfully ✅")
            # Clear cached metrics so they recompute from updated DB
            get_daily_metrics.clear()
            if result.stdout:
                with st.expander("Show pipeline output"):
                    st.code(result.stdout)
        else:
            st.error("Data update FAILED ❌")
            with st.expander("Show error output"):
                st.code(result.stderr or result.stdout)


# ---------------------------------------------------------
# RIGHT COLUMN: Today's Metrics (SQL-backed)
# ---------------------------------------------------------

with right_col:
    today = date.today()
    metrics = get_daily_metrics(today)

    st.subheader("Today's Metrics")

    st.caption(
        f"Last update time: {get_last_update_time()}  •  "
        f"Showing activity for {metrics['date']} "
        "(falls back to latest available if today has no data)."
    )

    # Top row: big totals
    totals_cols = st.columns(3)
    totals_cols[0].metric("Wash + Vac Total", format_currency(metrics["wash_plus_vac"]))
    totals_cols[1].metric("Bays Total", format_currency(metrics["wash_total"]))
    totals_cols[2].metric("Vacuums Total", format_currency(metrics["vac_total"]))

    st.write("")

    # Two cards: Bays and Vacuums
    bays_col, vacs_col = st.columns(2)

    with bays_col:
        st.markdown("#### Bays")
        st.metric("Bays Total", format_currency(metrics["wash_total"]))
        for bay_number in range(1, 8):
            amount = metrics["bay_totals"][bay_number]
            st.caption(f"Bay {bay_number}: {format_currency(amount)}")

    with vacs_col:
        st.markdown("#### Vacuums")
        st.metric("Vacuums Total", format_currency(metrics["vac_total"]))
        for vac_number in range(1, 7):
            amount = metrics["vacuum_totals"][vac_number]
            st.caption(f"Vac {vac_number}: {format_currency(amount)}")

# ---------------------------------------------------------
# BELOW BOTH COLUMNS: start another tool/section
# ---------------------------------------------------------

st.markdown("---")
# ⬇️ Anything you put here will be under both left+right columns.
# Example placeholder:
# st.subheader("Another Tool")
# st.write("You can start building another panel here.")


