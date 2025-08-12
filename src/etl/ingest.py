import json
from datetime import datetime
from typing import Dict
from ..db import init_db, get_conn, hash_row
from ..sources.quiver import fetch_congress_trades

def to_jsonable(v):
    # Coerce Pandas / numpy / datetime-ish into JSON-friendly types
    try:
        import pandas as pd
        import numpy as np
    except Exception:
        pd = None
        np = None

    # Pandas Timestamp/NaT
    if pd is not None:
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime().isoformat()
        if pd.isna(v):
            return None

    # datetime/date
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass

    # numpy scalars
    if np is not None:
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return float(v)

    return v
def estimate_amount(amount_range):
    """
    Convert Quiver amount to a numeric estimate.
    Handles numbers (e.g., 1001.0), ranges like "$1,001 - $15,000", and ">$5,000,000".
    """
    if amount_range is None:
        return None
    # Numeric already?
    try:
        return float(amount_range)
    except Exception:
        pass

    s = str(amount_range).replace("$", "").replace(",", "").strip()
    if "-" in s:
        a, b = s.split("-", 1)
        try:
            lo = float(a.strip())
            hi = float(b.strip())
            return (lo + hi) / 2.0
        except:
            return None
    if s.startswith(">") or s.endswith("+"):
        try:
            base = float(s.lstrip(">").rstrip("+").strip())
            return base * 1.25  # conservative midpoint buffer
        except:
            return None
    try:
        return float(s)
    except:
        return None


def normalize(row: Dict) -> Dict:
    try:
        days_lag = (datetime.fromisoformat(str(row.get("filing_date"))).date() -
                    datetime.fromisoformat(str(row.get("trade_date"))).date()).days
    except Exception:
        days_lag = None

    return {
        "filing_date": row.get("filing_date"),
        "trade_date": row.get("trade_date"),
        "member_name": row.get("member_name"),
        "chamber": row.get("chamber"),
        "party": row.get("party"),
        "committees": row.get("committees"),
        "ticker": row.get("ticker"),
        "company": row.get("company"),
        "sector": row.get("sector"),
        "market_cap_m": row.get("market_cap_m"),
        "adv_m": row.get("adv_m"),
        "transaction_type": row.get("transaction_type"),
        "amount_range": row.get("amount_range"),
        "est_amount": row.get("est_amount") or estimate_amount(row.get("amount_range")),
        "days_lag": days_lag,
    }

def run_ingest(lookback_days: int = 2):
    init_db()

    rows = fetch_congress_trades(days_back=lookback_days)

    print(f"\n[DEBUG] Fetched {len(rows)} trades from Quiver API (last {lookback_days} days):")
    if len(rows) > 0:
        try:
            print(rows.head(20).to_string(index=False))
        except Exception:
            print(rows.head(20))
    else:
        print("No trades returned by API — try increasing lookback_days.\n")

    conn = get_conn()
    cur = conn.cursor()

    for _, row in rows.iterrows():
        payload = {k: to_jsonable(v) for k, v in dict(row).items()}  # <-- sanitize here
        source = payload.pop("source", "quiver_api")
        row_h = hash_row(payload)

        # Insert raw
        try:
            cur.execute(
                "INSERT INTO trades_raw (source, row_hash, payload_json) VALUES (?, ?, ?)",
                (source, row_h, json.dumps(payload, sort_keys=True, separators=(",", ":"))),
            )
        except Exception:
            pass  # likely duplicate

        # Insert clean
        clean = normalize(payload)
        fields = ",".join(clean.keys())
        placeholders = ",".join(["?"] * len(clean))
        try:
            cur.execute(
                f"INSERT INTO trades_clean (row_hash, {fields}) VALUES (?, {placeholders})",
                (row_h, *clean.values()),
            )
        except Exception:
            pass  # likely duplicate

        # Upsert member metadata (simplified)
        cur.execute(
            "INSERT OR IGNORE INTO members (member_name, chamber, party, committees) VALUES (?, ?, ?, ?)",
            (clean["member_name"], clean["chamber"], clean["party"], clean["committees"]),
        )

    conn.commit()
    conn.close()
