# src/sources/quiver.py
import os
import pandas as pd
from datetime import datetime, timedelta

def fetch_congress_trades(days_back: int = 45):
    """
    Fetch recent Congressional trades using the official Quiver client.
    Filters by an 'effective date' (TransactionDate if present, else Filing/Report) >= today - days_back.
    Returns a JSON-friendly DataFrame with the columns our pipeline expects.
    """
    api_key = os.getenv("QUIVER_API_KEY")
    if not api_key:
        raise ValueError("Missing QUIVER_API_KEY in environment variables")

    # Use the official client so we don't guess endpoints
    from quiverquant import quiver as qq
    client = qq(api_key)

    # This returns recent congressional trades as a DataFrame
    df = client.congress_trading()
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=[
            "member_name","filing_date","trade_date","chamber","party","committees",
            "ticker","transaction_type","amount_range","market_cap_m","adv_m","est_amount","source"
        ])

    # Robust date parsing (tz-naive is fine since we compare dates)
    td = pd.to_datetime(df.get("TransactionDate"), errors="coerce")
    fd = pd.to_datetime(df.get("FilingDate"), errors="coerce")
    rd = pd.to_datetime(df.get("ReportDate"), errors="coerce")

    # Effective date: prefer TransactionDate, else Filing, else Report
    effective = td.copy()
    if effective is None or effective.empty:
        effective = pd.Series(pd.NaT, index=df.index)
    if fd is not None:
        effective = effective.fillna(fd)
    if rd is not None:
        effective = effective.fillna(rd)

    # Produce string dates for DB
    trade_date  = td.dt.date.astype("string") if td is not None else pd.Series([pd.NA]*len(df))
    filing_base = fd if fd is not None else (rd if rd is not None else None)
    filing_date = filing_base.dt.date.astype("string") if filing_base is not None else pd.Series([pd.NA]*len(df))

    df["_effective_dt"] = effective
    df["trade_date"]    = trade_date
    df["filing_date"]   = filing_date

    # Amount/range
    if "Amount" in df.columns:
        df["amount_range"] = df["Amount"].astype("string")
    elif "Range" in df.columns:
        df["amount_range"] = df["Range"].astype("string")
    else:
        df["amount_range"] = pd.NA

    # Standardize names our pipeline expects
    df["member_name"]      = df.get("Representative")
    df["chamber"]          = df.get("Chamber")
    df["party"]            = df.get("Party")
    df["committees"]       = df.get("Committee")
    df["ticker"]           = df.get("Ticker")
    df["transaction_type"] = df.get("Transaction")

    # Placeholders until we enrich
    df["market_cap_m"] = None
    df["adv_m"]        = None
    df["est_amount"]   = None
    df["source"]       = "quiver_api"

    # Debug counts & filter by effective date (days_back)
    print(f"[DEBUG] Quiver (client) returned {len(df)} rows before date filter")
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    kept = df[df["_effective_dt"].notna() & (df["_effective_dt"] >= cutoff)].copy()
    print(f"[DEBUG] Keeping {len(kept)} rows on/after {cutoff.date()} (effective date)")

    # Return only pipeline columns (as strings/None)
    cols = [
        "member_name","filing_date","trade_date","chamber","party","committees",
        "ticker","transaction_type","amount_range","market_cap_m","adv_m","est_amount","source"
    ]
    kept = kept[cols]
    for col in ("member_name","filing_date","trade_date","chamber","party","committees",
                "ticker","transaction_type","amount_range","source"):
        if col in kept.columns:
            kept[col] = kept[col].astype("string")

    return kept
