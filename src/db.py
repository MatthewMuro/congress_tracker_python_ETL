import os, json, sqlite3, hashlib
from pathlib import Path

DB_PATH = Path("data/congress.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SCHEMA = {
    "trades_raw": """
    CREATE TABLE IF NOT EXISTS trades_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        row_hash TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );""",
    "trades_clean": """
    CREATE TABLE IF NOT EXISTS trades_clean (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        row_hash TEXT NOT NULL UNIQUE,
        filing_date TEXT,
        trade_date TEXT,
        member_name TEXT,
        chamber TEXT,
        party TEXT,
        committees TEXT,
        ticker TEXT,
        company TEXT,
        sector TEXT,
        market_cap_m REAL,
        adv_m REAL,
        transaction_type TEXT,
        amount_range TEXT,
        est_amount REAL,
        days_lag INTEGER
    );""",
    "members": """
    CREATE TABLE IF NOT EXISTS members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_name TEXT NOT NULL UNIQUE,
        chamber TEXT,
        party TEXT,
        committees TEXT,
        excess_return_bps_3y REAL DEFAULT 0
    );""",
    "scores_daily": """
    CREATE TABLE IF NOT EXISTS scores_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        row_hash TEXT NOT NULL,
        track_record_score REAL,
        committee_score REAL,
        size_score REAL,
        cluster_score REAL,
        signal_score REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
}

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    for _, ddl in SCHEMA.items():
        cur.execute(ddl)
    conn.commit()
    conn.close()

def hash_row(obj: dict) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(s.encode()).hexdigest()
