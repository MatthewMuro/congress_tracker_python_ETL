from ..enrich.liquidity import enrich_liquidity
from ..db import get_conn
from ..config import (
    MIN_AVG_DOLLAR_VOLUME_MILLIONS,
    MAX_MARKET_CAP_MILLIONS,
    SIGNAL_THRESHOLD,
    SLACK_WEBHOOK_URL,
)
from ..alerts.slack import send_slack

def compute_component_scores(conn):
    cur = conn.cursor()

    # Track-record score per member (simple mapping from 3y excess return, bps/yr)
    cur.execute("SELECT member_name, COALESCE(excess_return_bps_3y, 0) FROM members")
    member_scores = {name: min(100.0, max(0.0, 50.0 + bps / 12.0)) for name, bps in cur.fetchall()}

    # Build per-member median trade size (est_amount) in Python (SQLite lacks percentile_cont)
    from collections import defaultdict
    cur.execute("SELECT member_name, est_amount FROM trades_clean WHERE est_amount IS NOT NULL")
    amounts = defaultdict(list)
    for name, amt in cur.fetchall():
        amounts[name].append(amt)

    medians = {}
    for name, vals in amounts.items():
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        if n == 0:
            medians[name] = 1.0
        elif n % 2 == 1:
            medians[name] = vals_sorted[n // 2]
        else:
            medians[name] = 0.5 * (vals_sorted[n // 2 - 1] + vals_sorted[n // 2])

    # Cluster counts: distinct members per ticker (simple global count)
    cur.execute("SELECT ticker, COUNT(DISTINCT member_name) FROM trades_clean GROUP BY ticker")
    cluster_counts = dict(cur.fetchall())

    # Recompute scores from scratch
    cur.execute("DELETE FROM scores_daily")

    cur.execute("""
        SELECT row_hash, member_name, ticker, est_amount, committees, market_cap_m, adv_m
        FROM trades_clean
    """)
    rows = cur.fetchall()
    for row_hash, member_name, ticker, est_amount, committees, mcap, adv in rows:
        track = member_scores.get(member_name, 50.0)

        # Simple committee relevance heuristic (tweak as you like)
        comm = 90.0 if committees and any(k in (committees or "") for k in ("Armed", "Energy", "Health")) else 50.0

        # Size score: trade size vs member's median (cap at 3x). If est_amount is None, treat as 0.
        med = medians.get(member_name, 1.0) or 1.0
        rel_size = max(0.0, min(3.0, ((est_amount or 0.0) / med)))
        size_score = 33.3 * rel_size  # ~0..100

        # Cluster score: more unique members buying the same ticker → higher
        ccount = cluster_counts.get(ticker, 1)
        cluster_score = 20.0 + min(80.0, (ccount - 1) * 20.0)

        signal = round(0.40 * track + 0.20 * comm + 0.20 * size_score + 0.20 * cluster_score, 1)

        cur.execute(
            "INSERT INTO scores_daily (row_hash, track_record_score, committee_score, size_score, cluster_score, signal_score) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (row_hash, track, comm, size_score, cluster_score, signal),
        )

    conn.commit()

def emit_alerts(conn, verbose_top_n: int = 5):
    cur = conn.cursor()

    # PRIMARY: rows that PASS threshold + liquidity filters
    # Use COALESCE so NULL 'adv_m'/'market_cap_m' don't exclude rows.
    cur.execute("""
        SELECT c.member_name, c.ticker, c.company, c.sector, c.committees, c.est_amount, c.market_cap_m, c.adv_m,
               s.signal_score
        FROM trades_clean c
        JOIN scores_daily s ON c.row_hash = s.row_hash
        WHERE COALESCE(c.adv_m, 9999) >= ? AND COALESCE(c.market_cap_m, 0) <= ? AND s.signal_score >= ?
        ORDER BY s.signal_score DESC
        LIMIT 10
    """, (MIN_AVG_DOLLAR_VOLUME_MILLIONS, MAX_MARKET_CAP_MILLIONS, SIGNAL_THRESHOLD))
    rows = cur.fetchall()

    if rows:
        lines = [f"*Top Congressional Trade Signals* (threshold {SIGNAL_THRESHOLD})\n"]
        for r in rows:
            member, ticker, company, sector, committees, est_amt, mcap, adv, score = r
            lines.append(
                f"• {ticker} — {company} ({sector}) | Score {score} | {member} [{committees}] "
                f"| est ${0 if est_amt is None else est_amt:,.0f} | mcap ${0 if mcap is None else mcap:,.0f}M | ADV ${0 if adv is None else adv:,.0f}M"
            )
        text = "\n".join(lines)
        if SLACK_WEBHOOK_URL:
            send_slack(text)
        print(text)
        return

    # VERBOSE FALLBACK: show top N by score (no threshold), still apply COALESCE liquidity guards
    cur.execute("""
        SELECT c.member_name, c.ticker, c.company, c.sector, c.committees, c.est_amount, c.market_cap_m, c.adv_m,
               s.signal_score
        FROM trades_clean c
        JOIN scores_daily s ON c.row_hash = s.row_hash
        WHERE COALESCE(c.adv_m, 9999) >= ? AND COALESCE(c.market_cap_m, 0) <= ?
        ORDER BY s.signal_score DESC
        LIMIT ?
    """, (MIN_AVG_DOLLAR_VOLUME_MILLIONS, MAX_MARKET_CAP_MILLIONS, verbose_top_n))
    top_rows = cur.fetchall()

    if not top_rows:
        msg = "No trades available after filters. Try widening caps/ADV in .env or ingesting live data."
        print(msg)
        if SLACK_WEBHOOK_URL:
            send_slack(msg)
        return

    lines = [f"No signals ≥ {SIGNAL_THRESHOLD}. Here are the top {verbose_top_n} by score (below threshold):\n"]
    for r in top_rows:
        member, ticker, company, sector, committees, est_amt, mcap, adv, score = r
        lines.append(
            f"• {ticker} — {company} ({sector}) | Score {score} | {member} [{committees}] "
            f"| est ${0 if est_amt is None else est_amt:,.0f} | mcap ${0 if mcap is None else mcap:,.0f}M | ADV ${0 if adv is None else adv:,.0f}M"
        )
    text = "\n".join(lines)
    print(text)
    if SLACK_WEBHOOK_URL:
        send_slack(text)

def run_scoring_and_alerts():
    conn = get_conn()
    try:
        enrich_liquidity(conn, max_tickers=50)
    except Exception as e:
        print(f"[WARN] Liquidity enrichment skipped: {e}")
    compute_component_scores(conn)
    emit_alerts(conn)
    conn.close()
