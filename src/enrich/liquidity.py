# src/enrich/liquidity.py
import yfinance as yf

def enrich_liquidity(conn, max_tickers: int = 50):
    """
    Look up market cap and approx dollar ADV for recent tickers with missing data,
    then write back into trades_clean.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ticker
        FROM trades_clean
        WHERE (market_cap_m IS NULL OR adv_m IS NULL)
          AND ticker IS NOT NULL AND ticker != ''
        ORDER BY rowid DESC
        LIMIT ?
    """, (max_tickers,))
    tickers = [t[0] for t in cur.fetchall()]
    if not tickers:
        return

    for t in tickers:
        try:
            info = yf.Ticker(t).fast_info
            mcap = info.get("market_cap") or info.get("marketCap")
            price = info.get("last_price") or info.get("lastPrice")
            avgvol = (info.get("ten_day_average_volume") or info.get("tenDayAverageVolume")
                      or info.get("average_volume") or info.get("averageVolume"))
            mcap_m = float(mcap) / 1_000_000.0 if mcap else None
            adv_m = (float(price) * float(avgvol) / 1_000_000.0) if (price and avgvol) else None
        except Exception:
            mcap_m, adv_m = None, None

        cur.execute("""
            UPDATE trades_clean
               SET market_cap_m = COALESCE(?, market_cap_m),
                   adv_m        = COALESCE(?, adv_m)
             WHERE ticker = ?
        """, (mcap_m, adv_m, t))
    conn.commit()
