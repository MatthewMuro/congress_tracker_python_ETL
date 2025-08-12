# Congressional Trade Tracker — Option C (Python ETL)

A small, production-ready pipeline to ingest Congressional STOCK Act disclosures from APIs (e.g., QuiverQuant / Unusual Whales), 
normalize & store them in SQLite, compute a composite **Signal Score**, and send **Slack alerts** for the best ideas.

## Features
- **SQLite** storage with raw and cleaned tables; dedup by hash
- **Scoring**: Track Record, Committee Relevance, Size vs member median, Cluster effect (30d same-ticker)
- **Filters**: Market cap / ADV guardrails
- **Alerts**: Slack webhook for high-score signals
- **Scheduling**: GitHub Actions nightly cron (free), or run locally via cron

## Quick Start
1. **Clone & install**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure env**
   Copy `.env.example` to `.env` and fill in values:
   - `DATABASE_URL` (default ok for SQLite)
   - `SLACK_WEBHOOK_URL` (create an Incoming Webhook in Slack)
   - `QUIVER_API_KEY` (optional for live data; otherwise sample data is used)

3. **Run once locally**
   ```bash
   python run_daily.py --days 2
   ```

4. **Schedule (GitHub Actions)**
   - Commit to a private repo.
   - Add repository secrets for env variables.
   - The provided workflow `.github/workflows/daily.yml` runs nightly UTC and posts alerts.

## Data Model (SQLite)
Tables:
- `trades_raw` — raw rows as JSON, with `source` and `row_hash`
- `trades_clean` — normalized schema (member/ticker/amounts/dates/committees/etc.); deduped by `row_hash`
- `members` — per-member metadata (party, chamber, committees, long-term track record metrics)
- `scores_daily` — per-day, per-row signal components + composite score

You can explore the DB using `sqlite-utils` or any SQLite browser.

## Signal Score
```
Signal = 0.40*TrackRecord + 0.20*CommitteeRelevance + 0.20*SizeScore + 0.20*ClusterScore
```
Range: 0–100. Alert threshold default: 70.

## Notes
- This repo ships with **sample data** so it runs out-of-the-box without API keys.
- Replace `sources/quiver.py` with live API calls once you have keys.
- Extend with additional sources by dropping in a new file under `src/sources/` and wiring it in `etl/ingest.py`.

