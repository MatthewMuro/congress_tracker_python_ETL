import argparse
from src.etl.ingest import run_ingest
from src.scoring.score import run_scoring_and_alerts

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=2, help='Lookback days for fetching trades')
    args = parser.parse_args()
    run_ingest(lookback_days=args.days)
    run_scoring_and_alerts()

if __name__ == "__main__":
    main()
