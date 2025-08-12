import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/congress.db")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
QUIVER_API_KEY = os.getenv("QUIVER_API_KEY", "")

MIN_AVG_DOLLAR_VOLUME_MILLIONS = float(os.getenv("MIN_AVG_DOLLAR_VOLUME_MILLIONS", "1"))
MAX_MARKET_CAP_MILLIONS = float(os.getenv("MAX_MARKET_CAP_MILLIONS", "10000"))
SIGNAL_THRESHOLD = float(os.getenv("SIGNAL_THRESHOLD", "70"))
