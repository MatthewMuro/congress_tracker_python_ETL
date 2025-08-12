import json, requests
from ..config import SLACK_WEBHOOK_URL

def send_slack(text: str):
    payload = {"text": text}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    resp.raise_for_status()
