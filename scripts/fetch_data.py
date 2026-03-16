"""
fetch_data.py — NSE FII/DII Auto-Fetch Script
=============================================
Run by GitHub Actions daily at 6 PM IST (Mon–Fri).
Writes:
  data/latest.json  — today's session data
  data/history.json — running archive of all sessions (up to 60 days)
"""

import requests, json, os, sys
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

NSE_API = "https://www.nseindia.com/api/fiidiiTradeReact"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}


def fetch_nse():
    """Fetch raw FII/DII data from NSE API."""
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = session.get(NSE_API, timeout=25)
    resp.raise_for_status()
    return resp.json()


def transform(raw):
    """Convert NSE raw response to a clean flat dict."""
    out = {
        "date":     "",
        "fii_buy":  0, "fii_sell": 0, "fii_net": 0,
        "dii_buy":  0, "dii_sell": 0, "dii_net": 0,
    }
    for row in raw:
        cat = (row.get("category") or "").upper()
        if "FII" in cat or "FPI" in cat:
            out["fii_buy"]  = float(row.get("buyValue",  0) or 0)
            out["fii_sell"] = float(row.get("sellValue", 0) or 0)
            out["fii_net"]  = float(row.get("netValue",  0) or 0)
            out["date"]     = row.get("date", "")
        elif "DII" in cat:
            out["dii_buy"]  = float(row.get("buyValue",  0) or 0)
            out["dii_sell"] = float(row.get("sellValue", 0) or 0)
            out["dii_net"]  = float(row.get("netValue",  0) or 0)

    out["_updated_at"] = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")
    out["_source"]     = "github-actions"
    return out


def update_history(latest):
    """Append today's data to history.json (keeps last 60 days)."""
    history_path = "data/history.json"
    try:
        with open(history_path) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    # Remove existing entry for same date (avoid duplicates)
    history = [row for row in history if row.get("date") != latest["date"]]
    # Prepend today
    history.insert(0, latest)
    # Keep only last 60 trading days
    history = history[:60]

    with open(history_path, "w") as f:
        json.dump(history, f)

    return history


if __name__ == "__main__":
    print(f"[{datetime.now(IST).strftime('%d-%b-%Y %H:%M IST')}] Fetching NSE FII/DII data...")

    try:
        raw  = fetch_nse()
        data = transform(raw)
    except Exception as e:
        print(f"❌ Fetch failed: {e}", file=sys.stderr)
        sys.exit(1)

    if not data["date"]:
        print("❌ No data returned from NSE (market may be closed).", file=sys.stderr)
        sys.exit(0)  # exit 0 so workflow doesn't fail on holidays

    print(f"✅ Date: {data['date']}  |  FII Net: {data['fii_net']}  |  DII Net: {data['dii_net']}")

    os.makedirs("data", exist_ok=True)

    # Write latest.json
    with open("data/latest.json", "w") as f:
        json.dump(data, f, indent=2)
    print("✅ Written → data/latest.json")

    # Update rolling history
    update_history(data)
    print("✅ Updated → data/history.json")
