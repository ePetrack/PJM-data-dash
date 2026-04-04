"""
fetch_pjm.py — PJM Data Miner 2 ingest script (optional, requires API key)

Fetches one or more PJM API feeds and writes the results as Parquet files
under the data/ directory.  Requires a registered API key from PJM.

NOTE: For zero-auth data ingest, use backfill_eia.py instead.  This script
is only needed if you want load forecasts or higher-frequency data that
EIA does not provide.

Usage:
    PJM_API_KEY=your-key python scripts/fetch_pjm.py --date yesterday --output data/

Environment:
    PJM_API_KEY   required — register free at https://apiportal.pjm.com
"""

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.pjm.com/api/v1"
MAX_ROWS = 50_000
RATE_LIMIT_SLEEP = 1  # seconds between requests (registered keys allow 600 req/min)
RETRIES = 3  # transient-error retries with exponential backoff

# Feeds to ingest and their subdirectory names
FEEDS = {
    "da_lmps": {
        "feed": "da_hrl_lmps",
        "params": {"type": "ZONE"},
        "description": "Day-ahead hourly LMPs (all zones)",
    },
    "rt_lmps": {
        "feed": "rt_hrl_lmps",
        "params": {"type": "ZONE"},
        "description": "Real-time hourly LMPs (all zones)",
    },
    "load": {
        "feed": "hrl_load_metered",
        "params": {},
        "description": "Hourly metered load",
    },
    "load_forecast": {
        "feed": "load_frcstd_7_day",
        "params": {"forecast_area": "DUQ"},
        "description": "7-day load forecast for DUQ zone",
    },
}


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def _request_with_retry(method, *args, retries: int = RETRIES, **kwargs):
    """Call *method*(*args, **kwargs) with exponential-backoff retry on transient errors."""
    for attempt in range(retries):
        try:
            resp = method(*args, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"    retrying in {wait}s ({e})")
                time.sleep(wait)
            else:
                raise


def get_subscription_key() -> str:
    """Return the PJM API subscription key from the environment."""
    env_key = os.environ.get("PJM_API_KEY")
    if env_key:
        return env_key
    print("ERROR: PJM_API_KEY environment variable is not set.")
    print()
    print("  Register for a free API key at: https://apiportal.pjm.com")
    print("  Then: export PJM_API_KEY=your-key-here")
    print()
    print("  For zero-auth data ingest, use backfill_eia.py instead:")
    print("    python scripts/backfill_eia.py --output data/")
    sys.exit(1)


def fetch_feed(
    session: requests.Session,
    feed_name: str,
    date_param: str,
    extra_params: dict,
) -> list[dict]:
    """
    Fetch all rows for a feed + date range, paginating as needed.
    Returns a flat list of row dicts.
    """
    rows: list[dict] = []
    start_row = 1

    while True:
        params = {
            "startRow": start_row,
            "rowCount": MAX_ROWS,
            "datetime_beginning_ept": date_param,
            **extra_params,
        }
        resp = _request_with_retry(
            session.get, f"{BASE_URL}/{feed_name}", params=params, timeout=30
        )

        data = resp.json()

        # The API wraps results differently depending on the endpoint
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("items", data.get("data", []))
        else:
            batch = []

        rows.extend(batch)

        total = int(resp.headers.get("X-TotalRows", len(rows)))
        if len(rows) >= total or len(batch) < MAX_ROWS:
            break

        start_row += MAX_ROWS
        time.sleep(RATE_LIMIT_SLEEP)

    return rows


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def parse_date(value: str) -> date:
    if value.lower() == "yesterday":
        return date.today() - timedelta(days=1)
    if value.lower() == "today":
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def format_date_param(start: date, end: date) -> str:
    if start == end:
        return start.strftime("%-m/%-d/%Y")
    return (
        f"{start.strftime('%-m/%-d/%Y')} 00:00 to "
        f"{end.strftime('%-m/%-d/%Y')} 23:00"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch PJM market data to Parquet")
    parser.add_argument("--date", default="yesterday", help="Start date (YYYY-MM-DD or 'yesterday')")
    parser.add_argument("--to", default=None, help="End date (YYYY-MM-DD, defaults to --date)")
    parser.add_argument("--output", default="data", help="Output directory")
    parser.add_argument("--feeds", nargs="+", choices=list(FEEDS.keys()), default=list(FEEDS.keys()),
                        help="Which feeds to fetch (default: all)")
    args = parser.parse_args()

    start_dt = parse_date(args.date)
    end_dt = parse_date(args.to) if args.to else start_dt
    date_param = format_date_param(start_dt, end_dt)
    output_dir = Path(args.output)

    print(f"Fetching PJM data for: {date_param}")

    key = get_subscription_key()
    session = requests.Session()
    session.headers.update({"Ocp-Apim-Subscription-Key": key})

    for feed_key in args.feeds:
        cfg = FEEDS[feed_key]
        feed_dir = output_dir / feed_key
        feed_dir.mkdir(parents=True, exist_ok=True)

        print(f"  [{feed_key}] {cfg['description']}...", end=" ", flush=True)
        try:
            rows = fetch_feed(session, cfg["feed"], date_param, cfg["params"])
        except requests.HTTPError as exc:
            print(f"ERROR: {exc}")
            sys.exit(1)

        if not rows:
            print("0 rows — skipping")
            continue

        df = pd.DataFrame(rows)
        out_file = feed_dir / f"{start_dt.isoformat()}_{end_dt.isoformat()}.parquet"
        df.to_parquet(out_file, index=False)
        print(f"{len(df):,} rows → {out_file}")

        # Respect rate limit between feeds
        time.sleep(RATE_LIMIT_SLEEP)

    print("Done.")


if __name__ == "__main__":
    main()
