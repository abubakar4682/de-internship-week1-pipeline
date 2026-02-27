"""
ingest.py
Task 2: Extract data from CSVs (in chunks) and fetch live currency rates from API.
Saves raw outputs to data/raw/ partitioned folders.
"""

import os
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
API_KEY       = os.getenv("EXCHANGE_RATE_API_KEY")
API_URL       = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
CHUNK_SIZE    = 50_000
TODAY         = datetime.now(timezone.utc).strftime("%Y-%m-%d")
MAX_RETRIES   = 3

RAW_CLICKSTREAM   = "data/raw/clickstream/clickstream.csv"
RAW_TRANSACTIONS  = "data/raw/transactions/transactions.csv"
RAW_CURRENCY_DIR  = f"data/raw/api_currency/{TODAY}"

# ─── Logging setup ────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
os.makedirs(RAW_CURRENCY_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/ingest_{TODAY}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ─── Task 2a: Read CSV in chunks ──────────────────────────────────────────────
def ingest_csv(source_path: str, dataset_name: str) -> pd.DataFrame:
    """Read a large CSV file in chunks and return a combined DataFrame."""
    if not os.path.exists(source_path):
        log.warning(f"⚠️  Missing input file: {source_path} — skipping {dataset_name}")
        return pd.DataFrame()

    chunks = []
    total_rows = 0

    log.info(f"📂 Reading {dataset_name} in chunks of {CHUNK_SIZE:,} rows...")
    for i, chunk in enumerate(pd.read_csv(source_path, chunksize=CHUNK_SIZE)):
        chunks.append(chunk)
        total_rows += len(chunk)
        log.info(f"   Chunk {i+1}: {len(chunk):,} rows loaded (total so far: {total_rows:,})")

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"✅ {dataset_name} ingested: {total_rows:,} total rows, {len(df.columns)} columns")
    return df


# ─── Task 2b: Fetch currency rates with retry + exponential backoff ───────────
def fetch_currency_rates() -> dict:
    """Fetch live USD conversion rates from ExchangeRate-API with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"🌐 Fetching currency rates (attempt {attempt}/{MAX_RETRIES})...")
            response = requests.get(API_URL, timeout=10)
            data = response.json()

            if response.status_code == 200 and data.get("result") == "success":
                rates = data["conversion_rates"]
                log.info(f"✅ Currency rates fetched: {len(rates)} currencies")
                log.info(f"   Sample — USD→INR: {rates.get('INR')}, USD→EUR: {rates.get('EUR')}, USD→GBP: {rates.get('GBP')}")

                # Save raw JSON response
                out_path = os.path.join(RAW_CURRENCY_DIR, "rates.json")
                with open(out_path, "w") as f:
                    json.dump(data, f, indent=2)
                log.info(f"💾 Raw currency JSON saved to: {out_path}")
                return rates

            else:
                log.warning(f"⚠️  API returned error: {data}")

        except requests.exceptions.RequestException as e:
            log.error(f"❌ Request failed on attempt {attempt}: {e}")

        # Exponential backoff: 2s, 4s, 8s
        wait = 2 ** attempt
        log.info(f"   Retrying in {wait}s...")
        time.sleep(wait)

    log.error("❌ All API attempts failed. Currency rates unavailable.")
    return {}


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info(f"🚀 Starting ingestion pipeline — {TODAY}")
    log.info("=" * 60)

    # Ingest CSVs
    clickstream_df   = ingest_csv(RAW_CLICKSTREAM, "Clickstream")
    transactions_df  = ingest_csv(RAW_TRANSACTIONS, "Transactions")

    # Fetch currency rates
    currency_rates = fetch_currency_rates()

    # Summary
    log.info("\n📊 Ingestion Summary:")
    log.info(f"   Clickstream rows  : {len(clickstream_df):,}")
    log.info(f"   Transactions rows : {len(transactions_df):,}")
    log.info(f"   Currency pairs    : {len(currency_rates)}")

    if len(clickstream_df) == 0:
        log.warning("⚠️  ALERT: Clickstream data is empty!")
    if len(transactions_df) == 0:
        log.warning("⚠️  ALERT: Transactions data is empty!")
    if not currency_rates:
        log.warning("⚠️  ALERT: Currency rates unavailable — USD enrichment will be skipped!")

    log.info("✅ Ingestion complete. Returning data for transformation.\n")
    return clickstream_df, transactions_df, currency_rates


if __name__ == "__main__":
    main()