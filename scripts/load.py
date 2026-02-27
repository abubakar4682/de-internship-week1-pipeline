"""
load.py
Task 4 & 5: Save cleaned outputs to partitioned folders + logging.
"""

import os
import logging
import pandas as pd
from datetime import datetime, timezone

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
log   = logging.getLogger(__name__)
BASE_OUTPUT = "data/processed"


def save_to_partition(df: pd.DataFrame, dataset_name: str) -> bool:
    if df.empty:
        log.warning(f"⚠️  ALERT: {dataset_name} output is empty — nothing to save!")
        return False

    partition_path = os.path.join(BASE_OUTPUT, dataset_name, f"ingest_date={TODAY}")
    os.makedirs(partition_path, exist_ok=True)
    out_file = os.path.join(partition_path, "data.csv")

    try:
        df.to_csv(out_file, index=False)
        size_kb = os.path.getsize(out_file) / 1024
        log.info(f"✅ Saved {dataset_name}: {len(df):,} rows → {out_file} ({size_kb:.1f} KB)")
        return True
    except Exception as e:
        log.error(f"❌ Failed to save {dataset_name}: {e}")
        return False


def log_summary(results: dict):
    log.info("\n" + "=" * 60)
    log.info("📊 PIPELINE SUMMARY")
    log.info("=" * 60)
    for dataset, (row_count, success) in results.items():
        status = "✅ OK" if success else "❌ FAILED"
        log.info(f"   {status}  {dataset:20s} | {row_count:>10,} rows")
    log.info("=" * 60)


def main(clean_clickstream, clean_transactions, clean_currency):
    log.info("=" * 60)
    log.info("💾 Starting load pipeline")
    log.info("=" * 60)

    results = {}
    for df, name in [
        (clean_clickstream,  "clickstream"),
        (clean_transactions, "transactions"),
        (clean_currency,     "currency"),
    ]:
        success = save_to_partition(df, name)
        results[name] = (len(df), success)

    log_summary(results)
    return results