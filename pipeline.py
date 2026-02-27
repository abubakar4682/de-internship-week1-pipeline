"""
pipeline.py — Master ETL runner
Run: python pipeline.py
"""

import os
import sys
import logging
from datetime import datetime, timezone

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/pipeline_{TODAY}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── This is the fix — point to scripts/ subfolder ───
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))

from ingest    import main as ingest_main
from transform import main as transform_main
from load      import main as load_main


def run():
    log.info("🚀 StoryPoints AI — Week 1 ETL Pipeline Starting")
    log.info(f"   Run date (UTC): {TODAY}\n")

    try:
        log.info("━" * 60)
        log.info("STAGE 1: INGEST")
        log.info("━" * 60)
        clickstream_raw, transactions_raw, currency_rates = ingest_main()

        log.info("\n" + "━" * 60)
        log.info("STAGE 2: TRANSFORM")
        log.info("━" * 60)
        clean_clickstream, clean_transactions, clean_currency = transform_main(
            clickstream_raw, transactions_raw, currency_rates
        )

        log.info("\n" + "━" * 60)
        log.info("STAGE 3: LOAD")
        log.info("━" * 60)
        results = load_main(clean_clickstream, clean_transactions, clean_currency)

        log.info("\n✅ Pipeline completed successfully!")
        log.info(f"📁 Outputs: data/processed/")
        log.info(f"📝 Logs:    logs/pipeline_{TODAY}.log")

    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run()