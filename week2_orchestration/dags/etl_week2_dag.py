"""
etl_week2_dag.py
StoryPoints AI — Week 2 Data Orchestration Pipeline

DAG Flow:
    ingest_clickstream
    ingest_transactions  → transform → validate → load → track_metadata
    ingest_currency_api

Author: StoryPoints AI Internship Week 2
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ─── Add Week 1 scripts to path ───────────────────────────────────────────────
sys.path.insert(0, "/opt/airflow/scripts")

# ─── Load config from environment variables ───────────────────────────────────
API_KEY         = os.getenv("EXCHANGE_RATE_API_KEY", "")
BASE_PATH       = os.getenv("BASE_PATH", "/opt/airflow")
ALERT_EMAIL     = os.getenv("ALERT_EMAIL", "admin@storypoints.com")
TODAY           = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ─── File paths ───────────────────────────────────────────────────────────────
RAW_CLICKSTREAM   = f"{BASE_PATH}/data/raw/clickstream/clickstream.csv"
RAW_TRANSACTIONS  = f"{BASE_PATH}/data/raw/transactions/transactions.csv"
RAW_CURRENCY_DIR  = f"{BASE_PATH}/data/raw/api_currency/{TODAY}"
PROCESSED_DIR     = f"{BASE_PATH}/data/processed"
METADATA_PATH     = f"{BASE_PATH}/week2_orchestration/metadata/run_log.csv"
ALERTS_LOG        = f"{BASE_PATH}/week2_orchestration/logs/alerts.log"

# ─── Default DAG arguments ────────────────────────────────────────────────────
default_args = {
    "owner": "storypoints_de",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# ─── Setup logging ────────────────────────────────────────────────────────────
os.makedirs(f"{BASE_PATH}/week2_orchestration/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{BASE_PATH}/week2_orchestration/logs/dag_{TODAY}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def ingest_clickstream(**context):
    """
    Task 1: Ingest clickstream CSV in chunks of 50,000 rows.
    Pushes row count to XCom for downstream tasks.
    """
    log.info("=" * 60)
    log.info("TASK: ingest_clickstream")
    log.info("=" * 60)

    if not os.path.exists(RAW_CLICKSTREAM):
        _log_alert(f"MISSING FILE: {RAW_CLICKSTREAM}")
        raise FileNotFoundError(f"Clickstream file not found: {RAW_CLICKSTREAM}")

    chunks = []
    total_rows = 0
    chunk_size = 50_000

    for i, chunk in enumerate(pd.read_csv(RAW_CLICKSTREAM, chunksize=chunk_size)):
        chunks.append(chunk)
        total_rows += len(chunk)
        log.info(f"   Chunk {i+1}: {len(chunk):,} rows (total: {total_rows:,})")

    df = pd.concat(chunks, ignore_index=True)

    # Save raw copy for transform stage
    os.makedirs(f"{BASE_PATH}/data/staging", exist_ok=True)
    df.to_csv(f"{BASE_PATH}/data/staging/clickstream_raw.csv", index=False)

    log.info(f"✅ Clickstream ingested: {total_rows:,} rows, {len(df.columns)} columns")

    # Push to XCom for metadata tracking
    context["ti"].xcom_push(key="clickstream_rows", value=total_rows)
    return total_rows


def ingest_transactions(**context):
    """
    Task 2: Ingest transactions CSV in chunks of 50,000 rows.
    Pushes row count to XCom for downstream tasks.
    """
    log.info("=" * 60)
    log.info("TASK: ingest_transactions")
    log.info("=" * 60)

    if not os.path.exists(RAW_TRANSACTIONS):
        _log_alert(f"MISSING FILE: {RAW_TRANSACTIONS}")
        raise FileNotFoundError(f"Transactions file not found: {RAW_TRANSACTIONS}")

    chunks = []
    total_rows = 0
    chunk_size = 50_000

    for i, chunk in enumerate(pd.read_csv(RAW_TRANSACTIONS, chunksize=chunk_size)):
        chunks.append(chunk)
        total_rows += len(chunk)
        log.info(f"   Chunk {i+1}: {len(chunk):,} rows (total: {total_rows:,})")

    df = pd.concat(chunks, ignore_index=True)

    os.makedirs(f"{BASE_PATH}/data/staging", exist_ok=True)
    df.to_csv(f"{BASE_PATH}/data/staging/transactions_raw.csv", index=False)

    log.info(f"✅ Transactions ingested: {total_rows:,} rows, {len(df.columns)} columns")

    context["ti"].xcom_push(key="transactions_rows", value=total_rows)
    return total_rows


def ingest_currency_api(**context):
    """
    Task 3: Fetch live USD currency rates from ExchangeRate API.
    Implements retry with exponential backoff.
    Saves raw JSON to data/raw/api_currency/YYYY-MM-DD/rates.json
    """
    import json
    import time
    import requests

    log.info("=" * 60)
    log.info("TASK: ingest_currency_api")
    log.info("=" * 60)

    api_key = API_KEY
    if not api_key:
        _log_alert("MISSING API KEY: EXCHANGE_RATE_API_KEY not set")
        raise ValueError("EXCHANGE_RATE_API_KEY not set in Airflow Variables or environment")

    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"   Fetching currency rates (attempt {attempt}/{max_retries})...")
            response = requests.get(url, timeout=10)
            data = response.json()

            if response.status_code == 200 and data.get("result") == "success":
                rates = data["conversion_rates"]
                os.makedirs(RAW_CURRENCY_DIR, exist_ok=True)
                out_path = os.path.join(RAW_CURRENCY_DIR, "rates.json")
                with open(out_path, "w") as f:
                    json.dump(data, f, indent=2)
                log.info(f"✅ Fetched {len(rates)} currency rates → saved to {out_path}")
                context["ti"].xcom_push(key="currency_pairs", value=len(rates))
                return rates

            log.warning(f"   API error: {data.get('error-type', 'unknown')}")

        except requests.exceptions.RequestException as e:
            log.error(f"   Request failed: {e}")

        wait = 2 ** attempt
        log.info(f"   Retrying in {wait}s...")
        time.sleep(wait)

    _log_alert("API FAILURE: All currency rate fetch attempts failed")
    log.warning("⚠️  Currency API failed — pipeline continues without rates")
    context["ti"].xcom_push(key="currency_pairs", value=0)
    return {}


def transform(**context):
    """
    Task 4: Apply transformations to ingested data.
    - Standardize column names to snake_case
    - Convert timestamps to UTC
    - Deduplicate rows
    - Enrich transactions with amount_in_usd
    """
    import json

    log.info("=" * 60)
    log.info("TASK: transform")
    log.info("=" * 60)

    # Load currency rates
    rates_path = os.path.join(RAW_CURRENCY_DIR, "rates.json")
    currency_rates = {}
    if os.path.exists(rates_path):
        with open(rates_path) as f:
            data = json.load(f)
            currency_rates = data.get("conversion_rates", {})
        log.info(f"   Loaded {len(currency_rates)} currency rates")

    results = {}

    # Transform clickstream
    staging_path = f"{BASE_PATH}/data/staging/clickstream_raw.csv"
    if os.path.exists(staging_path):
        df = pd.read_csv(staging_path)
        original = len(df)

        # Standardize columns
        df.columns = (df.columns.str.strip().str.lower()
            .str.replace(r"[\s\-]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True))

        # Convert timestamps to UTC
        ts_cols = [c for c in df.columns if any(x in c for x in ["time","date","timestamp"])]
        for col in ts_cols:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # Deduplicate
        df = df.drop_duplicates()
        df["ingest_date"] = TODAY

        df.to_csv(f"{BASE_PATH}/data/staging/clickstream_clean.csv", index=False)
        results["clickstream"] = {"original": original, "clean": len(df)}
        log.info(f"✅ Clickstream: {original:,} → {len(df):,} rows")

    # Transform transactions
    staging_path = f"{BASE_PATH}/data/staging/transactions_raw.csv"
    if os.path.exists(staging_path):
        df = pd.read_csv(staging_path)
        original = len(df)

        # Standardize columns
        df.columns = (df.columns.str.strip().str.lower()
            .str.replace(r"[\s\-]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True))

        # Convert timestamps to UTC
        ts_cols = [c for c in df.columns if any(x in c for x in ["time","date","timestamp"])]
        for col in ts_cols:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # Deduplicate
        df = df.drop_duplicates()

        # Enrich with amount_in_usd
        if currency_rates:
            currency_col = next((c for c in df.columns if "currency" in c), None)
            amount_col = next((c for c in df.columns if "amount" in c), None)
            if currency_col and amount_col:
                df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
                def to_usd(row):
                    cur = str(row[currency_col]).upper()
                    amt = row[amount_col]
                    if pd.isna(amt): return None
                    if cur == "USD": return round(amt, 4)
                    rate = currency_rates.get(cur)
                    return round(amt / rate, 4) if rate else None
                df["amount_in_usd"] = df.apply(to_usd, axis=1)
                log.info(f"   💱 amount_in_usd enriched for {df['amount_in_usd'].notna().sum():,} rows")

        df["ingest_date"] = TODAY
        df.to_csv(f"{BASE_PATH}/data/staging/transactions_clean.csv", index=False)
        results["transactions"] = {"original": original, "clean": len(df)}
        log.info(f"✅ Transactions: {original:,} → {len(df):,} rows")

    context["ti"].xcom_push(key="transform_results", value=str(results))
    return results


def validate(**context):
    """
    Task 5: Validate cleaned data before loading.
    Checks:
    - No null values in critical columns
    - Positive transaction amounts
    - Valid ISO currency codes
    - Row count within expected range
    Raises exception if critical validation fails.
    """
    log.info("=" * 60)
    log.info("TASK: validate")
    log.info("=" * 60)

    # Import validation module
    sys.path.insert(0, "/opt/airflow/week2_orchestration/validation")
    from validation import validate_clickstream, validate_transactions

    validation_results = {}
    all_passed = True

    # Validate clickstream
    clean_path = f"{BASE_PATH}/data/staging/clickstream_clean.csv"
    if os.path.exists(clean_path):
        df = pd.read_csv(clean_path)
        passed, issues = validate_clickstream(df)
        validation_results["clickstream"] = {"passed": passed, "issues": issues}
        if not passed:
            all_passed = False
            for issue in issues:
                _log_alert(f"VALIDATION FAILED - Clickstream: {issue}")
                log.error(f"❌ Clickstream validation: {issue}")
        else:
            log.info(f"✅ Clickstream validation passed")

    # Validate transactions
    clean_path = f"{BASE_PATH}/data/staging/transactions_clean.csv"
    if os.path.exists(clean_path):
        df = pd.read_csv(clean_path)
        passed, issues = validate_transactions(df)
        validation_results["transactions"] = {"passed": passed, "issues": issues}
        if not passed:
            all_passed = False
            for issue in issues:
                _log_alert(f"VALIDATION FAILED - Transactions: {issue}")
                log.error(f"❌ Transactions validation: {issue}")
        else:
            log.info(f"✅ Transactions validation passed")

    context["ti"].xcom_push(key="validation_results", value=str(validation_results))

    if not all_passed:
        raise ValueError(f"Validation failed! Check alerts.log for details.")

    log.info("✅ All validations passed!")
    return validation_results


def load(**context):
    """
    Task 6: Load validated clean data to partitioned output folders.
    Path: data/processed/<dataset>/ingest_date=YYYY-MM-DD/data.csv
    Idempotent: re-running overwrites existing partition.
    """
    log.info("=" * 60)
    log.info("TASK: load")
    log.info("=" * 60)

    loaded = {}

    for dataset in ["clickstream", "transactions"]:
        staging_path = f"{BASE_PATH}/data/staging/{dataset}_clean.csv"
        if not os.path.exists(staging_path):
            log.warning(f"⚠️  No clean data found for {dataset}")
            continue

        df = pd.read_csv(staging_path)
        partition_path = os.path.join(PROCESSED_DIR, dataset, f"ingest_date={TODAY}")
        os.makedirs(partition_path, exist_ok=True)
        out_file = os.path.join(partition_path, "data.csv")
        df.to_csv(out_file, index=False)

        size_kb = os.path.getsize(out_file) / 1024
        log.info(f"✅ Loaded {dataset}: {len(df):,} rows → {out_file} ({size_kb:.1f} KB)")
        loaded[dataset] = len(df)

    context["ti"].xcom_push(key="loaded_rows", value=str(loaded))
    return loaded


def track_metadata(**context):
    """
    Task 7: Track pipeline metadata — rows ingested, transformed,
    loaded, validation status, and run duration.
    Appends to metadata/run_log.csv.
    """
    import csv

    log.info("=" * 60)
    log.info("TASK: track_metadata")
    log.info("=" * 60)

    ti = context["ti"]

    # Pull XCom values from upstream tasks
    clickstream_rows  = ti.xcom_pull(task_ids="ingest_clickstream", key="clickstream_rows") or 0
    transactions_rows = ti.xcom_pull(task_ids="ingest_transactions", key="transactions_rows") or 0
    currency_pairs    = ti.xcom_pull(task_ids="ingest_currency_api", key="currency_pairs") or 0
    validation_results = ti.xcom_pull(task_ids="validate", key="validation_results") or "{}"
    loaded_rows       = ti.xcom_pull(task_ids="load", key="loaded_rows") or "{}"

    metadata = {
        "run_date": TODAY,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "clickstream_rows_ingested": clickstream_rows,
        "transactions_rows_ingested": transactions_rows,
        "currency_pairs_fetched": currency_pairs,
        "validation_status": "PASSED" if "False" not in str(validation_results) else "FAILED",
        "loaded_rows": loaded_rows,
        "status": "SUCCESS",
    }

    os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)
    file_exists = os.path.exists(METADATA_PATH)

    with open(METADATA_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=metadata.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(metadata)

    log.info(f"✅ Metadata tracked → {METADATA_PATH}")
    log.info(f"   Run summary: {metadata}")
    return metadata


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _log_alert(message: str):
    """Log alert messages to alerts.log file."""
    os.makedirs(os.path.dirname(ALERTS_LOG), exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(ALERTS_LOG, "a") as f:
        f.write(f"{timestamp} | ALERT | {message}\n")
    log.warning(f"🚨 ALERT logged: {message}")


def on_failure_callback(context):
    """Called automatically when any task fails."""
    task_id = context["task_instance"].task_id
    dag_id  = context["dag"].dag_id
    error   = context.get("exception", "Unknown error")
    message = f"DAG={dag_id} | TASK={task_id} | ERROR={error}"
    _log_alert(message)
    log.error(f"❌ Task failed: {message}")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="storypoints_etl_week2",
    description="StoryPoints AI Week 2 — ETL Pipeline with Validation & Monitoring",
    default_args=default_args,
    schedule_interval="0 9 * * *",   # Run daily at 9am UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["storypoints", "etl", "week2"],
    on_failure_callback=on_failure_callback,
) as dag:

    # ── Task 1: Ingest Clickstream ─────────────────────────────────────────
    t_ingest_clickstream = PythonOperator(
        task_id="ingest_clickstream",
        python_callable=ingest_clickstream,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 2: Ingest Transactions ───────────────────────────────────────
    t_ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_transactions,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 3: Ingest Currency API ───────────────────────────────────────
    t_ingest_currency = PythonOperator(
        task_id="ingest_currency_api",
        python_callable=ingest_currency_api,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 4: Transform ─────────────────────────────────────────────────
    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 5: Validate ──────────────────────────────────────────────────
    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 6: Load ──────────────────────────────────────────────────────
    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 7: Track Metadata ────────────────────────────────────────────
    t_metadata = PythonOperator(
        task_id="track_metadata",
        python_callable=track_metadata,
        trigger_rule="all_done",   # Always runs even if upstream fails
        on_failure_callback=on_failure_callback,
    )

    # ── DAG Dependencies (Task Flow) ──────────────────────────────────────
    #
    #  ingest_clickstream ──┐
    #  ingest_transactions ─┤→ transform → validate → load → track_metadata
    #  ingest_currency_api ─┘
    #
    [t_ingest_clickstream, t_ingest_transactions, t_ingest_currency] >> t_transform
    t_transform >> t_validate >> t_load >> t_metadata