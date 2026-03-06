📌 Overview
A production-style ETL (Extract, Transform, Load) pipeline built for StoryPoints AI's Data Engineering Internship. The project spans two weeks:

Week 1 — Build ETL scripts to ingest, transform, and load clickstream and transaction data
Week 2 — Orchestrate the pipeline using Apache Airflow with data validation, metadata tracking, and monitoring


🗂️ Project Structure
storypoints-week1/
├── pipeline.py                        # Week 1: Master ETL runner
├── README.md
├── .env                               # API keys (not committed)
├── .gitignore
├── scripts/
│   ├── ingest.py                      # Extract CSVs + API
│   ├── transform.py                   # Clean + enrich data
│   ├── load.py                        # Save + log
│   ├── explore.py                     # Schema exploration
│   └── setup_folders.py              # Project structure setup
├── week2_orchestration/
│   ├── dags/
│   │   └── etl_week2_dag.py          # Airflow DAG (7 tasks)
│   ├── validation/
│   │   └── validation.py             # Data quality checks
│   ├── metadata/
│   │   ├── metadata_tracker.py       # Metadata tracking
│   │   └── run_log.csv               # Pipeline run history
│   ├── logs/
│   │   ├── dag_YYYY-MM-DD.log        # Daily DAG logs
│   │   └── alerts.log                # Pipeline failure alerts
│   └── docker-compose.yaml           # Airflow Docker setup
├── data/
│   ├── raw/
│   │   ├── clickstream/clickstream.csv
│   │   ├── transactions/transactions.csv
│   │   └── api_currency/YYYY-MM-DD/rates.json
│   └── processed/
│       ├── clickstream/ingest_date=YYYY-MM-DD/data.csv
│       ├── transactions/ingest_date=YYYY-MM-DD/data.csv
│       └── currency/ingest_date=YYYY-MM-DD/data.csv
└── logs/
    └── pipeline_YYYY-MM-DD.log

WEEK 1 — ETL Pipeline
Architecture
DATA SOURCES
  clickstream.csv (200,000 rows)
  transactions.csv (100,000 rows)
  ExchangeRate API
        |
        v
STAGE 1: INGEST
  - Read CSVs in 50,000-row chunks
  - Fetch live currency rates via REST API
  - Retry logic with exponential backoff
        |
        v
STAGE 2: TRANSFORM
  - Standardize column names to snake_case
  - Convert all timestamps to UTC
  - Deduplicate rows
  - Enrich transactions with amount_in_usd
        |
        v
STAGE 3: LOAD
  - Write to partitioned output folders
  - Path: data/processed/<dataset>/ingest_date=YYYY-MM-DD/
  - Log record counts, file sizes, pipeline summary
Dataset Understanding (Task 1)
clickstream.csv
ColumnTypeDescriptionuser_idint64Unique user identifier (1 to 50,000)session_idstringUUID identifying a user sessionpage_urlstringPage visited (e.g. /home, /checkout)click_timestring to UTC datetimeTimestamp of the click eventdevicestringDevice type (desktop / mobile)locationstring2-letter country code (e.g. AU, GB, DE)
Shape: 200,000 rows x 6 columns | Nulls: None | Duplicates: None
transactions.csv
ColumnTypeDescriptiontxn_idstringUUID for each transactionuser_idint64Unique user identifier (1 to 50,000)amountfloat64Transaction amount in original currencycurrencystringISO currency code (USD, GBP, EUR, etc.)txn_timestring to UTC datetimeTimestamp of the transaction
Shape: 100,000 rows x 5 columns | Nulls: None | Duplicates: None | Amount range: $3.91 to $43,497.95
How to Run Week 1
bashgit clone <your-repo-url>
cd storypoints-week1
python3 -m venv venv
source venv/bin/activate
pip install pandas requests pyarrow python-dotenv

# Explore data
python scripts/explore.py

# Run full pipeline
python pipeline.py

WEEK 2 — Orchestration, Validation & Monitoring
DAG Architecture
ingest_clickstream  --|
ingest_transactions --|-> transform -> validate -> load -> track_metadata
ingest_currency_api --|
DAG Details
PropertyValueDAG IDstorypoints_etl_week2ScheduleDaily at 9:00 AM UTCTotal Tasks7 PythonOperatorsRetries3 (with 5 min delay)CatchupFalseTrigger Ruleall_success (track_metadata: all_done)
Task Descriptions
TaskDescriptioningest_clickstreamReads clickstream CSV in 50k chunks, saves to stagingingest_transactionsReads transactions CSV in 50k chunks, saves to stagingingest_currency_apiFetches live USD rates from ExchangeRate API with retrytransformCleans columns, converts timestamps, deduplicates, enrichesvalidateRuns 7 data quality checks before loadingloadSaves to partitioned folders (idempotent)track_metadataRecords run stats to run_log.csv (always runs)

Data Validation Rules (Task 3)
Clickstream Validations
CheckRuleRow countMust be > 0Null checkNo nulls in user_id, session_id, page_urluser_idMust be positive integerDevice typeMust be desktop, mobile, or tabletDuplicatesNo duplicate session_id + page_url combinations
Transaction Validations
CheckRuleRow countMust be > 0Null checkNo nulls in txn_id, user_id, amount, currencyAmountsAll amounts must be greater than 0Currency codesMust be valid ISO 4217 codesDuplicate IDsNo duplicate transaction IDs

Metadata Tracking (Task 4)
Every pipeline run is tracked in week2_orchestration/metadata/run_log.csv:
ColumnDescriptionrun_dateDate of pipeline runrun_timestampExact UTC timestampclickstream_rows_ingestedRows ingested from clickstreamtransactions_rows_ingestedRows ingested from transactionscurrency_pairs_fetchedNumber of currency rates fetchedvalidation_statusPASSED or FAILEDloaded_rowsRows loaded per datasetstatusSUCCESS or FAILED

Monitoring & Alerts (Task 5)
Every task has an on_failure_callback that logs to alerts.log:
2026-03-05T22:00:00+00:00 | ALERT | DAG=storypoints_etl_week2 | TASK=ingest_clickstream | ERROR=FileNotFoundError
Email alerts configured via default_args with 3 retries and 5 minute delay between retries.

How to Run Week 2
bashcd storypoints-week1/week2_orchestration

# Start Airflow
docker compose up airflow-init
docker compose up -d

# Access UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow

Tech Stack
ToolPurposePython 3.11Core languagePandasData processingApache Airflow 2.8.0Pipeline orchestrationDockerContainerized AirflowRequestsREST API callspython-dotenvSecret managementGitVersion control

Key Design Decisions

Chunked CSV reading — 50,000-row chunks to avoid memory issues
UTC standardization — all timestamps in UTC for global consistency
Exponential backoff — API retries with 2s, 4s, 8s delays
Hive-style partitioning — ingest_date=YYYY-MM-DD/ folder naming
Idempotency — reruns safely overwrite existing partitions
track_metadata uses all_done — always records run even on failure
<img width="1463" height="835" alt="Screenshot 2026-03-06 at 9 36 34 pm" src="https://github.com/user-attachments/assets/bcc2f423-8da1-491c-a98c-6a2f9dd16471" />

