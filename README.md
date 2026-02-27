# StoryPoints AI — Week 1 Data Engineering Pipeline

## 📌 Overview

A production-style **ETL (Extract, Transform, Load)** pipeline built for StoryPoints AI's Week 1 Data Engineering Internship. The pipeline ingests large-scale clickstream and transaction datasets alongside live currency conversion rates, applies real-world data transformations, and stores cleaned outputs in a partitioned folder structure that mirrors a production GCS bucket.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────┐  │
│  │  clickstream.csv │  │ transactions.csv │  │ Exchange │  │
│  │   (200,000 rows) │  │  (100,000 rows)  │  │ Rate API │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────┬─────┘  │
└───────────┼────────────────────┼────────────────────┼───────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                    STAGE 1: INGEST                          │
│                       ingest.py                             │
│  • Read CSVs in 50,000-row chunks (memory efficient)        │
│  • Fetch live currency rates via REST API                   │
│  • Retry logic with exponential backoff (2s → 4s → 8s)     │
│  • Save raw JSON to data/raw/api_currency/YYYY-MM-DD/       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   STAGE 2: TRANSFORM                        │
│                      transform.py                           │
│  • Standardize column names → snake_case                    │
│  • Convert all timestamps → UTC                             │
│  • Deduplicate rows                                         │
│  • Enrich transactions with amount_in_usd                   │
│  • Log null values and data quality warnings                │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     STAGE 3: LOAD                           │
│                        load.py                              │
│  • Write to partitioned output folders                      │
│  • Path: data/processed/<dataset>/ingest_date=YYYY-MM-DD/   │
│  • Log record counts, file sizes, and pipeline summary      │
│  • Alert if any dataset is empty or fails                   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   PARTITIONED OUTPUT                        │
│                                                             │
│  data/processed/clickstream/ingest_date=2026-02-27/         │
│  data/processed/transactions/ingest_date=2026-02-27/        │
│  data/processed/currency/ingest_date=2026-02-27/            │
└─────────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
storypoints-week1/
├── pipeline.py                  # Master ETL runner
├── README.md
├── .env                         # API keys (not committed)
├── .gitignore
├── scripts/
│   ├── ingest.py                # Task 2: Extract CSVs + API
│   ├── transform.py             # Task 3: Clean + enrich
│   ├── load.py                  # Task 4+5: Save + log
│   ├── explore.py               # Task 1: Schema exploration
│   └── setup_folders.py        # Project structure setup
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
```

---

## 📊 Dataset Understanding (Task 1)

### clickstream.csv

| Column | Type | Description |
|--------|------|-------------|
| user_id | int64 | Unique user identifier (1 – 50,000) |
| session_id | string | UUID identifying a user session |
| page_url | string | Page visited (e.g. /home, /checkout) |
| click_time | string → UTC datetime | Timestamp of the click event |
| device | string | Device type (desktop / mobile) |
| location | string | 2-letter country code (e.g. AU, GB, DE) |

**Shape:** 200,000 rows × 6 columns
**Null values:** ✅ None
**Duplicate rows:** ✅ None
**user_id range:** 1 – 50,000 (mean: 24,963)

---

### transactions.csv

| Column | Type | Description |
|--------|------|-------------|
| txn_id | string | UUID for each transaction |
| user_id | int64 | Unique user identifier (1 – 50,000) |
| amount | float64 | Transaction amount in original currency |
| currency | string | ISO currency code (USD, GBP, EUR, etc.) |
| txn_time | string → UTC datetime | Timestamp of the transaction |

**Shape:** 100,000 rows × 5 columns
**Null values:** ✅ None
**Duplicate rows:** ✅ None
**Amount range:** $3.91 – $43,497.95 (mean: $7,075.29)

**Enriched column added by pipeline:**
| Column | Type | Description |
|--------|------|-------------|
| amount_in_usd | float64 | Transaction amount converted to USD using live rates |

---

### ExchangeRate API

| Property | Value |
|----------|-------|
| Source | https://v6.exchangerate-api.com/v6/{KEY}/latest/USD |
| Format | JSON |
| Base currency | USD |
| Currencies covered | 160+ |
| Raw storage | data/raw/api_currency/YYYY-MM-DD/rates.json |

---

## ⚙️ Key Design Decisions

**Chunked CSV reading** — Files are read in 50,000-row chunks using `pd.read_csv(chunksize=50000)` to handle large datasets without memory issues.

**UTC standardization** — All timestamps are converted to UTC using `pd.to_datetime(utc=True)` to ensure consistency across global data sources.

**Exponential backoff** — API calls retry up to 3 times with delays of 2s, 4s, and 8s to handle transient network failures gracefully.

**Hive-style partitioning** — Outputs use `ingest_date=YYYY-MM-DD/` folder naming, compatible with BigQuery, Athena, and Spark for efficient date-based querying.

**Secret management** — API keys are stored in `.env` and loaded via `python-dotenv`, never hardcoded in source code.

**Idempotency** — Re-running the pipeline for the same date overwrites existing partitions, preventing data duplication.

---

## 🚀 How to Run

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd storypoints-week1
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install pandas requests pyarrow python-dotenv
```

### 4. Set up environment variables
```bash
cp .env.example .env
# Add your ExchangeRate API key to .env
```

### 5. Add raw data files
```
data/raw/clickstream/clickstream.csv
data/raw/transactions/transactions.csv
```

### 6. Explore the data (Task 1)
```bash
python scripts/explore.py
```

### 7. Run the full pipeline
```bash
python pipeline.py
```

---

## 📋 Pipeline Output Example

```
2026-02-27 01:55:31 [INFO] 🚀 StoryPoints AI — Week 1 ETL Pipeline Starting
2026-02-27 01:55:31 [INFO] STAGE 1: INGEST
2026-02-27 01:55:31 [INFO] 📂 Reading Clickstream in chunks of 50,000 rows...
2026-02-27 01:55:31 [INFO]    Chunk 1: 50,000 rows loaded (total so far: 50,000)
2026-02-27 01:55:32 [INFO]    Chunk 4: 50,000 rows loaded (total so far: 200,000)
2026-02-27 01:55:32 [INFO] ✅ Clickstream ingested: 200,000 total rows, 6 columns
2026-02-27 01:55:32 [INFO] ✅ Transactions ingested: 100,000 total rows, 5 columns
2026-02-27 01:55:32 [INFO] ✅ Currency rates fetched: 160 currencies
2026-02-27 01:55:32 [INFO] STAGE 2: TRANSFORM
2026-02-27 01:55:32 [INFO] ✅ Clickstream: 200,000 → 200,000 rows
2026-02-27 01:55:32 [INFO] ✅ Transactions: 100,000 → 100,000 rows
2026-02-27 01:55:32 [INFO] STAGE 3: LOAD
2026-02-27 01:55:33 [INFO] ✅ Saved clickstream: 200,000 rows (12.4 MB)
2026-02-27 01:55:33 [INFO] ✅ Saved transactions: 100,000 rows (8.1 MB)
2026-02-27 01:55:33 [INFO] ✅ Pipeline completed successfully!
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.11 | Core language |
| Pandas | Data processing and transformation |
| Requests | REST API calls |
| PyArrow | Parquet support |
| python-dotenv | Secret management |
| Git | Version control |

---

## 💡 Assumptions

- Raw CSV files are assumed to be UTF-8 encoded
- All transaction currencies are valid ISO 4217 codes
- Pipeline is designed to run once daily (date-partitioned)
- Local folder structure mirrors production GCS bucket paths

---

## 👤 Author

Built as part of the **StoryPoints AI Data Engineering Internship — Week 1**