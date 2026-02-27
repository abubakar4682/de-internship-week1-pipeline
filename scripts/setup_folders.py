import os
from datetime import datetime

TODAY = datetime.utcnow().strftime('%Y-%m-%d')

folders = [
    'data/raw/clickstream',
    'data/raw/transactions',
    f'data/raw/api_currency/{TODAY}',
    f'data/processed/clickstream/ingest_date={TODAY}',
    f'data/processed/transactions/ingest_date={TODAY}',
    f'data/processed/currency/ingest_date={TODAY}',
    'logs',
]

for folder in folders:
    os.makedirs(folder, exist_ok=True)
    print(f'Created: {folder}')

print(f'\nDone! Partition date: {TODAY}')