import logging, pandas as pd
from datetime import datetime, timezone
TODAY = datetime.now(timezone.utc).strftime('%Y-%m-%d')
log = logging.getLogger(__name__)

def standardize_columns(df):
    df.columns = (df.columns.str.strip().str.lower()
        .str.replace(r'[\s\-]+', '_', regex=True)
        .str.replace(r'[^\w]', '', regex=True))
    return df

def convert_timestamps_to_utc(df, cols):
    for col in cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
                log.info(f'Converted {col} to UTC')
            except Exception as e:
                log.warning(f'Could not convert {col}: {e}')
    return df

def transform_clickstream(df):
    if df.empty: return df
    log.info('Transforming clickstream...')
    original = len(df)
    df = standardize_columns(df)
    ts_cols = [c for c in df.columns if any(x in c for x in ['time','date','timestamp'])]
    df = convert_timestamps_to_utc(df, ts_cols)
    df['ingest_date'] = TODAY
    before = len(df)
    df = df.drop_duplicates()
    log.info(f'Removed {before - len(df)} duplicates')
    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    if not null_cols.empty: log.info(f'Nulls found:\n{null_cols.to_string()}')
    log.info(f'Clickstream: {original} to {len(df)} rows')
    return df

def transform_transactions(df, currency_rates):
    if df.empty: return df
    log.info('Transforming transactions...')
    original = len(df)
    df = standardize_columns(df)
    ts_cols = [c for c in df.columns if any(x in c for x in ['time','date','timestamp'])]
    df = convert_timestamps_to_utc(df, ts_cols)
    before = len(df)
    df = df.drop_duplicates()
    log.info(f'Removed {before - len(df)} duplicates')
    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    if not null_cols.empty: log.info(f'Nulls found:\n{null_cols.to_string()}')
    if currency_rates:
        currency_col = next((c for c in df.columns if 'currency' in c), None)
        amount_col = next((c for c in df.columns if 'amount' in c), None)
        if currency_col and amount_col:
            df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
            def to_usd(row):
                cur = str(row[currency_col]).upper()
                amt = row[amount_col]
                if pd.isna(amt): return None
                if cur == 'USD': return round(amt, 4)
                rate = currency_rates.get(cur)
                return round(amt / rate, 4) if rate else None
            df['amount_in_usd'] = df.apply(to_usd, axis=1)
            log.info(f'amount_in_usd enriched for {df["amount_in_usd"].notna().sum()} rows')
        else:
            log.warning('No currency/amount columns found')
    else:
        df['amount_in_usd'] = None
    df['ingest_date'] = TODAY
    log.info(f'Transactions: {original} to {len(df)} rows')
    return df

def transform_currency(currency_rates):
    if not currency_rates: return pd.DataFrame()
    df = pd.DataFrame(list(currency_rates.items()), columns=['currency_code','rate_vs_usd'])
    df['base_currency'] = 'USD'
    df['ingest_date'] = TODAY
    df['fetched_at_utc'] = datetime.now(timezone.utc).isoformat()
    log.info(f'Currency: {len(df)} rows')
    return df

def main(clickstream_df, transactions_df, currency_rates):
    log.info('=' * 50)
    log.info('Transformation pipeline')
    log.info('=' * 50)
    return (
        transform_clickstream(clickstream_df),
        transform_transactions(transactions_df, currency_rates),
        transform_currency(currency_rates)
    )