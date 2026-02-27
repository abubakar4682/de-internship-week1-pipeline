"""
explore.py
Task 1: Explore datasets — schemas, null values, duplicates.
Run: python scripts/explore.py
"""

import os
import pandas as pd

FILES = {
    "clickstream":  "data/raw/clickstream/clickstream.csv",
    "transactions": "data/raw/transactions/transactions.csv",
}


def explore(name: str, path: str):
    print(f"\n{'='*60}")
    print(f"📂 Dataset: {name.upper()}")
    print(f"{'='*60}")

    if not os.path.exists(path):
        print(f"⚠️  File not found: {path}")
        return

    df = pd.read_csv(path, nrows=100_000)

    print(f"\n📐 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"\n📋 Schema:\n{df.dtypes.to_string()}")
    print(f"\n🔍 Sample rows (first 3):\n{df.head(3).to_string()}")

    nulls   = df.isnull().sum()
    null_pct = (nulls / len(df) * 100).round(2)
    null_summary = pd.DataFrame({"null_count": nulls, "null_%": null_pct})
    null_summary = null_summary[null_summary["null_count"] > 0]

    print(f"\n❓ Null values:")
    if null_summary.empty:
        print("   ✅ No null values found")
    else:
        print(null_summary.to_string())

    print(f"\n🔁 Duplicate rows: {df.duplicated().sum():,}")
    print(f"\n📊 Statistics:\n{df.describe().to_string()}")


if __name__ == "__main__":
    print("🔎 StoryPoints AI — Dataset Exploration (Task 1)")
    for name, path in FILES.items():
        explore(name, path)
    print(f"\n✅ Exploration complete!")