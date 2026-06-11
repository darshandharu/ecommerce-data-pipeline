"""Generate simulated daily CDC files from the full Olist orders dataset.

For each simulated day this script emits, per cdc-enabled table, a CSV with an
``op`` column (I/U/D) and a ``cdc_timestamp``:

* INSERTs  — a fresh slice of unseen rows
* UPDATEs  — a few previously-seen rows with a mutated field
* DELETEs  — a small number of previously-seen keys flagged for removal

Output layout:   data/cdc/<YYYY-MM-DD>/<table>.csv

Usage::

    python scripts/generate_cdc_files.py --days 7 --start 2018-01-01
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

RAW = Path(os.getenv("RAW_PATH", "./data/raw"))
CDC = Path(os.getenv("CDC_PATH", "./data/cdc"))

# table -> (csv file, primary key columns, a column we can safely "update")
CDC_TABLES = {
    "orders": ("olist_orders_dataset.csv", ["order_id"], "order_status"),
    "order_items": ("olist_order_items_dataset.csv",
                    ["order_id", "order_item_id"], "freight_value"),
    "customers": ("olist_customers_dataset.csv", ["customer_id"], "customer_city"),
}


def _load(table_file: str) -> pd.DataFrame:
    path = RAW / table_file
    if not path.exists():
        raise SystemExit(f"Missing source file {path}. Run scripts/download_data.sh first.")
    return pd.read_csv(path)


def _mutate(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.copy()
    if df[col].dtype == object:
        df[col] = df[col].astype(str) + "_upd"
    else:
        df[col] = df[col] * 1.05  # nudge numeric values
    return df


def generate(days: int, start: str, daily_rows: int, seed: int = 42) -> None:
    random.seed(seed)
    start_date = datetime.strptime(start, "%Y-%m-%d")

    for table, (file, _pk, upd_col) in CDC_TABLES.items():
        full = _load(file)
        full = full.sample(frac=1, random_state=seed).reset_index(drop=True)
        seen = pd.DataFrame(columns=full.columns)
        cursor = 0

        for d in range(days):
            day = start_date + timedelta(days=d)
            day_str = day.strftime("%Y-%m-%d")
            ts = day.strftime("%Y-%m-%d %H:%M:%S")
            out_dir = CDC / day_str
            out_dir.mkdir(parents=True, exist_ok=True)

            # INSERTs: next slice of unseen rows
            inserts = full.iloc[cursor: cursor + daily_rows].copy()
            cursor += daily_rows
            inserts["op"] = "I"

            # UPDATEs / DELETEs only once we have a backlog of seen rows
            updates = pd.DataFrame(columns=full.columns)
            deletes = pd.DataFrame(columns=full.columns)
            if len(seen) > 20:
                upd_sample = seen.sample(min(10, len(seen)), random_state=seed + d)
                updates = _mutate(upd_sample, upd_col)
                updates["op"] = "U"

                del_sample = seen.sample(min(3, len(seen)), random_state=seed + d + 100)
                deletes = del_sample.copy()
                deletes["op"] = "D"

            batch = pd.concat([inserts, updates, deletes], ignore_index=True)
            batch["cdc_timestamp"] = ts
            batch.to_csv(out_dir / f"{table}.csv", index=False)

            seen = pd.concat([seen, inserts], ignore_index=True)
            print(f"{day_str} | {table:12s} I={len(inserts)} "
                  f"U={len(updates)} D={len(deletes)}")

    print(f"\nCDC files written under {CDC.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate simulated CDC files")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--daily-rows", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate(args.days, args.start, args.daily_rows, args.seed)
