#!/usr/bin/env python3
"""
crypto_iceberg_etl.py

This script will:
1. Load environment variables and secrets
2. Define the Iceberg schema and partition spec for daily crypto OHLCV
3. Create the Iceberg table if it doesn't exist
4. Discover top-50 USDT pairs and fetch 2 years of daily data
5. Append data to the Iceberg table
6. Create an "audit_branch" via PyIceberg’s ManageSnapshots API

Run with:
    python3 crypto_iceberg_etl.py
"""
import os
import time
import json
import requests
import pandas as pd
import pyarrow as pa
from datetime import datetime
from dotenv import load_dotenv

from scripts.binance.aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DateType, DoubleType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.expressions import NestedField

# 1. Load .env (if you have one)
load_dotenv()

# 2. Helper to discover top-N USDT symbols
def fetch_top_usdt_symbols(n: int = 50) -> list[str]:
    resp = requests.get("https://api.binance.com/api/v3/ticker/24hr")
    resp.raise_for_status()
    data = resp.json()
    usdt = [d for d in data if d["symbol"].endswith("USDT")]
    top = sorted(usdt, key=lambda x: float(x["quoteVolume"]), reverse=True)[:n]
    return [d["symbol"] for d in top]

# 3. Fetch OHLCV for one symbol (daily)
def fetch_ohlcv(symbol: str, limit: int = 730, interval: str = "1d") -> pd.DataFrame:
    resp = requests.get(
        "https://api.binance.com/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit}
    )
    resp.raise_for_status()
    raw = resp.json()

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    # Convert and cast
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    for col in ["open","high","low","close","volume",
                "quote_asset_volume","taker_buy_base","taker_buy_quote"]:
        df[col] = df[col].astype(float)
    df["number_of_trades"] = df["number_of_trades"].astype(int)
    df["symbol"] = symbol
    return df

def main():
    # Table identifier
    table_id = "chunghaw.crypto_daily_ohlcv"
    raw = get_secret("CATALOG_NAME")
    print("▶ CATALOG_NAME secret:", repr(raw))
    # 4. Load Iceberg catalog (Tabular)
    catalog = load_catalog(
        "academy",
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret("TABULAR_CREDENTIAL"),
        io=PyArrowFileIO()
    )

    # 5. Define schema & partition
    schema = Schema(
        NestedField(1, "symbol",              StringType(), required=True),
        NestedField(2, "event_date",          DateType(),   required=True),
        NestedField(3, "open",                DoubleType(), required=False),
        NestedField(4, "high",                DoubleType(), required=False),
        NestedField(5, "low",                 DoubleType(), required=False),
        NestedField(6, "close",               DoubleType(), required=False),
        NestedField(7, "volume",              DoubleType(), required=False),
        NestedField(8, "quote_asset_volume",  DoubleType(), required=False),
        NestedField(9, "number_of_trades",    LongType(),   required=False),
        NestedField(10,"taker_buy_base",       DoubleType(), required=False),
        NestedField(11,"taker_buy_quote",      DoubleType(), required=False),
    )
    partition_spec = PartitionSpec(fields=[
        PartitionField(source_id=2, field_id=1000, transform="identity", name="event_date")
    ])

    # 6. Create table if needed
    try:
        catalog.create_table(identifier=table_id, schema=schema, partition_spec=partition_spec)
        print(f"✅ Table {table_id} created.")
    except TableAlreadyExistsError:
        print(f"ℹ️ Table {table_id} already exists.")

    table = catalog.load_table(table_id)

    # 7. Discover symbols and fetch data
    symbols = fetch_top_usdt_symbols(50)
    print(f"Discovered top {len(symbols)} USDT pairs.")
    records = []

    for sym in symbols:
        print(f"Fetching {sym}…")
        df = fetch_ohlcv(sym)
        for _, row in df.iterrows():
            records.append({
                "symbol":            row["symbol"],
                "event_date":        row["open_time"].date(),
                "open":              row["open"],
                "high":              row["high"],
                "low":               row["low"],
                "close":             row["close"],
                "volume":            row["volume"],
                "quote_asset_volume":row["quote_asset_volume"],
                "number_of_trades":  row["number_of_trades"],
                "taker_buy_base":    row["taker_buy_base"],
                "taker_buy_quote":   row["taker_buy_quote"],
            })
        # Stay under Binance rate-limits
        time.sleep(0.1)

    if not records:
        print("ℹ️ No records fetched; exiting.")
        return

    # 8. Convert to Arrow Table and append
    df_all = pd.DataFrame(records).astype({
        "symbol": "string",
        "event_date": "datetime64[ns]",
        "open": "float", "high": "float", "low": "float", "close": "float",
        "volume": "float", "quote_asset_volume": "float",
        "number_of_trades": "Int64",
        "taker_buy_base": "float", "taker_buy_quote": "float"
    })
    arrow_schema = pa.schema([
        pa.field("symbol",              pa.string(), nullable=False),
        pa.field("event_date",          pa.date32(), nullable=False),
        pa.field("open",                pa.float64()),
        pa.field("high",                pa.float64()),
        pa.field("low",                 pa.float64()),
        pa.field("close",               pa.float64()),
        pa.field("volume",              pa.float64()),
        pa.field("quote_asset_volume",  pa.float64()),
        pa.field("number_of_trades",    pa.int64()),
        pa.field("taker_buy_base",      pa.float64()),
        pa.field("taker_buy_quote",     pa.float64()),
    ])
    pa_table = pa.Table.from_pandas(df_all, schema=arrow_schema, preserve_index=False)

    table.append(pa_table)
    print(f"✅ Appended {len(records)} rows to {table_id}")

    # 9. Create audit branch if missing
    refs = table.metadata.refs or {}
    if "audit_branch" not in refs:
        snap = table.current_snapshot()
        if snap is None:
            raise RuntimeError("No snapshot to branch from")
        table.manage_snapshots().create_branch(snap.snapshot_id, "audit_branch").commit()
        print("✅ Created audit_branch")
    else:
        print("ℹ️ audit_branch already exists")

if __name__ == "__main__":
    main()
