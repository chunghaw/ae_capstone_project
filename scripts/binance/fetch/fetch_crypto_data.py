import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

# 1. Discover current top-N USDT symbols by 24h volume
def fetch_top_usdt_symbols(n: int = 50) -> list[str]:
    url = "https://api.binance.com/api/v3/ticker/24hr"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    # Filter USDT pairs and sort by quoteVolume descending
    usdt = [d for d in data if d["symbol"].endswith("USDT")]
    top = sorted(usdt, key=lambda x: float(x["quoteVolume"]), reverse=True)[:n]
    return [d["symbol"] for d in top]

# 2. Fetch OHLCV for one symbol (daily by default)
def fetch_ohlcv(
    symbol: str,
    interval: str = "1d",
    limit: int = 730
) -> pd.DataFrame:
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    raw = resp.json()

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    # Convert timestamps
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    # Cast numeric columns
    num_cols = ["open","high","low","close","volume","quote_asset_volume",
                "taker_buy_base","taker_buy_quote"]
    df[num_cols] = df[num_cols].astype(float)
    df["number_of_trades"] = df["number_of_trades"].astype(int)

    # Annotate symbol
    df["symbol"] = symbol
    return df

def main():
    # Where to save combined CSV
    out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "top50_daily_ohlcv.csv")

    # 1) Get top 50 symbols
    symbols = fetch_top_usdt_symbols(50)
    print(f"Discovered {len(symbols)} symbols:", symbols)

    # 2) Fetch and accumulate
    all_dfs = []
    for sym in symbols:
        print(f"Fetching {sym} (daily, last 730 days)…")
        df = fetch_ohlcv(sym, interval="1d", limit=730)
        all_dfs.append(df)
        # Binance weight=2 per klines → can do ~600 calls/min
        time.sleep(0.1)

    # 3) Concat & save
    result = pd.concat(all_dfs, ignore_index=True)
    result.to_csv(out_path, index=False)
    print(f"Saved {len(result)} rows to {out_path}")

if __name__ == "__main__":
    main()
