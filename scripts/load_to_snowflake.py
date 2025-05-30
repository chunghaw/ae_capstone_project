# scripts/load_to_snowflake.py

import os, json
import pandas as pd
import pyarrow.dataset as ds
import botocore
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from botocore.exceptions import ClientError  
from scripts.binance.aws_secret_manager import get_secret  
from dotenv import load_dotenv

from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO

load_dotenv()

def get_tabular_catalog():
    warehouse = get_secret("CATALOG_NAME")
    credential = get_secret("TABULAR_CREDENTIAL")
    return load_catalog(
        "academy",
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=warehouse,
        credential=credential,
        io=PyArrowFileIO(),
    )

def fetch_from_iceberg(table_id="chunghaw.crypto_daily_ohlcv") -> pd.DataFrame:
    catalog = get_tabular_catalog()
    table = catalog.load_table(table_id)
    df = table.scan().to_pandas()
    return df

def get_snowflake_conn():
    try:
        # try pulling JSON from AWS Secrets Manager
        raw = get_secret("snowflake/creds")
        creds = json.loads(raw)
    except ClientError as e:
        # if it really is a missing‐secret error, fall back to .env
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            creds = {
                "user":      os.getenv("SNOWFLAKE_USER"),
                "password":  os.getenv("SNOWFLAKE_PASSWORD"),
                "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database":  os.getenv("SNOWFLAKE_DATABASE"),
                "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
            }
        else:
            # re‐raise any other AWS errors
            raise
    return snowflake.connector.connect(**creds)

def create_table_if_not_exists(conn, table_name: str, schema: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
      symbol               STRING,
      event_date           DATE,
      open                 FLOAT,
      high                 FLOAT,
      low                  FLOAT,
      close                FLOAT,
      volume               FLOAT,
      quote_asset_volume   FLOAT,
      number_of_trades     INT,
      taker_buy_base       FLOAT,
      taker_buy_quote      FLOAT
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def main():
    
    # 1) Pull raw from Iceberg
    df = fetch_from_iceberg()

    # 2) Derive event_date if needed
    if "event_date" not in df.columns and "open_time" in df.columns:
        df["open_date"] = pd.to_datetime(["open_time"]).dt.date
        df["event_date"] = df["open_date"].dt.date
    
    # 3) Select only the staging columns
    df = df[[
        "symbol","event_date","open","high","low","close",
        "volume","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote"
    ]]

    # 4) Connect & push
    conn = get_snowflake_conn()

    # Make sure table exists
    create_table_if_not_exists(conn, "crypto_daily_ohlcv", conn.schema)

    # now bulk‐load
    success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name="crypto_daily_ohlcv",
    schema=conn.schema,         # now picks up the SNOWFLAKE_SCHEMA you set
    database=conn.database,      # optional, but can be explicit
    quote_identifiers=False
)
    if success:
        print(f"✅ Loaded {nrows} rows into {conn.database}.{conn.schema}.crypto_daily_ohlcv")
    else:
        print("❌ write_pandas failed")

if __name__ == "__main__":
    main()
