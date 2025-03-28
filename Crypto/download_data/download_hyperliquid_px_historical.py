import json
import sys
import time
import requests
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt

import eth_account
from eth_account.signers.local import LocalAccount

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

def setup(base_url=None, skip_ws=False):
    config_path = os.path.dirname(__file__) + "/config.json"
    #config_path = "config.json"
    with open(config_path) as f:
        config = json.load(f)
    account: LocalAccount = eth_account.Account.from_key(config["secret_key"])
    address = config["account_address"]
    if address == "":
        address = account.address
    print("Running with account address:", address)
    if address != account.address:
        print("Running with agent address:", account.address)
    info = Info(base_url, skip_ws)
    user_state = info.user_state(address)
    spot_user_state = info.spot_user_state(address)
    margin_summary = user_state["marginSummary"]
    if float(margin_summary["accountValue"]) == 0 and len(spot_user_state["balances"]) == 0:
        print("Not running the example because the provided account has no equity.")
        url = info.base_url.split(".", 1)[1]
        error_string = f"No accountValue:\nIf you think this is a mistake, make sure that {address} has a balance on {url}.\nIf address shown is your API wallet address, update the config to specify the address of your account, not the address of the API wallet."
        raise Exception(error_string)
    exchange = Exchange(account, base_url, account_address=address)
    return address, info, exchange
address, info, exchange = setup(base_url=constants.MAINNET_API_URL, skip_ws=True)

### get coin list

json_data = info.meta()
coins_df = pd.DataFrame(json_data['universe'])

table_id = 'crypto_px.hyperliquid_coin_list'
job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
job = client.load_table_from_dataframe(
    coins_df, table_id, job_config=job_config
)
job.result()

def download_by_name(symbol, start_date, end_date):
    candle_data = []
    current_end = end_date
    current_start = current_end - timedelta(days=3)

    while current_start >= start_date:
        # Convert timestamps to milliseconds as required by API
        start_ts = int(current_start.timestamp() * 1000)
        end_ts = int(current_end.timestamp() * 1000)
        
        print(f"Downloading data from {current_start} to {current_end}")
        interval_data = info.candles_snapshot(name=symbol, interval='5m', startTime=start_ts, endTime=end_ts)
        
        if not interval_data:
            break
            
        candle_data.extend(interval_data)
        
        # Move window back 3 days
        current_end = current_start
        current_start = current_end - timedelta(days=3)

            
    df = pd.DataFrame(candle_data)
    df.columns = ['time_open', 'time_close', 'symbol', 'interval', 'open', 'close', 'high', 'low', 'volume', 'num']
    df["time_open"] = pd.to_datetime(df["time_open"], unit="ms")
    df["time_close"] = pd.to_datetime(df["time_close"], unit="ms")
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    df = df.sort_values(by='time_close')
    df["time_close_rd"] = df["time_close"].dt.round('min')

    return df

write_table_id = 'crypto_px.hyperliquid_px_5m'
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(days = 30)

collect_df = pd.DataFrame()

for id in coins_df.name: 
    print("downloading data for", id)
    try:
        df = download_by_name(id, start_date, end_date)

        if len(df) > 0:
            collect_df = pd.concat([collect_df, df])

        if len(collect_df) > 15000:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
            job = client.load_table_from_dataframe(
                collect_df, write_table_id, job_config=job_config
            )
            job.result()

            collect_df = pd.DataFrame()
    except Exception as e:
        print(str(e))
