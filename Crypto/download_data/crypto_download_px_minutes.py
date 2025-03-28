import json
import sys
import time
import requests
import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta, timezone

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

### Initiate tables: do once

def create_log_table(log_table):
    schema = [
            bigquery.SchemaField("exception_type", "STRING"),
            bigquery.SchemaField("exception_message", "STRING"),
            bigquery.SchemaField("coin_id", "STRING"),
            bigquery.SchemaField("get_date_start", "STRING"),
            bigquery.SchemaField("get_date_end", "STRING"),
            bigquery.SchemaField("timestamp", "STRING"),
        ]
    table_ref = log_table
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

def save_error(log_table, e): 
    exception_type = type(e).__name__
    exception_message = str(e)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Prepare the row to insert
    rows_to_insert = [{
        "exception_type": exception_type,
        "exception_message": exception_message,
        "coin_id": id,
        "timestamp": timestamp
    }]

    errors = client.insert_rows_json(log_table, rows_to_insert) 

def create_write_table(write_table_id):
    schema = [
            bigquery.SchemaField("time_open", "DATETIME"),
            bigquery.SchemaField("time_close", "DATETIME"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("interval", "STRING"),
            bigquery.SchemaField("open", "FLOAT64"),
            bigquery.SchemaField("close", "FLOAT64"),
            bigquery.SchemaField("high", "FLOAT64"),
            bigquery.SchemaField("low", "FLOAT64"),
            bigquery.SchemaField("volume", "FLOAT64"),
            bigquery.SchemaField("num", "INTEGER")
        ]
    table_ref = write_table_id
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

create_write_table("boreal-pride-417020.crypto_px.px_minutes")
create_write_table("boreal-pride-417020.crypto_px.px_minutes_update")

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

### download and save prices
write_table_id = 'boreal-pride-417020.crypto_px.px_minutes_update'

last_date = client.query("select max(time_close) as last_date from `crypto_px.px_minutes`").result().to_dataframe().last_date[0]

collect_df = pd.DataFrame()

for id in coins_df.name: 
    print("downloading data for", id)
    end_date = last_date.replace(tzinfo=timezone.utc)
    
    while end_date > datetime(2024, 8, 1, tzinfo=timezone.utc):
        
        start_date = end_date - timedelta(days = 50)
        # Convert timestamps to milliseconds as required by API
        start_ts = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)
        print(start_date)
        try:
            candle_data = info.candles_snapshot(name=id, interval='15m', startTime=start_ts, endTime=end_ts)
            
            df = pd.DataFrame(candle_data)
            df.columns = ['time_open', 'time_close', 'symbol', 'interval', 'open', 'close', 'high', 'low', 'volume', 'num']
            df["time_open"] = pd.to_datetime(df["time_open"], unit="ms")
            df["time_close"] = pd.to_datetime(df["time_close"], unit="ms")
            df['open'] = df['open'].astype(float)
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['volume'] = df['volume'].astype(float)

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
            print("error downloading ", id, str(e))

        end_date = start_date

if len(collect_df) > 0:
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        job = client.load_table_from_dataframe(
            collect_df, write_table_id, job_config=job_config
        )
        job.result()
    except Exception as e:
        print(str(e))

print('Finished downloading data')

query = """
delete from boreal-pride-417020.crypto_px.px_minutes 
where timestamp >= '{date}'
""".format(date = start_date.strftime("%Y-%m-%d %H:%M:%S"))
query_job = client.query(query)  
query_job.result()

query = """
insert into boreal-pride-417020.crypto_px.px_minutes
select * from boreal-pride-417020.crypto_px.px_minutes_update
"""
query_job = client.query(query)  
query_job.result()
print('Insert update table into prod table')

query = """
truncate table boreal-pride-417020.crypto_px.px_minutes_update
"""
query_job = client.query(query)  
query_job.result()

print('Success! truncating update table')