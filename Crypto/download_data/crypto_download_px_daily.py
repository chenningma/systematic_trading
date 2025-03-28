import json
import sys
import time
import requests
import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

from dotenv import load_dotenv
parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

api_key = os.getenv('COINGECKO_API_KEY')

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
            bigquery.SchemaField("timestamp", "DATETIME"),
            bigquery.SchemaField("prices", "FLOAT64"),
            bigquery.SchemaField("market_caps", "FLOAT64"),
            bigquery.SchemaField("total_volumes", "FLOAT64"),
            bigquery.SchemaField("id", "STRING"),
        ]
    table_ref = write_table_id
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

# create_write_table("boreal-pride-417020.crypto_px.px_hourly_update")

### get coin list
query_job = client.query("""
(select 
a.id
from crypto_px.top_coins_list a
where a.market_cap > 5000000
)
union distinct
(
select
id
from `crypto_px.solana_coin_id`
)
""")
coins_df = query_job.result().to_dataframe()

last_date = client.query("select max(timestamp) as last_date from `crypto_px.px_hourly`").result().to_dataframe().last_date[0]

### download and save prices
write_table_id = 'boreal-pride-417020.crypto_px.px_hourly_update'
log_table = 'boreal-pride-417020.monitor.download_crypto_px'

vs_currency="usd"

start_date = last_date.floor('d')
end_date = datetime.now(timezone.utc)
print("getting data for ", start_date.strftime("%Y-%m-%d %H:%M:%S"), ' to ', end_date.strftime("%Y-%m-%d %H:%M:%S"))

request_count = 0
collect_df = pd.DataFrame()
for id in coins_df.id: 
    print("downloading data for", id)

    url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/market_chart/range"
    headers = {"x-cg-pro-api-key": api_key}
    params = {
        "vs_currency": vs_currency,
        "from": str(start_date.timestamp()),
        "to": str(end_date.timestamp())
       
    }
    try: 
        response = requests.get(url, headers = headers, params=params)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        prices = pd.DataFrame(data["prices"], columns=["timestamp", "prices"])
        mcap = pd.DataFrame(data["market_caps"], columns=["timestamp", "market_caps"])
        volume = pd.DataFrame(data["total_volumes"], columns=["timestamp", "total_volumes"])

        df = prices.merge(mcap, how="outer", on = 'timestamp')
        df = df.merge(volume, how="outer", on = 'timestamp')
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["id"] = id

        print(str(len(df)), "rows downloaded")
        #df = df.loc[df['timestamp'].dt.date >= date_cutoff.date(), :]
        if len(df) > 0 :
            collect_df = pd.concat([collect_df, df])

        if (request_count > 0) & (request_count % 40 == 0):
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

    request_count += 1

if (request_count % 40 != 0):
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
delete from boreal-pride-417020.crypto_px.px_hourly 
where timestamp >= '{date}'
""".format(date = start_date.strftime("%Y-%m-%d %H:%M:%S"))
query_job = client.query(query)  
query_job.result()

query = """
insert into boreal-pride-417020.crypto_px.px_hourly 
select * from boreal-pride-417020.crypto_px.px_hourly_update
"""
query_job = client.query(query)  
query_job.result()
print('Insert update table into prod table')

query = """
truncate table boreal-pride-417020.crypto_px.px_hourly_update
"""
query_job = client.query(query)  
query_job.result()

print('Success! truncating update table')