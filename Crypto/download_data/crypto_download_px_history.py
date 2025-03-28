import json
import sys
import time
import requests
import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta

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

### create log table

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


### get coin list
query_job = client.query("""
with tb1 as (
select 
coalesce(a.id, b.id) as id
from crypto_px.top_coins_list a
full join `crypto_px.solana_coin_id` b on a.id=b.id 
where a.market_cap > 5000000
),
tb2 as (
select
distinct id
from `crypto_px.px_hourly`
)
select
tb1.id
from tb1 
left join tb2 on tb1.id = tb2.id
where tb2.id is NULL
""")
coins_df = query_job.result().to_dataframe()

def generate_dates(start_date, end_date, step_days=90):
    """
    Generate a list of dates spaced by a specific number of days.

    Args:
        start_date (str): The start date in "YYYY-MM-DD" format.
        end_date (str): The end date in "YYYY-MM-DD" format.
        step_days (int): The number of days to step by (default is 90).

    Returns:
        list: A list of dates (as strings) spaced by step_days within the range.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []

    current_date = start
    while current_date <= end:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=step_days)

    return dates

# Get Date list
start_date = "2024-01-01"
end_date = datetime.today().strftime("%Y-%m-%d")
date_list = generate_dates(start_date, end_date)
date_list.append(end_date)

### download and save prices
write_table_id = 'boreal-pride-417020.crypto_px.px_hourly_history_update'
log_table = 'boreal-pride-417020.monitor.download_crypto_px'

vs_currency="usd"
request_count = 0

collect_df = pd.DataFrame()
for id in coins_df.id: 

    for i in range(0, len(date_list)-1):

        start_date = pd.to_datetime(date_list[i])
        end_date = pd.to_datetime(date_list[i+1])

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

            collect_df = pd.concat([collect_df, df])

            if (request_count > 0) & (request_count % 80 == 0):
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                )
                job = client.load_table_from_dataframe(
                    collect_df, write_table_id, job_config=job_config
                )
                job.result()

                collect_df = pd.DataFrame()

        except Exception as e:
            print("error: ", id)
            exception_type = type(e).__name__
            exception_message = str(e)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Prepare the row to insert
            rows_to_insert = [{
                "exception_type": exception_type,
                "exception_message": exception_message,
                "coin_id": id,
                "get_date_start": start_date.strftime("%Y-%m-%d"),
                "get_date_end": end_date.strftime("%Y-%m-%d"),
                "timestamp": timestamp
            }]

            errors = client.insert_rows_json(log_table, rows_to_insert) 

        request_count += 1
    
