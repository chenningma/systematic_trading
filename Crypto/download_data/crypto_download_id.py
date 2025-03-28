import json
import sys
import time
import requests
import pandas as pd
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

url = 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'
response = requests.get(url)
response.raise_for_status()
data = response.json()


### Coingecko
def get_all_coin_ids(api_key):
    """
    Fetch all coin IDs from CoinGecko.

    Returns:
        pd.DataFrame: A DataFrame containing coin IDs, symbols, and names.
    """
    url = url = 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        platform = [x['platforms'] for x in data]
        # Convert to DataFrame
        df = pd.concat([pd.DataFrame(data), pd.DataFrame(platform)], axis = 1)
        return df
    except requests.exceptions.RequestException as e:
        print("An error occurred: ", str(e))
        return pd.DataFrame()

# Fetch all coin IDs
coins_df = get_all_coin_ids(api_key)

table_id = 'boreal-pride-417020.crypto_px.coin_id'
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)
job = client.load_table_from_dataframe(
    coins_df, table_id, job_config=job_config
)
job.result()

### Solana Coins
solana_df = coins_df.loc[coins_df.solana.notnull(), ['id', 'symbol', 'name', 'ethereum', 'solana']]

### Jupiter
url = "https://tokens.jup.ag/tokens_with_markets"

response = requests.get(url)
response.raise_for_status()
data = response.json()

jupiter_df = pd.DataFrame(data)
jupiter_df = jupiter_df.loc[:, ['address', 'name', 'symbol']]
jupiter_df.columns = ['address', 'jupiter_name', 'jupiter_symbol']
solana_df = solana_df.merge(jupiter_df, left_on = 'solana', right_on = 'address')

table_id = 'boreal-pride-417020.crypto_px.solana_coin_id'
job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
job = client.load_table_from_dataframe(
    solana_df, table_id, job_config=job_config
)
job.result()

### get markets

table_id = 'crypto_px.solana_ecosystem_list'

query = 'truncate table crypto_px.solana_ecosystem_list'
job = client.query(query)
job.result()

for i in range(1, 15):
    url = "https://pro-api.coingecko.com/api/v3/coins/markets"
    headers = {"x-cg-pro-api-key": api_key}
    params = {
        "vs_currency": "usd",
        "category": "solana-ecosystem",
        "per_page": 250,
        "page": i
    }

    response = requests.get(url, headers = headers, params=params)
    response.raise_for_status()
    coin_list = response.json()
    coin_list_df = pd.DataFrame(coin_list)

    if len(coin_list_df) == 0:
        break

    coin_list_df['fully_diluted_valuation'] = round(coin_list_df['fully_diluted_valuation'])
    coin_list_df['market_cap'] = coin_list_df['market_cap'].fillna(0).astype('int')
    coin_list_df['market_cap_rank'] = coin_list_df['market_cap_rank'].fillna(0).astype('int')
    coin_list_df['fully_diluted_valuation'] = coin_list_df['fully_diluted_valuation'].fillna(0).astype('int')

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_dataframe(
        coin_list_df, table_id, job_config=job_config
    )
    job.result()

table_id = 'crypto_px.solana_memecoin_list'
query = 'truncate table crypto_px.solana_memecoin_list'
job = client.query(query)
job.result()

for i in range(1, 15):
    url = "https://pro-api.coingecko.com/api/v3/coins/markets"
    headers = {"x-cg-pro-api-key": api_key}
    params = {
        "vs_currency": "usd",
        "category": "solana-meme-coins",
        "per_page": 250,
        "page": i
    }

    response = requests.get(url, headers = headers, params=params)
    response.raise_for_status()
    coin_list = response.json()
    coin_list_df = pd.DataFrame(coin_list)

    if len(coin_list_df) == 0:
        break

    coin_list_df['fully_diluted_valuation'] = round(coin_list_df['fully_diluted_valuation'])
    coin_list_df['market_cap'] = coin_list_df['market_cap'].fillna(0).astype('int')
    coin_list_df['market_cap_rank'] = coin_list_df['market_cap_rank'].fillna(0).astype('int')
    coin_list_df['fully_diluted_valuation'] = coin_list_df['fully_diluted_valuation'].fillna(0).astype('int')

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_dataframe(
        coin_list_df, table_id, job_config=job_config
    )
    job.result()

### get all coins with marketcap
table_id = 'crypto_px.top_coins_list'

query = 'truncate table crypto_px.top_coins_list'
job = client.query(query)
job.result()

for i in range(1, 15):
    url = 'https://pro-api.coingecko.com/api/v3/coins/markets'
    headers = {"x-cg-pro-api-key": api_key}
    params = {
        "vs_currency": "usd",
        "per_page": 250,
        "page": i
    }
    response = requests.get(url, headers = headers, params=params)
    response.raise_for_status()
    coin_list = response.json()
    coin_list_df = pd.DataFrame(coin_list)

    numeric_cols = coin_list_df.select_dtypes(include=['number']).columns
    coin_list_df[numeric_cols] = coin_list_df[numeric_cols].astype(float)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_dataframe(
        coin_list_df, table_id, job_config=job_config
    )
    job.result()