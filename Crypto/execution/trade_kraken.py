import json
import sys
import time
import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from dotenv import load_dotenv
load_dotenv('./.env')

import urllib.parse
import hashlib
import hmac
import base64

def get_kraken_signature(urlpath, data, secret):

    if isinstance(data, str):
        encoded = (str(json.loads(data)["nonce"]) + data).encode()
    else:
        encoded = (str(data["nonce"]) + urllib.parse.urlencode(data)).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def get_current_px(trade_ptf):
    px_list = list()
    for i in range(0, len(trade_ptf)):
        try:
            quote_ticker = trade_ptf.altname[i]
            url = "https://api.kraken.com/0/public/OHLC?pair={pair}".format(pair = quote_ticker)

            payload = {}
            headers = {
            'Accept': 'application/json'
            }

            response = requests.request("GET", url, headers=headers, data=payload)
            json_data = response.json()
            this_px = pd.DataFrame(json_data['result'][quote_ticker], columns = ['timestamp', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'])
            this_px.timestamp = pd.to_datetime(this_px.timestamp, unit='s')
            this_px = this_px.loc[len(this_px)-1, :].close
            px_list.append([quote_ticker, float(this_px)])
        except Exception as e:
            print(str(e))
    px_list_df = pd.DataFrame(px_list, columns = ['altname', 'px'])
    return px_list_df

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

### get tradeable pairs
url = "https://api.kraken.com/0/public/AssetPairs"

payload = {}
headers = {
  'Accept': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
json_data = response.json()
trade_book = pd.DataFrame(json_data['result']).transpose()

## Get current ptf
api_nonce = str(int(time.time()*1000))
url = "https://api.kraken.com/0/private/Balance"
payload = json.dumps({
"nonce": api_nonce
})

api_sec = os.getenv("KRAKEN_PRIVATE_KEY")
signature = get_kraken_signature("/0/private/Balance", payload, api_sec)
headers = {
'Content-Type': 'application/json',
'Accept': 'application/json',
'API-Key': os.getenv("KRAKEN_PUB_KEY"),
'API-Sign': signature
}

response = requests.request("POST", url, headers=headers, data=payload)
json_data = response.json()
current_ptf = pd.Series(json_data['result'])

current_ptf = pd.DataFrame(current_ptf).reset_index()
current_ptf.columns = ['symbol', 'quantity']
current_ptf.loc[:, 'quantity'] = current_ptf.loc[:, 'quantity'].astype(float)

current_balance = current_ptf.merge(trade_book, how='inner', left_on = 'symbol', right_on = 'base')
current_balance = current_balance.loc[current_balance.quote == 'ZUSD', :].reset_index(drop=True)
current_px_df = get_current_px(current_balance)
current_balance = current_balance.merge(current_px_df, how = 'left', on = 'altname')
current_balance.loc[:, 'usd_balance'] = current_balance.loc[:, 'quantity'] * current_balance.loc[:, 'px'] 
usd_balance =  current_balance.usd_balance.sum()

balance_df = pd.DataFrame([[datetime.now(timezone.utc), usd_balance]], columns = ['timestamp', 'usd_balance'])
write_table_id = 'monitor.kraken_ptf_balance'
job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
job = client.load_table_from_dataframe(
    balance_df, write_table_id, job_config=job_config
)
job.result()

### Get new ptf

yesterday = datetime.now(timezone.utc) - timedelta(days=1)
query = '''
SELECT 
upper(t.symbol) as symbol,
p.ticker,
p.date,
p.ptf_count,
i.address
FROM crypto_px.prod_ranked_combo_top_coins p
left join crypto_px.top_coins_list t on p.ticker = t.id
left join `crypto_px.train_coin_list` s on p.ticker = s.id
left join crypto_px.solana_coin_id i on s.id = i.id
where  p.ptf_count <= 50 and p.date = '{date}'
'''.format(date = yesterday.strftime("%Y-%m-%d"))
query_job = client.query(query)
ptf = query_job.result().to_dataframe()

query = '''
SELECT 
p.id,
p.date,
p.px_close
FROM crypto_px.px_daily p
where p.date = '{date}'
'''.format(date = yesterday.strftime("%Y-%m-%d"))
query_job = client.query(query)
px_list = query_job.result().to_dataframe()

# kraken tradeable
trade_ptf = ptf.merge(trade_book, how='inner', left_on = 'symbol', right_on = 'base')
trade_ptf = trade_ptf.loc[trade_ptf.quote == 'ZUSD', :].reset_index(drop=True)

old_ptf = current_ptf.loc[~current_ptf.symbol.isin(['USDC', 'ZUSD']), :]
old_ptf = old_ptf.merge(trade_book, how='inner', left_on = 'symbol', right_on = 'base')
old_ptf = old_ptf.loc[old_ptf.quote == 'ZUSD', :].reset_index(drop=True)

# get quote

px_list_df = get_current_px(trade_ptf)
new_ptf = trade_ptf.merge(px_list_df, how = 'left', on = 'altname')

# create orders
size = 0
new_ptf.loc[:, 'target'] = round(size, 0)
new_ptf.loc[:, 'quantity'] = round(new_ptf.target / new_ptf.px, 2)

order_tbl = new_ptf.loc[(new_ptf.quantity != 0), ['altname', 'quantity']]
order_tbl = order_tbl.merge(old_ptf.loc[:, ['altname', 'quantity']], how = 'outer', on = 'altname')

order_tbl.columns = ['altname', 'new_quantity', 'old_quantity']
order_tbl = order_tbl.fillna(0)

order_tbl.loc[:, 'trade_quantity'] = order_tbl.loc[:, 'new_quantity'] - order_tbl.loc[:, 'old_quantity']

sell_tbl = order_tbl.loc[order_tbl.trade_quantity < 0, :].reset_index(drop=True)
buy_tbl = order_tbl.loc[order_tbl.trade_quantity > 0, :].reset_index(drop=True)
#buy_tbl = buy_tbl.loc[~buy_tbl.altname.isin(['USDCUSD', 'USDSUSD', 'TUSDUSD', 'USDTUSD']), :]

error_list = []

if len(sell_tbl) > 0: 
    for i in range(1, len(sell_tbl)):
        this_trade = sell_tbl.iloc[i, :]

        api_nonce = str(int(time.time()*1000))

        url = "https://api.kraken.com/0/private/AddOrder"
        payload = json.dumps({
        "nonce": api_nonce,
        "ordertype": "market",
        "type": "sell",
        "volume": str(this_trade.trade_quantity * -1),
        "pair": this_trade.altname
        })

        api_sec = os.getenv("KRAKEN_PRIVATE_KEY")
        signature = get_kraken_signature("/0/private/AddOrder", payload, api_sec)
        headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'API-Key': os.getenv("KRAKEN_PUB_KEY"),
        'API-Sign': signature
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        json_data = response.json()
        error = json_data["error"]
        if len(error) > 0:
            error_list.append([this_trade.altname, this_trade.trade_quantity, error, datetime.now()])

if len(buy_tbl) > 0: 
    for i in range(1, len(buy_tbl)):
        this_buy = buy_tbl.iloc[i, :]

        api_nonce = str(int(time.time()*1000))

        url = "https://api.kraken.com/0/private/AddOrder"
        payload = json.dumps({
        "nonce": api_nonce,
        "ordertype": "market",
        "type": "buy",
        "volume": str(this_buy.trade_quantity),
        "pair": this_buy.altname
        })

        api_sec = os.getenv("KRAKEN_PRIVATE_KEY")
        signature = get_kraken_signature("/0/private/AddOrder", payload, api_sec)
        headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'API-Key': os.getenv("KRAKEN_PUB_KEY"),
        'API-Sign': signature
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        json_data = response.json()
        error = json_data["error"]
        if len(error) > 0:
            error_list.append([this_trade.altname, this_trade.trade_quantity, error, datetime.now()])

error_list_df = pd.DataFrame(error_list, columns = ['altname', 'trade_quantity', 'error', 'timestamp'])

write_table_id = 'monitor.kraken_trade_errors'
job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
job = client.load_table_from_dataframe(
    error_list_df, write_table_id, job_config=job_config
)
job.result()

