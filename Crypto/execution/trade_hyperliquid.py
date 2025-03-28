import json
import os
import requests
import pandas as pd
import numpy as np
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

def setup(base_url=None, skip_ws=False):
    config_path = os.path.dirname(__file__) + "/Crypto/config.json"
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

def get_positions():
    user_state = info.user_state(address)
    positions = []
    for position in user_state["assetPositions"]:
        positions.append(position["position"])
    positions_df = pd.DataFrame(positions)
    return positions_df

# get tradeable universe

json_data = info.meta()
trade_book = pd.DataFrame(json_data['universe'])

table_id = 'crypto_px.hyperliquid_coin_list'
job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
job = client.load_table_from_dataframe(
    trade_book, table_id, job_config=job_config
)
job.result()

# Get the user state and print out position information
positions_df = get_positions()

### Get new ptf
yesterday = datetime.now(timezone.utc) - timedelta(days=1)
query = '''
SELECT 
upper(t.symbol) as symbol,
p.ticker,
p.date,
p.ptf_count,
i.address,
l.max_leverage
FROM crypto_px.prod_ranked_combo_top_coins p
left join crypto_px.top_coins_list t on p.ticker = t.id
left join `crypto_px.train_coin_list` s on p.ticker = s.id
left join crypto_px.solana_coin_id i on s.id = i.id
left join crypto_px.coins_max_leverage l on upper(t.symbol) = l.symbol
where  p.ptf_count <= 30 and p.date = '{date}'
'''.format(date = yesterday.strftime("%Y-%m-%d"))
query_job = client.query(query)
ptf = query_job.result().to_dataframe()

trade_ptf = ptf.merge(trade_book, how='inner', left_on = 'symbol', right_on = 'name')

if len(positions_df) > 0:
    positions_df.loc[:, 'szi'] = positions_df.loc[:, 'szi'].astype(float)
    pos_positions = positions_df.loc[positions_df.szi > 0, :]

    if len(pos_positions) > 0: 
        idx = np.in1d(pos_positions.coin, ptf.symbol)
        sell_ptf = pos_positions.loc[~idx, :]

        if len(sell_ptf) > 0:
            for coin in sell_ptf.coin:
                order_result = exchange.market_close(coin)
                if order_result["status"] == "ok":
                    for status in order_result["response"]["data"]["statuses"]:
                        try:
                            filled = status["filled"]
                            print(f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}')
                        except KeyError:
                            print(f'Error: {status["error"]}')
                    
### get latest price

json_data = info.all_mids()
px_tbl = pd.DataFrame(json_data.items(), columns = ['symbol', 'px'])
trade_ptf = trade_ptf.merge(px_tbl, how = 'left', on = 'symbol')
trade_ptf.loc[:, 'px'] = trade_ptf.loc[:, 'px'].astype(float)

position_size = 2000
trade_ptf.loc[:, 'px_stoploss'] = trade_ptf.loc[:, 'px'] * .85
trade_ptf.loc[:, 'px_takeprofit'] = trade_ptf.loc[:, 'px'] * 1.2
trade_ptf.loc[:, 'size'] = position_size / trade_ptf.loc[:, 'px']
trade_ptf.loc[:, 'size_round'] = trade_ptf.loc[:, 'size'].apply(lambda x: round(x))

if len(positions_df) > 0:
    idx = np.in1d(trade_ptf.symbol, positions_df.coin)
    buy_ptf = trade_ptf.loc[~idx, :]
else:
    buy_ptf = trade_ptf.copy()

buy_ptf = buy_ptf.loc[buy_ptf.symbol != 'STRAX', :].reset_index(drop = True)
### Place Orders
# Place an order that should rest by setting the price very low
for i in range(0, len(buy_ptf)):
    try:
        coin = buy_ptf.loc[i, 'symbol']
        is_buy = True
        sz = buy_ptf.loc[i, 'size_round']
        sl = round(buy_ptf.loc[i, 'px_stoploss'], 2)
        tp = round(buy_ptf.loc[i, 'px_takeprofit'], 2)
        leverage = int(buy_ptf.loc[i, 'max_leverage'])

        print(f"We try to Market {'Buy' if is_buy else 'Sell'} {sz} {coin}.")
        # Set leverage to 21x (cross margin)
        exchange.update_leverage(leverage, coin)
        
        order_result = exchange.market_open(coin, is_buy, sz, None, 0.01)
        #order_result = exchange.order(symbol, is_buy, 0.2, 1100, {"trigger": {"isMarket": True}})
        print(order_result)

        # Place a stop order
        stop_order_type = {"trigger": {"triggerPx": sl if is_buy else tp, "isMarket": True, "tpsl": "sl"}}
        stop_result = exchange.order(coin, not is_buy, sz, sl if is_buy else tp, stop_order_type, reduce_only=True)

        # Place a tp order
        tp_order_type = {"trigger": {"triggerPx": tp if is_buy else sl, "isMarket": True, "tpsl": "tp"}}
        tp_result = exchange.order(coin, not is_buy, sz, tp if is_buy else sl, tp_order_type, reduce_only=True)
        #print(tp_result)

    except Exception as e:
        print(str(e))

# get positions
new_positions = get_positions()
new_positions = new_positions.drop(columns = ['leverage', 'cumFunding'])
new_positions.loc[:, 'as_of_date'] = datetime.now(timezone.utc)

table_id = 'monitor.hyperliquid_portfolio'
job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
job = client.load_table_from_dataframe(
    new_positions, table_id, job_config=job_config
)
job.result()