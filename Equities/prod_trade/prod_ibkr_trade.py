import requests
import json
import pandas as pd
import os 
from google.cloud import bigquery
import numpy as np
from datetime import datetime, timedelta
import time

from ib_insync import *

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame

from dotenv import load_dotenv
parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

# Set your Alpaca API credentials
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
market_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

snapshot_table_id = 'boreal-pride-417020.monitor.ibkr_ptf_snapshot'

def get_latest_px_alpaca_bulk(market_data_client, trade_list):
    latest_price_list = []

    for trade_symbol in trade_list:
        request_params = StockBarsRequest(
            symbol_or_symbols=trade_symbol,
            timeframe=TimeFrame.Day
        )
        hist = market_data_client.get_stock_bars(request_params).df
        hist = hist.reset_index()
        
        if not hist.empty:
            latest_price = hist.copy()
            latest_price.columns = ['ticker', 'time_close', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']
            latest_price.loc[:, 'time_open'] = latest_price.loc[:, 'time_close']
            latest_price_list.append(latest_price)
    
    if latest_price_list:
        latest_price_table = pd.concat(latest_price_list, ignore_index=True)
    else:
        latest_price_table = pd.DataFrame()
    
    return latest_price_table

def create_ptf_snapshot(account_id, snap_type):
    snapshot = ib.positions(account = account_id)
    ticker = []
    for pos in snapshot:
        this_contract = pos.contract.symbol
        ticker.append(this_contract)
    snapshot = pd.concat([pd.DataFrame(snapshot), pd.DataFrame(ticker, columns = ['ticker'])], axis = 1)

    snapshot = snapshot.drop(columns = ['contract'])
    px_tbl = get_latest_px_alpaca_bulk(market_data_client,snapshot.ticker)
    to_add = px_tbl.loc[:, ['ticker', 'close']]
    to_add.columns = ['ticker', 'px']
    
    snapshot = snapshot.merge(to_add, how = 'left', on='ticker')
    snapshot.loc[:, 'snapshot_type'] = snap_type
    snapshot.loc[:, 'as_of_date'] = datetime.now() 
    return snapshot

def execute_trade(account_id, new_ptf):
    # get cash
    acct_tbl = ib.accountValues(account = account_id)
    acct_tbl = pd.DataFrame(acct_tbl)

    cash = acct_tbl.loc[acct_tbl.tag == 'NetLiquidation', 'value'].values[0]
    cash = float(cash)

    # create orders
    size = cash/50
    new_ptf.loc[:, 'target'] = round(size, 0)
    new_ptf.loc[:, 'quantity'] = round(new_ptf.target / new_ptf.px, 0)
    new_ptf.loc[:, 'quantity_frac'] = round(new_ptf.target / new_ptf.px, 2)

    ## get existing positions
    pos_tbl = create_ptf_snapshot(account_id, 'end')
    pos_tbl = pos_tbl.rename(columns = {'old_ticker':'ticker'})

    # create master table
    all_tbl = pos_tbl.merge(new_ptf, how = 'outer', on = 'ticker')

    all_tbl.loc[:, 'position'] = all_tbl.position.fillna(0)
    all_tbl.loc[:, 'quantity'] = all_tbl.quantity.fillna(0)
    all_tbl.loc[:, 'quantity_frac'] = all_tbl.quantity_frac.fillna(0)

    all_tbl.loc[:, 'trade_quantity'] = all_tbl.quantity - all_tbl.position
    all_tbl.loc[:, 'px'] = all_tbl.px_x.combine_first(all_tbl.px_y)

    order_tbl = all_tbl.loc[(all_tbl.trade_quantity != 0) & (all_tbl.px != 10000) , ['ticker', 'trade_quantity']]

    manual = new_ptf.loc[new_ptf.quantity == 0, ['ticker', 'quantity_frac']]
    sell_tbl = order_tbl.loc[order_tbl.trade_quantity < 0, :].reset_index(drop=True)
    buy_tbl = order_tbl.loc[order_tbl.trade_quantity > 0, :].reset_index(drop=True)

    # sell positions
    for i in range(0, len(sell_tbl)):
        try:
            this_contract = Stock(symbol = sell_tbl.loc[i, 'ticker'], exchange = 'SMART', currency='USD')
            order = MarketOrder('SELL', sell_tbl.loc[i, 'trade_quantity']*-1)
            order.account = account_id
            trade = ib.placeOrder(this_contract, order)
        except Exception as e:
            print(str(e))

    # buy positions
    for i in range(0, len(buy_tbl)):
        try:
            this_contract = Stock(symbol = buy_tbl.loc[i, 'ticker'], exchange = 'SMART', currency='USD')
            order = MarketOrder('BUY', buy_tbl.loc[i, 'trade_quantity'])
            order.account = account_id
            trade = ib.placeOrder(this_contract, order)
        except Exception as e:
            print(str(e))

    print("Manual Trades:")
    print(manual)

### Check trades
def check_trades(account_id, new_ptf):
    new_pos = ib.positions(account = account_id)

    old_ticker = []
    for pos in new_pos:
        this_contract = pos.contract.symbol
        old_ticker.append(this_contract)

    new_pos = pd.concat([pd.DataFrame(new_pos), pd.DataFrame(old_ticker, columns = ['old_ticker'])], axis = 1)

    # create master table
    new_pos = new_pos.merge(new_ptf, how = 'outer', left_on = 'old_ticker', right_on = 'ticker')

    clean = new_pos.loc[new_pos.ticker.isnull(), :]
    print("Cleanup Trades for {}".format(account_id))
    print(clean)
    return clean

# connect to TWS
util.startLoop()
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

latest_date = datetime.today() + pd.offsets.MonthEnd(-1)

query_job = client.query("""
SELECT 
model_id
FROM boreal-pride-417020.prod.meta_combo_ptfs p
where date = '{}'
""".format(latest_date.strftime('%Y-%m-%d')))

model_id = query_job.result().to_dataframe().model_id

query_job = client.query("""
SELECT 
p.*
FROM prod.backtest_ptfs_combo p
where p.model_id = {} and date = '{}'
""".format(model_id[0], latest_date.strftime('%Y-%m-%d')))

stock_list = query_job.result().to_dataframe()

new_ptf = stock_list.copy()

# get price
trade_list = new_ptf.ticker

px_tbl = get_latest_px_alpaca_bulk(market_data_client, trade_list)

to_add = px_tbl.loc[:, ['ticker', 'close']]
to_add.columns = ['ticker', 'px']
new_ptf = new_ptf.merge(to_add, how = 'left', on = 'ticker')

new_ptf.loc[:, 'px'] = new_ptf.px.fillna(10000)

## calculate new position
### Traditional IRA Trades
account_id = os.getenv('IBKR_ACCOUNT2') 
execute_trade(account_id, new_ptf)
time.sleep(10)
# cancel all open orders
ib.reqGlobalCancel()

# current ptf snapshot
snapshot = create_ptf_snapshot(account_id, 'start')

# write to db
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
)
job = client.load_table_from_dataframe(
                snapshot, snapshot_table_id, job_config=job_config
            )
job.result()

#### Brokerage Trades
account_id = os.getenv('IBKR_BROKERAGE_ACCOUNT')
execute_trade(account_id, new_ptf)
time.sleep(10)

### Clean up trades
cleanup = check_trades(os.getenv('IBKR_ACCOUNT2'), new_ptf)
cleanup = check_trades(os.getenv('IBKR_BROKERAGE_ACCOUNT'), new_ptf)

# cancel all open orders
ib.reqGlobalCancel()

ib.disconnect()

