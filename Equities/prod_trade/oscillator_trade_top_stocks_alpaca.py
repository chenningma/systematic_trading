import argparse
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

import clickhouse_connect
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetAssetsRequest
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from dotenv import load_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

client = clickhouse_connect.get_client(
        host='fcnre0n2gm.us-central1.gcp.clickhouse.cloud',
        user='default',
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        secure=True
    )
# Set your Alpaca API credentials
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')


def calc_signals(px_tbl, short_period=5, long_period=34):
    # Calculate pct_change
    px_tbl['pct_change'] = px_tbl['close'] / px_tbl['close'].shift(1) - 1

    # Calculate the Awesome Oscillator
    px_tbl['median_px'] = (px_tbl['high'] + px_tbl['low']) / 2
    px_tbl['ao_5_34'] = px_tbl['median_px'].rolling(short_period).mean() - px_tbl['median_px'].rolling(long_period).mean()

    # Calculate AO bar colors (green/red) based on change from previous bar
    px_tbl['ao_color'] = np.where(px_tbl['ao_5_34'].notna(), 
                                np.where(px_tbl['ao_5_34'] >= px_tbl['ao_5_34'].shift(1), 1, -1),
                                np.nan)

    # Define long and short signals
    px_tbl['long_signal'] = 0
    px_tbl['short_signal'] = 0

    # Calculate long signal
    for i in range(4, len(px_tbl)-1):
        if (
            px_tbl['ao_color'].iloc[i-3] == -1 and 
            px_tbl['ao_color'].iloc[i-2] == -1 and 
            px_tbl['ao_color'].iloc[i-1] == 1 and
            px_tbl['ao_color'].iloc[i] == 1):
            px_tbl.loc[px_tbl.index[i], 'long_signal'] = 1
            px_tbl.loc[px_tbl.index[i]:, 'long_signal'] = 1
        
        elif (
            px_tbl.loc[px_tbl.index[i-1], 'long_signal'] == 1 and
            px_tbl['ao_color'].iloc[i] == -1):
            px_tbl.loc[px_tbl.index[i], 'long_signal'] = -1
            px_tbl.loc[px_tbl.index[i+1]:, 'long_signal'] = 0

    # Calculate short signal
    for i in range(4, len(px_tbl)-1):
        if (px_tbl['ao_color'].iloc[i-3] == 1 and 
            px_tbl['ao_color'].iloc[i-2] == 1 and 
            px_tbl['ao_color'].iloc[i-1] == -1 and
            px_tbl['ao_color'].iloc[i] == -1):
            px_tbl.loc[px_tbl.index[i], 'short_signal'] = -1
            px_tbl.loc[px_tbl.index[i]:, 'short_signal'] = -1
        
        elif (
            px_tbl.loc[px_tbl.index[i-1], 'short_signal'] == -1 and
            px_tbl['ao_color'].iloc[i] == 1):
            px_tbl.loc[px_tbl.index[i], 'short_signal'] = 1
            px_tbl.loc[px_tbl.index[i+1]:, 'short_signal'] = 0

    return px_tbl

def get_latest_px_alpaca(market_data_client,trade_symbol):
    
    request_params = StockBarsRequest(
        symbol_or_symbols=trade_symbol,
        timeframe=TimeFrame.Day
        )
    hist = market_data_client.get_stock_bars(request_params).df
    hist = hist.reset_index()
        
    if not hist.empty:
        latest_price = hist.copy()
        latest_price.columns = ['ticker','time_close', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']
        latest_price.loc[:, 'time_open'] = latest_price.loc[:, 'time_close'] 
    else:
        latest_price = None

    return latest_price

def aggregate_candlestick(candles_high_freq):

    latest = candles_high_freq.iloc[-1:]

    latest["high"] = candles_high_freq["high"].max()
    latest["low"] = candles_high_freq["low"].min()
    latest["time_open"] = candles_high_freq["time_open"].min()

    latest = latest.loc[:,['ticker','time_open','time_close', 'close', 'high', 'low']]

    return latest

def get_coin_list():
    query = '''
    with initial_date as (
    select
    ticker,
    greatest(min(date), '2020-01-31') as first_date
    from transformed.marketcap_volume_rank
    group by 1
    )
    select 
    a.*,
    b.closeadj as px_open,
    c.closeadj as px_close,
    (c.closeadj/b.closeadj - 1) * 100 as pct_return_5y,
    d.first_date,
    e.marketcap,
    e.mcap_pct_rank  
    from test.top_300_stocks_oscillator_backtest a
    left join initial_date d on a.symbol = d.ticker
    left join prices.px b on a.symbol = b.ticker and b.date = d.first_date
    left join prices.px c on a.symbol = c.ticker and c.date = '2024-12-31'
    left join transformed.marketcap_volume_rank e on a.symbol = e.ticker and d.first_date = e.date
    where e.mcap_pct_rank <0.3
    qualify row_number() over (partition by a.symbol order by a.return_5m desc) = 1
    order by a.return_5m desc
    '''
    query_job = client.query(query)
    coin_list = query_job.result().to_dataframe()

    return coin_list

def get_asset_info(trading_client, trade_symbol):
    
    search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
    assets = trading_client.get_all_assets(search_params)
    asset_info = next((asset for asset in assets if asset.symbol == trade_symbol), None)
    print(f"Symbol: {asset_info.symbol}, Tradable: {asset_info.tradable}, Shortable: {asset_info.shortable}")
    return asset_info

def buy_position(trading_client, trade_symbol, quantity):

    try:
        order_data = MarketOrderRequest(
            symbol=trade_symbol,
            qty=quantity,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        )
        order = trading_client.submit_order(order_data)
        print("Successfully bought {} with quantity {}.".format(trade_symbol, quantity))
    except Exception as e:
        print("Error buying for {}: {}".format(trade_symbol, str(e)))

def sell_position(trading_client, trade_symbol, quantity):
    try:
        order_data = MarketOrderRequest(
            symbol=trade_symbol,
            qty=quantity,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC
        )
        order = trading_client.submit_order(order_data)
        print("Successfully sold {} with quantity {}.".format(trade_symbol, quantity))
    except Exception as e:
        print("Error selling for {}: {}".format(trade_symbol, str(e)))

def execute_trades_based_on_signals(trading_client, trade_symbol, px_tbl, this_quantity):
    latest_signals = px_tbl.iloc[-1]
    long_signal = latest_signals['long_signal']
    short_signal = latest_signals['short_signal']

    try:
        current_position = trading_client.get_open_position(trade_symbol)
    except Exception as e:
        current_position = None
    
    if current_position:
        print("Current position: {}".format(current_position.qty))
        diff = this_quantity - abs(int(current_position.qty))
        
        if short_signal == 1 and int(current_position.qty) <= 0:
            print("Closing short for {}".format(trade_symbol))
            buy_position(trading_client, trade_symbol, quantity=abs(int(current_position.qty)))
        
        if long_signal == -1 and int(current_position.qty) >= 0:
            print("Closing long for {}".format(trade_symbol))
            sell_position(trading_client, trade_symbol, quantity=abs(int(current_position.qty)))
        
        if long_signal == 1 and diff > 0:
            print("Adding to position for {}".format(trade_symbol))
            buy_position(trading_client, trade_symbol, quantity=diff)
        if long_signal == 1 and diff < 0:
            print("Cutting position for {}".format(trade_symbol))
            sell_position(trading_client, trade_symbol, quantity=abs(diff))
        
        if short_signal == -1 and diff > 0:
            print("Adding to position for {}".format(trade_symbol))
            sell_position(trading_client, trade_symbol, quantity=diff)
        if short_signal == -1 and diff < 0:
            print("Cutting position for {}".format(trade_symbol))
            buy_position(trading_client, trade_symbol, quantity=abs(diff))
           
    else:
        if long_signal == 1:
            print("Opening long for {}".format(trade_symbol))
            buy_position(trading_client, trade_symbol, quantity=this_quantity)
        elif short_signal == -1:
            print("Opening short for {}".format(trade_symbol))
            sell_position(trading_client, trade_symbol, quantity=this_quantity)

def alpaca_trade(trading_client, market_data_client, coin_list, trade_symbol, trade_amount):

    asset_info = get_asset_info(trading_client, trade_symbol)

    if not asset_info.shortable:
        print("{} is not shortable".format(trade_symbol))
        sys.exit()

    ### calculate trade signals
    short_per = int(coin_list.loc[coin_list['symbol'] == trade_symbol, 'short_period'].values[0])
    long_per = int(coin_list.loc[coin_list['symbol'] == trade_symbol, 'long_period'].values[0])

    start_date = (datetime.now() - timedelta(days=5*long_per)).strftime('%Y-%m-%d')

    query = '''
        select 
            ticker,
            date as time_open,
            date as time_close,
            high,
            low,
            closeadj as close
        from `prices.px`
        where ticker = '{symbol}'
        and date >= '{date}'
        order by time_close
        '''.format(date=start_date, symbol=trade_symbol)
    query_job = client.query(query)
    px_5m = query_job.result().to_dataframe()

    latest_price = get_latest_px_alpaca(market_data_client,trade_symbol)
    latest_price = latest_price.loc[:,['ticker','time_open', 'time_close', 'high', 'low', 'close']]
    today_candle = aggregate_candlestick(latest_price)

    px_5m = pd.concat([px_5m, today_candle])
    px_5m = px_5m.reset_index(drop=True)

    px_tbl = calc_signals(px_5m, short_period=short_per, long_period=long_per)
    print(f"Signal: {px_tbl.time_close.iloc[-1]} - Long: {px_tbl.long_signal.iloc[-1]} - Short: {px_tbl.short_signal.iloc[-1]}")
    
    ### execute trades
    trade_quantity = round(trade_amount / px_tbl.iloc[-1]['close'])
    execute_trades_based_on_signals(trading_client, trade_symbol, px_tbl, trade_quantity)

def main(trade_symbol, trade_amount):
    
    coin_list = get_coin_list()
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=False)
    market_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    alpaca_trade(trading_client, market_data_client, coin_list, trade_symbol, trade_amount)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Alpaca trade script')
    parser.add_argument('--symbol', type=str, help='The symbol of the stock to trade')
    parser.add_argument('--amount', type=float, help='The amount of money to trade with')

    args = parser.parse_args()
    trade_symbol = args.symbol
    trade_amount = args.amount

    main(trade_symbol, trade_amount)
