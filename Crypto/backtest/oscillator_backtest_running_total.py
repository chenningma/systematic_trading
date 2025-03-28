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


def calc_px_interval(px_1m, interval):

    rows = round(interval / 5)

    px_1m["high_calc"] = px_1m["high"].rolling(window=rows, min_periods=rows).max()
    px_1m["low_calc"] = px_1m["low"].rolling(window=rows, min_periods=rows).min()

    if interval < 60:
        px_calc = px_1m.loc[px_1m.time_close.dt.minute % interval == 0,['time_open', 'time_close','close', 'high_calc', 'low_calc']]
    else:
        px_calc = px_1m.loc[px_1m.time_close.dt.hour % (interval/60) == 0,['time_open', 'time_close','close', 'high_calc', 'low_calc']]

    px_calc.columns = ['time_open','time_close', 'close', 'high', 'low']

    return px_calc

def calc_signals(px_tbl):
    # Calculate pct_change
    px_tbl['pct_change'] = px_tbl['close'] / px_tbl['close'].shift(1) - 1

    # Calculate the Awesome Oscillator
    px_tbl['median_px'] = (px_tbl['high'] + px_tbl['low']) / 2
    px_tbl['ao_5_34'] = px_tbl['median_px'].rolling(5).mean() - px_tbl['median_px'].rolling(34).mean()

    # Calculate AO bar colors (green/red) based on change from previous bar
    px_tbl['ao_color'] = np.where(px_tbl['ao_5_34'].notna(), 
                                np.where(px_tbl['ao_5_34'] >= px_tbl['ao_5_34'].shift(1), 1, -1),
                                np.nan)

    # Define long and short signals
    px_tbl['long_signal'] = 0
    px_tbl['short_signal'] = 0

    # Calculate long signal
    # Above zero line, two red bars (decreasing), followed by green
    for i in range(4, len(px_tbl)-1):
        # Check for long entry pattern (-1,-1,1,1)
        if (
            px_tbl['ao_color'].iloc[i-3] == -1 and 
            px_tbl['ao_color'].iloc[i-2] == -1 and 
            px_tbl['ao_color'].iloc[i-1] == 1 and
            px_tbl['ao_color'].iloc[i] == 1):
            px_tbl.loc[px_tbl.index[i], 'long_signal'] = 1
            # Propagate long signal forward
            px_tbl.loc[px_tbl.index[i]:, 'long_signal'] = 1
        
        # Check for close long pattern (1,1,-1) if currently in long position
        elif (
            px_tbl.loc[px_tbl.index[i-1], 'long_signal'] == 1 and
            px_tbl['ao_color'].iloc[i] == -1):
            px_tbl.loc[px_tbl.index[i], 'long_signal'] = -1
            # Clear any forward propagated long signals
            px_tbl.loc[px_tbl.index[i+1]:, 'long_signal'] = 0  # Changed to i+1 to keep close_long signal

    # Calculate short signal
    for i in range(4, len(px_tbl)-1):
        # Check for short entry pattern (1,1,-1,-1)
        if (px_tbl['ao_color'].iloc[i-3] == 1 and 
            px_tbl['ao_color'].iloc[i-2] == 1 and 
            px_tbl['ao_color'].iloc[i-1] == -1 and
            px_tbl['ao_color'].iloc[i] == -1):
            px_tbl.loc[px_tbl.index[i], 'short_signal'] = -1
            # Propagate short signal forward
            px_tbl.loc[px_tbl.index[i]:, 'short_signal'] = -1
        
        # Check for close short pattern (-1,-1,1) if currently in short position
        elif (
            px_tbl.loc[px_tbl.index[i-1], 'short_signal'] == -1 and
            px_tbl['ao_color'].iloc[i] == 1):
            px_tbl.loc[px_tbl.index[i], 'short_signal'] = 1
            # Clear any forward propagated short signals
            px_tbl.loc[px_tbl.index[i+1]:, 'short_signal'] = 0  # Changed to i+1 to keep close_short signal

    return px_tbl

def plot_signals(px_tbl):
    fig_tbl = px_tbl.loc[px_tbl['time_close'] > '2025-01-01']

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

    # Top plot - close price
    ax1.plot(fig_tbl['time_close'], fig_tbl['close'], label='Close Price')
    ax1.legend()
    ax1.set_title('Close Price')

    # Bottom plot - AO bars and saucer signals
    colors = ['red' if x == -1 else 'green' for x in fig_tbl['ao_color']]
    # Calculate bar width based on average time difference between points
    time_diffs = pd.Series(fig_tbl['time_close']).diff().mean()
    bar_width = time_diffs * 0.8  # Make bars slightly narrower than time interval

    ax2.bar(fig_tbl['time_close'], fig_tbl['ao_5_34'], color=colors, 
            label='Awesome Oscillator', width=bar_width)

    # Plot buy signals
    buy_mask = fig_tbl['long_signal'] == 1
    ax2.scatter(fig_tbl.loc[buy_mask, 'time_close'], 
            fig_tbl.loc[buy_mask, 'ao_5_34'],
            color='lime', marker='^', s=100, label='Buy Signal')

    close_long_mask = fig_tbl['long_signal'] == -1
    ax2.scatter(fig_tbl.loc[close_long_mask, 'time_close'],
            fig_tbl.loc[close_long_mask, 'ao_5_34'],
            color='darkgreen', marker='x', s=100, label='Close Long Signal')

    # Plot sell signals
    sell_mask = fig_tbl['short_signal'] == -1
    ax2.scatter(fig_tbl.loc[sell_mask, 'time_close'],
            fig_tbl.loc[sell_mask, 'ao_5_34'],
            color='red', marker='v', s=100, label='Sell Signal')

    close_short_mask = fig_tbl['short_signal'] == 1
    ax2.scatter(fig_tbl.loc[close_short_mask, 'time_close'],
            fig_tbl.loc[close_short_mask, 'ao_5_34'],
            color='darkred', marker='x', s=100, label='Close Short Signal')

    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax2.legend()
    ax2.set_title('Awesome Oscillator and Trade Signals')

    plt.tight_layout()
    plt.show()

def calculate_returns(px_tbl, fees):
    trade_tbl = px_tbl[['time_open', 'time_close', 'close', 'pct_change','long_signal', 'short_signal']]

    # Initialize long_return column with zeros
    trade_tbl['long_trade'] = 0

    # iterate through trade_tbl and fill long_return
    for i in range(1, len(trade_tbl)):
        # Fill returns when we see a second consecutive buy signal (1)
        if trade_tbl['long_signal'].iloc[i] == 1 and trade_tbl['long_signal'].iloc[i-1] == 0:
            trade_tbl.loc[trade_tbl.index[i], 'long_trade'] = 1
        # Also fill returns when we see a close signal (-1)  
        elif trade_tbl['long_signal'].iloc[i] == -1:
            trade_tbl.loc[trade_tbl.index[i], 'long_trade'] = -1

    # Initialize short_return column with zeros
    trade_tbl['short_trade'] = 0

    # iterate through trade_tbl and fill short_return
    for i in range(1, len(trade_tbl)):
        # Fill returns when we see a second consecutive buy signal (1)
        if trade_tbl['short_signal'].iloc[i] == -1 and trade_tbl['short_signal'].iloc[i-1] == 0:
            trade_tbl.loc[trade_tbl.index[i], 'short_trade'] = 1
        # Also fill returns when we see a close signal (-1)  
        elif trade_tbl['short_signal'].iloc[i] == 1:
            trade_tbl.loc[trade_tbl.index[i], 'short_trade'] = -1

    # Mark trades
    long_trades = trade_tbl.loc[(trade_tbl['long_trade'] != 0)]
    short_trades = trade_tbl.loc[(trade_tbl['short_trade'] != 0)]

    long_trades.loc[:, 'trade_return'] = long_trades['close'] / long_trades['close'].shift(1) - 1
    short_trades.loc[:, 'trade_return'] = (short_trades['close'] / short_trades['close'].shift(1) - 1)*-1

    long_trades = long_trades.loc[long_trades['long_trade'] == -1]
    short_trades = short_trades.loc[short_trades['short_trade'] == -1]

    long_trades.loc[:, 'trade_cost'] = fees
    short_trades.loc[:, 'trade_cost'] = fees

    long_trades.loc[:, 'total_return'] = long_trades['trade_return'] + long_trades['trade_cost'] 
    short_trades.loc[:, 'total_return'] = short_trades['trade_return'] + short_trades['trade_cost'] 

    fnl_tbl = pd.concat([long_trades, short_trades])
    fnl_tbl = fnl_tbl.sort_values(by = 'time_open')

    # Calculate trade return

    fnl_tbl['total_return_index'] = 100 * (1 + fnl_tbl['trade_return']).cumprod()

    result = fnl_tbl[['time_open','time_close', 'close', 'long_signal', 'short_signal', 'long_trade', 'short_trade', 'trade_return', 'total_return_index']]

    return result

def plot_total_return_index(result):
    plt.figure(figsize=(12, 6))
    plt.plot(result['time_close'], result['total_return_index'])
    plt.title('Strategy Total Return Index')
    plt.xlabel('Date')
    plt.ylabel('Total Return Index')
    plt.grid(True)
    plt.show()

## 
query = '''
SELECT 
distinct symbol
FROM crypto_px.hyperliquid_px_5m
'''
query_job = client.query(query)
coin_list = query_job.result().to_dataframe()


## 
query = '''
with tb1 as (
select
date_start,
date_end,
 DATE_DIFF(date_end, date_start, DAY) AS days_diff 
FROM `boreal-pride-417020`.`crypto_px`.`ao_backtest`
)
select
distinct
date_start
from tb1
where days_diff < 5
'''
query_job = client.query(query)
start_date = query_job.result().to_dataframe()

if len(start_date) > 0:
    start_date = start_date['date_start'].iloc[0]

    query = '''
    delete FROM `boreal-pride-417020`.`crypto_px`.`ao_backtest`
    where date(date_start) = '{start_date}'
    '''.format(start_date = start_date.strftime("%Y-%m-%d"))
    query_job = client.query(query)

    initial_start = pd.to_datetime(start_date)

else:
    query = '''
    select
    max(date_end) as date_end
    from `boreal-pride-417020`.`crypto_px`.`ao_backtest`
    '''
    query_job = client.query(query)
    last_date = query_job.result().to_dataframe().date_end.iloc[0]
    initial_start = pd.to_datetime(last_date)

## 
# Calculate date ranges
end_date = datetime.now(timezone.utc)
    
backtest_results = []
for this_symbol in coin_list['symbol']:

    query = '''
    SELECT 
    *
    FROM crypto_px.hyperliquid_px_5m
    where symbol = '{symbol}' 
    order by time_close
    '''.format(symbol = this_symbol)
    query_job = client.query(query)
    px_5m = query_job.result().to_dataframe()

    px_5m["time_close"] = px_5m["time_close"].dt.round('min')
    
    #px_10m = calc_px_interval(px_5m, 10)
    #px_15m = calc_px_interval(px_5m, 15)
    #px_20m = calc_px_interval(px_5m, 20)

    ### Calculate signals
    px_tbl = calc_signals(px_5m)
    
    ### Start rolling backtest
    try:
        
        this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= initial_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= end_date.strftime("%Y-%m-%d %H:%M:%S"))]
        print(f"Running backtest for {this_symbol} from {initial_start} to {end_date}")
        
        result = calculate_returns(this_px_tbl, fees=-0.00045)
        result['timestamp'] = result['time_open'].apply(lambda x: int(x.timestamp() * 1000))
        
        fnl_return_5m = result['total_return_index'].iloc[-1]
        print(f"Total Return (5m): {fnl_return_5m:.2f}")

        this_result = [this_symbol, initial_start, end_date, fnl_return_5m]
        backtest_results.append(this_result)
    
    except Exception as e:
        print(str(e))

backtest_results_df = pd.DataFrame(backtest_results, columns=['symbol','date_start','date_end', 'return_5m'])

# Write backtest results to BigQuery
table_id = 'crypto_px.ao_backtest'
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(backtest_results_df, table_id, job_config=job_config)
job.result()
print("Backtest results written to BigQuery table")