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

def calc_signals(px_tbl, short_period=5, long_period=34):
    
    signal_tbl = px_tbl.copy()
    # Calculate pct_change
    signal_tbl['pct_change'] = signal_tbl['close'] / signal_tbl['close'].shift(1) - 1

    # Calculate the Awesome Oscillator
    signal_tbl['median_px'] = (signal_tbl['high'] + signal_tbl['low']) / 2
    signal_tbl['ao_5_34'] = signal_tbl['median_px'].rolling(short_period).mean() - signal_tbl['median_px'].rolling(long_period).mean()

    # Calculate AO bar colors (green/red) based on change from previous bar
    signal_tbl['ao_color'] = np.where(signal_tbl['ao_5_34'].notna(), 
                                np.where(signal_tbl['ao_5_34'] >= signal_tbl['ao_5_34'].shift(1), 1, -1),
                                np.nan)

    # Define long and short signals
    signal_tbl['long_signal'] = 0
    signal_tbl['short_signal'] = 0

    # Calculate long signal
    # Above zero line, two red bars (decreasing), followed by green
    for i in range(4, len(signal_tbl)-1):
        # Check for long entry pattern (-1,-1,1,1)
        if (
            signal_tbl['ao_color'].iloc[i-3] == -1 and 
            signal_tbl['ao_color'].iloc[i-2] == -1 and 
            signal_tbl['ao_color'].iloc[i-1] == 1 and
            signal_tbl['ao_color'].iloc[i] == 1):
            signal_tbl.loc[signal_tbl.index[i], 'long_signal'] = 1
            # Propagate long signal forward
            signal_tbl.loc[signal_tbl.index[i]:, 'long_signal'] = 1
        
        # Check for close long pattern (1,1,-1) if currently in long position
        elif (
            signal_tbl.loc[signal_tbl.index[i-1], 'long_signal'] == 1 and
            signal_tbl['ao_color'].iloc[i] == -1):
            signal_tbl.loc[signal_tbl.index[i], 'long_signal'] = -1
            # Clear any forward propagated long signals
            signal_tbl.loc[signal_tbl.index[i+1]:, 'long_signal'] = 0  # Changed to i+1 to keep close_long signal

    # Calculate short signal
    for i in range(4, len(signal_tbl)-1):
        # Check for short entry pattern (1,1,-1,-1)
        if (signal_tbl['ao_color'].iloc[i-3] == 1 and 
            signal_tbl['ao_color'].iloc[i-2] == 1 and 
            signal_tbl['ao_color'].iloc[i-1] == -1 and
            signal_tbl['ao_color'].iloc[i] == -1):
            signal_tbl.loc[signal_tbl.index[i], 'short_signal'] = -1
            # Propagate short signal forward
            signal_tbl.loc[signal_tbl.index[i]:, 'short_signal'] = -1
        
        # Check for close short pattern (-1,-1,1) if currently in short position
        elif (
            signal_tbl.loc[signal_tbl.index[i-1], 'short_signal'] == -1 and
            signal_tbl['ao_color'].iloc[i] == 1):
            signal_tbl.loc[signal_tbl.index[i], 'short_signal'] = 1
            # Clear any forward propagated short signals
            signal_tbl.loc[signal_tbl.index[i+1]:, 'short_signal'] = 0  # Changed to i+1 to keep close_short signal

    return signal_tbl

def plot_signals(px_tbl, lookback=500):
    # Convert time_close to datetime if it's not already
    px_tbl['time_close'] = pd.to_datetime(px_tbl['time_close'])
    
    # Filter for recent data (last 100 points) to make the plot more readable
    fig_tbl = px_tbl.tail(lookback).copy()
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

    # Top plot - close price
    ax1.plot(fig_tbl['time_close'], fig_tbl['close'], label='Close Price', color='blue')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    ax1.set_title('Close Price')

    # Bottom plot - AO bars and signals
    colors = ['red' if x == -1 else 'green' for x in fig_tbl['ao_color']]
    
    # Calculate bar width based on time differences
    time_deltas = fig_tbl['time_close'].diff().dt.total_seconds()
    avg_delta = time_deltas.median()
    bar_width = timedelta(seconds=avg_delta * 0.8)  # Make bars slightly narrower than time interval

    # Plot AO bars
    ax2.bar(fig_tbl['time_close'], fig_tbl['ao_5_34'], 
            color=colors, 
            width=bar_width,
            label='Awesome Oscillator')

    # Plot signals
    # Buy signals
    buy_mask = fig_tbl['long_signal'] == 1
    ax2.scatter(fig_tbl.loc[buy_mask, 'time_close'], 
            fig_tbl.loc[buy_mask, 'ao_5_34'],
            color='lime', marker='^', s=100, label='Buy Signal')

    # Close long signals
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
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    ax2.set_title('Awesome Oscillator and Trade Signals')

    # Format x-axis
    plt.gcf().autofmt_xdate()  # Angle and align the tick labels so they look better
    
    # Add spacing between subplots
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
# Calculate date ranges
end_date = datetime.now(timezone.utc)
initial_start = pd.to_datetime('2024-12-01').tz_localize('UTC')

# Create 15-day intervals
date_ranges = []
current_start = initial_start
while current_start < end_date:
    current_end = min(current_start + timedelta(days=5), end_date)
    date_ranges.append((current_start, current_end))
    current_start = current_end
    
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
    for this_start, this_end in date_ranges:
        try:
            
            this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
            print(f"Running backtest for {this_symbol} from {this_start} to {this_end}")
            
            result = calculate_returns(this_px_tbl, fees=-0.00045)
            result['timestamp'] = result['time_open'].apply(lambda x: int(x.timestamp() * 1000))
            
            fnl_return_5m = result['total_return_index'].iloc[-1]
            print(f"Total Return (5m): {fnl_return_5m:.2f}")

            this_result = [this_symbol, this_start, this_end, fnl_return_5m]
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




### individual coin backtest

def calc_signals_test(px_tbl, short_period=5, long_period=34):
    
    signal_tbl = px_tbl.copy()
    # Calculate pct_change
    signal_tbl['pct_change'] = signal_tbl['close'] / signal_tbl['close'].shift(1) - 1

    # Calculate the Awesome Oscillator
    signal_tbl['median_px'] = (signal_tbl['high'] + signal_tbl['low']) / 2
    signal_tbl['ao_5_34'] = signal_tbl['median_px'].rolling(short_period).mean() - signal_tbl['median_px'].rolling(long_period).mean()

    # Calculate AO bar colors (green/red) based on change from previous bar
    signal_tbl['ao_color'] = np.where(signal_tbl['ao_5_34'].notna(), 
                                np.where(signal_tbl['ao_5_34'] >= signal_tbl['ao_5_34'].shift(1), 1, -1),
                                np.nan)

    # Define long and short signals
    signal_tbl['long_signal'] = 0
    signal_tbl['short_signal'] = 0

    # Calculate long signal
    # Above zero line, two red bars (decreasing), followed by green
    for i in range(4, len(signal_tbl)-1):
        # Check for long entry pattern (-1,-1,1,1)
        if (
            signal_tbl['ao_color'].iloc[i-3] == -1 and 
            signal_tbl['ao_color'].iloc[i-2] == -1 and 
            signal_tbl['ao_color'].iloc[i-1] == 1 and
            signal_tbl['ao_color'].iloc[i] == 1):
            signal_tbl.loc[signal_tbl.index[i], 'long_signal'] = 1
            # Propagate long signal forward
            signal_tbl.loc[signal_tbl.index[i]:, 'long_signal'] = 1
        
        # Check for close long pattern (1,1,-1, -1) if currently in long position
        elif (
            signal_tbl.loc[signal_tbl.index[i-1], 'long_signal'] == 1 and
            signal_tbl['ao_color'].iloc[i-3] == 1 and 
            signal_tbl['ao_color'].iloc[i-2] == 1 and 
            signal_tbl['ao_color'].iloc[i-1] == -1 and
            signal_tbl['ao_color'].iloc[i] == -1):
            signal_tbl.loc[signal_tbl.index[i], 'long_signal'] = -1
            # Clear any forward propagated long signals
            signal_tbl.loc[signal_tbl.index[i+1]:, 'long_signal'] = 0  # Changed to i+1 to keep close_long signal

    # Calculate short signal
    for i in range(4, len(signal_tbl)-1):
        # Check for short entry pattern (1,1,-1,-1)
        if (
            signal_tbl['ao_color'].iloc[i-3] == 1 and 
            signal_tbl['ao_color'].iloc[i-2] == 1 and 
            signal_tbl['ao_color'].iloc[i-1] == -1 and
            signal_tbl['ao_color'].iloc[i] == -1):
            signal_tbl.loc[signal_tbl.index[i], 'short_signal'] = -1
            # Propagate short signal forward
            signal_tbl.loc[signal_tbl.index[i]:, 'short_signal'] = -1
        
        # Check for close short pattern (-1,-1,1,1) if currently in short position
        elif (
            signal_tbl.loc[signal_tbl.index[i-1], 'short_signal'] == -1 and
            signal_tbl['ao_color'].iloc[i-3] == -1 and 
            signal_tbl['ao_color'].iloc[i-2] == -1 and 
            signal_tbl['ao_color'].iloc[i-1] == 1 and
            signal_tbl['ao_color'].iloc[i] == 1):
            signal_tbl.loc[signal_tbl.index[i], 'short_signal'] = 1
            # Clear any forward propagated short signals
            signal_tbl.loc[signal_tbl.index[i+1]:, 'short_signal'] = 0  # Changed to i+1 to keep close_short signal

    return signal_tbl

this_symbol = 'FARTCOIN'

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

### Calculate signals
px_tbl = calc_signals_test(px_5m, 12, 32)
#px_tbl = calc_signals_test(px_5m, 19, 135)

px_tbl.to_csv(f'{this_symbol}_signals.csv', index=False)

# Plot AO with saucer signals
#signals = px_tbl[['time_close', 'long_signal', 'short_signal']]
plot_signals(px_tbl, 500)

### Start rolling backtest
this_start = datetime.now(timezone.utc) - timedelta(days=5)
this_end = datetime.now(timezone.utc)

this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
print(f"Running backtest for {this_symbol} from {this_start} to {this_end}")

result = calculate_returns(this_px_tbl, fees=-0.00045)
result['timestamp'] = result['time_open'].apply(lambda x: int(x.timestamp() * 1000))

output = this_px_tbl.merge(result.loc[:, ['time_open', 'time_close', 'trade_return']], how='left', on=['time_open', 'time_close'])
output.loc[:, 'total_return'] = output['trade_return'].fillna(0)

output['total_return_index'] = output['trade_return'].fillna(0).apply(lambda x: 1 + x)
output.loc[0, 'total_return_index'] = 100
output['total_return_index'] = output['total_return_index'].cumprod()

fnl_return_5m = output['total_return_index'].iloc[-1]
print(f"Total Return (5m): {fnl_return_5m:.2f}")

# Plot total return index
plot_total_return_index(result)

result.to_csv(f'{this_symbol}_backtest.csv', index=False)

top_results = []
for short_per in range(5, 30):
    for long_per in range(30, 36):
        px_tbl = calc_signals_test(px_5m, short_period=short_per, long_period=long_per)

        # Plot AO with saucer signals
        #signals = px_tbl[['time_close', 'long_signal', 'short_signal']]
        #plot_signals(px_tbl)

        ### Start rolling backtest
       
        this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
        result = calculate_returns(this_px_tbl, fees=-0.00045)

        output = this_px_tbl.merge(result.loc[:, ['time_open', 'time_close', 'trade_return']], how='left', on=['time_open', 'time_close'])
        output.loc[:, 'total_return'] = output['trade_return'].fillna(0)

        output['total_return_index'] = output['trade_return'].fillna(0).apply(lambda x: 1 + x)
        output.loc[0, 'total_return_index'] = 100
        output['total_return_index'] = output['total_return_index'].cumprod()

        fnl_return_5m = output['total_return_index'].iloc[-1]
        print(f"Total Return (5m): {fnl_return_5m:.2f}")

        top_results.append([ short_per, long_per, fnl_return_5m])

top_results_df = pd.DataFrame(top_results, columns=['short_period','long_period', 'return_5m'])

top_results_df.to_csv(f'{this_symbol}_ao_oscillator_periods.csv', index=False)