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

def calc_rsi(px_tbl, period=14):
    # Calculate price changes
    delta = px_tbl['close'].diff()
    
    # Separate gains and losses
    gains = delta.copy()
    losses = delta.copy()
    gains[gains < 0] = 0
    losses[losses > 0] = 0
    losses = abs(losses)
    
    # Calculate average gains and losses
    avg_gains = gains.rolling(window=period).mean()
    avg_losses = losses.rolling(window=period).mean()
    
    # Calculate RS and RSI
    rs = avg_gains / avg_losses
    rsi = 100 - (100 / (1 + rs))

    # Create DataFrame with RSI and timestamps
    rsi_df = pd.DataFrame({
        'time_open': px_tbl['time_open'],
        'time_close': px_tbl['time_close'], 
        'rsi': rsi
    })
    
    return rsi_df

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

    trade_tbl = px_tbl[['time_open', 'time_close', 'close', 'long_signal', 'short_signal']]
    
    # Calculate percentage change in close price
    trade_tbl['pct_change'] = trade_tbl['close'] / trade_tbl['close'].shift(1) - 1

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

    result = fnl_tbl[['time_open','time_close', 'close', 'long_signal', 'short_signal', 'long_trade', 'short_trade', 'trade_return']]

    return result

def plot_total_return_index(result):
    plt.figure(figsize=(12, 6))
    plt.plot(result['time_close'], result['total_return_index'])
    plt.title('Strategy Total Return Index')
    plt.xlabel('Date')
    plt.ylabel('Total Return Index')
    plt.grid(True)
    plt.show()

def calc_stoch_rsi(rsi_df, period=14):
    # Calculate StochRSI 
    rsi_values = rsi_df['rsi']
    
    # Calculate min and max RSI values over the period
    min_rsi = rsi_values.rolling(window=period).min()
    max_rsi = rsi_values.rolling(window=period).max()
    
    # Calculate StochRSI
    stoch_rsi = (rsi_values - min_rsi) / (max_rsi - min_rsi)
    
    # Create DataFrame with StochRSI and timestamps
    stoch_rsi_df = pd.DataFrame({
        'time_open': rsi_df['time_open'],
        'time_close': rsi_df['time_close'],
        'stoch_rsi': stoch_rsi
    })
    
    return stoch_rsi_df

def calc_stoch_rsi_ma(stoch_rsi_df, period):
    # Calculate moving average of StochRSI
    stoch_rsi_ma = stoch_rsi_df['stoch_rsi'].rolling(window=period).mean()
    
    # Create DataFrame with StochRSI MA and timestamps
    stoch_rsi_ma_df = pd.DataFrame({
        'time_open': stoch_rsi_df['time_open'],
        'time_close': stoch_rsi_df['time_close'],
        f'stoch_rsi_smoothed': stoch_rsi_ma
    })
    
    return stoch_rsi_ma_df

def create_signal(stoch_rsi_df, indicator='stoch_rsi_smoothed', low=0.2, high=0.8):
    # Create a new DataFrame with the signals
    signals = pd.DataFrame({
        'time_open': stoch_rsi_df['time_open'],
        'time_close': stoch_rsi_df['time_close'],
        'stoch_rsi': stoch_rsi_df[indicator]
    })
    
    # Initialize signal columns
    signals['long_signal'] = 0
    signals['short_signal'] = 0
    
    # Calculate long signal
    for i in range(0, len(signals)):
        # Check for long entry condition
        if signals['stoch_rsi'].iloc[i-1] < low and signals['stoch_rsi'].iloc[i] > low:
            signals.loc[signals.index[i]:, 'long_signal'] = 1
            
        # Check for close long condition if currently in long position
        elif (signals.loc[signals.index[i], 'long_signal'] == 1 and 
            signals['stoch_rsi'].iloc[i-1] > high and signals['stoch_rsi'].iloc[i] < high):
            signals.loc[signals.index[i], 'long_signal'] = -1
            # Clear any forward propagated long signals
            signals.loc[signals.index[i+1]:, 'long_signal'] = 0

    # Calculate short signal
    for i in range(0, len(signals)):
        # Check for short entry condition
        if signals['stoch_rsi'].iloc[i-1] > high and signals['stoch_rsi'].iloc[i] < high:
            signals.loc[signals.index[i]:, 'short_signal'] = -1
            
        # Check for close short condition if currently in short position
        elif (signals.loc[signals.index[i], 'short_signal'] == -1 and 
            signals['stoch_rsi'].iloc[i-1] < low and signals['stoch_rsi'].iloc[i] > low):
            signals.loc[signals.index[i], 'short_signal'] = 1
            # Clear any forward propagated short signals
            signals.loc[signals.index[i+1]:, 'short_signal'] = 0

    return signals


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

backtest_results = []
for this_symbol in coin_list['symbol']:

    try:
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
        px_20m = calc_px_interval(px_5m, 20)

        ### Calculate signals
        input_tbl = px_20m
        rsi = calc_rsi(input_tbl, period=14)

        # Calculate Stochastic RSI
        stoch_rsi = calc_stoch_rsi(rsi, period=14)
        stoch_rsi_smoothed = calc_stoch_rsi_ma(stoch_rsi, period=3)

        # Generate signals from StochRSI
        px_tbl = create_signal(stoch_rsi_smoothed, indicator='stoch_rsi_smoothed', long=0.2, long_close=0.45, short=0.8, short_close=0.55)
        px_tbl = px_tbl.merge(input_tbl, how='left', on=['time_open', 'time_close'])

        ### Start rolling backtest
        this_start = datetime.now(timezone.utc) - timedelta(days=30)
        this_end = datetime.now(timezone.utc)

        this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
        print(f"Running backtest for {this_symbol} from {this_start} to {this_end}")

        result = calculate_returns(this_px_tbl, fees=-0.00045)

        output = px_tbl.merge(result.loc[:, ['time_open', 'time_close', 'trade_return']], how='left', on=['time_open', 'time_close'])
        output.loc[:, 'total_return'] = output['trade_return'].fillna(0)

        output['total_return_index'] = output['trade_return'].fillna(0).apply(lambda x: 1 + x)
        output['total_return_index'].iloc[0] = 100
        output['total_return_index'] = output['total_return_index'].cumprod()

        output = output.loc[:, ['time_open', 'time_close', 'close', 'stoch_rsi', 'long_signal', 'short_signal', 'total_return', 'total_return_index']]
        output['timestamp'] = output['time_open'].apply(lambda x: int(x.timestamp() * 1000))

        #plot_total_return_index(output)
        #output.to_csv(f'{this_symbol}_backtest.csv', index=False)

        fnl_return = output['total_return_index'].iloc[-1]
        print(f"Total Return: {fnl_return:.2f}")

        this_result = [this_symbol, this_start, this_end, fnl_return]
            
    except Exception as e:
        print(str(e))       

backtest_results_df = pd.DataFrame(backtest_results, columns=['symbol','date_start','date_end', 'return'])






### individual coin backtest
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
#px_15m = calc_px_interval(px_5m, 15)
px_20m = calc_px_interval(px_5m, 20)
#px_30m = calc_px_interval(px_5m, 30)

### Calculate signals
input_tbl = px_20m
rsi = calc_rsi(input_tbl, period=14)

def calc_stoch_rsi(rsi_df, period=14):
    # Calculate StochRSI 
    rsi_values = rsi_df['rsi']
    
    # Calculate min and max RSI values over the period
    min_rsi = rsi_values.rolling(window=period).min()
    max_rsi = rsi_values.rolling(window=period).max()
    
    # Calculate StochRSI
    stoch_rsi = (rsi_values - min_rsi) / (max_rsi - min_rsi)
    
    # Create DataFrame with StochRSI and timestamps
    stoch_rsi_df = pd.DataFrame({
        'time_open': rsi_df['time_open'],
        'time_close': rsi_df['time_close'],
        'stoch_rsi': stoch_rsi
    })
    
    return stoch_rsi_df

# Calculate Stochastic RSI
stoch_rsi = calc_stoch_rsi(rsi, period=14)

def calc_stoch_rsi_ma(stoch_rsi_df, period):
    # Calculate moving average of StochRSI
    stoch_rsi_ma = stoch_rsi_df['stoch_rsi'].rolling(window=period).mean()
    
    # Create DataFrame with StochRSI MA and timestamps
    stoch_rsi_ma_df = pd.DataFrame({
        'time_open': stoch_rsi_df['time_open'],
        'time_close': stoch_rsi_df['time_close'],
        f'stoch_rsi_smoothed': stoch_rsi_ma
    })
    
    return stoch_rsi_ma_df

stoch_rsi_smoothed = calc_stoch_rsi_ma(stoch_rsi, period=3)

def create_signal(stoch_rsi_df, indicator='stoch_rsi_smoothed', long=0.2, long_close=0.8, short=0.8, short_close=0.2):
    # Create a new DataFrame with the signals
    signals = pd.DataFrame({
        'time_open': stoch_rsi_df['time_open'],
        'time_close': stoch_rsi_df['time_close'],
        'stoch_rsi': stoch_rsi_df[indicator]
    })
    
    # Initialize signal columns
    signals['long_signal'] = 0
    signals['short_signal'] = 0
    
    # Calculate long signal
    for i in range(0, len(signals)):
        # Check for long entry condition
        if signals['stoch_rsi'].iloc[i-1] > long and signals['stoch_rsi'].iloc[i] < long:
            signals.loc[signals.index[i]:, 'long_signal'] = 1
            
        # Check for close long condition if currently in long position
        elif (signals.loc[signals.index[i], 'long_signal'] == 1 and 
              signals['stoch_rsi'].iloc[i] > long_close):
            signals.loc[signals.index[i], 'long_signal'] = -1
            # Clear any forward propagated long signals
            signals.loc[signals.index[i+1]:, 'long_signal'] = 0

    # Calculate short signal
    for i in range(0, len(signals)):
        # Check for short entry condition
        if signals['stoch_rsi'].iloc[i-1] < short and signals['stoch_rsi'].iloc[i] > short:
            signals.loc[signals.index[i]:, 'short_signal'] = -1
            
        # Check for close short condition if currently in short position
        elif (signals.loc[signals.index[i], 'short_signal'] == -1 and 
              signals['stoch_rsi'].iloc[i] < short_close):
            signals.loc[signals.index[i], 'short_signal'] = 1
            # Clear any forward propagated short signals
            signals.loc[signals.index[i+1]:, 'short_signal'] = 0

    return signals

# Generate signals from StochRSI
px_tbl = create_signal(stoch_rsi_smoothed, indicator='stoch_rsi_smoothed', long=0.2, long_close=0.45, short=0.8, short_close=0.55)
px_tbl = px_tbl.merge(input_tbl, how='left', on=['time_open', 'time_close'])

### Start rolling backtest
this_start = datetime.now(timezone.utc) - timedelta(days=30)
this_end = datetime.now(timezone.utc)

this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
print(f"Running backtest for {this_symbol} from {this_start} to {this_end}")

result = calculate_returns(this_px_tbl, fees=-0.00045)

output = px_tbl.merge(result.loc[:, ['time_open', 'time_close', 'trade_return']], how='left', on=['time_open', 'time_close'])
output.loc[:, 'total_return'] = output['trade_return'].fillna(0)

output['total_return_index'] = output['trade_return'].fillna(0).apply(lambda x: 1 + x)
output['total_return_index'].iloc[0] = 100
output['total_return_index'] = output['total_return_index'].cumprod()

output = output.loc[:, ['time_open', 'time_close', 'close', 'stoch_rsi', 'long_signal', 'short_signal', 'total_return', 'total_return_index']]
output['timestamp'] = output['time_open'].apply(lambda x: int(x.timestamp() * 1000))

plot_total_return_index(output)
#output.to_csv(f'{this_symbol}_backtest.csv', index=False)

fnl_return_5m = output['total_return_index'].iloc[-1]
print(f"Total Return (5m): {fnl_return_5m:.2f}")

this_result = [this_symbol, this_start, this_end, fnl_return_5m]
# Plot total return index




