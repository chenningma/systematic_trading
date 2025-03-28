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

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

from dotenv import load_dotenv, find_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

credentials_path = parent_dir + '/' +os.getenv('GCP_CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()


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
    # Convert time_close to datetime if it's not already
    px_tbl['time_close'] = pd.to_datetime(px_tbl['time_close'])
    
    # Filter for recent data (last 100 points) to make the plot more readable
    fig_tbl = px_tbl.tail(1000).copy()
    
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
select 
ticker as symbol
from transformed.marketcap_volume_rank
where date = '2023-12-31'
order by mcap_pct_rank
limit 500
'''
query_job = client.query(query)
coin_list = query_job.result().to_dataframe()

query = '''
select 
distinct symbol
from test.top_300_stocks_oscillator_backtest_feb25
'''
query_job = client.query(query)
existing = query_job.result().to_dataframe()

download_coins = coin_list[~coin_list['symbol'].isin(existing['symbol'])]['symbol']

## 
# Calculate date ranges
this_end = datetime.now()
this_start = this_end - timedelta(days = 500)

for this_symbol in coin_list['symbol']:
    try:
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
        and date >= '2019-01-01'
        order by time_close
        '''.format(symbol = this_symbol)
        query_job = client.query(query)
        px_5m = query_job.result().to_dataframe()

        px_5m["time_close"] = px_5m["time_close"].dt.round('min')

        ### Calculate signals
        top_results = []
        for short_per in range(5, 30):
            for long_per in range(30, 240, 7):
                px_tbl = calc_signals(px_5m, short_period=short_per, long_period=long_per)

                ### Start rolling backtest
                this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
                result = calculate_returns(this_px_tbl, fees=0)

                output = this_px_tbl.merge(result.loc[:, ['time_open', 'time_close', 'trade_return']], how='left', on=['time_open', 'time_close'])
                output.loc[:, 'total_return'] = output['trade_return'].fillna(0)

                output['total_return_index'] = output['trade_return'].fillna(0).apply(lambda x: 1 + x)
                output.loc[0, 'total_return_index'] = 100
                output['total_return_index'] = output['total_return_index'].cumprod()

                fnl_return_5m = output['total_return_index'].iloc[-1]

                top_results.append([this_symbol, short_per, long_per, fnl_return_5m])

        top_results_df = pd.DataFrame(top_results, columns=['symbol','short_period','long_period', 'return_5m'])
            # Write backtest results to BigQuery
        table_id = 'test.top_300_stocks_oscillator_backtest_feb25'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(top_results_df, table_id, job_config=job_config)
        job.result()
        print("Backtest results written to BigQuery table")
    except Exception as e:
        print(str(e))







### individual coin backtest
this_symbol = 'RDDT'

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
    and date >= '2020-01-01'
    order by time_close
    '''.format(symbol = this_symbol)
query_job = client.query(query)
px_5m = query_job.result().to_dataframe()

px_5m["time_close"] = px_5m["time_close"].dt.round('min')

### Calculate signals
top_results = []
this_start = datetime.now() - timedelta(days = 500)
this_end = datetime.now()

for short_per in range(5, 30):
    for long_per in range(30, 120, 5):
        try:
            px_tbl = calc_signals(px_5m, short_period=short_per, long_period=long_per)

            ### Start rolling backtest
            this_px_tbl = px_tbl.loc[(px_tbl['time_close'] >= this_start.strftime("%Y-%m-%d %H:%M:%S")) & (px_tbl['time_close'] <= this_end.strftime("%Y-%m-%d %H:%M:%S"))]
            print(f"Running backtest for {this_symbol} from {this_start} to {this_end}")

            result = calculate_returns(px_tbl, fees=-0.00045)
            result['timestamp'] = result['time_open'].apply(lambda x: int(x.timestamp() * 1000))

            fnl_return_5m = result['total_return_index'].iloc[-1]
            print(f"Total Return (5m): {fnl_return_5m:.2f}")

            top_results.append([this_symbol, short_per, long_per, fnl_return_5m])
        except Exception as e:
            print(str(e))

top_results_df = pd.DataFrame(top_results, columns=['symbol','short_period','long_period', 'return_5m'])


top_short_period = top_results_df.sort_values(by = 'return_5m', ascending = False).short_period.values[0]
top_long_period = top_results_df.sort_values(by = 'return_5m', ascending = False).long_period.values[0]
print(top_short_period, top_long_period)
px_tbl = calc_signals(px_5m, short_period=top_short_period, long_period=top_long_period)

# Plot AO with saucer signals
#signals = px_tbl[['time_close', 'long_signal', 'short_signal']]
plot_signals(px_tbl)

### Start rolling backtest

result = calculate_returns(px_tbl, fees=0)
result['timestamp'] = result['time_open'].apply(lambda x: int(x.timestamp() * 1000))

fnl_return_5m = result['total_return_index'].iloc[-1]
print(f"Total Return (5m): {fnl_return_5m:.2f}")

this_result = [this_symbol, this_start, this_end, fnl_return_5m]
# Plot total return index
plot_total_return_index(result)




