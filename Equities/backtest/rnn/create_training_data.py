import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import numpy as np

from google.cloud import bigquery
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

from dotenv import load_dotenv, find_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

credentials_path = parent_dir + '/' +os.getenv('TEST_CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()


### Get Universe
def get_universe_query(input_table, start_date_filter, marketcap_rank_filter, volume_rank_filter, filter_ipos = True, min_trading_days = 180):
    if filter_ipos:
        query = f'''
            SELECT
               a.*
            FROM
                {input_table} a
            LEFT JOIN 
                (select 
                ticker, date_start, value 
                from transformed.weekly_marketcap_volume_rank 
                where field = 'marketcap_pct_rank') b on a.ticker = b.ticker and a.date_start = b.date_start
            LEFT JOIN 
                (select 
                ticker, date_start, value 
                from transformed.weekly_marketcap_volume_rank
                where field = 'volume_avg_mean_4w_pct_rank') c on a.ticker = c.ticker and a.date_start = c.date_start
            LEFT JOIN transformed.first_trading_date d on a.ticker = d.ticker
            WHERE 
                a.date_start >= '{start_date_filter}'
                and b.value < {marketcap_rank_filter} 
                and c.value < {volume_rank_filter}
                and a.date_start >= DATE_ADD(d.first_date, INTERVAL {min_trading_days} DAY)
            ORDER BY
                ticker,
                date_start
            '''
    else:
        query = f'''
            SELECT
               a.*
            FROM
                {input_table} a
            LEFT JOIN 
                (select 
                ticker, date_start, value 
                from transformed.weekly_marketcap_volume_rank 
                where field = 'marketcap_pct_rank') b on a.ticker = b.ticker and a.date_start = b.date_start
            LEFT JOIN 
                (select 
                ticker, date_start, value 
                from transformed.weekly_marketcap_volume_rank
                where field = 'volume_avg_mean_4w_pct_rank') c on a.ticker = c.ticker and a.date_start = c.date_start
            WHERE 
                a.date_start >= '{start_date_filter}'
                and b.value < {marketcap_rank_filter} 
                and c.value < {volume_rank_filter}
            ORDER BY
                ticker,
                date_start
            '''
    return query

######## Create Training Data ########
query = '''
create or replace table training_data.weekly_training_data as (
select 
a.ticker,
a.date_start,
a.date_end,
a.marketcap,
a.volume_avg,
c.* except(ticker, date_start, date_end)
from transformed.weekly_levels a
left join transformed.weekly_px_return c on a.ticker = c.ticker and a.date_start = c.date_start
)
'''
query_job = client.query(query)
query_job.result()

### Filter to Universe & Save
input_table = 'training_data.weekly_training_data'
start_date_filter = '2014-12-31'
marketcap_rank_filter = 0.3
volume_rank_filter = 0.75
universe_query = get_universe_query(input_table, start_date_filter, marketcap_rank_filter, volume_rank_filter)

query = f'''
        CREATE OR REPLACE TABLE training_data.weekly_training_data_medcap as (
        {universe_query}
        )
    '''
query_job = client.query(
    query
)
query_job.result()

### Get Training Set for XGBoost

query = '''
select t.* 
from training_data.weekly_training_data_medcap t
order by t.date_start, t.marketcap 
'''
query_job = client.query(query)
train_data = query_job.to_dataframe()

# Create Y variable
train_data['return_next_week'] = train_data.groupby('ticker')['px_close_pct_1w'].shift(-1)
check = train_data.loc[train_data.ticker == 'CVNA', ['ticker', 'date_start', 'date_end', 'px_close_pct_1w', 'return_next_week']]
train_data = train_data.dropna(subset = ['return_next_week'])

# Create quintile ranks (1-5) for each column
quintile_ranks = train_data.loc[:, ['ticker', 'date_start', 'date_end']]
for col in train_data.select_dtypes(['int', 'float']).columns:
    ranks = train_data.groupby('date_start')[col].transform(
        lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1)
    quintile_ranks[f'{col}_quintile'] = ranks

check = quintile_ranks.loc[quintile_ranks.date_end == '2025-03-07', ['ticker', 'date_start', 'date_end', 'px_close_pct_1w_quintile']]

# Get Training Data
all_data = quintile_ranks.reset_index(drop=True)

# Time-based Train-Test Split
def split_train_test_by_time(all_data, date_col, percentage):

    date_list = all_data.loc[:, date_col].sort_values().unique()

    cutoff = int(len(date_list) * percentage)
    train_date = pd.DataFrame(date_list[0:cutoff], columns = [date_col])

    train = all_data.merge(train_date, how = 'inner', on =date_col).reset_index(drop=True)
    test = all_data.loc[all_data[date_col] >= date_list[cutoff], :].reset_index(drop=True)

    return train, test

date_col = 'date_start'
percentage = 0.8
train, test = split_train_test_by_time(all_data, date_col, percentage)

# Set Target, Variables
Y_var = 'return_next_week_quintile'
X_var = train.select_dtypes(['int', 'float']).columns.tolist()
X_var.remove(Y_var)

# Create train / test set
Y_train = train.loc[:, Y_var]
X_train = train.loc[:, X_var]




