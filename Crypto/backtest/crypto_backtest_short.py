import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import numpy as np

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
import itertools

from Equities.backtest.functions import *

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

print(datetime.now())

query_job = client.query("""
    select 
    a.id as ticker,
    a.* except(id)
    from crypto_px.top_coins_train_data_ranked a  
    where a.date > '2024-03-01'   
    """)
all_data = query_job.result().to_dataframe()

query_job = client.query("""
    select 
    date,
    id as ticker,
    next_day_return * -1 as next_month_return                  
    from crypto_px.top_coins_train_data_daily    
    """)
returns = query_job.result().to_dataframe()

### create combos
var_tbl = pd.read_csv(os.getcwd()+'/Crypto/crypto_rank_incl.csv')
all_vars = var_tbl.loc[var_tbl.incl == 1, 'variable']

def create_combos():
    combos = [list(x) for x in itertools.combinations(all_vars.tolist(), 3)]
    combo_tbl = pd.DataFrame(combos, columns = ['var1', 'var2', 'var3'])
    combo_tbl = combo_tbl.reset_index(names = 'model_id')

    combo_table_id = 'crypto_px.combo_3_var_list'
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
                combo_tbl, combo_table_id, job_config=job_config
            )


def create_combo_factor_ptf_by_percentile(mfactor_rank, col = "percentile_rank", Top_cutoff = 0.2):
        all_dates = mfactor_rank['date'].unique()
        
        multi_factor=pd.DataFrame()
        for date in all_dates:
            X = mfactor_rank.loc[mfactor_rank.date == date, :]
            Y = X.dropna(subset=[col]).sort_values([col]).reset_index(drop=True)
            Z = Y.loc[Y[col] <= Top_cutoff, :]
            multi_factor = pd.concat([multi_factor, Z])
        
        return(multi_factor)

### get combos
query_job = client.query("""
    select * from crypto_px.combo_3_var_list
    """)
combos = query_job.result().to_dataframe()

keys = all_data.loc[:, ['ticker', 'date']]

query_job = client.query("""
    truncate table crypto_px.top_coins_combo_3var_backtest
    """)
job = query_job.result()

backtest_result_table_id = 'crypto_px.backtest_shorts'

results = pd.DataFrame()
for i in range(0, len(combos)):
    this_combo = combos.loc[i, ['var1', 'var2', 'var3']].tolist()

    values = all_data.loc[:, this_combo]

    ranked_data = pd.concat([keys, values], axis = 1)
    ranked_data = ranked_data.dropna()

    ranked_data.loc[:, 'ComboRank'] = ranked_data.loc[:, this_combo].mean(axis = 1)
    ranked_data['percentile_rank'] = ranked_data.groupby(['date'])['ComboRank'].rank(pct=True)

    ranked_data = ranked_data.sort_values(by=['date', 'ComboRank'])
    #ranked_data = ranked_data.merge(this_data, how = 'left', on = ['ticker', 'date'])

    ptf = create_combo_factor_ptf_by_percentile(ranked_data,col = 'percentile_rank', Top_cutoff = .2)
    ptf = ptf.reset_index(names = "ptf_count")
    ptf = ptf.loc[ptf.ptf_count < 25, :]

    all_portfolio = ptf.merge(returns, how = 'left', on = ['ticker', 'date'])

    cost_per_trade = .015
    monthly_return = create_monthly_return(all_portfolio, cost_per_trade)
    end = len(monthly_return)-2
    start = end - 90
    sharpe_5y = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12))

    stat_tbl = create_performance_stats(monthly_return, start, end)
    to_add =  pd.DataFrame(zip([ 'var_1', 'var_2', 'var_3'], this_combo))
    stat_tbl = pd.concat([stat_tbl, to_add])

    fnl_stat_tbl =stat_tbl.set_index(0).transpose()
    fnl_stat_tbl.loc[:, 'model_id'] = combos.model_id[i]

    results = pd.concat([results, fnl_stat_tbl])

    if i % 1000 == 0:
        print(datetime.now())
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
                    results, backtest_result_table_id, job_config=job_config
                )
        results = pd.DataFrame()

print(datetime.now())
