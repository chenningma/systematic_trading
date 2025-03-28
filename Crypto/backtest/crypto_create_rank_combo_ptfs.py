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


read_table_id = 'crypto_px.train_data_ranked'
query_job = client.query("""
    select 
    a.id as ticker,
    a.* except(id)
    from {tbl} a  
    where a.date > '2024-03-01'   
    """.format(tbl = read_table_id))
all_data = query_job.result().to_dataframe()

query_job = client.query("""
    select 
    date,
    id as ticker,
    next_day_return as next_month_return                  
    from crypto_px.train_data_daily    
    """)
returns = query_job.result().to_dataframe()

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
    select * from crypto_px.combo_3var_shortlist
    """)
combos = query_job.result().to_dataframe()

keys = all_data.loc[:, ['ticker', 'date']]

# save location
backtest_table_id = 'boreal-pride-417020.crypto_px.combo_3var_ptfs'  # save table
monthly_return_table_id = 'boreal-pride-417020.crypto_px.combo_3var_monthly_return' 
performance_table_id = 'boreal-pride-417020.crypto_px.combo_3var_performance'

query_job = client.query("""
    truncate table boreal-pride-417020.crypto_px.combo_3var_ptfs
    """)
job = query_job.result()

query_job = client.query("""
    truncate table boreal-pride-417020.crypto_px.combo_3var_monthly_return
    """)
job = query_job.result()

query_job = client.query("""
    truncate table boreal-pride-417020.crypto_px.combo_3var_performance
    """)
job = query_job.result()


for i in range(0, len(combos)):
    this_combo = combos.loc[i, ['var_1', 'var_2', 'var_3']].tolist()
    model_id = combos.model_id[i]

    values = all_data.loc[:, this_combo]

    ranked_data = pd.concat([keys, values], axis = 1)
    ranked_data = ranked_data.dropna()

    ranked_data.loc[:, 'ComboRank'] = ranked_data.loc[:, this_combo].mean(axis = 1)
    ranked_data['percentile_rank'] = ranked_data.groupby(['date'])['ComboRank'].rank(pct=True)

    ranked_data = ranked_data.sort_values(by=['date', 'ComboRank'])
    #ranked_data = ranked_data.merge(this_data, how = 'left', on = ['ticker', 'date'])

    ptf = create_combo_factor_ptf_by_percentile(ranked_data,col = 'percentile_rank', Top_cutoff = .2)
    ptf = ptf.reset_index(names = "ptf_count")
    ptf = ptf.loc[ptf.ptf_count < 50, :]

    all_portfolio = ptf.merge(returns, how = 'left', on = ['ticker', 'date'])
    all_portfolio.columns = ['ptf_count', 'ticker', 'date', 'var_1', 'var_2', 'var_3', 'ComboRank', 'percentile_rank', 'next_month_return']
    
    cost_per_trade = .015
    monthly_return = create_monthly_return(all_portfolio, cost_per_trade)

    # save backtest

    description = ','.join(this_combo)
    all_portfolio.loc[:, 'model_id'] = model_id
    all_portfolio.loc[:, 'description'] = description
    all_portfolio.loc[:, 'train_dataset'] = read_table_id
    all_portfolio['run_date'] = datetime.now()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    all_portfolio, backtest_table_id, job_config=job_config
                )

    # save monthly return
    monthly_return.loc[:, 'model_id'] = model_id
    monthly_return.loc[:, 'description'] = description
    monthly_return.loc[:, 'train_dataset'] = read_table_id
    monthly_return['run_date'] = datetime.now()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    monthly_return, monthly_return_table_id, job_config=job_config
                )

    # save backtest performance
    stat_tbl1 = create_performance_stats_v2(monthly_return, 5)
    stat_tbl2 = create_performance_stats_v2(monthly_return, 7.5)
    stat_tbl3 = create_performance_stats_v2(monthly_return, 15)

    stat_tbl = pd.concat([stat_tbl1, stat_tbl2])
    stat_tbl = pd.concat([stat_tbl, stat_tbl3])

    stat_tbl.loc[:, 'model_id'] =  model_id
    stat_tbl.loc[:, 'description'] = description

    stat_tbl = stat_tbl.loc[stat_tbl.field != 'drawdown_dt', :]

    job_config = bigquery.LoadJobConfig(
        #schema = [ \
        #bigquery.SchemaField("5y_drawdown_dt", bigquery.enums.SqlTypeNames.DATE), \
        #bigquery.SchemaField("drawdown_dt", bigquery.enums.SqlTypeNames.DATE)], \
        write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    stat_tbl, performance_table_id, job_config=job_config
                )