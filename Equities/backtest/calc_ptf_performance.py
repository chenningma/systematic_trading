import pandas as pd
import os 
import numpy as np
import math 
from datetime import datetime

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from functions import *

import matplotlib

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

backtest_tbl = 'train.backtest_ptfs_202406'

client = bigquery.Client()
query_job = client.query("""
    SELECT distinct backtest_id
    FROM {table}
    """.format(table = backtest_tbl))

id_list = query_job.result().to_dataframe()

stat_tbl = list()
for backtest_id in range(698, 704):
    query_job = client.query("""
    SELECT *
    FROM {table} where backtest_id = {id}
    """.format(table = backtest_tbl, id=backtest_id))

    ptf = query_job.result().to_dataframe()

    # calculate return
    this_ptf = ptf.copy()
    cost_per_trade = .01
    monthly_return = create_monthly_return(this_ptf, cost_per_trade)

    end = len(monthly_return)-2
    start = end - 5*12
    return_5y = monthly_return.loc[end, 'total_return_idx'] / monthly_return.loc[start, 'total_return_idx'] - 1
    std_5y = monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12)
    sharpe_5y = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12))
    drawdown_5y = monthly_return.loc[start:end, 'total_return'].min()
    drawdown_5y_dt = monthly_return.loc[monthly_return.total_return == drawdown_5y, 'date'].values[0]

    start = 0
    std = monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12)
    sharpe = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[0:end, 'total_return'].std() * np.sqrt(12))
    drawdown = monthly_return.loc[start:end, 'total_return'].min()
    drawdown_dt = monthly_return.loc[monthly_return.total_return == drawdown, 'date'].values[0]

    start = end - 18
    return_18m = monthly_return.loc[end, 'total_return_idx'] / monthly_return.loc[start, 'total_return_idx'] - 1
    std_18m = monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12)
    sharpe_18m = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[0:end, 'total_return'].std() * np.sqrt(12))

    model_id = this_ptf.loc[0, 'model_id']
    #this_stat = [backtest_id, model_id, return_18m, std_18m, sharpe_18m, return_5y, std_5y, sharpe_5y, drawdown_5y, drawdown_5y_dt, std, sharpe, drawdown, drawdown_dt]
    this_stat = [backtest_id, model_id, return_5y, std_5y, sharpe_5y, drawdown_5y, drawdown_5y_dt, std, sharpe, drawdown, drawdown_dt]
    stat_tbl.append(this_stat)

#fnl_stat_tbl = pd.DataFrame(stat_tbl, columns = ['backtest_id', 'model_id', 'return_18m', 'std_18m', 'sharpe_18m', 'return_5y', 'std_5y', 'sharp_5y', 'max_5y_drawdown', '5y_drawdown_dt', 'std_all', 'sharpe_all', 'max_drawdown', 'drawdown_dt'])
fnl_stat_tbl = pd.DataFrame(stat_tbl, columns = ['backtest_id', 'model_id', 'return_5y_latest', 'std_5y', 'sharp_5y', 'max_5y_drawdown', '5y_drawdown_dt', 'std_all', 'sharpe_all', 'max_drawdown', 'drawdown_dt'])

write_table_id = 'boreal-pride-417020.train.backtest_ptfs_performance'
job_config = bigquery.LoadJobConfig(
    #schema = [ \
    #bigquery.SchemaField("5y_drawdown_dt", bigquery.enums.SqlTypeNames.DATE), \
    #bigquery.SchemaField("drawdown_dt", bigquery.enums.SqlTypeNames.DATE)], \
    write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(
                fnl_stat_tbl, write_table_id, job_config=job_config
            )


### Graphs

subset = [698, 699, 649]
all_month_return = pd.DataFrame()
for backtest_id in subset:
    query_job = client.query("""
    SELECT *
    FROM {table} where backtest_id = {id}
    and date >='2019-12-31' and date <= '2024-05-31'
    """.format(table = backtest_tbl, id=backtest_id))

    ptf = query_job.result().to_dataframe()

    # calculate return
    this_ptf = ptf.copy()
    cost_per_trade = .01
    monthly_return = create_monthly_return(this_ptf, cost_per_trade)
    monthly_return.loc[:, 'backtest_id'] = backtest_id

    all_month_return = pd.concat([all_month_return, monthly_return])

plot_tb = all_month_return.copy()
plot_tb = plot_tb.loc[:, ['date', 'total_return_idx', 'backtest_id']]

plot = plot_tb.pivot(index='date', columns='backtest_id', values='total_return_idx').plot()


subset = [13, 17, 53, 54, 23, 41]
score_tb = pd.DataFrame()
for backtest_id in subset:
    query_job = client.query("""
    SELECT distinct date, score
    FROM {table} where backtest_id = {id}
    """.format(table = backtest_tbl, id=backtest_id))

    this_score = query_job.result().to_dataframe()
    this_score.loc[:, 'backtest_id'] = backtest_id

    score_tb = pd.concat([score_tb, this_score])

plot_tb = score_tb.copy()
plot_tb = plot_tb.sort_values(by = ['backtest_id', 'date']).reset_index(drop = True)
#plot_tb = plot_tb.set_index('date')
plot_tb.loc[:, 'score_ma'] = plot_tb.groupby(['backtest_id'])['score'].rolling(12).mean().reset_index(drop = True)

plot_tb = plot_tb.reset_index()
plot = plot_tb.pivot(index='date', columns='backtest_id', values='score_ma').plot()

