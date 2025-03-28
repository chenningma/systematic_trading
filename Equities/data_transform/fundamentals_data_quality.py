import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import numpy as np

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from functions import read_table

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

dimension = 'ARQ'
query_job = client.query("""
        SELECT 
        f.* EXCEPT (ev, evebit, pb,pe,ps),                                               
        FROM financials.sharadar_fundamentals f
        where f.dimension = '{}'
        order by f.ticker, f.datekey              
        """.format(dimension))
raw_data = query_job.result().to_dataframe()

### count NAs across all data
def data_quality_all(raw_data, write_table_id):
    metrics_list =  raw_data.select_dtypes(include=[float])
    info_list = list()
    for x in metrics_list.columns.values:
        this_col = metrics_list.loc[:, x]
        zeros = this_col[this_col==0]
        neg = this_col[this_col<0]
        NAs = this_col[this_col.isnull()]
        info_list.append([x, len(this_col), len(zeros), len(neg), len(NAs)])

    info_list = pd.DataFrame(info_list, columns = ['indicator', 'num', 'count_zero', 'count_neg', 'count_NA'])
    info_list.loc[:, 'pct_zero'] = info_list.count_zero / info_list.num
    info_list.loc[:, 'pct_neg'] = info_list.count_neg / info_list.num
    info_list.loc[:, 'pct_NA'] = info_list.count_NA / info_list.num
    info_list.loc[:, 'has_neg'] = 0
    info_list.loc[info_list.pct_neg > 0.01, 'has_neg'] = 1

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
                    info_list, write_table_id, job_config=job_config
                )
write_table_id = 'boreal-pride-417020.financials.ARQ_fundamentals_data_quality'
data_quality_all(raw_data, write_table_id)

### count NAs across tickers
def data_quality_by_field(raw_data, write_table_id):
    this_field =  raw_data.select_dtypes(include=[float]).columns.values
    metrics_list =  raw_data.loc[:, this_field]

    X = metrics_list == 0.0
    Y = pd.concat([raw_data.ticker, X], axis = 1)
    fill_list = pd.melt(Y, id_vars = ['ticker'])
    fill_list = fill_list.loc[fill_list.value == True, :]

    fill_count = fill_list.groupby(['ticker','variable'])['value'].count()
    fill_count = fill_count.reset_index()
    fill_count.columns = ['ticker', 'indicator', 'count_zero']

    fnl_tbl = fill_count.copy()

    ###  negatives
    X = metrics_list < 0.0
    Y = pd.concat([raw_data.ticker, X], axis = 1)
    fill_list = pd.melt(Y, id_vars = ['ticker'])
    fill_list = fill_list.loc[fill_list.value == True, :]

    fill_count = fill_list.groupby(['ticker','variable'])['value'].count()
    fill_count = fill_count.reset_index()
    fill_count.columns = ['ticker', 'indicator', 'count_neg']

    fnl_tbl = fnl_tbl.merge(fill_count, how = 'outer', on = ['ticker', 'indicator'])

    ### NAs
    X = metrics_list == np.nan
    Y = pd.concat([raw_data.ticker, X], axis = 1)
    fill_list = pd.melt(Y, id_vars = ['ticker'])
    fill_list = fill_list.loc[fill_list.value == True, :]

    fill_count = fill_list.groupby(['ticker','variable'])['value'].count()
    fill_count = fill_count.reset_index()
    fill_count.columns = ['ticker', 'indicator', 'count_NA']

    fnl_tbl = fnl_tbl.merge(fill_count, how = 'outer', on = ['ticker', 'indicator'])


    num_count = raw_data.groupby(['ticker'])['datekey'].count().reset_index()
    num_count.columns = ['ticker', 'num']
    fnl_tbl = fnl_tbl.merge(num_count, how = 'left', on = 'ticker')

    fnl_tbl = fnl_tbl.fillna(0)

    fnl_tbl.loc[:, 'pct_zero'] = fnl_tbl.count_zero / fnl_tbl.num
    fnl_tbl.loc[:, 'pct_neg'] = fnl_tbl.count_neg / fnl_tbl.num
    fnl_tbl.loc[:, 'has_neg'] = 0
    fnl_tbl.loc[fnl_tbl.pct_neg > 0.01, 'has_neg'] = 1
    fnl_tbl.loc[:, 'pct_NA'] = fnl_tbl.count_NA / fnl_tbl.num

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
        fnl_tbl, write_table_id, job_config=job_config
    )

write_table_id = 'boreal-pride-417020.financials.ARQ_fundamentals_data_quality_by_ticker'  
data_quality_by_field(raw_data, write_table_id)

