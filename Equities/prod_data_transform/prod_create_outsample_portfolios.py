import pandas as pd
pd.options.mode.chained_assignment = None
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime
import itertools

import xgboost as xgb
from sklearn.metrics import confusion_matrix, roc_curve, auc, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV
import shap 

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

from functions import *

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
bqstorageclient = bigquery_storage.BigQueryReadClient()
client = bigquery.Client()

# get grid
backtest_grid = pd.read_csv('prod_strategy_specs.csv')

# get mcap_tbl
query_job = client.query("""
select 
ticker,
date,
marketcap,
mcap_pct_rank
from `transformed.marketcap_volume_rank`                      
   """)
mcap_tbl = query_job.result().to_dataframe()

# get dates
postfix = datetime.today().strftime("%Y%m")
read_table_id = 'training_data.training_data_ipo_delay_' + postfix
query_job = client.query("""
select 
distinct
date
from {}                   
   """.format(read_table_id))
datelist = query_job.result().to_dataframe()
cutoff = len(datelist) - 1

### create all combos
backtest_table_id = 'boreal-pride-417020.prod.outsample_ptfs'

for i in range(0, len(backtest_grid)):

    this_grid = backtest_grid.loc[i, :]
    print(this_grid)

    backtest_id = this_grid['backtest_id']
    # get training data
    project_id = "boreal-pride-417020"
    dataset_id = "training_data"
    read_table_prefix = this_grid['train_dataset']   ### vary
    postfix = datetime.today().strftime("%Y%m")
    read_table_id = read_table_prefix + '_' + postfix

    raw_data = read_table(bqstorageclient, project_id, dataset_id, read_table_id)

    # get model params
    model_result_table_id = 'boreal-pride-417020.train.model_results_classifier'
    model_id = this_grid['model_id']  ### vary
    Y_field, xgb_params = get_model_tbl_from_id(model_result_table_id, model_id)

    Y_field = this_grid['dependent_var_name'] ### vary 

    client = bigquery.Client()
    query_job = client.query("""
    select 
    distinct variable,
    from `train.model_variables_classifier`
    where model_id = {}                    
    """.format(model_id))

    all_vars = query_job.result().to_dataframe()
    incl = all_vars.variable.values

    # create training data set
    all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)
    all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
    all_data = all_data.dropna(subset = ['date', 'ticker'])

    dependent_var_name = Y_field

    algo = this_grid['algorithm']
    
    ### Backtest Algo 2
    if algo == 'backtest_classifier_base':
        all_portfolio = backtest_classifier_base(all_data, Y_field, incl, xgb_params, cutoff)    
        
    ### Backtest Algo 3
    if algo == 'run_backtest_pred_threshold --0.6':
        pred_threshold = 0.6
        all_portfolio = run_backtest_pred_threshold(all_data, Y_field, incl, xgb_params,pred_threshold, cutoff)    

    ### Backtest Algo 4

    if algo == 'run_backtest_replace100':
        all_portfolio = run_backtest_replace100(all_data, Y_field, incl, xgb_params, cutoff)    

    ### Backtest Algo 5
    if algo == 'backtest_classifier_top_mcap':
        all_portfolio = backtest_classifier_top_mcap(all_data, Y_field, incl, xgb_params, cutoff, mcap_tbl)    

    ### Backtest Algo 6
    if algo == 'run_backtest_pred_threshold_top_mcap --0.6':
        pred_threshold = 0.6
        all_portfolio = run_backtest_pred_threshold_top_mcap(all_data, Y_field, incl, xgb_params,pred_threshold, cutoff, mcap_tbl)    

    ### Backtest Algo 7
    if algo == 'backtest_classifier_base_removeNAs':
        all_portfolio = backtest_classifier_base_removeNAs(all_data, Y_field, incl, xgb_params, cutoff)   

    if algo == 'run_backtest_pred_threshold_removeNAs --0.6':
        pred_threshold = 0.6
        all_portfolio = run_backtest_pred_threshold_removeNAs(all_data, Y_field, incl, xgb_params, pred_threshold, cutoff)

    write_prod_strategy_to_db(all_portfolio,backtest_id, model_id, dependent_var_name, algo, read_table_id, backtest_table_id)
    del all_portfolio