import pandas as pd
pd.options.mode.chained_assignment = None
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime, timedelta
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
backtest_grid = pd.read_csv('backtest_grid.csv')

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


### create all combos
set1 = backtest_grid.train_dataset.dropna()
set2 = backtest_grid.dependent_var_name.dropna()
set3 = backtest_grid.model_id.dropna()

all_set = [set1.tolist(), set2.to_list(), set3.to_list()]
test_grid = list(itertools.product(*all_set))

test_grid = pd.DataFrame(test_grid, columns = ['train_dataset', 'dependent_var_name', 'model_id'])

backtest_table_id = 'boreal-pride-417020.train.backtest_ptfs_202406'
cutoff = 240

for i in range(0, len(test_grid)):

   this_grid = test_grid.loc[i, :]
   print(this_grid)

   # set postfix
   postfix =  datetime.today() + timedelta(days = 1)
   postfix = postfix.strftime("%Y%m")

   # get training data
   
   project_id = "boreal-pride-417020"
   dataset_id = "training_data"
   read_table_prefix = this_grid['train_dataset']   ### vary
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

   ### Backtest Algo 1
   all_portfolio = backtest_classifier_base_removeNAs(all_data, Y_field, incl, xgb_params, cutoff)    

   description = 'backtest_classifier_base_removeNAs'

   backtest_id = write_backtest_to_db(all_portfolio,model_id, Y_field, description, read_table_id, backtest_table_id)

   if False: 
      ### Backtest Algo 2
      pred_threshold = 0.6  
      all_portfolio = run_backtest_pred_threshold_removeNAs(all_data, Y_field, incl, xgb_params, pred_threshold, cutoff)    

      description = 'run_backtest_pred_threshold_removeNAs --0.6'

      backtest_id = write_backtest_to_db(all_portfolio,model_id, Y_field, description, read_table_id, backtest_table_id)

      ### Backtest Algo 3

      all_portfolio = backtest_classifier_base(all_data, Y_field, incl, xgb_params, cutoff)    

      description = 'backtest_classifier_base'

      backtest_id = write_backtest_to_db(all_portfolio,model_id, Y_field, description, read_table_id, backtest_table_id)

      ### Backtest Algo 4

      pred_threshold = 0.6
      all_portfolio = run_backtest_pred_threshold(all_data, Y_field, incl, xgb_params,pred_threshold, cutoff)    

      description = 'run_backtest_pred_threshold --0.6'

      backtest_id = write_backtest_to_db(all_portfolio,model_id, Y_field, description, read_table_id, backtest_table_id)

      ### Backtest Algo 5

      all_portfolio = run_backtest_replace100(all_data, Y_field, incl, xgb_params, cutoff)    

      description = 'run_backtest_replace100'

      backtest_id = write_backtest_to_db(all_portfolio,model_id, Y_field, description, read_table_id, backtest_table_id)

