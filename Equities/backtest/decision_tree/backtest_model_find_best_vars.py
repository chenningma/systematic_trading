import pandas as pd
pd.options.mode.chained_assignment = None
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime
import itertools
import random 

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

time_start = datetime.now()

### config ##############################

# get model params

#model_result_table_id = 'boreal-pride-417020.train.model_results_classifier'
#start_model_id = 132  ### vary
#Y_field, xgb_params = get_model_tbl_from_id(model_result_table_id, start_model_id)

Y_field = 'is_top_100_next_month' ### vary 

xgb_params = {
    'n_estimators': 200,
    'max_depth': 3,
    'learning_rate': .15,
    'gamma':  40,
    "subsample": 1.0,
    "colsample_bytree": 1.0,
    "scale_pos_weight": 15
}

# backtest configs
cutoff = 240 ### vary

# save location
backtest_table_id = 'boreal-pride-417020.train.backtest_ptfs_8technicals_202406' # save table
performance_table_id = 'boreal-pride-417020.train.backtest_8technicals_202406_performance'

### config ends ##############################

### create data inputs

#### train dataset 1 
# get training dataset file
project_id = "boreal-pride-417020"
dataset_id = "training_data"
read_table_prefix = "training_data_top60pct_ex_biotech_202406"  ### vary

read_table_id = read_table_prefix  # read table

# create training data set
raw_data = read_table(bqstorageclient, project_id, dataset_id, read_table_id)
all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)

#### random generate vars to test

def generate_var_df(max_model_id):
    var_tbl = pd.read_csv('variables.csv')
    all_vars = var_tbl.loc[var_tbl.incl == 1, 'variable']

    var_list = list()
    for i in range(0, 15000):
        this_var_list = random.sample(list(all_vars), 8)
        var_list.append(this_var_list)

    var_list_df = pd.DataFrame(var_list).reset_index()
    var_list_df.columns = ['model_id', 'var1', 'var2', 'var3', 'var4', 'var5', 'var6', 'var7', 'var8']

    var_list_df.loc[:, 'model_id'] = var_list_df.loc[:, 'model_id'] + max_model_id

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    var_list_df, 'train.model_variables_8technicals', job_config=job_config
                )


query_job = client.query('''
select * from train.model_variables_8technicals
where model_id >= 10000 and model_id < 15000
order by model_id 
'''
)

var_list_df = query_job.result().to_dataframe()


max_sharpe = 0.3
for j in range(0, len(var_list_df)):
    
    X_field = var_list_df.loc[j, ['var1', 'var2', 'var3', 'var4', 'var5', 'var6', 'var7', 'var8']]
    this_all_data = pd.concat([all_data, raw_data.loc[:, X_field]], axis = 1)


    ### backtest 1: 
    cutoff = 240
    all_portfolio = backtest_classifier_base_short(this_all_data, Y_field, X_field, xgb_params, cutoff)

    ### calc performance
    cost_per_trade = .01
    monthly_return = create_monthly_return(all_portfolio, cost_per_trade)
    end = len(monthly_return)-2
    start = end - 5*12
    sharpe_5y = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12))
    
    ### compare with past performance

    if sharpe_5y > max_sharpe:
        # write model X_var
        model_id = var_list_df.loc[j, ['model_id']]

        # save backtest
        #description = 'xgboost_classifier_base'
        #backtest_id = write_backtest_to_db(all_portfolio, model_id, Y_field, description, read_table_id, backtest_table_id)

        # save backtest performance
        stat_tbl = create_performance_stats(monthly_return, start, end)
        to_add =  pd.DataFrame(zip([ 'model_id'], [model_id]))
        stat_tbl = pd.concat([stat_tbl, to_add])
      
        fnl_stat_tbl =stat_tbl.set_index(0).transpose()

        job_config = bigquery.LoadJobConfig(
            #schema = [ \
            #bigquery.SchemaField("5y_drawdown_dt", bigquery.enums.SqlTypeNames.DATE), \
            #bigquery.SchemaField("drawdown_dt", bigquery.enums.SqlTypeNames.DATE)], \
            write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
                        fnl_stat_tbl, performance_table_id, job_config=job_config
                    )
        print('written model' + str(model_id)+ '| sharpe:' + str(sharpe_5y))
        max_sharpe = max(sharpe_5y, max_sharpe)