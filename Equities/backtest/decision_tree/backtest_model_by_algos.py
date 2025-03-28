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

### config ##############################

# get model params
model_result_table_id = 'boreal-pride-417020.train.model_results_classifier'
model_id = 133  ### vary
Y_field, xgb_params = get_model_tbl_from_id(model_result_table_id, model_id)

#Y_field = 'is_top_100_next_month' ### vary 

# Get model vars
client = bigquery.Client()
query_job = client.query("""
select 
distinct variable,
from `train.model_variables_classifier`
where model_id = {}                    
""".format(model_id))

all_vars = query_job.result().to_dataframe()
incl = all_vars.variable.values

# backtest configs
cutoff = 120 ### vary

# save location
backtest_table_id = 'boreal-pride-417020.train.backtest_ptfs_202407' # save table

### config ends ##############################

# get training dataset file
project_id = "boreal-pride-417020"
dataset_id = "training_data"
read_table_prefix = "training_data_top60pct_ipo_delay_202407"  ### vary

read_table_id = read_table_prefix  # read table

# create training data set
raw_data = read_table(bqstorageclient, project_id, dataset_id, read_table_id)

## get marketcap tbl
query_job = client.query("""
   SELECT distinct date, ticker
   FROM train.training_data_top50pct where date > '2008-12-31'
   """)

mcap_tbl = query_job.result().to_dataframe()

## create training data
all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)
all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
all_data = all_data.dropna()

### create ranked table

## reverse order variables
var_tbl = pd.read_csv('variables.csv')
all_vars = var_tbl.loc[:, 'variable']

vars_list = var_tbl.loc[var_tbl.reverse == 1, 'variable'].tolist()

X = raw_data.loc[:, vars_list] * -1
Y = raw_data.drop(columns = vars_list)

fnl_raw_data = pd.concat([Y, X], axis = 1)

keys = fnl_raw_data.loc[:, ['ticker', 'date']]

ranked_data = raw_data.set_index(['ticker', 'date']).groupby(['date'])[incl].rank(ascending = False)
new_col = [s + "_rank" for s in incl]
ranked_data.columns = new_col
ranked_data = ranked_data.reset_index()

all_data = all_data.merge(ranked_data, how = 'left', on = ['ticker', 'date'])

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

### Calc monthly returns

this_ptf = all_portfolio.copy()
cost_per_trade = .01
monthly_return = create_monthly_return(this_ptf, cost_per_trade)
end = len(monthly_return)-2
start = end - 12*5

stat_tbl = create_performance_stats(monthly_return, start, end)
to_add =  pd.DataFrame(zip(['backtest_id', 'model_id'], [backtest_id, model_id]))
stat_tbl = pd.concat([stat_tbl, to_add])

### Explain model
# XGB model
date_list = all_data.loc[:, 'date'].sort_values().unique()
cutoff = len(date_list) - 6
train_date = pd.DataFrame(date_list[0:(cutoff)], columns = ['date'])
test_date = pd.DataFrame(date_list[cutoff:(cutoff+1)], columns = ['date'])

train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
X_train = train.loc[:, incl]
Y_train = train.loc[:, Y_field]

test = all_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
X_test = test.loc[:, incl]
Y_test = test.loc[:, Y_field]

model = xgb.XGBClassifier(**xgb_params)
model.fit(X_train, Y_train)

# shap explainer
def shap_explainer(): 
    explainer = shap.TreeExplainer(model)
    sv = explainer(X_test)
    contrib_tbl = pd.DataFrame(np.c_[sv.values], columns = list(X_test.columns))
    
    idx = test.loc[:, ['ticker', 'date']]
    contrib_tbl = pd.concat([idx, contrib_tbl], axis = 1)

    shap.plots.beeswarm(sv, max_display = 50)

    # individual explainer
    idx = test.loc[:, ['ticker', 'date']]
    X_data = pd.concat([idx, X_test], axis = 1)

    this_x = X_data.loc[X_data.ticker == 'WULF', :].index.values
    shap.plots.waterfall(sv[int(this_x)],max_display = 50)

shap_explainer()