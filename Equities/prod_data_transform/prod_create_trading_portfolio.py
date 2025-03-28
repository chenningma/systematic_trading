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

def print_runtime(time_start):
    time_stop = datetime.now()
    runtime = time_stop - time_start
    print(runtime)

time_start = datetime.now()

### config ##############################

# get model params
model_result_table_id = 'boreal-pride-417020.train.model_results_classifier'
model_id = 132  ### vary
Y_field, xgb_params = get_model_tbl_from_id(model_result_table_id, model_id)

Y_field = 'is_top_100_next_month' ### vary 

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
#cutoff = 120 ### vary

# save location
backtest_table_id = 'boreal-pride-417020.prod.backtest_ptfs'  # save table
performance_table_id = 'boreal-pride-417020.prod.backtest_ptfs_performance'
### config ends ##############################

### create data inputs

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

#### train dataset 1 
# get training dataset file
project_id = "boreal-pride-417020"
dataset_id = "training_data"
read_table_prefix = "training_data_top60pct_ipo_delay_ex_biotech"  ### vary
postfix = datetime.today().strftime("%Y%m")
ex_biotech_read_table_id = read_table_prefix + '_' + postfix # read table

# create training data set
raw_data = read_table(bqstorageclient, project_id, dataset_id, ex_biotech_read_table_id)

all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)
all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
all_data = all_data.dropna(subset = ['date', 'ticker'])

ex_biotech = all_data.copy()

#### train dataset 2
# get training dataset file
project_id = "boreal-pride-417020"
dataset_id = "training_data"
read_table_prefix = "training_data_top60pct_ipo_delay_biotech"  ### vary
postfix = datetime.today().strftime("%Y%m")
biotech_read_table_id = read_table_prefix + '_' + postfix # read table

# create training data set
raw_data = read_table(bqstorageclient, project_id, dataset_id, biotech_read_table_id)

all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)
all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
all_data = all_data.dropna(subset = ['date', 'ticker'])

biotech = all_data.copy()

####### Run Models #######
date_list = all_data.date.unique()
cutoff = len(date_list) - 1
### Run ex_biotech model
ex_biotech_portfolio = backtest_classifier_base_removeNAs(ex_biotech, Y_field, incl, xgb_params, cutoff)    

### Run biotech model
biotech_portfolio = backtest_classifier_base(biotech, Y_field, incl, xgb_params, cutoff)    

### combine 2 portfolios 
ex_biotech_portfolio.loc[:, 'strategy'] = 'ex_biotech'
biotech_portfolio.loc[:, 'strategy'] = 'biotech'
all_portfolio = pd.concat([ex_biotech_portfolio, biotech_portfolio], axis = 0).sort_values(by= ['date', 'pred'], ascending = False)

all_portfolio.loc[:, 'rank'] = all_portfolio.groupby(['date'])['pred'].rank(ascending = False, method = 'dense')
all_portfolio.loc[:, 'strat_rank'] = all_portfolio.groupby(['date', 'strategy'])['pred'].rank(ascending = False, method = 'first')

all_portfolio = all_portfolio.reset_index(drop=True)
fnl_portfolio = all_portfolio.loc[all_portfolio['rank'] <= 50, ['pred', 'ticker', 'next_month_return', 'dependent_var', 'date', 'score']]

### calc performance
cost_per_trade = .01
monthly_return = create_monthly_return(fnl_portfolio, cost_per_trade)
end = len(monthly_return)-2
start = end - 12
sharpe_5y = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12))

print('calc backtest performance')
print_runtime(time_start)

# XGB model
date_list = ex_biotech.loc[:, 'date'].sort_values().unique()
cutoff = len(date_list) - 12
train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

train = ex_biotech.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
X_train = train.loc[:, incl]
Y_train = train.loc[:, Y_field]

test = ex_biotech.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
X_test = test.loc[:, incl]
Y_test = test.loc[:, Y_field]

model = xgb.XGBClassifier(**xgb_params)
model.fit(X_train, Y_train)

# RMSE Computation 
pred = model.predict(X_test) 
score = f1_score(Y_test, pred)
print(score)
importance = model.get_booster().get_score(importance_type = 'gain')

# shap explainer
def shap_explainer(): 
    explainer = shap.TreeExplainer(model)
    sv = explainer(X_train)
    contrib_tbl = pd.DataFrame(np.c_[sv.values], columns = list(X_train.columns))

    shap.plots.beeswarm(sv, max_display = 50)

    # individual explainer
    idx = train.loc[:, ['ticker', 'date']]
    X_data = pd.concat([idx, X_train], axis = 1)

    this_x = X_data.loc[X_data.date == '2022-01-31', :].index.values
    shap.plots.waterfall(sv[int(this_x)])

### Write results ###

# write model
model_result_table_id = 'boreal-pride-417020.prod.model_params'
model_variable_table_id = 'boreal-pride-417020.prod.model_variables'
model_id = write_model_to_db(Y_field, incl, xgb_params, score, importance, model_result_table_id, model_variable_table_id)

# save backtest
read_table_id = ex_biotech_read_table_id+ ',' + biotech_read_table_id
description = '2ptf combo; biotech capped at 100%; ex_biotech: run_backtest_pred_threshold_removeNAs --0.6 + biotech: backtest_classifier_base'
backtest_id = write_backtest_to_db(fnl_portfolio,model_id, Y_field, description, read_table_id, backtest_table_id)

# save backtest performance
stat_tbl = create_performance_stats(monthly_return, start, end)
to_add =  pd.DataFrame(zip(['backtest_id', 'model_id'], [backtest_id, model_id]))
stat_tbl = pd.concat([stat_tbl, to_add])

fnl_stat_tbl =stat_tbl.set_index(0).transpose()

performance_table_id = 'boreal-pride-417020.prod.backtest_ptfs_performance'
job_config = bigquery.LoadJobConfig(
    #schema = [ \
    #bigquery.SchemaField("5y_drawdown_dt", bigquery.enums.SqlTypeNames.DATE), \
    #bigquery.SchemaField("drawdown_dt", bigquery.enums.SqlTypeNames.DATE)], \
    write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(
                fnl_stat_tbl, performance_table_id, job_config=job_config
            )
print('write to db')
print_runtime(time_start)

