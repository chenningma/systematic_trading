import pandas as pd
import os 

import numpy as np
import math 
from datetime import datetime

import xgboost as xgb
from sklearn.metrics import confusion_matrix, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.metrics import mean_squared_error as MSE
from sklearn.ensemble import RandomForestRegressor
import shap 

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from functions import *

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
bqstorageclient = bigquery_storage.BigQueryReadClient()
client = bigquery.Client()

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
variable,
from `train.model_variables_classifier`
where model_id = {}
order by importance desc 
limit 15                    
""".format(model_id))

all_vars = query_job.result().to_dataframe()
incl = all_vars.variable.values

# backtest configs
cutoff = 240 ### vary

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
all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
all_data = all_data.dropna(subset = ['date', 'ticker'])

date_list = all_data.loc[:, 'date'].sort_values().unique()
train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
X_train = train.loc[:, incl]

test = all_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
X_test = test.loc[:, incl]

#search_list = ['is_top_100_next_month', 'is_top_50_next_3month']

#for dep_var in search_list:
    
# xgboost classifier
Y_train = train.loc[:, Y_field]

xgb_model = xgb.XGBClassifier(n_estimators = 200)

params = {
    'max_depth': [3,4,5],
    'learning_rate': [0.05, 0.1,.15, 0.20, 0.25],
    'gamma': [10, 20, 30, 40, 50, 60],
    "subsample":[1.0],
    "colsample_bytree":[0.1, 0.3, 0.5,0.7,1.0],
    "scale_pos_weight": [5, 7, 10, 15]
}

grid = RandomizedSearchCV(
    estimator = xgb_model,
    param_distributions = params,
    scoring = 'f1',
    n_iter = 100,
    n_jobs = -1,
    cv = 3,
    verbose = 2,
)

# Model fitting
grid = grid.fit(X_train, Y_train)
print("Best Parameters for", str(Y_field), ":", grid.best_params_)

param_tbl = pd.DataFrame(grid.cv_results_)
param_tbl.to_csv(str(Y_field)+'_cv_results.csv')