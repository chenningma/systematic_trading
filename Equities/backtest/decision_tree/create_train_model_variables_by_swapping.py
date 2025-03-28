import pandas as pd
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

import xgboost as xgb
from sklearn.metrics import confusion_matrix, roc_curve, auc, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error as MSE
from functions import * 

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
bqstorageclient = bigquery_storage.BigQueryReadClient()
  
# read/write config
project_id = "boreal-pride-417020"
dataset_id = "training_data"

read_table_prefix = "training_data_top60pct_ipo_delay"
postfix = datetime.today().strftime("%Y%m")
read_table_id = read_table_prefix + '_' + postfix

raw_data = read_table(bqstorageclient, project_id, dataset_id, read_table_id)

var_table = pd.read_csv('this_model_id_xvars_replace.csv')

# get model configs
model_result_table_id = 'boreal-pride-417020.train.model_results_classifier'
starting_model_id = 130
Y_field, xgb_params = get_model_tbl_from_id(model_result_table_id, starting_model_id)

date_list = raw_data.loc[:, 'date'].sort_values().unique()
cutoff = len(date_list) - 5*12
train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

train = raw_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
Y_train = train.loc[:, Y_field]

test = raw_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
Y_test = test.loc[:, Y_field]

test_var_tbl = var_table.loc[var_table.start==0, :].reset_index()

for i in range(0, len(test_var_tbl)):
    try:
        original_var = test_var_tbl.loc[i, 'variable']

        this_prefix = test_var_tbl.loc[i, 'prefix']
        if test_var_tbl.loc[i, 'suffix'] == 'yoy':
            suffix = '3mom'
        else:
            suffix = 'd3mom'

        replace_var = this_prefix + '_' + suffix

        all_vars = var_table.loc[var_table.variable != original_var, 'variable'].values.tolist()

        fnl_vars = all_vars + [original_var]
        X_train = train.loc[:, fnl_vars]
        X_test = test.loc[:, fnl_vars]

        model = xgb.XGBClassifier(**xgb_params)
        model.fit(X_train, Y_train)
        pred = model.predict(X_test) 
        score_og = f1_score(Y_test, pred)

        fnl_vars = all_vars + [replace_var]
        X_train = train.loc[:, fnl_vars]
        X_test = test.loc[:, fnl_vars]

        model = xgb.XGBClassifier(**xgb_params)
        model.fit(X_train, Y_train)
        pred = model.predict(X_test) 
        score_replace = f1_score(Y_test, pred)

        if score_replace > score_og:
            var_table.loc[var_table.variable == original_var, 'variable'] = replace_var 
            print('replaced: ', str(original_var))

    except:
        print("error: ", str(replace_var))  

incl = var_table.variable.values
X_train = train.loc[:, incl]
X_test = test.loc[:, incl]

model = xgb.XGBClassifier(**xgb_params)
model.fit(X_train, Y_train)
pred = model.predict(X_test) 

# Importance calc
score = f1_score(Y_test, pred)
importance = model.get_booster().get_score(importance_type = 'gain')

model_result_table_id = 'boreal-pride-417020.train.model_results_classifier'
model_variable_table_id = 'boreal-pride-417020.train.model_variables_classifier'
model_id = write_model_to_db(Y_field, incl, xgb_params, score, importance, model_result_table_id, model_variable_table_id)




