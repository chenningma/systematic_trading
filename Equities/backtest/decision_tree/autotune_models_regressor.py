import pandas as pd
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime

import xgboost as xgb
from sklearn.metrics import confusion_matrix, roc_curve, auc, f1_score
from sklearn.model_selection import train_test_split,TimeSeriesSplit, RandomizedSearchCV
from sklearn.metrics import mean_squared_error as MSE
from sklearn.ensemble import RandomForestRegressor
import shap 

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

bqstorageclient = bigquery_storage.BigQueryReadClient()
project_id_billing = 'boreal-pride-417020'

def read_table():

    bqstorageclient = bigquery_storage.BigQueryReadClient()

    project_id = "boreal-pride-417020"
    dataset_id = "train"
    table_id = "training_data_100vars"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=["country_name", "region_name"] 
    )

    parent = "projects/{}".format(project_id_billing)

    requested_session = types.ReadSession(
        table=table,
        data_format=types.DataFormat.ARROW,
        #read_options=read_options,
    )
    read_session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    stream = read_session.streams[0] #read every stream from 0 to 3
    reader = bqstorageclient.read_rows(stream.name)

    frames = []
    for message in reader.rows().pages:
        frames.append(message.to_dataframe())
    dataframe = pd.concat(frames)
    print(dataframe.head())

    return dataframe

raw_data = read_table()

client = bigquery.Client()
query_job = client.query("""
   SELECT distinct variable
   FROM train.model_variables where model_id = 1
   """)

incl = query_job.result().to_dataframe()
incl = incl.variable.values

search_list = ['next_month_return', 'next_3m_return', 'next_6m_return']

for dep_var in search_list:
    all_data = pd.concat([raw_data.loc[:, ['ticker', 'date']], raw_data.loc[:, dep_var]], axis = 1)
    all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
    all_data = all_data.dropna()

    date_list = all_data.loc[:, 'date'].sort_values().unique()
    cutoff = len(date_list) - 5*12
    train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
    test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

    train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
    X_train = train.loc[:, incl]
    Y_train = train.loc[:, dep_var]

    test = all_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
    X_test = test.loc[:, incl]
    Y_test = test.loc[:, dep_var]

    # xgboost regression
    xgb_model = xgb.XGBRegressor(n_estimators = 500)

    params = { 
        'max_depth': [3,4,5],
        'learning_rate': [0.05, 0.1,.15, 0.20, 0.25],
        'gamma': [10, 20, 30, 40, 50, 60],
        "subsample":[0.05, 0.1, 0.3, 0.5,0.7,1.0],
        "colsample_bytree":[0.1, 0.3, 0.5,0.7,1.0],
    }

    grid = RandomizedSearchCV(
        estimator = xgb_model,
        param_distributions = params,
        scoring = 'neg_mean_squared_error',
        n_jobs = -1,
        n_iter = 100,
        cv = TimeSeriesSplit(n_splits=3),
        verbose = 0,
    )

    # Model fitting
    grid = grid.fit(X_train, Y_train)
    print("Best Parameters for", str(dep_var), ":", grid.best_params_)

    param_tbl = pd.DataFrame(grid.cv_results_)
    param_tbl.to_csv(str(dep_var)+'_cv_results.csv')
