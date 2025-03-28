import pandas as pd
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime

import xgboost as xgb
from sklearn.metrics import confusion_matrix, roc_curve, auc, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error as MSE
from sklearn.ensemble import RandomForestRegressor
import shap 

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

bqstorageclient = bigquery_storage.BigQueryReadClient()
client = bigquery.Client()

project_id_billing = 'boreal-pride-417020'
def read_table(project_id, dataset_id, table_id):

    bqstorageclient = bigquery_storage.BigQueryReadClient()
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


project_id = "boreal-pride-417020"
dataset_id = "train"
table_id = "training_data_100vars_top60pct_80pct"
raw_data = read_table(project_id, dataset_id, table_id)


## get marketcap tbl
query_job = client.query("""
   SELECT date, ticker
   FROM transformed.ticker_tbl_top_50_pct_marketcap where date > '2008-12-31'
   """)

mcap_tbl = query_job.result().to_dataframe()

# get model parameters
#model_list = pd.read_csv('cv_results.csv')
#dep_var_list = model_list.dependent_var.unique()
# set up variables for writing to db
def backtest_regressor_base(all_data, xgb_params, mcap_tbl, top_mcap_flag): 
    date_list = all_data.loc[:, 'date'].sort_values().unique()
    cutoff = 120
    train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
    train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)

    all_portfolio = pd.DataFrame()
    start = cutoff+1
    for k in range(start,len(date_list)):

        prev_month = date_list[k-1]
        latest_month = date_list[k]

        latest_data = all_data.loc[all_data.date == latest_month, :].reset_index(drop = True)
        
        # match with marketcap
        if top_mcap_flag == 1: 
            latest_data = latest_data.merge(mcap_tbl, how = 'inner', on = ['ticker', 'date'])

        train = pd.concat([train, latest_data])

        X_train = train.loc[:, incl]
        Y_train = train.loc[:, Y_field]

        model = xgb.XGBRegressor(**xgb_params)
        model.fit(X_train, Y_train)

        # make predictions
        
        pred = pd.DataFrame(model.predict(latest_data.loc[:, incl]), columns = ['pred']) 
        score = np.sqrt(MSE(latest_data.loc[:, Y_field], pred))
        print(str(latest_month), ': ', str(score))

        to_add = latest_data.loc[:, ['ticker', 'next_month_return']]
        pred = pd.concat([pred, to_add], axis = 1)

        to_add = latest_data.loc[:, Y_field]
        to_add.name = 'dependent_var'
        pred = pd.concat([pred, to_add], axis = 1)

        top_predict = pred.sort_values(by = 'pred', ascending = False).reset_index(drop=True)

        ## top 50, full replacement
        top_cutoff = 50
        portfolio = top_predict.iloc[0:top_cutoff, :]

        portfolio.loc[:, 'date'] = latest_month
        portfolio.loc[:, 'score'] = score 

        all_portfolio = pd.concat([all_portfolio, portfolio])

    return all_portfolio

def write_to_db(all_portfolio, description, write_table_id):
    client = bigquery.Client()
    query_job = client.query("""
    SELECT max(backtest_id) as latest
    FROM {}
    """.format(write_table_id))

    last_num = query_job.result().to_dataframe()
    backtest_id = last_num.iloc[0,0] + 1

    all_portfolio.loc[:, 'backtest_id'] = backtest_id
    all_portfolio.loc[:, 'model_id'] = model_id
    all_portfolio.loc[:, 'description'] = description
    all_portfolio.loc[:, 'prediction_type'] = 'probability'
    all_portfolio.loc[:, 'score_type'] = 'f1'
    all_portfolio['dependent_var'] = all_portfolio['dependent_var'].astype(float)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                all_portfolio, write_table_id, job_config=job_config
            )

write_table_id = 'boreal-pride-417020.train.backtest_ptfs_train_data_top60pct_80pct'

for model_id in range(14, 17):
    #model_id = 12
    query_job = client.query("""
    SELECT *
    FROM train.model_results where model_id = {}
    """.format(model_id))

    model_tbl = query_job.result().to_dataframe()

    Y_field = model_tbl.loc[0, 'dependent_var']

    client = bigquery.Client()
    query_job = client.query("""
    SELECT distinct variable
    FROM train.model_variables where model_id = {}
    """.format(model_id))

    incl = query_job.result().to_dataframe()
    incl = incl.variable.values

    all_data = pd.concat([raw_data.loc[:, ['ticker', 'date']], raw_data.loc[:, Y_field]], axis = 1)
    if 'next_month_return' not in all_data.columns.values:
        all_data = pd.concat([all_data, raw_data.loc[:, 'next_month_return']], axis = 1)
    all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
    all_data = all_data.dropna()

    ### Classifier
    params = model_tbl.loc[model_tbl.model_id == model_id, ['parameter', 'value']]
    params['value'] = params['value'].astype(float)
    xgb_params = dict(zip(params.parameter, params.value))
    xgb_params['n_estimators'] = int(xgb_params['n_estimators'])
    xgb_params['max_depth'] = int(xgb_params['max_depth'])

    ### backtest 1: only predict on top marketcap
    
    top_mcap_flag = 0
    all_portfolio = backtest_regressor_base(all_data,xgb_params, mcap_tbl, top_mcap_flag)

    description = str(Y_field) + \
    ' xgboost_regressor; top 50; full replacement' + \
    ' predict only top mcap: ' + str(top_mcap_flag)

    write_to_db(all_portfolio, description, write_table_id)
