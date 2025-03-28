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

# get model parameters
model_tbl = pd.read_csv('cv_results.csv')
dep_var_list = ['next_month_return', 'next_3m_return', 'next_6m_return']

client = bigquery.Client()
query_job = client.query("""
   SELECT distinct variable
   FROM train.model_variables where model_id = 1
   """)

incl = query_job.result().to_dataframe()
incl = incl.variable.values

for dep_var in dep_var_list:
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

    ### Regressor
    #search_list = [7, 10, 15, 20]
    #for i in search_list: 
    params = model_tbl.loc[model_tbl.dependent_var == dep_var, ['parameter', 'value']]
    params['value'] = params['value'].astype(float)
    xgb_params = dict(zip(params.parameter, params.value))
    xgb_params['n_estimators'] = int(xgb_params['n_estimators'])
    xgb_params['max_depth'] = int(xgb_params['max_depth'])
    
    model = xgb.XGBRegressor(**xgb_params)
    model.fit(X_train, Y_train)
    pred = model.predict(X_test) 

    # RMSE Computation 
    score = MSE(Y_test, pred)
    print(score)

    # get importance
    importance = model.get_booster().get_score(importance_type = 'gain')
    gain_tbl = pd.DataFrame([importance], columns = importance.keys()).transpose()

    client = bigquery.Client()
    query_job = client.query("""
    SELECT max(model_id) as latest
    FROM train.model_results
    """)

    last_num = query_job.result().to_dataframe()
    model_id = last_num.iloc[0,0] + 1

    parameters_tbl = pd.DataFrame([xgb_params], columns = xgb_params.keys()).transpose().reset_index()
    parameters_tbl.columns = ['parameter', 'value']
    parameters_tbl['value'] = parameters_tbl['value'].astype(str)
    parameters_tbl.loc[:, 'model_id'] = model_id
    parameters_tbl.loc[:, 'model_type'] = 'XGBRegressor'
    parameters_tbl.loc[:, 'model_run_date'] = datetime.now()
    parameters_tbl.loc[:, 'score'] = score
    parameters_tbl.loc[:, 'score_type'] = 'rmse'
    parameters_tbl.loc[:, 'dependent_var'] = dep_var

    # Get importance
    gain_tbl = gain_tbl.reset_index()
    gain_tbl.columns = ['variable', 'importance']
    gain_tbl = gain_tbl.merge(pd.DataFrame(incl, columns=['variable']), how = 'outer', on ='variable').fillna(0)
    gain_tbl.loc[:, 'model_id'] = model_id
    #gain_tbl.to_csv('variables.csv')

    client = bigquery.Client()
    table_id = 'boreal-pride-417020.train.model_results'
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    parameters_tbl, table_id, job_config=job_config
                )
    table_id = 'boreal-pride-417020.train.model_variables'
    job = client.load_table_from_dataframe(
                    gain_tbl, table_id, job_config=job_config
                )

##### Train from existing

client = bigquery.Client()
query_job = client.query("""
   SELECT *
   FROM train.model_results where model_id >13
   """)

model_tbl = query_job.result().to_dataframe()

model_id_list = list(model_tbl.model_id.unique())

for model_id in model_id_list:
    
    dep_var = model_tbl.loc[model_tbl.model_id == model_id, 'dependent_var'].unique()

    client = bigquery.Client()
    query_job = client.query("""
    SELECT variable
    FROM train.model_variables where model_id = {}
    order by importance desc
    limit 50
    """.format(model_id))

    incl = query_job.result().to_dataframe()
    incl = incl.variable.values

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

    ### Classifier
    params = model_tbl.loc[model_tbl.model_id == model_id, ['parameter', 'value']]
    params['value'] = params['value'].astype(float)
    xgb_params = dict(zip(params.parameter, params.value))
    xgb_params['n_estimators'] = int(xgb_params['n_estimators'])
    xgb_params['max_depth'] = int(xgb_params['max_depth'])

    model = xgb.XGBRegressor(**xgb_params)
    model.fit(X_train, Y_train)
    pred = model.predict(X_test) 

    # RMSE Computation 
    score = np.sqrt(MSE(Y_test, pred))
    print(score)

    # get importance
    importance = model.get_booster().get_score(importance_type = 'gain')
    gain_tbl = pd.DataFrame([importance], columns = importance.keys()).transpose()


    client = bigquery.Client()
    query_job = client.query("""
    SELECT max(model_id) as latest
    FROM train.model_results
    """)

    last_num = query_job.result().to_dataframe()
    model_id = last_num.iloc[0,0] + 1

    parameters_tbl = pd.DataFrame([xgb_params], columns = xgb_params.keys()).transpose().reset_index()
    parameters_tbl.columns = ['parameter', 'value']
    parameters_tbl['value'] = parameters_tbl['value'].astype(str)
    parameters_tbl.loc[:, 'model_id'] = model_id
    parameters_tbl.loc[:, 'model_type'] = 'XGBClassifier'
    parameters_tbl.loc[:, 'model_run_date'] = datetime.now()
    parameters_tbl.loc[:, 'score'] = score
    parameters_tbl.loc[:, 'score_type'] = 'f1'
    parameters_tbl.loc[:, 'dependent_var'] = dep_var[0]

    # Get importance
    gain_tbl = gain_tbl.reset_index()
    gain_tbl.columns = ['variable', 'importance']
    gain_tbl = gain_tbl.merge(pd.DataFrame(incl, columns=['variable']), how = 'outer', on ='variable').fillna(0)
    gain_tbl.loc[:, 'model_id'] = model_id
    #gain_tbl.to_csv('variables.csv')

    client = bigquery.Client()
    table_id = 'boreal-pride-417020.train.model_results'
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    parameters_tbl, table_id, job_config=job_config
                )
    table_id = 'boreal-pride-417020.train.model_variables'
    job = client.load_table_from_dataframe(
                    gain_tbl, table_id, job_config=job_config
                )

