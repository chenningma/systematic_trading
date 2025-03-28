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

def read_table(project_id, dataset_id, table_id):

    bqstorageclient = bigquery_storage.BigQueryReadClient()
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=["country_name", "region_name"] 
    )

    parent = "projects/{}".format(project_id)

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

def write_model_to_db(xgb_params, score, Y_field, importance, model_result_table_id, model_variable_table_id):
    client = bigquery.Client()
    query_job = client.query("""
    SELECT max(model_id) as latest
    FROM {}
    """.format(model_result_table_id))

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
    parameters_tbl.loc[:, 'dependent_var'] = Y_field
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    parameters_tbl, model_result_table_id, job_config=job_config
                )
    
    gain_tbl = pd.DataFrame([importance], columns = importance.keys()).transpose()
    gain_tbl = gain_tbl.reset_index()
    gain_tbl.columns = ['variable', 'importance']
    gain_tbl = gain_tbl.merge(pd.DataFrame(incl, columns=['variable']), how = 'outer', on ='variable').fillna(0)
    gain_tbl.loc[:, 'model_id'] = model_id
    #gain_tbl.to_csv('variables.csv')

    job = client.load_table_from_dataframe(gain_tbl, model_variable_table_id, job_config=job_config)
    return model_id
 
# read/write config
project_id = "boreal-pride-417020"
dataset_id = "train"
read_table_id = "training_xincl_data_top60pct_ipo_delay"
bqstorageclient = bigquery_storage.BigQueryReadClient()
client = bigquery.Client()

raw_data = read_table(project_id, dataset_id, read_table_id)

### Classifier Config
xgb_params = {
        'n_estimators': 500,
        'max_depth': 3,
        'learning_rate': .15,
        'gamma': 50,
        "subsample": 1,
        "colsample_bytree": 0.7,
        "scale_pos_weight": 10
    }

Y_field = 'is_top_100_next_month'

# get all models
#client = bigquery.Client()
#query_job = client.query("""
#   SELECT *
#   FROM train.model_results where model_id >1
#   """)

#model_tbl = query_job.result().to_dataframe()
#model_id_list = list(model_tbl.model_id.unique())
#for model_id in model_id_list:
    
client = bigquery.Client()
query_job = client.query("""
    select 
    variable,
    avg(importance) as mean_import
    from `train.model_variables_is_top_100_next_month`
    where model_id > 21
    group by variable
    order by mean_import desc
    limit 100                           
   """)

incl = query_job.result().to_dataframe()
incl = incl.variable.values

all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)
all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
all_data = all_data.dropna(subset = ['date', 'ticker'])
#all_data = all_data.dropna()

date_list = all_data.loc[:, 'date'].sort_values().unique()
cutoff = len(date_list) - 8*12
train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
X_train = train.loc[:, incl]
Y_train = train.loc[:, Y_field]

test = all_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
X_test = test.loc[:, incl]
Y_test = test.loc[:, Y_field]

model = xgb.XGBClassifier(**xgb_params)
model.fit(X_train, Y_train)
pred = model.predict(X_test) 

# RMSE Computation 
score = f1_score(Y_test, pred)
print(score)

# get importance
importance = model.get_booster().get_score(importance_type = 'gain')

model_result_table_id = 'boreal-pride-417020.train.model_results_is_top_100_next_month'
model_variable_table_id = 'boreal-pride-417020.train.model_variables_is_top_100_next_month'
model_id = write_model_to_db(xgb_params, score, Y_field, importance, model_result_table_id, model_variable_table_id)

